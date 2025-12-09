"""The main script to generate Open English Termnet from Wikidata and OEWN"""
import yaml
from tqdm import tqdm
from collections import defaultdict
import sqlite3
import json
import csv
import os
import pickle
import argparse
from open_english_namenet import WIKIDATA_DB, WORDNET_SOURCE, load_wordnet_data, fetch_in_chunks, read_wikidata_with_prop_vals, get_labels_and_defn, oewn_extract, wikidata_extract
from glob import glob


def process_entry(qid, cursor, hyps, wd2entry, f, lexfiles, addendum, lemmas=[], inst=True, mero=[]):
    label, definition = get_labels_and_defn(qid, cursor)
    if label == [] or definition == "":
        return (None, None)
    if qid in wd2entry:
        ssid, _ = wd2entry[qid]
        lexfile = lexfiles[ssid]
        if lexfile not in addendum:
            print(f"Creating addendum for {lexfile}")
            addendum[lexfile] = {}
        if ssid not in addendum[lexfile]:
            addendum[lexfile][ssid] = {}
        data = addendum[lexfile][ssid]
        data.setdefault("definition", []).append(definition)
        if inst:
            if "instance_hypernym" not in data:
                data["instance_hypernym"] = []
            data["instance_hypernym"].extend(hyps)
            data["instance_hypernym"] = list(set(data["instance_hypernym"]))
        else:
            if "hypernym" not in data:
                data["hypernym"] = []
            data["hypernym"].extend(hyps)
            data["hypernym"] = list(set(data["hypernym"]))
        if not lemmas:
            for l in label:
                if l not in data.get("members", []):
                    data.setdefault("members", []).append(l)
        for l in lemmas:
            if l not in data.get("members", []):
                data.setdefault("members",[]).append(l)
        if "mero_member" in data:
            data["mero_member"].extend(mero)
            data["mero_member"] = list(set(data["mero_member"]))
        else:
            if mero:
                data["mero_member"] = list(set(mero))
        return (ssid, data)
    else:
        new_id = qid + "-n"
        if inst:
            entry = {
                "definition": [definition],
                "instance_hypernym": hyps,
                "members": lemmas if lemmas else label,
                "partOfSpeech": "n",
                "wikidata": qid
            }
        else:
            entry = {
                "definition": [definition],
                "hypernym": hyps,
                "members": lemmas if lemmas else label,
                "partOfSpeech": "n",
                "wikidata": qid
            }
        if mero:
            entry[1]["mero_member"] = list(set(mero))
        yaml.dump({new_id: entry}, f, sort_keys=False)
        return new_id, entry

def is_hyp(ssid1, ssid2, hyps):
    if ssid1 not in hyps:
        return False
    if ssid2 in hyps.get(ssid1, []):
        return True
    return any(is_hyp(hyp, ssid2, hyps) for hyp in hyps.get(ssid1, []))

def dedupe_hyps(wn_hyps, hyps):
    wn_hyps = sorted(list(set(wn_hyps)))
    wn_hyps = [wh for wh in wn_hyps
               if not any(is_hyp(wh2, wh, hyps) for wh2 in wn_hyps if wh2 != wh)]
    return wn_hyps

def find_taxon_hyps(qid, cursor, wd2hypernym, rank, seen=set()):
    if qid in seen:
        return []
    seen.add(qid)
    if qid in wd2hypernym:
        if rank == "":
            return [wd2hypernym[qid]]
        else:
            return [h[0] for h in wd2hypernym[qid] if rank in h[1]]
    cursor.execute("SELECT properties FROM properties WHERE qid = ?", (qid,))
    result = cursor.fetchone()
    if not result:
        print(f"No properties for {qid}")
        return []
    data = json.loads(result[0])
    if "P171" not in data:
        return []
    return [t for parent in data["P171"] for t in find_taxon_hyps(parent, cursor, wd2hypernym, rank, seen)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Open English Termnet from Wikidata and OEWN.")
    parser.add_argument("--output_folder", type=str, help="Output file path", default="data")
    parser.add_argument("--oewn", type=str, help="Path to OEWN data", default=WORDNET_SOURCE)
    parser.add_argument("--wd", type=str, help="Path to Wikidata database", default=WIKIDATA_DB)
    parser.add_argument("--overlaps", type=str, help="Path to manually anntoated overlaps", default="overlaps_evaluated.csv")
    parser.add_argument("--linked_occupations", type=str, help="Path to manually annotated occupations", default="linked_occupations_reviewed.csv")
    parser.add_argument("--taxon_ssids", type=str, help="Path to manually annotated taxon SSIDs", default="taxon_ssids_reviewed.csv")
    parser.add_argument("--taxon2common", type=str, help="Path to taxon to common names", default="taxon2common_reviewed.csv")
    parser.add_argument("--skip_overlaps", action="store_true", help="Skip processing overlaps")
    parser.add_argument("--skip_humans", action="store_true", help="Skip processing humans")
    parser.add_argument("--skip_taxons", action="store_true", help="Skip processing taxons")
    parser.add_argument("--update_addendums", action="store_true", help="Update addendums")
    args = parser.parse_args()

    # Load WordNet data
    wikidata_links, hyps, wn_lemmas, wd2entry, lexfiles = load_wordnet_data(with_wd2data=True, with_lexfiles=True)

    wn_lemmas['09596003-n'] = "Titaness"

    # Invert wd2entry to entry2wd
    entry2wd = {}
    for wd, (ssid, data) in wd2entry.items():
        entry2wd[ssid] = wd

    db = sqlite3.connect(args.wd)
    cursor = db.cursor()

    output_folder = f"{args.output_folder}/automatic"
    addendum_folder = f"{args.output_folder}/addendum"

    # Load addendums
    addendums = {}
    for file in tqdm(glob(f"{addendum_folder}/*.yaml"), desc="Loading addendums"):
        filename = file.split("/")[-1]
        with open(file, "r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=yaml.CLoader)
            addendums[filename] = data

    if not args.skip_overlaps:
        overlaps_by_wikidata = defaultdict(list)
        overlaps_by_oewn = defaultdict(list)

        # Read overlaps which states which Wikidata parents map to which OEWN synsets
        with open(args.overlaps, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["Accept"].strip().upper() == "TRUE":
                    oewn_id = oewn_extract(row["SSID"].strip())
                    wikidata_id = wikidata_extract(row["QID"])
                    overlaps_by_wikidata[wikidata_id].append(oewn_id)
                    overlaps_by_oewn[oewn_id].append(wikidata_id)


        print(len(overlaps_by_wikidata), "Wikidata items with overlaps")

        # Read Wikidata P31 (instance of) property values for all Wikidata items which have overlaps
        wikidata_props = read_wikidata_with_prop_vals(cursor, "P31", overlaps_by_wikidata.keys(), "overlap_instances")

        seen = set()

        print("Human in set", "Q5" in overlaps_by_wikidata)

        # For each OEWN synset with overlaps, create entries for all Wikidata items which map to it
        for wn_hyp, wds in tqdm(overlaps_by_oewn.items(), desc="Processing OEWN synsets", position=0):
            lemma = wn_lemmas[wn_hyp].replace(' ', '_').lower()
            if "," in lemma:
                lemma = lemma.split(",")[0]
            with open(f"{output_folder}/noun.{lemma}.yaml", "w") as f1:
                for wd in wds:
                    for entity, superclazzes in tqdm(wikidata_props.get(wd, {}).items(), desc=f"Processing {lemma} -> {wd}", position=1, leave=False):
                        if entity in seen:
                            continue
                        seen.add(entity)
                        if "Q5" in superclazzes or "Q16521" in superclazzes:
                            continue
                        wn_hyps = [wh for superclazz in superclazzes for wh in overlaps_by_wikidata.get(superclazz, [])]
                        wn_hyps = dedupe_hyps(wn_hyps, hyps)
                        process_entry(entity, cursor, wn_hyps, wd2entry, f1, lexfiles, addendums)
                        #new_id, entry = make_entry(entity, cursor, wn_hyps, wd2entry)
                        #write_entry(f1, new_id, entry, args.curated)


    if not args.skip_humans:
        occupation_by_qid = defaultdict(list)

        with open(args.linked_occupations, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                qid = wikidata_extract(row["QID"])
                oewn = oewn_extract(row["Linked"])
                occupation_by_qid[qid].append(oewn)

        wikidata_props = read_wikidata_with_prop_vals(cursor, "P31", ["Q5"], "human_instances")

        with open(f"{output_folder}/noun.human.yaml", "w") as f:
            for entity, superclazzes in tqdm(wikidata_props.get("Q5", {}).items(), desc="Processing humans"):
                cursor.execute("SELECT properties FROM properties WHERE qid = ?", (entity,))
                result = cursor.fetchone()
                if not result:
                    continue

                data = json.loads(result[0])
                
                wn_hyps = ["02474924-n"]
                if "P21" in data and "Q6581097" in data["P21"]:
                    wn_hyps.append("09647338-n")
                elif "P21" in data and "Q6581072" in data["P21"]:
                    wn_hyps.append("09642198-n")
            
                if "P106" in data:
                    wn_hyps.extend([wh 
                                 for occ in data["P106"]
                                 for wh in occupation_by_qid.get(occ, [])])

                wn_hyps = dedupe_hyps(wn_hyps, hyps)

                process_entry(entity, cursor, wn_hyps, wd2entry, f, lexfiles, addendums)

    if not args.skip_taxons:
        wikidata_props = read_wikidata_with_prop_vals(cursor, "P31", ["Q16521"], "taxon_instances")

        wd2hypernym = defaultdict(list)

        with open(args.taxon_ssids, "r") as f:
            reader = csv.DictReader(f)
            taxon_ssids_by_qid = defaultdict(list)
            for row in reader:
                if len(row["Wikidata"].strip()) > 2:
                    qids = [wikidata_extract(qid.strip()) for qid in row["Wikidata"].strip().split(",")]
                    oewn = oewn_extract(row["SSID"].strip())
                    for qid in qids:
                        wd2hypernym[qid].append((oewn, row["Lemma"].strip()))

        taxon2common = {}

        with open(args.taxon2common, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                taxon_ssid = oewn_extract(row["SSID 1"].strip())
                common_ssid = oewn_extract(row["SSID 2"].strip())
                if row["Accept"] == "TRUE" and taxon_ssid in entry2wd:
                    if entry2wd[taxon_ssid] in taxon2common:
                        print(f"Warning: Duplicate taxon {taxon_ssid} as {common_ssid} and {taxon2common[entry2wd[taxon_ssid]]}")
                    taxon2common[entry2wd[taxon_ssid]] = common_ssid


        children = defaultdict(list)

        with open(f"{output_folder}/noun.taxon_working.csv", "w") as f:
            writer = csv.writer(f)
            for entity_superclazzes in tqdm(wikidata_props.get("Q16521", {}).items(), desc="Processing taxons"):
                entity, superclazzes = entity_superclazzes
                cursor.execute("SELECT properties FROM properties WHERE qid = ?", (entity,))
                result = cursor.fetchone()
                if not result:
                    print(f"No properties for {entity}")
                    continue

                data = json.loads(result[0])

                if "P171" in data:
                    for superclazz in data["P171"]:
                        children[superclazz].append(entity)

                if "P105" not in data:
                    #print(f"No taxon rank for {entity}")
                    continue

                rank_qid = data["P105"][0]

                cursor.execute("SELECT label FROM labels_en WHERE qid = ?", (rank_qid,))
                result = cursor.fetchone()

                if not result:
                    print(f"No label for rank {rank_qid} of {entity}")
                    continue

                rank = json.loads(result[0])[0]

                cursor.execute("SELECT data_properties FROM data_properties WHERE qid = ?", (entity,))
                result = cursor.fetchone()

                if not result:
                    #print(f"No data properties for {entity}")
                    continue

                data_props = json.loads(result[0])

                if "P225" not in data_props:
                    #print(f"No scientific name for {entity}")
                    continue

                sci_name = data_props["P225"][0][0]

                if " " in sci_name:
                    words = sci_name.split(" ")
                    if (len(words) == 2 and words[0][0].isupper() and words[1][0].islower()) or \
                            (len(words) == 3 and words[0][0].isupper() and words[1][0].islower() and words[2][0].islower()):
                        wn_hyps = find_taxon_hyps(entity, cursor, taxon2common, "")
                        wn_hyps = dedupe_hyps(wn_hyps, hyps)
                    writer.writerow([entity, sci_name, rank, json.dumps(wn_hyps)])
                else:
                    wn_hyps = find_taxon_hyps(entity, cursor, wd2hypernym, rank)
                    wn_hyps = dedupe_hyps(wn_hyps, hyps)
                    if not wn_hyps:
                        wn_hyps = ["08008892-n"]
                
                    writer.writerow([entity, sci_name, rank, json.dumps(wn_hyps)])

        with open(f"{output_folder}/noun.taxon.yaml", "w") as f:
            with open(f"{output_folder}/noun.species.yaml", "w") as f_species:
                csv_line_count = sum(1 for line in open(f"{output_folder}/noun.taxon_working.csv"))
                with open(f"{output_folder}/noun.taxon_working.csv", "r") as f_csv:
                    reader = csv.reader(f_csv)

                    for row in tqdm(reader, desc="Writing taxons", total=csv_line_count):
                        entity = row[0]
                        sci_name = row[1]
                        rank = row[2]
                        wn_hyps = json.loads(row[3])
                        if " " in sci_name:
                            process_entry(entity, cursor, wn_hyps, wd2entry, f, lexfiles, addendums,
                                          inst=False)
                            #new_id, entry = make_entry(entity, cursor, wn_hyps, wd2entry,
                            #                           inst=False)
                            #write_entry(f_species, new_id, entry, args.curated)

                        else:
                            childs = []
                            for c in children.get(entity, []):
                                if c in wd2entry:
                                    childs.append(wd2entry[c][0])
                                else:
                                    childs.append(c + "-n")

                            process_entry(entity, cursor, wn_hyps, wd2entry, f, lexfiles, addendums,
                                          lemmas=[f"{rank} {sci_name}", f"{sci_name}"],
                                          inst=False,
                                          mero=childs)
                            #new_id, entry = make_entry(entity, cursor, wn_hyps, wd2entry,
                            #                           lemmas=[f"{rank} {sci_name}", f"{sci_name}"],
                            #                           inst=False,
                            #                           mero=childs)
                            #write_entry(f, new_id, entry, args.curated)

        os.remove(f"{output_folder}/noun.taxon_working.csv")

    if args.update_addendums:
        for filename, data in addendums.items():
            with open(f"{addendum_folder}/{filename}", "w", encoding="utf-8") as f:
                yaml.dump(data, f, sort_keys=True)
                    


