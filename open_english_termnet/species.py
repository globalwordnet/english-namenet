### Link the species in WordNet to Wikidata using the taxonomy.
import argparse
from open_english_termnet import read_wikidata_with_prop_vals, WIKIDATA_DB, WORDNET_SOURCE, oewn_extract, wikidata_extract
import sqlite3
import json
from collections import defaultdict
from tqdm import tqdm
from glob import glob
import yaml
import csv
import os

WORDNET_PREFIX = "https://en-word.net/id/oewn-"
WD_PREFIX = "http://www.wikidata.org/entity/"

def compatible_qids(qids1, qids2):
    if isinstance(qids1, str):
        if isinstance(qids2, str):
            return qids1 == qids2
        else:
            return qids1 in qids2
    else:
        if isinstance(qids2, str):
            return qids2 in qids1
        else:
            return any(qid in qids2 for qid in qids1)


def get_wikidata_desc(cursor, qid):
    cursor.execute("SELECT description FROM descriptions_en WHERE qid = ?", (qid,))
    result = cursor.fetchone()
    if not result:
        return ""
    return result[0]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Link species in WordNet to Wikidata using taxonomy.")
    args = parser.parse_args()

    db = sqlite3.connect(WIKIDATA_DB)
    cursor = db.cursor()

    wikidata_props = read_wikidata_with_prop_vals(cursor, "P31", ["Q16521"], "taxon_instances")

    original_combinations = {}

    lemma2ssid = defaultdict(list)
    meros = defaultdict(list)
    wikidata = {}
    hyps = {}
    defs = {}
    wikidata_inv = {}

    for file in tqdm([f"{WORDNET_SOURCE}/src/yaml/noun.plant.yaml", f"{WORDNET_SOURCE}/src/yaml/noun.animal.yaml"], desc="Loading WordNet data"):
        data = yaml.safe_load(open(file, "r"))
        for ssid, entry in data.items():
            defs[ssid] = entry.get("definition", [""])[0]
            for lemma in entry.get("members", []):
                lemma2ssid[lemma].append(ssid)
            for mero_member in entry.get("mero_member", []):
                meros[mero_member].append(ssid)
            hyps[ssid] = entry.get("hypernym", [])
            if "wikidata" in entry:
                if isinstance(entry["wikidata"], str):
                    wikidata[entry["wikidata"]] = ssid
                elif isinstance(entry["wikidata"], list):
                    for wd in entry["wikidata"]:
                        wikidata[wd] = ssid
                wikidata_inv[ssid] = entry["wikidata"]

    for ssid, hyp_list in hyps.items():
        if len(meros[ssid]) == 0:
            for hyp in hyp_list:
                meros[ssid] += meros.get(hyp, [])

    print(meros["11829906-n"])

    sci_name_to_qid = defaultdict(list)
    parent_taxon = {}

    for entity, superclazz in tqdm(wikidata_props.get("Q16521", {}).items(), desc="Processing taxons"):
        cursor.execute("SELECT properties FROM properties WHERE qid = ?", (entity,))
        result = cursor.fetchone()
        if not result:
            print(f"No properties for {entity}")
            continue

        data = json.loads(result[0])

        if "P105" not in data:
            continue

        if "Q7432" not in data["P105"]:
            continue

        if "P1403" in data:
            for val in data["P1403"]:
                original_combinations[val] = entity

        if "P12765" in data:
            for val in data["P12765"]:
                original_combinations[entity] = val

        if "P12766" in data:
            for val in data["P12766"]:
                original_combinations[entity] = val

        if "P171" in data:
            parent_taxon[entity] = data["P171"][0]

        cursor.execute("SELECT data_properties FROM data_properties WHERE qid = ?", (entity,))
        result = cursor.fetchone()

        if not result:
            continue

        data_props = json.loads(result[0])

        if "P225" not in data_props:
            continue

        sci_name = data_props["P225"][0][0]

        if " " in sci_name:
            if sci_name in lemma2ssid:
                sci_name_to_qid[sci_name].append(entity)

    # Debug (print first 5 items)
    print("Sample sci_name_to_qid items:", list(sci_name_to_qid.items())[:5])
    print("Sample lemma2ssid items:", list(lemma2ssid.items())[:5])
    print("Sample meros items:", list(meros.items())[:5])
    print("Sample parent_taxon items:", list(parent_taxon.items())[:5])
    

    with open("species_review.csv", "w", newline='') as csvfile:
        fieldnames = ["Scientific Name", "SSID", "QID", "WordNet Definition", "Wikidata Description",
                      "Status"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for sci_name, qids in tqdm(sci_name_to_qid.items(), desc="Linking species", total=len(sci_name_to_qid)):
            if len(qids) > 1:
                for qid in qids:
                    for ssid in lemma2ssid.get(sci_name, []):
                        writer.writerow({"Scientific Name": sci_name, "SSID": ssid, "QID": qid, 
                                         "WordNet Definition": defs.get(ssid, ""),
                                         "Wikidata Description": get_wikidata_desc(cursor, qid),
                                         "Status": "Multiple"})
                continue
            ssids = lemma2ssid.get(sci_name, []) 
            if len(ssids) > 1:
                for ssid in ssids:
                    for qid in qids:
                        writer.writerow({"Scientific Name": sci_name, "SSID": ssid, "QID": qid, 
                                         "WordNet Definition": defs.get(ssid, ""),
                                         "Wikidata Description": get_wikidata_desc(cursor, qid),
                                         "Status": "Multiple"})
                continue
            qid = qids[0]
            ssid = ssids[0]
            taxon_match = False
            for mero in meros.get(ssid, []):
                pt = parent_taxon.get(qid, "")
                if wikidata.get(pt, "") == mero:
                    taxon_match = True
                    break

            if not taxon_match:
                writer.writerow({"Scientific Name": sci_name, "SSID": ssid, "QID": qid, 
                                 "WordNet Definition": defs.get(ssid, ""),
                                 "Wikidata Description": get_wikidata_desc(cursor, qid),
                                 "Status": "No taxon match"})
            else:
                writer.writerow({"Scientific Name": sci_name, "SSID": ssid, "QID": qid, 
                                 "WordNet Definition": defs.get(ssid, ""),
                                 "Wikidata Description": get_wikidata_desc(cursor, qid),
                                 "Status": "OK"})


    conflict_accepts = defaultdict(list)

    if os.path.exists("species_conflicts_reviewed.csv"):
        reader = csv.DictReader(open("species_conflicts_reviewed.csv", "r"))
        for row in reader:
            ssid = oewn_extract(row["SSID"])
            existing_qid = wikidata_extract(row["Existing QID"])
            new_qid = wikidata_extract(row["New QID"])
            if row["Accept Existing"] == "TRUE" and existing_qid not in conflict_accepts[ssid]:
                conflict_accepts[ssid].append(existing_qid)
            if row["Accept New"] == "TRUE" and new_qid not in conflict_accepts[ssid]:
                conflict_accepts[ssid].append(new_qid)


    print(wikidata_inv.get("01571533-n", ""))

    if os.path.exists("species_reviewed.csv"):
        with open("species_reviewed.csv", "r") as f:
            with open("species_conflicts.csv", "w", newline='') as conflict_f:
                writer = csv.writer(conflict_f)
                writer.writerow(["SSID", "Existing QID", "New QID", "WordNet Definition", "Existing Wikidata Description", "New Wikidata Description"])
                with open("changes.yaml", "w") as out_f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row["Accept"] == "TRUE" or row["Status"] == "OK":
                            if row["QID"] in original_combinations:
                                qid = [original_combinations[row["QID"]], row["QID"]]
                            else:
                                qid = row["QID"]

                            if row["SSID"] in wikidata_inv:
                                if not compatible_qids(wikidata_inv[row["SSID"]], qid):
                                    qid = conflict_accepts.get(row["SSID"], [])
                                    if len(qid) == 1:
                                        qid = qid[0]
                                    #if isinstance(wikidata_inv[row["SSID"]], list):
                                    #    for existing_qid in wikidata_inv[row["SSID"]]:
                                    #        if isinstance(qid, list):
                                    #            for new_qid in qid:
                                    #                writer.writerow([
                                    #                    WORDNET_PREFIX + row["SSID"], WD_PREFIX + existing_qid, WD_PREFIX + new_qid, defs.get(row["SSID"], ""), get_wikidata_desc(cursor, existing_qid), get_wikidata_desc(cursor, new_qid)])
                                    #        else:
                                    #            writer.writerow([WORDNET_PREFIX + row["SSID"], WD_PREFIX + existing_qid, WD_PREFIX + qid, defs.get(row["SSID"], ""), get_wikidata_desc(cursor, existing_qid), get_wikidata_desc(cursor, qid)])
                                    #elif isinstance(qid, list):
                                    #    for new_qid in qid:
                                    #        writer.writerow([WORDNET_PREFIX + row["SSID"], WD_PREFIX + wikidata_inv[row["SSID"]], WD_PREFIX + new_qid, defs.get(row["SSID"], ""), get_wikidata_desc(cursor, wikidata_inv[row["SSID"]]), get_wikidata_desc(cursor, new_qid)])
                                    #else:
                                    #    writer.writerow([WORDNET_PREFIX + row["SSID"], WD_PREFIX + wikidata_inv[row["SSID"]], WD_PREFIX + qid, defs.get(row["SSID"], ""), get_wikidata_desc(cursor, wikidata_inv[row["SSID"]]), get_wikidata_desc(cursor, qid)])
                            if isinstance(wikidata_inv.get(row["SSID"], ""), list):
                                if isinstance(qid, list):
                                    for qid2 in wikidata_inv[row["SSID"]]:
                                        qid3 = qid.copy()
                                        if qid2 not in qid3:
                                             qid3.append(qid2)
                                        qid = qid3
                                else:
                                    qid2 = wikidata_inv[row["SSID"]]
                                    if qid not in qid2:
                                        qid2.append(qid)
                                    qid = qid2
                            else:
                                if wikidata_inv.get(row["SSID"], "") not in qid:
                                    qid.append(wikidata_inv.get(row["SSID"], ""))
                            print(f"- change_wikidata:\n    synset: {row['SSID']}\n    wikidata: {qid}", file=out_f)


            


        
  

