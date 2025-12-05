## Find all the links between taxons in Wikidata and OEWN
from open_english_namenet import WORDNET_SOURCE, WIKIDATA_DB
from glob import glob
import yaml
from tqdm import tqdm
from collections import defaultdict
import sqlite3
import json
from open_english_namenet import fetch_in_chunks
import csv
import editdistance
import os
import pickle


taxon_names = ["genus", "family", "order", "class", "phylum", "kingdom"]

name_exceptions = {
        "oedogoniales": "Oedogoniales",
        "gasterosteus": "Gasterosteus",
        "bombycilla": "Bombycilla",
        "mantophasmatodea": "Mantophasmatodea",
        "lemmus": "Lemmus"
        }

# First get all the matching lemmas from WordNet
def get_taxon_names_from_oewn():
    """
    Load WordNet data from YAML files and extract relevant information.
    """
    oewn_taxon_names = {}
    defns = {}

    for file in tqdm(glob(f"{WORDNET_SOURCE}/src/yaml/[nva]*.yaml"), desc="Loading WordNet data"):
        data = yaml.safe_load(open(file, "r"))
        for ssid, entry in data.items():
            # Find any lemmas of the form "<taxon> <Taxonname>"
            for lemma in entry.get("members", []):
                for taxon_name in taxon_names:
                    if lemma.startswith(taxon_name + " "):
                        name = lemma[len(taxon_name) + 1:]
                        # Name must be a single word starting with a capital letter
                        if name and name[0].isupper() and " " not in name:
                            oewn_taxon_names[(taxon_name, name)] = ssid
                        elif name in name_exceptions:
                            oewn_taxon_names[(taxon_name, name_exceptions[name])] = ssid
                        #else:
                        #    print(f"Invalid taxon name: {name} in lemma {lemma} from entry {ssid}")
            if "definition" in entry:
                defns[ssid] = entry["definition"][0]
    return oewn_taxon_names, defns


wd_taxon_qid_to_name = {
        "Q34740": "genus",
        "Q7432": "species",
        "Q767728": "variety",
        "Q164280": "subfamily",
        "Q35409": "family",
        "Q36602": "order",
        "Q37517": "class",
        "Q38348": "phylum",
        "Q36732": "kingdom",
        "Q5867959": "suborder",
        "Q68947": "subspecies",
        "Q3238261": "subgenus",
        "Q2136103": "superfamily",
        "Q4886": "cultivar",
        "Q227936": "tribe",
        "Q5867051": "subclass",
        "Q125838332": "oogenus",
        "Q3025161": "series",
        "Q3181348": "section",
        "Q3965313": "subtribe",
        "Q279749": "form"
        }

def get_taxon_names_from_wikidata():
    """
    Load taxon names from Wikidata.
    """
    wd_taxon_names = defaultdict(list)

    missed_taxons = set()

    db = sqlite3.connect(WIKIDATA_DB)
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM properties")
    total_count = cursor.fetchone()[0]

    cursor.execute("SELECT qid, properties FROM properties")
    for row in tqdm(fetch_in_chunks(cursor), desc="Processing Wikidata properties", total=total_count):
        qid = row[0]
        data = json.loads(row[1])
        if "P105" in data:
            cursor2 = db.cursor()
            cursor2.execute("SELECT data_properties FROM data_properties WHERE qid = ?", (qid,))
            pdata_row = cursor2.fetchone()
            pdata = json.loads(pdata_row[0]) if pdata_row else {}
            cursor2.close()
            taxon_class = data["P105"][0]
            if taxon_class not in wd_taxon_qid_to_name:
                missed_taxons.add(taxon_class)
            elif "P225" in pdata:
                for name in pdata["P225"]:
                    wd_taxon_names[(wd_taxon_qid_to_name.get(taxon_class, "unknown"), name[0])] += [qid]

    db.close()
    print(f"Missed taxons: {missed_taxons}")
    return wd_taxon_names


def build_index(taxon_name_map):
    """
    Build an index from the taxon name map for faster lookups.
    """
    index = defaultdict(list)
    for (taxon, name), values in tqdm(taxon_name_map.items(), desc="Building index"):
        # Use character 4-grams as keys for the index
        for i in range(len(name) - 3):
            ngram = name[i:i+4].lower()
            index[ngram].append((taxon, name, values))
    return index

def find_by_ngram(name, index):
    """
    Find any element in the index where the taxon name contains the given n-gram.
    """
    results = []
    for i in range(len(name) - 3):
        ngram = name[i:i+4].lower()
        if ngram in index:
            results.extend(index[ngram])
    return results

def find_similar(name, index):
    """
    Find any element in the dictionary where the taxon name differs by at most two characters.
    """
    similar = {}
    for (taxon, target_name, values) in find_by_ngram(name, index):
        if editdistance.eval(name.lower(), target_name.lower()) <= 1:
            similar[target_name] = (taxon, name, target_name, values)
        
    return similar.values()

def get_wd_definition(qid, cursor):
    """
    Get the definition of a Wikidata QID.
    """
    cursor.execute(
        "SELECT description FROM descriptions_en WHERE qid = ?",
        (qid,),
    )
    result = cursor.fetchone()
    if not result:
        return ""
    else:
        return result[0]

if __name__ == "__main__":
    if os.path.exists("oewn_taxon_names.pickle"):
        with open("oewn_taxon_names.pickle", "rb") as f:
            oewn_taxon_names, oewn_defns = pickle.load(f)
    else:
        oewn_taxon_names, oewn_defns = get_taxon_names_from_oewn()
        pickle.dump((oewn_taxon_names, oewn_defns), open("oewn_taxon_names.pickle", "wb"))
    if os.path.exists("wd_taxon_names.pickle"):
        with open("wd_taxon_names.pickle", "rb") as f:
            wd_taxon_names = pickle.load(f)
    else:
        wd_taxon_names = get_taxon_names_from_wikidata()
        pickle.dump(wd_taxon_names, open("wd_taxon_names.pickle", "wb"))

    wd2taxon = {
            wd: taxon
            for taxon, wds in wd_taxon_names.items()
            for wd in wds
            }

    oewn2taxon = defaultdict(list)
    for (taxon_name, name), oewn in oewn_taxon_names.items():
        oewn2taxon[oewn].append((taxon_name, name))

    # Find size of intersection and each set
    oewn_set = set(oewn_taxon_names.keys())
    wd_set = set(wd_taxon_names.keys())
    intersection = oewn_set.intersection(wd_set)
    print(f"Size of intersection: {len(intersection)}")
    print(f"Size of OEWN set: {len(oewn_set)}")
    print(f"Size of WD set: {len(wd_set)}")

    oewn_2_wd = {}

    for taxon_name, oewn in oewn_taxon_names.items():
        if oewn not in oewn_2_wd:
            oewn_2_wd[oewn] = set()
        if taxon_name in wd_taxon_names:
            oewn_2_wd[oewn].update(wd_taxon_names[taxon_name])

    
    db = sqlite3.connect(WIKIDATA_DB)
    cursor = db.cursor()

    with open("taxon_linking.csv", "w") as f:

        writer = csv.writer(f)
        writer.writerow(["Taxon", "Name", "Definition", "OEWN", "Taxon", "Name", "Definition", "Wikidata QID", "Type"])
        for oewn, wds in oewn_2_wd.items():
            is_unique = len(wds) == 1
            for wd in wds:
                taxon_name, name = wd2taxon[wd]
                writer.writerow([taxon_name, name, oewn_defns.get(oewn, ""),
                                 "https://en-word.net/id/oewn-" + oewn, taxon_name, name, get_wd_definition(wd, cursor),
                                 "https://www.wikidata.org/entity/" + wd, "Exact" if is_unique else "Conflict"])

        index = build_index(wd_taxon_names)

        for oewn, wds in tqdm(oewn_2_wd.items(), "Finding similar names"):
            if len(wds) == 0:
                similars = []
                for oewn_taxon_name, name in oewn2taxon[oewn]:
                    similars += find_similar(name, index)
                if len(similars) > 0:
                    for wd_taxon_name, name, wd_name, similar_wds in similars:
                        for wd in similar_wds:
                            writer.writerow([oewn_taxon_name, name, oewn_defns.get(oewn, ""),
                                             "https://en-word.net/id/oewn-" + oewn, wd_taxon_name, wd_name, get_wd_definition(wd, cursor),
                                             "https://www.wikidata.org/entity/" + wd, "Similar"])
                else:
                    for oewn_taxon_name, name in oewn2taxon[oewn]:
                        # No match found, write the taxon name and OEWN
                        writer.writerow([oewn_taxon_name, name, oewn_defns.get(oewn, ""),
                                         "https://en-word.net/id/oewn-" + oewn, "", "", "", "", "No match"])
    db.close()

