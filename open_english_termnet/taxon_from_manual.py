import argparse
import csv
import os
import pickle
import yaml
from collections import defaultdict
from glob import glob
from open_english_termnet import WIKIDATA_DB, WORDNET_SOURCE
import sqlite3
import json
from tqdm import tqdm


OEWN_PREFIX = "https://en-word.net/id/oewn-"
WD_PREFIX = "https://www.wikidata.org/entity/"

def find_holos(ssid, oewn2wd, mero_graph):
    for mero in mero_graph.get(ssid, []):
        if mero in oewn2wd:
            yield oewn2wd[mero]
        else:
            yield from find_holos(mero, oewn2wd, mero_graph)

def validate_holo(holo, mero, cursor, wd2oewn, max_depth=5):
    if max_depth <= 0:
        return []
    cursor.execute("SELECT properties FROM properties WHERE qid = ?", (mero,))
    result = json.loads(cursor.fetchone()[0])
    parents = result.get("P171", [])
    if any(parent == holo for parent in parents):
        return []
    for parent in parents:
        if parent in wd2oewn:
            return [(holo, parent)]
    return [x for parent in parents for x in validate_holo(parent, mero, cursor, wd2oewn, max_depth-1)]

def get_name_and_defn(wd, cursor):
    cursor.execute("SELECT label FROM labels_en WHERE qid = ?", (wd,))
    result = cursor.fetchone()
    label = json.loads(result[0])[0] if result else ""
    cursor.execute("SELECT description FROM descriptions_en WHERE qid = ?", (wd,))
    result = cursor.fetchone()
    definition = result[0] if result else ""
    return label, definition

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process manual taxon reviews.")
    parser.add_argument("--manual_review_path", type=str, help="Path to the manual review CSV files.", default="taxon_linking_manual.csv")
    args = parser.parse_args()

    oewn2wd = {}
    wd2oewn = {}

    with open(args.manual_review_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Accept"] == "TRUE":
                oewn = row["OEWN"][len(OEWN_PREFIX):]
                wd = row["Wikidata QID"][len(WD_PREFIX):]
                if oewn in oewn2wd:
                    print(f"Warning: Duplicate OEWN {oewn} as {wd} and {oewn2wd[oewn]}")
                if wd in wd2oewn:
                    print(f"Warning: Duplicate WD {wd} as {oewn} and {wd2oewn[wd]}")
                oewn2wd[oewn] = wd
                wd2oewn[wd] = oewn

    if os.path.exists("mero_graph.pickle"):
        with open("mero_graph.pickle", "rb") as f:
            mero_graph = pickle.load(f)
    else:
        mero_graph = defaultdict(list)

        for file in tqdm(glob(f"{WORDNET_SOURCE}/src/yaml/[nva]*.yaml"), desc="Loading WordNet data"):
            data = yaml.safe_load(open(file, "r"))
            for ssid, entry in data.items():
                if "mero_member" in entry:
                    for ssid2 in entry["mero_member"]:
                        mero_graph[ssid2].append(ssid)

        with open("mero_graph.pickle", "wb") as f:
            pickle.dump(mero_graph, f)

    print(len(mero_graph), "mero relations found")

    db = sqlite3.connect(WIKIDATA_DB)
    cursor = db.cursor()

    with open("parent_taxon_disagreements.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["Mero QID", "Mero Label", "Mero Definition", "WN Holo QID", "WN Holo Label", "WN Holo Definition", "WD Holo QID", "WD Holo Label", "WD Holo Definition"])
        for oewn in tqdm(oewn2wd.keys(), desc="Validating holonyms"):
            wd_mero = oewn2wd[oewn]
            for wd_holo in find_holos(oewn, oewn2wd, mero_graph):
                for holo1, holo2 in validate_holo(wd_holo, wd_mero, cursor, wd2oewn):
                    mero_label, mero_defn = get_name_and_defn(wd_mero, cursor)
                    holo1_label, holo1_defn = get_name_and_defn(holo1, cursor)
                    holo2_label, holo2_defn = get_name_and_defn(holo2, cursor)
                    writer.writerow([
                        wd_mero, mero_label, mero_defn,
                        holo1, holo1_label, holo1_defn,
                        holo2, holo2_label, holo2_defn
                    ])
            


