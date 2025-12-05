from open_english_namenet import WIKIDATA_DB, fetch_in_chunks, load_wordnet_data, WORDNET_SOURCE
from glob import glob
import yaml
from tqdm import tqdm
from collections import defaultdict
import sqlite3
import json
import csv
import argparse
import os
import pickle

WORDNET_PREFIX = "https://en-word.net/id/oewn-"
WIKIDATA_PREFIX = "https://www.wikidata.org/entity/"

def find_all_hypos(ssid, hypos):
    """
    Recursively find all hyponyms of a given WordNet SSID.
    """
    if ssid not in hypos:
        return []
    result = []
    for hypo in hypos[ssid]:
        result.append(hypo)
        result.extend(find_all_hypos(hypo, hypos))
    return result

def is_wikidata_language(qid, cursor):
    """
    Check if a given Wikidata QID is a language.
    """
    cursor.execute("SELECT properties FROM properties WHERE qid = ?", (qid,))
    result = cursor.fetchone()
    if not result:
        return False
    data = json.loads(result[0])
    return "P31" in data and ("Q34770" in data["P31"]  # P31 is 'instance of', Q34770 is 'language'
            or "Q33742" in data["P31"]  # Q34771 is 'natural language'
            or "Q20162172" in data["P31"]  # Q20162172 is 'human language'
            or "Q1288568" in data["P31"]  # Q1288568 is 'modern language'
            or "Q33384" in data["P31"]  # Q33384 is 'dialect'
            or  "Q45762" in data["P31"]  # Q45762 is 'dead language'
            or "Q436240" in data["P31"]  # Q436240 is 'ancient language'
            or "Q941501" in data["P31"]  # Q941501 is 'language group'
            or "Q25295" in data["P31"])  # Q25295 is 'language family'

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
    parser = argparse.ArgumentParser(description="Process manual taxon reviews.")
    args = parser.parse_args()

    if os.path.exists("oewn_hypos.pickle"):
        with open("oewn_hypos.pickle", "rb") as f:
            hyps, labels, defns = pickle.load(f)
    else:
        hyps = {}
        labels = {}
        defns = {}

        for file in tqdm(glob(f"{WORDNET_SOURCE}/src/yaml/[nva]*.yaml"), desc="Loading WordNet data"):
            data = yaml.safe_load(open(file, "r"))
            for ssid, entry in data.items():
                hyps[ssid] = []
                if "hypernym" in entry:
                    hyps[ssid].extend(entry["hypernym"])
                if "instance_hypernym" in entry:
                    hyps[ssid].extend(entry["instance_hypernym"])
                labels[ssid] = entry["members"]
                defns[ssid] = entry.get("definition", [""])[0]

        with open("oewn_hypos.pickle", "wb") as f:
            pickle.dump((hyps, labels, defns), f)
            

    hypos = defaultdict(list)
    for ssid, hypos_list in tqdm(hyps.items(), desc="Finding hyponyms"):
        for hypo in hypos_list:
            hypos[hypo].append(ssid)

    NATURAL_LANGUAGE = "06916947-n"

    db =  sqlite3.connect(WIKIDATA_DB)  
    cursor = db.cursor()

    all_labels = defaultdict(list)

    for language in find_all_hypos(NATURAL_LANGUAGE, hypos):
        for label in labels.get(language, []):
            # only count labels with at least one capital letter
            if any(c.isupper() for c in label):
                all_labels[label.lower()].append(language)

    cursor.execute("SELECT label, qid FROM labels_en")

    labels2qid = defaultdict(list)

    if os.path.exists("labels2qid.pickle"):
        with open("labels2qid.pickle", "rb") as f:
            labels2qid = pickle.load(f)
    else:
        for row in tqdm(cursor.fetchall(), desc="Processing Wikidata labels"):
            label, qid = row
            wd_labels = json.loads(label)
            for l in wd_labels:
                l = l.lower()
                if l in all_labels:
                    labels2qid[l].append(qid)

        with open("labels2qid.pickle", "wb") as f:
            pickle.dump(labels2qid, f)


    with open("languages.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["Language", "WordNet SSID", "Wikidata QID", "WordNet Definition", "Wikidata Definition", "Exact"])
        for language in tqdm(find_all_hypos(NATURAL_LANGUAGE, hypos), desc="Processing languages"):
            matches = []
            for label in labels[language]:
                matches.extend(labels2qid.get(label.lower(), []))
            matches = set(matches)
            matches = [m for m in matches if is_wikidata_language(m, cursor)]
            if not matches:
                writer.writerow([", ".join(labels[language]), WORDNET_PREFIX + language, "", defns.get(language, ""), "", "None"])
            elif len(matches) == 1:
                writer.writerow([", ".join(labels[language]), WORDNET_PREFIX + language, WIKIDATA_PREFIX + matches[0],
                                 defns.get(language, ""), get_wd_definition(matches[0], cursor), "Exact"])
            else:
                for m in matches:
                    writer.writerow([", ".join(labels[language]), WORDNET_PREFIX + language, WIKIDATA_PREFIX + m,
                                     defns.get(language, ""), get_wd_definition(m, cursor), "Multiple"])
