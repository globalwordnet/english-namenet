"""
Prepares the manual alignment for taxon generation
"""
import os
import csv
import sqlite3
import json
import pickle
import argparse
from open_english_namenet import load_wordnet_data, WORDNET_SOURCE
from collections import defaultdict
from tqdm import tqdm
from glob import glob
import yaml

taxon_names = ["genus", "family", "order", "class", "phylum", "kingdom", "suborder", "subfamily", "subclass", "superfamily", "division", "subgenus", "subdivision", "superorder", "tribe", "subphylum", "superclass"]


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

def is_taxon_name(name):
    """
    Check if a given name is a taxon name.
    """
    if any(name.startswith(taxon + " ") for taxon in taxon_names):
        return True
    else:
        words = name.split(" ")
        if len(words) == 2 and words[0][0].isupper() and words[1][0].islower():
            return True
        if len(words) == 1 and words[0][0].isupper():
            return True
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare manual taxon alignment for taxon generation.")
    parser.add_argument("--manual_review_path", type=str, help="Path to write the manual review CSV files.", default="taxon_linking_manual.csv")
    args = parser.parse_args()

    wikidata_links, hyps, wn_lemmas = load_wordnet_data()

    mero_member = defaultdict(list)

    for file in tqdm(glob(f"{WORDNET_SOURCE}/src/yaml/[nva]*.yaml"), desc="Loading WordNet data"):
        data = yaml.safe_load(open(file, "r"))
        for ssid, entry in data.items():
            if "mero_member" in entry:
                mero_member[ssid] = entry["mero_member"]
            
 

    hypos = defaultdict(list)
    for ssid, hypos_list in tqdm(hyps.items(), desc="Finding hyponyms"):
        for hypo in hypos_list:
            hypos[hypo].append(ssid)

    taxon_ssids = set()
    seens_ssids = set()

    with open("taxon2common.csv", "w") as f:
        writer = csv.writer(f)
        for ssid in find_all_hypos("08008892-n", hypos):
            if ssid in seens_ssids:
                continue
            seens_ssids.add(ssid)
            lemmas = wn_lemmas.get(ssid, "").split(", ")
            if any(is_taxon_name(lemma) for lemma in lemmas):
                for mero in mero_member[ssid]:
                    mero_lemmas = wn_lemmas.get(mero, "").split(", ")
                    if all(not is_taxon_name(lemma) for lemma in mero_lemmas):
                        writer.writerow([ssid, wn_lemmas.get(ssid, ""), mero, wn_lemmas.get(mero, "")])

            continue
        taxon_ssids.add(ssid)

    with open("taxon_ssids.csv", "w") as f:
        writer = csv.writer(f)
        for ssid in sorted(taxon_ssids):
            writer.writerow([ssid, wn_lemmas.get(ssid, "")])



