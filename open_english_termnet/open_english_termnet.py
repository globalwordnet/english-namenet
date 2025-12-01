import yaml
from glob import glob
from tqdm import tqdm
import os
import pickle
import json
from collections import defaultdict
# Common utility code

WORDNET_SOURCE = "/home/jmccrae/projects/globalwordnet/english-wordnet/"
WIKIDATA_DB = "/home/jmccrae/projects/jmccrae/oewn-wd-linking/wikidata.db"

def calculate_transitive_hyps(hyps):
    """
    Calculate the transitive closure of the hypernym graph.
    """
    changes = len(hyps)
    while changes > 0:
        changes = 0
        for h1 in list(hyps.keys()):
            for h2 in hyps.get(h1, []):
                for h3 in hyps.get(h2, []):
                    if h3 not in hyps[h1]:
                        hyps[h1].append(h3)
                        changes += 1
    return hyps

def load_wordnet_data(with_wd2data=False):
    """
    Load WordNet data from YAML files and extract relevant information.
    """
    if os.path.exists("wordnet_data.pickle"):
        with open("wordnet_data.pickle", "rb") as f:
            wikidata_links, hyps, wn_lemmas, wd2data = pickle.load(f)
        if with_wd2data:
            return wikidata_links, hyps, wn_lemmas, wd2data
        else:
            return wikidata_links, hyps, wn_lemmas
    else:
        wikidata_links = {}
        hyps = {}
        wn_lemmas = {}
        wd2data = {}

        for file in tqdm(glob(f"{WORDNET_SOURCE}/src/yaml/[nva]*.yaml"), desc="Loading WordNet data"):
            data = yaml.safe_load(open(file, "r"))
            for ssid, entry in data.items():
                wn_lemmas[ssid] = ", ".join(entry["members"])
                hyps[ssid] = []
                if "hypernym" in entry:
                    hyps[ssid] = entry["hypernym"]
                if "instance_hypernym" in entry:
                    hyps[ssid] += entry["instance_hypernym"]
                if "wikidata" in entry:
                    if isinstance(entry["wikidata"], str):
                        wikidata_links[ssid] = [entry["wikidata"]]
                    else:
                        wikidata_links[ssid] = entry["wikidata"]
                    for wd in wikidata_links[ssid]:
                        wd2data[wd] = (ssid, entry)

        hyps = calculate_transitive_hyps(hyps)

        with open("wordnet_data.pickle", "wb") as f:
            pickle.dump((wikidata_links, hyps, wn_lemmas, wd2data), f)

        if with_wd2data:
            return wikidata_links, hyps, wn_lemmas, wd2data
        else:
            return wikidata_links, hyps, wn_lemmas


def fetch_in_chunks(cursor, size=1000):
    """
    Fetch rows from the cursor in chunks to avoid memory issues.
    """
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

def read_wikidata_properties(cursor, props=["P31"]):
    """
    Read Wikidata properties from the database and return a mapping of property IDs to their details.
    """
    if os.path.exists(f"wikidata_properties_{'_'.join(props)}.pickle"):
        with open(f"wikidata_properties_{'_'.join(props)}.pickle", "rb") as f:
            return pickle.load(f)
    else:
        cursor.execute("SELECT COUNT(*) FROM properties")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT qid, properties FROM properties")
        properties = {prop: {} for prop in props}
        for row in tqdm(fetch_in_chunks(cursor), desc="Reading Wikidata properties", total=total):
            qid, props_json = row
            prop_dict = json.loads(props_json)
            for prop in props:
                if prop in prop_dict:
                    properties[prop][qid] = prop_dict[prop]

        with open(f"wikidata_properties_{'_'.join(props)}.pickle", "wb") as f:
            pickle.dump(properties, f)
        return properties

def read_wikidata_with_prop_vals(cursor, prop, values, key=None):
    """
    Read Wikidata entries that have a specific property with a given value.
    """
    if key is not None and os.path.exists(f"wikidata_with_{key}.pickle"):
        with open(f"wikidata_with_{key}.pickle", "rb") as f:
            return pickle.load(f)
    else:
        cursor.execute("SELECT COUNT(*) FROM properties")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT qid, properties FROM properties")
        results = defaultdict(dict)
        for row in tqdm(fetch_in_chunks(cursor), desc=f"Reading Wikidata with {key}", total=total):
            qid, props_json = row
            prop_dict = json.loads(props_json)
            for v in prop_dict.get(prop, []):
                if v in values:
                    results[v][qid] = prop_dict[prop]

        if key is not None:
            with open(f"wikidata_with_{key}.pickle", "wb") as f:
                pickle.dump(results, f)
        return results


def get_labels_and_defn(qid, cursor):
    """
    Fetch the English label and description for a given Wikidata QID.
    """
    cursor.execute("SELECT label FROM labels_en WHERE qid = ?", (qid,))
    result = cursor.fetchone()
    labels = json.loads(result[0]) if result else []
    cursor.execute("SELECT description FROM descriptions_en WHERE qid = ?", (qid,))
    result = cursor.fetchone()
    definition = result[0] if result else ""
    return labels, definition


def oewn_extract(oewn):
    """
    Extract the OEWN ID from the given string.
    """
    oewn = oewn[-10:]  # Ensure we only take the last 10 characters
    # Check that the form of the OEWN is eight digits, a hyphen and a letter (n, v, a, or r)
    if len(oewn) == 10 and oewn[8] == '-' and oewn[9] in 'nvar' and oewn[:8].isdigit():
        return oewn
    else:
        return None

def wikidata_extract(wikidata):
    """
    Extract the Wikidata ID from the given string.
    """
    if "Q" in wikidata:
        wikidata = wikidata.split("Q")[-1].strip()
        if wikidata.isdigit():
            return "Q" + wikidata

    return None


