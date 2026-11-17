import yaml
from glob import glob
from tqdm import tqdm
import os
import sys
import pickle
import json
import csv
from collections import defaultdict
# Common utility code

WORDNET_SOURCE = "/home/jmccrae/projects/globalwordnet/english-wordnet/"
WIKIDATA_DB = "wikidata.db"

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

def wordnet_yaml_files(wordnet_source=None, pattern="[nva]*.yaml"):
    """
    List the OEWN YAML files under `wordnet_source` (default `WORDNET_SOURCE`).
    Raises an error if there are none, so that a wrong path is not silently
    treated as an empty wordnet.
    """
    source = wordnet_source or WORDNET_SOURCE
    files = sorted(glob(os.path.join(source, "src", "yaml", pattern)))
    if not files:
        raise FileNotFoundError(
            f"No OEWN YAML files matching '{pattern}' found in {os.path.join(source, 'src', 'yaml')}. "
            "Check the --oewn path.")
    return files

def wordnet_signature(files):
    """
    A fingerprint (path, size, modification time) of the OEWN files, used to
    detect when a cached pickle is out of date or was built from another OEWN.
    """
    return [(os.path.abspath(f), os.stat(f).st_size, os.stat(f).st_mtime_ns) for f in files]

def load_cache(path, signature):
    """Load a pickle written with `save_cache`, or None if missing or out of date."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            cached_signature, data = pickle.load(f)
    except Exception:
        cached_signature, data = None, None
    if cached_signature != signature:
        print(f"Ignoring {path}: it is out of date or was built from a different OEWN", file=sys.stderr)
        return None
    return data

def save_cache(path, signature, data):
    with open(path, "wb") as f:
        pickle.dump((signature, data), f)

def load_wordnet_data(with_wd2data=False, with_lexfiles=False, wordnet_source=None, with_unlinked_lemmas=False):
    """
    Load WordNet data from YAML files and extract relevant information.

    If `with_unlinked_lemmas` is set, a final dictionary is also returned that
    maps each lemma of a synset with no Wikidata link to the list of such synsets.
    """
    files = wordnet_yaml_files(wordnet_source)
    signature = wordnet_signature(files)
    cached = load_cache("wordnet_data.pickle", signature)
    if cached is not None and len(cached) != 6:
        cached = None  # Cache from an older version of this code
    if cached is not None:
        wikidata_links, hyps, wn_lemmas, wd2data, lexfiles, unlinked_lemmas = cached
    else:
        wikidata_links = {}
        hyps = {}
        wn_lemmas = {}
        wd2data = {}
        lexfiles = {}
        unlinked_lemmas = defaultdict(list)

        for file in tqdm(files, desc="Loading WordNet data"):
            lexfile = file.split("/")[-1]
            data = yaml.safe_load(open(file, "r"))
            for ssid, entry in data.items():
                wn_lemmas[ssid] = ", ".join(entry["members"])
                hyps[ssid] = []
                lexfiles[ssid] = lexfile
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
                        # A name (-e) and a common noun synset may share a QID;
                        # Wikidata items are attached to the name
                        if wd not in wd2data or not wd2data[wd][0].endswith("-e") or ssid.endswith("-e"):
                            wd2data[wd] = (ssid, entry)
                else:
                    for lemma in entry["members"]:
                        unlinked_lemmas[lemma].append(ssid)

        hyps = calculate_transitive_hyps(hyps)

        save_cache("wordnet_data.pickle", signature, (wikidata_links, hyps, wn_lemmas, wd2data, lexfiles, dict(unlinked_lemmas)))

    result = [wikidata_links, hyps, wn_lemmas]
    if with_wd2data:
        result.append(wd2data)
    if with_lexfiles:
        result.append(lexfiles)
    if with_unlinked_lemmas:
        result.append(unlinked_lemmas)
    return tuple(result)


def load_deprecations(wordnet_source=None):
    """
    Read OEWN's deprecations.csv as a map from deprecated synset ID to the ID that supersedes it.
    """
    path = os.path.join(wordnet_source or WORDNET_SOURCE, "src", "deprecations.csv")
    deprecations = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                old, new = oewn_extract(row["ID"]), oewn_extract(row["SUPERSEDED_BY"])
                if old and new:
                    deprecations[old] = new
    return deprecations

def make_ssid_resolver(ssids, deprecations):
    """
    Returns a function that maps a synset ID from manually reviewed data to the
    current OEWN ID: IDs that exist are unchanged, a `-n` ID that is now a name
    becomes the `-e` ID, and deprecated IDs are replaced by the synset that
    supersedes them. Returns None if no current synset can be found.
    """
    def resolve(ssid):
        seen = set()
        while ssid and ssid not in seen:
            if ssid in ssids:
                return ssid
            seen.add(ssid)
            if ssid.endswith("-n") and ssid[:-1] + "e" in ssids:
                return ssid[:-1] + "e"
            ssid = deprecations.get(ssid)
        return None
    return resolve


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


class WikidataCache:
    """
    Holds the Wikidata data for a known set of items, read with a few
    sequential scans of the database tables. Looking items up one at a time in
    the (very large) database is orders of magnitude slower, as every lookup is
    a random read. Items that were not prefetched fall back to a query.
    """
    def __init__(self, cursor):
        self.cursor = cursor
        self.labels = {}
        self.descriptions = {}
        self.properties = {}
        self.data_properties = {}
        self._fetched = {}

    def _scan(self, table, column, qids, convert=lambda x: x):
        out = {}
        self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total = self.cursor.fetchone()[0]
        self.cursor.execute(f"SELECT qid, {column} FROM {table}")
        for qid, value in tqdm(fetch_in_chunks(self.cursor, 100000), desc=f"Reading {table}", total=total):
            if qid in qids:
                out[qid] = convert(value)
        return out

    def prefetch(self, qids, labels=True, properties=(), data_properties=()):
        """
        Read the label and description of `qids`, and the given properties
        (only those are kept, to save memory) from `properties` and `data_properties`.
        """
        qids = set(qids)
        if labels:
            self.labels.update(self._scan("labels_en", "label", qids))
            self.descriptions.update(self._scan("descriptions_en", "description", qids))
        if properties:
            keep = set(properties)
            self.properties.update(self._scan("properties", "properties", qids,
                lambda v: {k: x for k, x in json.loads(v).items() if k in keep}))
        if data_properties:
            keep = set(data_properties)
            self.data_properties.update(self._scan("data_properties", "data_properties", qids,
                lambda v: {k: x for k, x in json.loads(v).items() if k in keep}))

    def clear(self):
        self.labels.clear()
        self.descriptions.clear()
        self.properties.clear()
        self.data_properties.clear()
        self._fetched.clear()

    def iterate(self, items, size=2000000, **prefetch_args):
        """
        Yield `items` (pairs whose first element is a QID), prefetching the data
        for each block of `size` items, so that memory use stays bounded.
        """
        items = list(items)
        for start in range(0, len(items), size):
            block = items[start:start + size]
            self.clear()
            self.prefetch([item[0] for item in block], **prefetch_args)
            yield from block
        self.clear()

    def get_labels_and_defn(self, qid):
        if qid in self.labels or qid in self.descriptions:
            return json.loads(self.labels[qid]) if qid in self.labels else [], self.descriptions.get(qid, "")
        if qid not in self._fetched:
            self._fetched[qid] = get_labels_and_defn(qid, self.cursor)
        return self._fetched[qid]

    def get_label(self, qid):
        """The first English label of an item, or None"""
        labels, _ = self.get_labels_and_defn(qid)
        return labels[0] if labels else None

    def get_properties(self, qid):
        """The (prefetched) properties of an item, or None if it has none"""
        if qid in self.properties:
            return self.properties[qid]
        self.cursor.execute("SELECT properties FROM properties WHERE qid = ?", (qid,))
        result = self.cursor.fetchone()
        return json.loads(result[0]) if result else None

    def get_data_properties(self, qid):
        if qid in self.data_properties:
            return self.data_properties[qid]
        self.cursor.execute("SELECT data_properties FROM data_properties WHERE qid = ?", (qid,))
        result = self.cursor.fetchone()
        return json.loads(result[0]) if result else None


def get_labels_and_defn(qid, cursor):
    """
    Fetch the English label and description for a given Wikidata QID.
    `cursor` may also be a `WikidataCache`.
    """
    if isinstance(cursor, WikidataCache):
        return cursor.get_labels_and_defn(qid)
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
    if len(oewn) == 10 and oewn[8] == '-' and oewn[9] in 'nvare' and oewn[:8].isdigit():
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


