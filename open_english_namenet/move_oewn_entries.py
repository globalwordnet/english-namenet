"""Move all the data from Open English Wordnet to Open English Namenet."""
import argparse
from glob import glob
import yaml
from collections import defaultdict
from tqdm import tqdm

TAXON = "08008892-n"

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="Move all the data from Open English Wordnet to Open English Namenet."
        )

    argparser.add_argument(
        "oewn_path",
        type=str,
        help="Path to the Open English Wordnet data file.",
    )

    argparser.add_argument(
        "--curated",
        type=str,
        help="Path to the curated Open English Namenet data file.",
        default="data/curated/")

    argparser.add_argument(
        "--addendum",
        type=str,
        help="Path to the addendum Open English Namenet data file.",
        default="data/addendum/")
    args = argparser.parse_args()

    oewn_path = args.oewn_path
    if not oewn_path.endswith("/"):
        oewn_path += "/"

    addenum_path = args.addendum
    if not addenum_path.endswith("/"):
        addenum_path += "/"

    curated_path = args.curated
    if not curated_path.endswith("/"):
        curated_path += "/"


    instance_entries = set()
    hypernyms = {}
    members = {}
    synset2senses = defaultdict(list)
    for file in tqdm(glob(oewn_path + "src/yaml/*.yaml"), desc="Reading OEWN YAML files for instance entries"):
        if "noun" in file:
            with open(file, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=yaml.CLoader)
                if data:
                    instance_entries.update(
                            ssid for ssid, entry in data.items()
                            if "instance_hypernym" in entry)
                for ssid, entry in data.items():
                    if "hypernym" in entry:
                        hypernyms[ssid] = entry["hypernym"]
                    if "members" in entry:
                        members[ssid] = entry["members"]
        if "entries" in file:
            with open(file, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=yaml.CLoader)
                if data:
                    for key, value in data.items():
                        for by_pos in value.values():
                            for sense in by_pos.get("sense", []):
                                synset2senses[sense["synset"]].append(sense["id"])


    taxons = set([TAXON])
    taxon_size = 1
    while True:
        print("Taxon size:", len(taxons))
        # if any hypernyms are in taxons, add their members to taxons
        new_taxons = set()
        for ssid in hypernyms:
            if any(h in taxons for h in hypernyms[ssid]):
                if ssid not in taxons:
                    new_taxons.add(ssid)
        taxons = taxons.union(new_taxons)
        if len(taxons) == taxon_size:
            break
        taxon_size = len(taxons)

    for taxon in taxons:
        # add a taxon to instance entries if at least one member has a capital letter
        if taxon in members:
            if any(any(c.isupper() for c in member) for member in members[taxon]):
                instance_entries.add(taxon)

    instance_senses = set(
            sense_id for ssid in instance_entries for sense_id in synset2senses.get(ssid, []))

    addendum_entries = defaultdict(lambda: defaultdict(dict))

    for file in tqdm(glob(oewn_path + "src/yaml/*.yaml"), desc="Processing OEWN YAML files"):
        filename = file.split("/")[-1]
        if "noun" in file or "verb" in file or "adj" in file or "adv" in file:
            curated = {}
            with open(file, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=yaml.CLoader)
                to_del = []
                for key, value in data.items():
                    if key in instance_entries:
                        to_del.append(key)
                    else:
                        rel_to_del = []
                        for rel, vs in value.items():
                            if any(isinstance(v, str) and v in instance_entries for v in vs):
                                addendum_entries[filename][key][rel] = [
                                    v for v in vs if isinstance(v, str) and v in instance_entries
                                ]
                                value[rel] = [
                                    v for v in vs if not (isinstance(v, str) and v in instance_entries)
                                ]
                                if not value[rel]:
                                    rel_to_del.append(rel)
                        for rel in rel_to_del:
                            del value[rel]
                for key in to_del:
                    curated[key] = data[key]
                    del data[key]
            with open(file, "w", encoding="utf-8") as f:
                yaml.dump(data, f, Dumper=yaml.CDumper, allow_unicode=True)
            if curated:
                with open(curated_path + filename, "w", encoding="utf-8") as f:
                    yaml.dump(curated, f, Dumper=yaml.CDumper, allow_unicode=True)
        elif "entries" in file:
            curated = {}
            with open(file, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=yaml.CLoader)
                to_del = []
                for key, value in data.items():
                    target_synsets = [sense["synset"] for by_pos in value.values() for sense in by_pos.get("sense", [])]
                    if all(synset in instance_entries for synset in target_synsets):
                        to_del.append(key)
                    else:
                        pos_to_del = []
                        for pos, by_pos in value.items():
                            new_senses = []
                            for sense in by_pos.get("sense", []):
                                rel_to_del = []
                                for rel, vs in sense.items():
                                    if any(isinstance(v, str) and v in instance_senses for v in vs):
                                        addendum_entries[filename][key].setdefault(pos, {}).setdefault("sense", []).append(
                                            {
                                                "id": sense["id"],
                                                **{rel: [v for v in vs if isinstance(v, str) and v in instance_senses]}
                                            }
                                        )
                                        sense[rel] = [
                                            v for v in vs if not (isinstance(v, str) and v in instance_senses)
                                        ]
                                        if not sense[rel]:
                                            rel_to_del.append(rel)
                                for rel in rel_to_del:
                                    del sense[rel]
                                if sense["synset"] not in instance_entries:
                                    new_senses.append(sense)
                                else:
                                    addendum_entries[filename][key].setdefault(pos, {}).setdefault("sense", []).append(sense)
                            by_pos["sense"] = new_senses
                            if not by_pos["sense"]:
                                pos_to_del.append(pos)
                        for pos in pos_to_del:
                            del value[pos]

                for key in to_del:
                    curated[key] = data[key]
                    del data[key]


            with open(file, "w", encoding="utf-8") as f:
                yaml.dump(data, f, Dumper=yaml.CDumper, allow_unicode=True)
            if curated:
                with open(curated_path + filename, "w", encoding="utf-8") as f:
                    yaml.dump(curated, f, Dumper=yaml.CDumper, allow_unicode=True)

    for filename, entries in addendum_entries.items():
        with open(addenum_path + filename, "w", encoding="utf-8") as f:
            yaml.dump(dict(entries), f, Dumper=yaml.CDumper, allow_unicode=True)
            


