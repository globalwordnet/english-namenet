"""This script extracts the Wikidata links from a manually curated CSV file and 
outputs them as changes to be processed with EWE"""
import argparse
import csv
from collections import defaultdict
from open_english_namenet import load_wordnet_data, oewn_extract, wikidata_extract
import sys

def print_match(oewn, wikidata):
    """
    Print the OEWN and Wikidata IDs in the required format.
    """
    print(f"- change_wikidata:\n    synset: {oewn}")
    if isinstance(wikidata, str):
        print(f"    wikidata: {wikidata}")
    elif isinstance(wikidata, list):
        print("    wikidata: [{', '.join(wikidata)}]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process manual Wikidata links.")
    parser.add_argument("manual_review_csf", type=str, help="Path to the manual review CSV file.")
    parser.add_argument("oewn_col_idx", type=int, help="Index of the OEWN column in the CSV file (0-based).")
    parser.add_argument("wikidata_col_idx", type=int, help="Index of the Wikidata column in the CSV file (0-based).")
    parser.add_argument("accept_col_idx", type=int, help="Index of the Accept column in the CSV file (0-based).")
    parser.add_argument("wikidata_manual_col_idx", type=int, help="Index of the Wikidata manual column in the CSV file (0-based).")
    parser.add_argument("--wordnet_manual_col_idx", type=int, default=-1, help="Index of the WordNet manual column in the CSV file (0-based).")
    parser.add_argument("--ignore_col_idx", type=int, default=-1, help="Index of the Ignore column in the CSV file (0-based).")
    args = parser.parse_args()

    matches = defaultdict(list)

    wikidata_links, _, _ = load_wordnet_data()

    with open(args.manual_review_csf, "r") as f:
        reader = csv.reader(f)
        # Assume header is present and skip it
        next(reader)

        for row in reader:
            if row[args.accept_col_idx].strip().upper() == "TRUE":
                oewn_id = oewn_extract(row[args.oewn_col_idx].strip())
                wikidata_id = wikidata_extract(row[args.wikidata_col_idx].strip())
                
                if oewn_id and wikidata_id:
                    if wikidata_id in wikidata_links.get(oewn_id, []):
                        continue
                    print_match(oewn_id, wikidata_id)
                    matches[wikidata_id].append(oewn_id)
            elif args.ignore_col_idx == -1 or row[args.ignore_col_idx].strip().upper() != "TRUE":
                oewn_id = oewn_extract(row[args.oewn_col_idx].strip())
                wikidata_id = wikidata_extract(row[args.wikidata_manual_col_idx].strip())

                if oewn_id and wikidata_id:
                    if wikidata_id in wikidata_links.get(oewn_id, []):
                        continue
                    print_match(oewn_id, wikidata_id)
                    matches[wikidata_id].append(oewn_id)
                elif args.wordnet_manual_col_idx != -1:
                    oewn_id = oewn_extract(row[args.wordnet_manual_col_idx].strip())

                    if oewn_id and wikidata_id:
                        if wikidata_id in wikidata_links.get(oewn_id, []):
                            continue
                        print_match(oewn_id, wikidata_id)
                        matches[wikidata_id].append(oewn_id)

        for wikidata, oewns in matches.items():
            if len(oewns) > 1:
                print(f"Warning: Multiple OEWN IDs {', '.join(oewns)} for Wikidata ID {wikidata}", file=sys.stderr)
                
            else:
                oewn = oewns[0]
                if oewn in wikidata_links and set(wikidata_links.get(oewn,[])) != set([wikidata]):
                    print(f"Warning: OEWN {oewn} has different Wikidata links: {wikidata_links[oewn]} vs {wikidata}", file=sys.stderr)



