import yaml
from glob import glob
import sqlite3
import json
from collections import Counter
import csv
from tqdm import tqdm
from open_english_termnet import load_wordnet_data, WIKIDATA_DB

HYP_IGNORE = set([ "00001740-n", "00001930-n", "00002452-n", 
               "00002684-n", "00007347-n", "00021007-n",
               "00029976-n", "00002137-n", "04431553-n" ])

if __name__ == "__main__":
    db = sqlite3.connect(WIKIDATA_DB)
    cursor = db.cursor()

    overlaps = Counter()

    wikidata_links, hyps, wn_lemmas = load_wordnet_data()

    for ssid, entry_wikidata in tqdm(wikidata_links.items()):
        for wd in wikidata:
            cursor.execute(
                "SELECT * FROM properties WHERE qid = ?",
                (wd,),
            )
            result = cursor.fetchone()
            if result:
                links = json.loads(result[1])
                if "P31" in links:
                    for qid2 in links["P31"]:
                        for h in hyps[ssid]:
                            if h not in HYP_IGNORE:
                                overlaps[(qid2, h)] += 1
                if "P279" in links:
                    for qid2 in links["P279"]:
                        for h in hyps[ssid]:
                            if h not in HYP_IGNORE:
                                overlaps[(qid2, h)] += 1


    with open("overlaps.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["QID", "SSID", "Wordnet Lemmas", "Wikidata Labels", "Count"])
        for (qid, ssid), count in overlaps.most_common():
            if count < 5:
                continue
            cursor.execute(
                "SELECT * FROM labels_en WHERE qid = ?",
                (qid,),
            )
            result = cursor.fetchone()
            if result:
                wikidata_labels = ", ".join(json.loads(result[1])[:3])
            else:
                wikidata_labels = "NO LABELS"

            writer.writerow(["http://www.wikidata.org/entity/" + qid, 
                             "https://en-word.net/id/oewn-" + ssid, 
                             wn_lemmas[ssid], wikidata_labels, count])

    db.close()

