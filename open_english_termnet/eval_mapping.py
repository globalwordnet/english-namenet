import csv
import sqlite3
from collections import Counter
import json
from tqdm import tqdm
from open_english_termnet import WIKIDATA_DB, load_wordnet_data, fetch_in_chunks
import csv

WD_URL_LEN = len("https://www.wikidata.org/wiki/Q")
WN_URL_LEN = len("https://en-word.net/id/oewn-")
WIKIDATA_DB = "/home/jmccrae/projects/jmccrae/oewn-wd-linking/wikidata.db"

if __name__ == "__main__": 
    wd2wn = {}

    with open("overlaps_evaluated.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Extra Type"]:
                wd2wn[row["QID"][WD_URL_LEN:]] = (row["Extra Type"], row["Wordnet Lemmas"], row["Wikidata Labels"])
            elif row["Accept"] == "TRUE":
                wd2wn[row["QID"][WD_URL_LEN:]] = (row["SSID"][WN_URL_LEN:], 
                                                   row["Wordnet Lemmas"], 
                                                   row["Wikidata Labels"])

    _, hyps, wn_lemmas = load_wordnet_data()

    conflicts = Counter()

    db = sqlite3.connect(WIKIDATA_DB)

    cursor = db.cursor()

    cursor.execute("SELECT COUNT(*) FROM properties")
    total_count = cursor.fetchone()[0]    

    cursor.execute("SELECT qid, properties FROM properties")

    for row in tqdm(fetch_in_chunks(cursor), desc="Processing properties", total=total_count):
        qid = row[0]
        data = json.loads(row[1])
        x = set()
        if "P31" in data:
            for broader in data["P31"]:
                if broader in wd2wn:
                    x.add(wd2wn[broader][0])
        x = list(x)
        if len(x) > 0:
            # Discard broader terms
            for b in x:
                for b2 in x:
                    if b != b2 and b2 in hyps.get(b, []):
                        x.remove(b2)
        # Remove 'human' as this is always allowed as a secondary
        if "02474924-n" in x:  # 'human' in WordNet
            x.remove("02474924-n") 
        if len(x) > 0:
            for b in x:
                for b2 in x:
                    if b < b2:
                        if b == "08648560-n" and b2 == "10251212-n":
                            print(f"Conflict: {b} vs {b2} for QID {qid}")
                        conflicts[(b, b2)] += 1

    with open("conflicts.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["ID1", "ID2", "Lemmas 1", "Lemmas 2", "Count"])
        for (ssid1, ssid2), count in conflicts.most_common():
            if count < 5:
                continue
            writer.writerow([ssid1, ssid2, wn_lemmas[ssid1], wn_lemmas[ssid2], count])
