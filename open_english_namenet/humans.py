import sqlite3
from tqdm import tqdm
from collections import Counter, defaultdict
import json
from open_english_namenet import WIKIDATA_DB, fetch_in_chunks, load_wordnet_data
import csv

manual_review_sections = [ "manual_review_babel.csv",
                          #"manual_review_conflict.csv",
                          "manual_review_gf.csv",
                          "manual_review_multi.csv",
                          #"manual_review_none.csv",
                          "manual_review_yovisto.csv" ]


MANUAL_REVIEW_PATH = "/home/jmccrae/projects/jmccrae/oewn-wd-linking/"
WD_URL_LEN = len("https://www.wikidata.org/wiki/Q")

def wikidata_superclasses(qid, cursor) -> list[str]:
    """
    Fetch superclasses of a given Wikidata QID.
    """
    cursor.execute(
        "SELECT properties FROM properties WHERE qid = ?",
        (qid,),
    )
    result = cursor.fetchone()
    if not result:
        return []
    data = json.loads(result[0])
    return data.get("P279", [])  # P279 is the property for 'subclass of'

if __name__ == "__main__":
    manual_reviews = defaultdict(list)

    wikidata_links, hyps, wn_lemmas = load_wordnet_data()

    for manual_review_section in tqdm(manual_review_sections, desc="Loading manual review sections"):
        with open(MANUAL_REVIEW_PATH + manual_review_section) as f:
            reader = csv.DictReader(f)
            for row in reader:
                manual_reviews[row["WDID"][WD_URL_LEN:]].append(row | {
                        "Section": manual_review_section
                        })


    confirmed = {
            v: k for k, vs in wikidata_links.items() for v in vs }

    db = sqlite3.connect(WIKIDATA_DB)
    cursor = db.cursor()

    cursor.execute("SELECT COUNT(*) FROM properties")
    total_count = cursor.fetchone()[0]

    cursor.execute("SELECT qid, properties FROM properties")

    occupations = Counter()

    for row in tqdm(fetch_in_chunks(cursor), desc="Processing properties", total=total_count):
        qid = row[0]
        data = json.loads(row[1])
        if "P31" in data and "Q5" in data["P31"]:
            if "P106" in data:
                for occupation in data["P106"]:
                    occupations[occupation] += 1

    linked_occupations = 0

    with open("linked_occupations.csv", "w") as f:
        with open("occupations_broader.csv", "w") as f_broader:
            writer_broader = csv.writer(f_broader)
            writer = csv.writer(f)
            writer.writerow(["QID", "Labels", "Frequency", "Linked", "Lemma", "WN Description", "Manual Review Section","Accept"])
            writer_broader.writerow(["QID", "Labels", "Frequency", "Linked", "Lemma", "WN Description", "Manual Review Section","Accept"])
            for occupation, frequency in tqdm(occupations.most_common(), desc="Processing occupations"):
                cursor.execute(
                    "SELECT * FROM labels_en WHERE qid = ?",
                    (occupation,),
                )
                result = cursor.fetchone()
                if result:
                    labels = ", ".join(json.loads(result[1])[:3])
                else:
                    labels = "Unknown"
                if occupation in confirmed:
                    writer.writerow([
                        "http://www.wikidata.org/entity" + occupation,
                        labels,
                        frequency,
                        "https://en-word.net/id/oewn-" + confirmed[occupation],
                        "",
                        "",
                        "Confirmed",
                        "TRUE"])

                elif occupation in manual_reviews:
                    for manual_review in manual_reviews[occupation]:
                        writer.writerow([
                            "http://www.wikidata.org/entity/" + occupation,
                            labels,
                            frequency,
                            "https://en-word.net/id/oewn-" + manual_review.get("OEWNID", ""),
                            manual_review.get("Lemma", ""),
                            manual_review.get("WN Description", ""),
                            manual_review.get("Section", ""),
                            ""
                        ])
                else:
                    superclasses = wikidata_superclasses(occupation, cursor)
                    while not any(sc in manual_reviews or sc in confirmed for sc in superclasses):
                        if len(superclasses) == 0:
                            break
                        superclasses = [sc for superclass in superclasses for sc in wikidata_superclasses(superclass, cursor)]
                    found = False
                    for sc in superclasses:
                        if sc in confirmed:
                            writer_broader.writerow([
                                "http://www.wikidata.org/entity/" + occupation,
                                labels,
                                frequency,
                                "https://en-word.net/id/oewn-" + confirmed[sc],
                                "",
                                "",
                                "Confirmed",
                                "TRUE"])
                            linked_occupations += 1
                            found = True
                        elif sc in manual_reviews:
                            for manual_review in manual_reviews[sc]:
                                writer_broader.writerow([
                                    "http://www.wikidata.org/entity/" + occupation,
                                    labels,
                                    frequency,
                                    "https://en-word.net/id/oewn-" + manual_review.get("OEWNID", ""),
                                    manual_review.get("Lemma", ""),
                                    manual_review.get("WN Description", ""),
                                    manual_review.get("Section", ""),
                                    ""
                                ])
                            found = True
                    if not found:
                        writer_broader.writerow([
                            "http://www.wikidata.org/entity/" + occupation,
                            labels,
                            frequency,
                            "",
                            "",
                            "",
                            "",
                            ""
                        ])






