"""Runs generate.py (overlaps, humans and taxon passes) on a tiny OEWN and Wikidata database"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import yaml

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


class EndToEndTest(unittest.TestCase):
    def test_generate(self):
        with tempfile.TemporaryDirectory() as tmp:
            write(f"{tmp}/oewn/src/yaml/noun.test.yaml",
                  "00000001-n:\n  members:\n  - city\n  definition:\n  - a city\n"
                  "00000002-n:\n  members:\n  - writer\n  definition:\n  - an author\n"
                  "00000003-n:\n  members:\n  - Paris\n  definition:\n  - an unlinked synset\n")
            write(f"{tmp}/overlaps_evaluated.csv",
                  "QID,SSID,Accept\nhttp://www.wikidata.org/entity/Q1000,https://en-word.net/id/oewn-00000001-n,TRUE\n")
            write(f"{tmp}/linked_occupations_reviewed.csv",
                  "QID,Linked\nhttp://www.wikidata.org/entity/Q300,https://en-word.net/id/oewn-00000002-n\n")
            write(f"{tmp}/taxon_ssids_reviewed.csv", "Wikidata,SSID,Lemma\n")
            write(f"{tmp}/taxon2common_reviewed.csv", "SSID 1,SSID 2,Accept\n")
            db = sqlite3.connect(f"{tmp}/wd.db")
            for t, c in [("properties", "qid,properties"), ("data_properties", "qid,data_properties"),
                         ("labels_en", "qid,label"), ("descriptions_en", "qid,description"), ("wiki_en", "qid,wiki")]:
                db.execute(f"create table {t}({c})")
            items = {  # qid: (label, description, properties)
                "Q100": ("Lyon", "city in France", {"P31": ["Q1000"]}),
                "Q101": ("Paris", "capital of France", {"P31": ["Q1000"]}),  # clashes with an unlinked OEWN synset
                "Q200": ("Jane Doe", "a writer", {"P31": ["Q5"], "P21": ["Q6581072"], "P106": ["Q300"]}),
                "Q500": ("lion", "big cat", {"P31": ["Q16521"], "P105": ["Q7432"], "P171": ["Q501"]}),
                "Q501": ("Panthera", "genus of cats", {"P31": ["Q16521"], "P105": ["Q34740"]}),
                "Q502": ("tiger", "big cat", {"P31": ["Q16521"], "P105": ["Q7432"], "P171": ["Q501"]}),
                "Q503": ("hybrid", "a hybrid", {"P31": ["Q16521"], "P105": ["Q7432"], "P171": ["Q501"]}),
                "Q7432": ("species", "taxonomic rank", {}),
                "Q34740": ("genus", "taxonomic rank", {}),
            }
            data_properties = {"Q500": {"P225": [["Panthera leo", "x"]]}, "Q501": {"P225": [["Panthera", "x"]]},
                               "Q502": {"P225": [["Panthera  tigris", "x"]]},  # double space
                               "Q503": {"P225": [["Panthera leo × tigris", "x"]]}}  # not a binomial
            for qid, (label, desc, props) in items.items():
                db.execute("insert into properties values (?, ?)", (qid, json.dumps(props)))
                db.execute("insert into labels_en values (?, ?)", (qid, json.dumps([label])))
                db.execute("insert into descriptions_en values (?, ?)", (qid, desc))
            for qid, props in data_properties.items():
                db.execute("insert into data_properties values (?, ?)", (qid, json.dumps(props)))
            db.commit()
            db.close()
            result = subprocess.run(
                [sys.executable, f"{ROOT}/open_english_namenet/generate.py", "--oewn", f"{tmp}/oewn",
                 "--wd", f"{tmp}/wd.db"],
                cwd=tmp, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr[-2000:])

            city = yaml.safe_load(open(f"{tmp}/data/automatic/noun.city.yaml"))
            self.assertEqual(list(city), ["Q100-e"])
            self.assertEqual(city["Q100-e"]["partOfSpeech"], "e")
            self.assertEqual(city["Q100-e"]["instance_hypernym"], ["00000001-n"])

            human = yaml.safe_load(open(f"{tmp}/data/automatic/noun.human.yaml"))
            self.assertEqual(list(human), ["Q200-e"])
            self.assertIn("00000002-n", human["Q200-e"]["instance_hypernym"])

            taxa = yaml.safe_load(open(f"{tmp}/data/automatic/noun.taxon.yaml"))
            self.assertEqual(sorted(taxa), ["Q500-e", "Q501-e", "Q502-e"])  # the hybrid is skipped
            self.assertEqual(taxa["Q502-e"]["members"], ["tiger"])
            self.assertIn("Skipped 1 taxa", result.stdout)
            self.assertEqual(taxa["Q501-e"]["members"], ["genus Panthera", "Panthera"])
            self.assertEqual(taxa["Q501-e"]["hypernym"], ["08008892-n"])
            self.assertEqual(sorted(taxa["Q501-e"]["mero_member"]), ["Q500-e", "Q502-e"])  # not the skipped Q503
            self.assertEqual(taxa["Q500-e"]["partOfSpeech"], "e")

            clashes = open(f"{tmp}/data/automatic/skipped_clashes.csv").read()
            self.assertIn("Q101,Paris,00000003-n", clashes)


if __name__ == "__main__":
    unittest.main()
