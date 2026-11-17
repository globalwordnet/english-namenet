import io
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))

import open_english_namenet as oen
import generate


def make_db(items):
    db = sqlite3.connect(":memory:")
    db.execute("create table labels_en(qid, label)")
    db.execute("create table descriptions_en(qid, description)")
    for qid, label in items.items():
        db.execute("insert into labels_en values (?, ?)", (qid, f'["{label}"]'))
        db.execute("insert into descriptions_en values (?, ?)", (qid, f"desc of {label}"))
    return db.cursor()


class ClashTest(unittest.TestCase):
    def test_new_entry_skipped_on_clash(self):
        cursor = make_db({"Q1": "September"})
        out, log = io.StringIO(), []
        result = generate.process_entry("Q1", cursor, ["00000002-n"], {}, out, {}, {},
                                        unlinked_lemmas={"September": ["15200000-n"]}, clash_log=log)
        self.assertEqual(result, (None, None))
        self.assertEqual(out.getvalue(), "")
        self.assertEqual(log, [("Q1", "September", "15200000-n")])

    def test_clash_on_any_lemma(self):
        cursor = make_db({"Q1": "Ignored"})
        log = []
        result = generate.process_entry("Q1", cursor, [], {}, io.StringIO(), {}, {},
                                        lemmas=["Genus Foo", "Foo"],
                                        unlinked_lemmas={"Foo": ["00000003-n"]}, clash_log=log)
        self.assertEqual(result, (None, None))
        self.assertEqual(log, [("Q1", "Foo", "00000003-n")])

    def test_no_clash_creates_entry(self):
        cursor = make_db({"Q1": "Zanzibarland"})
        out, log = io.StringIO(), []
        new_id, entry = generate.process_entry("Q1", cursor, ["00000002-n"], {}, out, {}, {},
                                               unlinked_lemmas={"September": ["15200000-n"]}, clash_log=log)
        self.assertEqual(new_id, "Q1-e")
        self.assertIn("Q1-e", out.getvalue())
        self.assertEqual(entry["partOfSpeech"], "e")
        self.assertEqual(log, [])

    def test_linked_item_not_affected_by_clash(self):
        # An item already linked in OEWN is merged into its synset, not skipped
        cursor = make_db({"Q1": "September"})
        addendum = {}
        wd2entry = {"Q1": ("15200000-n", {})}
        result = generate.process_entry("Q1", cursor, [], wd2entry, io.StringIO(), {"15200000-n": "noun.time.yaml"}, addendum,
                                        unlinked_lemmas={"September": ["15200001-n"]}, clash_log=[])
        self.assertEqual(result[0], "15200000-n")


class LoadUnlinkedTest(unittest.TestCase):
    def test_unlinked_lemmas(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "src", "yaml"))
            with open(os.path.join(tmp, "src", "yaml", "noun.time.yaml"), "w") as f:
                f.write("00000001-n:\n  members:\n  - September\n  - Sep\n  definition:\n  - a month\n"
                        "00000002-n:\n  members:\n  - March\n  definition:\n  - a month\n  wikidata: Q110\n")
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                _, _, _, unlinked = oen.load_wordnet_data(wordnet_source=tmp, with_unlinked_lemmas=True)
            finally:
                os.chdir(cwd)
        self.assertEqual(unlinked, {"September": ["00000001-n"], "Sep": ["00000001-n"]})


if __name__ == "__main__":
    unittest.main()
