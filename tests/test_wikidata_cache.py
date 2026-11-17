import json
import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))

import open_english_namenet as oen


def make_db():
    db = sqlite3.connect(":memory:")
    db.execute("create table labels_en(qid, label)")
    db.execute("create table descriptions_en(qid, description)")
    db.execute("create table properties(qid, properties)")
    db.execute("create table data_properties(qid, data_properties)")
    for i in range(1, 6):
        q = f"Q{i}"
        db.execute("insert into labels_en values (?, ?)", (q, json.dumps([f"label{i}", f"alt{i}"])))
        if i != 3:
            db.execute("insert into descriptions_en values (?, ?)", (q, f"desc{i}"))
        db.execute("insert into properties values (?, ?)", (q, json.dumps({"P21": ["Q6581072"], "P106": [f"Q{i}0"], "P999": ["x"]})))
        db.execute("insert into data_properties values (?, ?)", (q, json.dumps({"P225": [[f"Sci {i}", "x"]], "P1": [["y"]]})))
    return db.cursor()


class WikidataCacheTest(unittest.TestCase):
    def setUp(self):
        self.cursor = make_db()
        self.cache = oen.WikidataCache(self.cursor)
        self.cache.prefetch(["Q1", "Q3", "Q4"], properties=["P21", "P106"], data_properties=["P225"])

    def test_matches_direct_queries(self):
        for q in ["Q1", "Q2", "Q3", "Q4", "Q5", "Q99"]:
            self.assertEqual(self.cache.get_labels_and_defn(q), oen.get_labels_and_defn(q, self.cursor), q)

    def test_prefetch_only_reads_requested(self):
        self.assertEqual(set(self.cache.labels), {"Q1", "Q3", "Q4"})
        self.assertEqual(set(self.cache.properties), {"Q1", "Q3", "Q4"})

    def test_only_requested_properties_kept(self):
        self.assertEqual(self.cache.get_properties("Q1"), {"P21": ["Q6581072"], "P106": ["Q10"]})
        self.assertEqual(self.cache.get_data_properties("Q1"), {"P225": [["Sci 1", "x"]]})

    def test_fallback_for_items_not_prefetched(self):
        self.assertEqual(self.cache.get_properties("Q2")["P999"], ["x"])
        self.assertEqual(self.cache.get_data_properties("Q2")["P1"], [["y"]])
        self.assertIsNone(self.cache.get_properties("Q99"))

    def test_iterate_in_blocks(self):
        seen = []
        for qid, x in self.cache.iterate([("Q1", "a"), ("Q2", "b"), ("Q3", "c")], size=2, properties=["P21"]):
            seen.append((qid, x, set(self.cache.labels)))
        self.assertEqual([s[:2] for s in seen], [("Q1", "a"), ("Q2", "b"), ("Q3", "c")])
        self.assertEqual(seen[0][2], {"Q1", "Q2"})  # first block
        self.assertEqual(seen[2][2], {"Q3"})  # second block replaces the first
        self.assertEqual(self.cache.labels, {})  # cleared at the end

    def test_get_label(self):
        self.assertEqual(self.cache.get_label("Q4"), "label4")
        self.assertEqual(self.cache.get_label("Q2"), "label2")
        self.assertIsNone(self.cache.get_label("Q99"))

    def test_process_entry_accepts_cache(self):
        # An item with a label but no description is skipped, as before
        import io
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))
        import generate
        self.assertEqual(generate.process_entry("Q3", self.cache, [], {}, io.StringIO(), {}, {}), (None, None))
        out = io.StringIO()
        new_id, _ = generate.process_entry("Q4", self.cache, [], {}, out, {}, {})
        self.assertEqual(new_id, "Q4-e")


if __name__ == "__main__":
    unittest.main()
