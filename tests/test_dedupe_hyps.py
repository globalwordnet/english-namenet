import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))

import open_english_namenet as oen
import generate


class DedupeTest(unittest.TestCase):
    def setUp(self):
        # a -> b -> c, d -> c, and e is unrelated
        direct = {"a": ["b"], "b": ["c"], "c": [], "d": ["c"], "e": []}
        self.hyps = oen.calculate_transitive_hyps({k: list(v) for k, v in direct.items()})

    def test_is_hyp(self):
        self.assertTrue(generate.is_hyp("a", "b", self.hyps))
        self.assertTrue(generate.is_hyp("a", "c", self.hyps))  # transitive
        self.assertFalse(generate.is_hyp("c", "a", self.hyps))
        self.assertFalse(generate.is_hyp("a", "d", self.hyps))
        self.assertFalse(generate.is_hyp("unknown", "a", self.hyps))

    def test_dedupe_removes_hypernyms_of_other_entries(self):
        self.assertEqual(generate.dedupe_hyps(["c", "a", "b", "a"], self.hyps), ["a"])
        self.assertEqual(generate.dedupe_hyps(["a", "d", "e"], self.hyps), ["a", "d", "e"])
        self.assertEqual(generate.dedupe_hyps(["b", "c", "d"], self.hyps), ["b", "d"])


if __name__ == "__main__":
    unittest.main()
