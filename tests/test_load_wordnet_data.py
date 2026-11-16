import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))

import open_english_namenet as oen


def write_oewn(root, members="Alpha"):
    os.makedirs(os.path.join(root, "src", "yaml"))
    with open(os.path.join(root, "src", "yaml", "noun.test.yaml"), "w") as f:
        f.write(f"00000001-n:\n  members:\n  - {members}\n  definition:\n  - a test\n  wikidata: Q1\n")


class LoadWordnetDataTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)  # pickles are written to the current directory

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def test_bad_path_raises(self):
        with self.assertRaises(FileNotFoundError):
            oen.load_wordnet_data(wordnet_source=os.path.join(self.tmp.name, "missing"))

    def test_loads_from_given_source(self):
        write_oewn(os.path.join(self.tmp.name, "a"))
        links, _, lemmas = oen.load_wordnet_data(wordnet_source=os.path.join(self.tmp.name, "a"))
        self.assertEqual(lemmas, {"00000001-n": "Alpha"})
        self.assertEqual(links, {"00000001-n": ["Q1"]})

    def test_cache_invalidated_by_other_source(self):
        write_oewn(os.path.join(self.tmp.name, "a"), "Alpha")
        write_oewn(os.path.join(self.tmp.name, "b"), "Beta")
        oen.load_wordnet_data(wordnet_source=os.path.join(self.tmp.name, "a"))
        _, _, lemmas = oen.load_wordnet_data(wordnet_source=os.path.join(self.tmp.name, "b"))
        self.assertEqual(lemmas, {"00000001-n": "Beta"})

    def test_cache_invalidated_by_changed_file(self):
        root = os.path.join(self.tmp.name, "a")
        write_oewn(root, "Alpha")
        oen.load_wordnet_data(wordnet_source=root)
        write_oewn_path = os.path.join(root, "src", "yaml", "noun.test.yaml")
        with open(write_oewn_path, "a") as f:
            f.write("00000002-n:\n  members:\n  - Gamma\n  definition:\n  - another\n")
        _, _, lemmas = oen.load_wordnet_data(wordnet_source=root)
        self.assertIn("00000002-n", lemmas)

    def test_old_format_pickle_ignored(self):
        import pickle
        root = os.path.join(self.tmp.name, "a")
        write_oewn(root)
        with open("wordnet_data.pickle", "wb") as f:
            pickle.dump(({}, {}, {"x": "stale"}, {}, {}), f)
        _, _, lemmas = oen.load_wordnet_data(wordnet_source=root)
        self.assertEqual(lemmas, {"00000001-n": "Alpha"})


if __name__ == "__main__":
    unittest.main()
