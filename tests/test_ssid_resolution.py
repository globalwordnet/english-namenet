import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))

import open_english_namenet as oen


class ResolverTest(unittest.TestCase):
    def setUp(self):
        self.ssids = {"00000001-n", "00000002-e", "00000005-n", "00000006-e"}
        self.deprecations = {"00000003-n": "00000001-n", "00000004-n": "00000003-n", "00000007-n": "00000002-n"}
        self.resolve = oen.make_ssid_resolver(self.ssids, self.deprecations)

    def test_existing_unchanged(self):
        self.assertEqual(self.resolve("00000001-n"), "00000001-n")
        self.assertEqual(self.resolve("00000002-e"), "00000002-e")

    def test_name_moved_to_e(self):
        self.assertEqual(self.resolve("00000002-n"), "00000002-e")

    def test_n_kept_if_it_exists_even_if_e_does_too(self):
        ssids = self.ssids | {"00000005-e"}
        self.assertEqual(oen.make_ssid_resolver(ssids, {})("00000005-n"), "00000005-n")

    def test_deprecated_chain(self):
        self.assertEqual(self.resolve("00000004-n"), "00000001-n")

    def test_deprecated_then_name(self):
        self.assertEqual(self.resolve("00000007-n"), "00000002-e")

    def test_unknown_and_none(self):
        self.assertIsNone(self.resolve("99999999-n"))
        self.assertIsNone(self.resolve(None))

    def test_deprecation_cycle_terminates(self):
        resolve = oen.make_ssid_resolver(set(), {"00000001-n": "00000002-n", "00000002-n": "00000001-n"})
        self.assertIsNone(resolve("00000001-n"))


class ExtractTest(unittest.TestCase):
    def test_oewn_extract_accepts_names(self):
        self.assertEqual(oen.oewn_extract("https://en-word.net/id/oewn-08337853-e"), "08337853-e")
        self.assertEqual(oen.oewn_extract("ewn-01916702-v"), "01916702-v")
        self.assertIsNone(oen.oewn_extract("08337853-x"))

    def test_load_deprecations(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "src"))
            with open(os.path.join(tmp, "src", "deprecations.csv"), "w") as f:
                f.write('"ID","ILI","SUPERSEDED_BY","SUPERSEDING_ILI","REASON"\n'
                        '"ewn-06974800-n","i1","ewn-06975194-n","i2","Duplicate"\n'
                        '"ewn-00000009-n","i3","","","Removed"\n')
            self.assertEqual(oen.load_deprecations(tmp), {"06974800-n": "06975194-n"})
        self.assertEqual(oen.load_deprecations("/nonexistent"), {})


class SharedQidTest(unittest.TestCase):
    def test_name_preferred_for_shared_qid(self):
        for order in (["00000001-n", "00000002-e"], ["00000002-e", "00000001-n"]):
            with tempfile.TemporaryDirectory() as tmp:
                os.makedirs(os.path.join(tmp, "src", "yaml"))
                with open(os.path.join(tmp, "src", "yaml", "noun.a.yaml"), "w") as f:
                    for ssid in order:
                        f.write(f"{ssid}:\n  members:\n  - X\n  definition:\n  - d\n  wikidata: Q1\n")
                cwd = os.getcwd()
                os.chdir(tmp)
                try:
                    _, _, _, wd2 = oen.load_wordnet_data(wordnet_source=tmp, with_wd2data=True)
                finally:
                    os.chdir(cwd)
                self.assertEqual(wd2["Q1"][0], "00000002-e")


if __name__ == "__main__":
    unittest.main()
