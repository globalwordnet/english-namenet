import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "open_english_namenet"))

import generate


class ConsolidateFilesTest(unittest.TestCase):
    def test_empty_removed_and_small_merged(self):
        with tempfile.TemporaryDirectory() as tmp:
            def write(name, text):
                with open(f"{tmp}/{name}", "w") as f:
                    f.write(text)
            write("name.namenet_empty.yaml", "")
            write("name.namenet_a.yaml", "Q1-e:\n  members:\n  - A\n")
            write("name.namenet_b.yaml", "Q2-e:\n  members:\n  - B")  # no trailing newline
            write("name.namenet_big.yaml", "Q3-e:\n  members:\n  - " + "C" * 500 + "\n")
            write("skipped_clashes.csv", "x")
            self.assertEqual(generate.consolidate_files(tmp, 200), (1, 2))
            self.assertEqual(sorted(os.listdir(tmp)),
                             ["name.namenet_big.yaml", "name.namenet_misc.yaml", "skipped_clashes.csv"])
            import yaml
            misc = yaml.safe_load(open(f"{tmp}/name.namenet_misc.yaml"))
            self.assertEqual(sorted(misc), ["Q1-e", "Q2-e"])

    def test_no_small_files_creates_no_misc(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(f"{tmp}/name.namenet_big.yaml", "w") as f:
                f.write("x" * 300)
            self.assertEqual(generate.consolidate_files(tmp, 200), (0, 0))
            self.assertEqual(os.listdir(tmp), ["name.namenet_big.yaml"])


if __name__ == "__main__":
    unittest.main()
