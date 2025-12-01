### Helper script to show the size of the generated resource
from glob import glob


if __name__ == "__main__":
    print("|File                                         | Synsets     | Lemmas      | New         |")
    print("|---------------------------------------------|-------------|-------------|-------------|")

    total_synsets = 0
    total_entries = 0
    total_new = 0
    for file in sorted(glob("oenn/*.yaml")):
        with open(file, "r") as f:
            in_members = False
            synsets = 0
            entries = 0
            new_entry = 0
            for line in f:
                if not line[0].isspace():
                    if not line.startswith("Q"):
                        new_entry += 1
                    synsets += 1
                if line.strip().startswith("members:"):
                    in_members = True
                if in_members and line.strip().startswith("- "):
                    entries += 1
            print(f"|{file:<45}| {synsets:11} | {entries:11} | {new_entry:11} |")
            total_synsets += synsets
            total_entries += entries
            total_new += new_entry
    print("|---------------------------------------------|-------------|-------------|-------------|")
    print(f"|Total                                        | {total_synsets:11} | {total_entries:11} | {total_new:11} |")

