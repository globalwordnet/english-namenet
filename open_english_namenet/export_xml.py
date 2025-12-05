"""Script to export Open English Namenet to the GWC XML format."""

import argparse
import yaml
from glob import glob
from collections import defaultdict
import tempfile
from xml.sax.saxutils import escape as xml_escape
import os
from tqdm import tqdm
import shelve
import gzip

def escape(s : str) -> str:
    """
    Escape a sense key for OEWN
    """
    return (s.replace("-", "--")
            .replace("'", "-ap-").replace(" ", "_")
            .replace("!", "-excl-").replace("#", "-num-")
            .replace("$", "-dollar-").replace("%", "-percnt-")
            .replace("&", "-amp-").replace("(", "-lpar-")
            .replace(")", "-rpar-").replace("*", "-ast-")
            .replace("+", "-plus-").replace(",", "-comma-")
            .replace("/", "-sol-").replace("{", "-lbrace-")
            .replace("|", "-vert-").replace("}", "-rbrace-")
            .replace("~", "-tilde-").replace("¢", "-cent-")
            .replace("£", "-pound-").replace("§", "-sect-")
            .replace("©", "-copy-").replace("®", "-reg-")
            .replace("°", "-deg-").replace("´", "-acute-")
            .replace("¶", "-para-").replace("º", "-ordm-"))

def lemma2entryid(lemma):
    return f"oenn-{escape(lemma)}-n"

def convert_entry(entry_id, entry_data, output_file, lex_file):
    """Convert a single entry to GWC XML format and write to output file."""
    member_str = " ".join(lemma2entryid(member) for member in entry_data.get("members", []))
    output_file.write(f'  <Synset id="oenn-{entry_id}" ili="in" members="{member_str}" partOfSpeech="n" lexfile="{lex_file}">\n')
    temp_lines = 2
    for definition in entry_data.get("definition", []):
        output_file.write(f'     <Definition>{definition}</Definition>\n')
        temp_lines += 1
    for instance_hypernym in entry_data.get("instance_hypernym", []):
        output_file.write(f'     <SynsetRelation relType="instance_hypernym" targetSynset="oenn-{instance_hypernym}"/>\n')
        temp_lines += 1
    for hypernym in entry_data.get("hypernym", []):
        output_file.write(f'     <SynsetRelation relType="hypernym" targetSynset="oenn-{hypernym}"/>\n')
        temp_lines += 1
    for mero_member in entry_data.get("mero_member", []):
        output_file.write(f'     <SynsetRelation relType="mero_member" targetSynset="oenn-{mero_member}"/>\n')
        temp_lines += 1
    output_file.write('  </Synset>\n')

    return temp_lines

def process_block(block, temp_file, lex_file, entries):
    data = yaml.load(block, Loader=yaml.CLoader)
    for entry_id, entry_data in data.items():
        for member in entry_data.get("members", []):
            if member not in entries:
                entries[member] = []
            entries[member].append(entry_id)
        return convert_entry(entry_id, entry_data, temp_file, lex_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export Open English Namenet to the GWC XML format."
    )
    parser.add_argument(
        "input_folder",
        type=str,
        help="Path to the input Open English Namenet file.",
        default="data/",
    )
    parser.add_argument(
        "output_file",
        type=str,
        help="Path to the output GWC XML file.",
        default="oenn.xml",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Year of the OEWN version.",
        default="2025")
    args = parser.parse_args()

    input_folder = args.input_folder
    if not input_folder.endswith("/"):
        input_folder += "/"

    with shelve.open("entries.db", flag='n', writeback=True) as entries:
        # Create a temp file to write the entries into
        temp_file_path = tempfile.mkstemp()[1]
        
        temp_lines = 0

        with open(temp_file_path, "w", encoding="utf-8") as temp_file:
            for file in tqdm(glob(input_folder + "*.yaml"), desc="Reading YAML files"):
                lex_file = file.split("/")[-1].replace(".yaml", "")
                block = ""
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith((" ", "\t")):
                            block += line
                        else:
                            if block:
                                process_block(block, temp_file, lex_file, entries)
                            block = line
                    if block:
                        process_block(block, temp_file, lex_file, entries)

    with gzip.open(args.output_file, "wt", encoding="utf-8") as output_file:
        output_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        output_file.write('<!DOCTYPE LexicalResource SYSTEM "http://globalwordnet.github.io/schemas/WN-LMF-1.3.dtd">\n')
        output_file.write('<LexicalResource xmlns:dc="https://globalwordnet.github.io/schemas/dc/">\n')
        output_file.write('  <Lexicon id="oenn"\n')
        output_file.write('           label="Open Engish Namenet"\n')
        output_file.write('           language="en"\n')
        output_file.write('           email="john@mccr.ae"\n')
        output_file.write('           license="https://creativecommons.org/licenses/by/4.0"\n')
        output_file.write(f'           version="{args.year}"\n')
        output_file.write('           url="https://github.com/globalwordnet/english-namenet">\n')
        with shelve.open("entries.db", flag='r') as entries:
            lemma = entries.dict.firstkey().decode('utf-8')
            while lemma is not None:
                synsets = entries[lemma]
                entry_id = lemma2entryid(lemma)
                output_file.write(f'    <LexicalEntry id="{entry_id}">\n')
                output_file.write(f'      <Lemma writtenForm="{xml_escape(lemma)}" partOfSpeech="n"/>\n')
                for synset in synsets:
                    synset_str = f"{synset}"
                    output_file.write(f'      <Sense id="{entry_id[:-2]}-{synset}" synset="{synset_str}"/>\n')
                output_file.write('    </LexicalEntry>\n')
                lemma = entries.dict.nextkey(lemma)
                if lemma:
                    lemma = lemma.decode('utf-8')

        with open(temp_file_path, "r", encoding="utf-8") as temp_file:
            for line in tqdm(temp_file, desc="Writing Synsets", total=temp_lines):
                output_file.write(line)

        output_file.write('  </Lexicon>\n')
        output_file.write('</LexicalResource>\n')

        # Remove the temp file
        os.remove(temp_file_path)
        os.remove("entries.db")



