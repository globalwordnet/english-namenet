#!/bin/bash

# Get the Wikidata dump
if [ ! -f "latest-all.json.bz2" ]; then
    wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 -O latest-all.json.bz2
fi

# Convert to CSV files
if [ ! -f "properties.csv" ]; then
    cargo run --release -- -l en
fi

sqlite3 ../wikidata.db << END_SCRIPT
.import properties.csv properties --csv
create index properties_index on properties(qid);
.import data_properties.csv data_properties --csv
create index data_properties_index on data_properties(qid);
.import labels_en.csv labels_en --csv
create index labels_en_index on labels_en(qid);
.import descriptions_en.csv descriptions_en --csv
create index descriptions_en_index on descriptions_en(qid);
.import wiki_en.csv wiki_en --csv
create index wiki_en_index on wiki_en(qid);
create index wiki_en_wiki on wiki_en(wiki);
END_SCRIPT

#rm *.csv
