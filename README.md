# Open English Namenet

This project builds an extension of Open English Wordnet (OEWN) by adding names from
Wikidata. The resulting resource, Open English Namenet, contains millions of 
extra synsets. This repository includes the manual entries from OEWN as well
as code for building the extended resource.

## Environment Setup

This project is developed using Python and Poetry for dependency management. To set up the environment, follow these steps:

1. **Install Poetry**: If you haven't already, install Poetry by following the instructions at [https://python-poetry.org/docs/#installation](https://python-poetry.org/docs/#installation).
2. **Clone the Repository**: Clone this repository to your local machine.
3. **Install Dependencies**: Navigate to the project directory and run the following command to install the required dependencies:
   ```bash
   poetry install
   ```
4. **Activate the Virtual Environment**: To activate the virtual environment created by Poetry, run:
   ```bash
   poetry shell
   ```

## Building Wikidata Database

The Wikidata database is required to generate the Open English Namenet. 
It requires Cargo (Rust's package manager), sqlite3, and wget to be installed on your system.

To install Cargo, follow the instructions at [https://doc.rust-lang.org/cargo/getting-started/installation.html](https://doc.rust-lang.org/cargo/getting-started/installation.html).

Once you have Cargo installed, run the following command to build the Wikidata database:

```bash
cd wikidata_db
bash build_db.sh
```

This downloads the most recent Wikidata dump and takes 6-8 hours and a lot of disk space to process.

You can delete `wikidata_db/latest-all.json.bz2` after the database has been built to save space 
or to restart with a newer dump.


## Obtaining Open English Wordnet

OEWN can be cloned from its GitHub repository. Run the following command to clone OEWN:

```bash
git clone https://github.com/globalwordnet/english-wordnet.git
```


## Generating Open English Namenet

To generate the Open English Namenet, run the following command from the project root:

```bash
python open_english_namenet/generate.py --oewn /path/to/english-wordnet --wd wikidata.db
```


