# Wikidata's entity change extraction

## Change Extraction

### Prerequisites

Install required libraries listed in `requirements.txt`.

### Configuration

A `.env` template file is provided with the necessary variables to connect to the PostgreSQL database. This database stores the following tables:
- `revision`
- `value_changes`
- `reference_change`
- `qualifier_change`
- `value_change_metadata`

**Important**: Update the `.env` file with your database credentials. 

**Note**: Table schemas are automatically created when running the parsers.

## Repository Structure
```bash
├── data/           # Auxiliary datasets for populating the database
├── scripts/        # Core parsing classes 
│   ├── dump_parser.py              # Processes XML files, extracts pages
│   ├── page_parser.py              # Processes a page (all edit history for an entity)
│   ├── fetch_entity_types.py      # Queries Wikidata SPARQL for entity types (RUNNING)
│   └── fetch_wd_entity_labels.py  # Queries Wikidata SPARQL for entity labels (RUNNING)
├── download/       # Scripts for downloading XML files + list of download links
└── test/           # Test XML files, testing scripts, and example revision texts
```

## Running the Parser

### Basic Usage

Run the parser with the following command:

```bash
python3 -m main [options]
```

**Options:**

-f FILE: Path to .xml.bz2 file to process (for single file processing)

**Note:** Processing each file takes approximately 1 hour (using 4 processes in parallel).

### Parallelization
By default, main.py uses the following parallelization strategy:
- Creates *files_in_parallel* processes (from config.json) that call DumpParser (*dump_parser.py*)
- Each DumpParser creates *pages_in_parallel* processes (from config.json) to call PageParser (*page_parser.py*) which processes a page (all revisions for an entity)

The system must support at least *files_in_parallel* × *pages_in_parallel* cores.

### Output Files
The parser generates three output files:

- `processed_files.txt`: List of processed files (for tracking)
- `parser_output.log`: Logs from dump_parser and page_parser
- `parser_log_files.json`: Summary with number of entities, processing time, and file size

### Config file
The `config.json` file contains the following parameters:

 | Parameter | Description | Default | 
 | - | - | - | 
 | language | Language for labels/descriptionsen | (English) | 
 | files_in_parallel |  Number of XML files to process simultaneously | 4 | 
 | pages_in_parallel | Number of pages per XML file to process simultaneously | 4 | 
 | batch_changes_store | Batch size for storing changes in DB |  10000 | 
 | max_files | Maximum number of files to process | - (use 2125 for all files) | 

## Batch Processing
### Running Multiple Files Until Limit
Use the run_parser.sh script to process files in batches until reaching *TOTAL* (input param):
```bash
> bashchmod +x run_parser.sh
> ./run_parser.sh <TOTAL_PARAM> &
```

**Note:** This script runs main.py with the same configuration set in config.json until the TOTAL number of files is processed.

### Run in detach
To run the parser in the background.
- Create tmux session: `tmux new -s session_name`
- Run script: `./run_parser.sh &`
- Get out of session: `ctrl + b` and then press `d`
- To go inside the session again: `tmux attach -t session_name`

## Test
In test/ there are 2 test files (*test.xml* and *example.xml*) to run the parser.
To run the parser on the test files run 
```bash
python3 -m test -f <file_name>
``` 
where file_name is either *example.xml* or *test.xml*.

Morevoer, the file *example_revision_text.json* provides an example of a real revision text (contains claims, labels, descriptions, qualifiers, references).

## Downloading wikidata dump 
**Note:** All files have already been downloaded to the server and they can be found in `/san2/data/wikidata-history-dumps`

Inside download/ run:
```bash
chmod +x download_wikidumps.sh
nohup bash download_wikidumps.sh > download_output.log 2>&1 &
```

*xml_download_links.txt* contains the download links for the xml.bz2 files of the dump 20250601

To obtain new download links use the scrapper in *scripts/utils* (change link to wikidata service url and folder to save links in *scripts/const*)

