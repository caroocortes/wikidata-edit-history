# wikidata-edit-history

See requirement.txt for libraries to install.

A .env template file is provided with the variables needed to connect to the postgres DB which will store the tables revision, change and change_metadata.
*Change the values to comply with your DB.*

When running the parsers, the table schemas are automatically created.

### Structure of the repo
- *data*: contains auxiliary datasets to populate the DB 
- *scripts*: contains classes DumpParser and PageParser which parse Wikidata's XML files.
    - dump_parser: processes XML files, extracts pages
    - page_parser: processes a page (all edit history for an entity)
    - fetch_entity_types: queries WD SPARQL query service to obtain entity types (*SCRIPT IS STILL RUNNING*)
    - fetch_wd_entity_labels: queries WD SPARQL query service to obtain entity labels (for all entities in WD) (*SCRIPT IS STILL RUNNING*)
- *download*: containts script to download XML files + list of links of XML files.
- *test*: contains 2 xml test files + script for testing the parser. Also contains examples of revision texts

### Download wikidata dumps - *ALL FILES HAVE ALREADY BEEN DOWNLOADED TO A SERVER*

Inside download/ run:
```chmod +x download_wikidumps.sh```
```nohup bash download_wikidumps.sh > download_output.log 2>&1 &```

xml_download_links.txt contains the download links for the xml.bz2 files of the dump 20250601

To obtain new download links use the scrapper in *scripts/utils* (change link to wikidata service url and folder to save links in *scripts/const*)

### Run parser on wikidata dumps

The simple way of running the parser is with the following command: `python3 -m  main [options]`.

`[options]`:
`-f`: path to .xml.bz2 file to process (for single file).

The parsing of each file takes approx. 50 minutes (using 4 processes in parallel to process pages inside file).

By default, main.py creates *files_in_parallel* (config.json) processes that call DumpParser. DumpParser creates *pages_in_parallel* (config.json) processes which call PageParser and process a page (all revisions for an entity).
Therefore, the system where the main.py is run needs to support at least *files_in_parallel x pages_in_parallel* cores.

**Config file**
*config.json* contains the following parameters:
- language: specifies the language of labels/descriptions (by default it's english - 'en')
- files_in_parallel: number of xml.bz2 files to run in parallel
- pages_in_parallel: number of pages of a single xml.bz2 to process in parallel
- batch_changes_store: size of batch of changes to store in DB (page_parser accumulates changes until batch_changes_store and then saves them to the DB)
- max_files: max files to process (for all files use 2125)

**Run multiple files at a time until max_files files are reached**
The script *run_parser.sh* can be used to run batches of files until a certain TOTAL amount of files is processed.

chmod +x run_parser.sh
./run_parser.sh &

The run generates 3 files:
- processed_files.txt: stores the list of files processed. This is used to check which files have already been processed.
- parser_output.log: log of dump_parser + page_parser.
- parser_log_files.json: summary of processed files with number of entities, time of processing

**Run in detach**
- Create tmux session: `tmux new -s session_name``
- Run script: `./run_parser.sh &`
- Save PID to file (optional, but useful): `echo $! > parser.pid``
- Get out of session: `ctrl + b` and then press `d``
- To go inside the session again: `tmux attach -t session_name`

### Test files
In test/ there are 2 test files (*test.xml* and *example.xml*) to run the parser.
To run the parser on the test files run ```python3 -m test -f <file_name>``` where file_name is either ```example.xml``` or ```test.xml```.
The file example_revision_text.json provides an example of a real revision text (contains claims, labels, descriptions, qualifiers, references).