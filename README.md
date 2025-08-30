# wikidata-edit-history

See requirement.txt for libraries to install.

A .env template file is provided with the variables needed to connect to the postgres DB which will store the tables revision, change and change_metadata.
*Change the values to comply with your DB.*

When running the parsers, the table schemas are automatically created.

### Structure of the repo

- *scripts*: contains classes DumpParser and PageParser which parse Wikidata's XML files.
- *download*: containts script to download XML files + list of links of XML files.
- *notebooks*: contains notebooks for data exploration
- *test*: contains 2 xml test files + script for testing the parser.

### Download wikidata dumps

Inside download/ run:
```chmod +x download_wikidumps.sh```
```nohup bash download_wikidumps.sh > download_output.log 2>&1 &```

xml_download_links.txt contains the download links for the xml.bz2 files of the dump 20250601

To obtain new download links use the scrapper in *scripts/utils* (change link to wikidata service url and folder to save links in *scripts/const*)

### Run parser on wikidata dumps

The simple way of running the parser is with the following command: `python3 -m  main [options]`.

`[options]`:
`-f`: path to .xml.bz2 file to process (for single file processing).
`-n`: Limit the number of files to process when running on a directory.
`-dir`: Directory containing .xml.bz2 files to process. Required.

The parsing of each file takes approx. 50 minutes.
We provide a script run_parser.sh to run multiple files in parallel (BATCH_SIZE), with an option to set the maximum number of files to process (MAX_FILES).

By default, main.py creates a process for each file (BATCH_SIZE) and calls DumpParser. DumpParser creates NUM_PAGE_PROCESS (can be found in scripts/const.py) processes which call page_parser and process a page (all revisions for an entity).
Therefore, the system where the main.py is run needs to support at least BATCH_SIZE * NUM_PAGE_PROCESS cores.

**Run BATCH_SIZE files at a time until MAX_FILES files are reached**
chmod +x run_parser.sh
./run_parser.sh &

Inside run_parser the variables BATCH_SIZE and MAX_FILES can be modified.

**Run in detach**
- Create tmux session: `tmux new -s session_name``
- Run main.py: `nohup python3 -m main -n NUMBER_FILES -d DIR_DUMPS > parser_output.log 2>&1 &``
- Save PID to file (optional, but useful): `echo $! > parser.pid``
- Get out of session: `ctrl + b` and then press `d``
- To go inside the session again: `tmux attach -t session_name`

### Test files
In test/ there are 2 test files (*test.xml* and *example.xml*) to run the parser.