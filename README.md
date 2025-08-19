# wikidata-edit-history

### Download wikidata dumps

Inside download run:
```chmod +x download_wikidumps.sh```
```nohup bash download_wikidumps.sh > download_output.log 2>&1 &```

xml_download_links.txt contains the download links for the xml.bz2 files of the dump 20250601

To obtain new download links use the scrapper in *scripts/utils* (change link to wikidata service url and folder to save links in *scripts/const*)

### Run parser on wikidata dumps
`python3 -m  main [options]`

`[options]`:
`-f`: path to .xml.bz2 file to process (for single file processing).
`-n`: Limit the number of files to process when running on a directory.
`-dir`: Directory containing .xml.bz2 files to process. Required.


**Run in detach**
- Create tmux session: `tmux new -s session_name``
- Run main.py: `nohup python3 -m main -n NUMBER_FILES -d DIR_DUMPS > parser_output.log 2>&1 &``
- Save PID to file (optional, but useful): `echo $! > parser.pid``
- Get out of session: `ctrl + b` and then press `d``
- To go inside the session again: `tmux attach -t session_name`

**Run 2 files at a time until 30 files are reached**
chmod +x run_parser.sh
./run_parser.sh &

### Test files
In test/ there are 2 test files (*test.xml* and *example.xml*) to run the parser.

### Change magnitude
- globecoordinate: havershine distance (distance in km for 2 points)
- time: distance in days
- string: levenshtein distance