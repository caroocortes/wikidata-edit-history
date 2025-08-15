# wikidata-edit-history

### Download wikidata dumps

Inside download run:
```chmod +x download_wikidumps.sh```
```nohup bash download_wikidumps.sh > download_output.log 2>&1 &```

xml_download_links.txt contains the download links for the xml.bz2 files of the dump 20250601

### Run parser on wikidata dumps
```python
python3 -m  main [options]
```

-f: Process a single .xml.bz2 file.
-n: Limit the number of files to process when running on a directory.
-dir: Directory containing .xml.bz2 files to process. Required.


**Run in detach**
nohup python3 -m main -n NUMBER_FILES -d DIR_DUMPS > parser_output.log 2>&1 &
echo $! > parser.pid 

### Test files
In test/ there are 2 test files (*test.xml* and *example.xml*) to run the parser.

### Change magnitude
- globecoordinate: havershine distance (distance in km for 2 points)
- time: distance in days
- string: levenshtein distance