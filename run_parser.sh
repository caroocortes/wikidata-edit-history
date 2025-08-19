#!/bin/bash

LOG="parser_bash_script.log"

# keep looping until main.py has nothing left to process
while true; do
    echo "Starting a new batch run at $(date)" >> $LOG
    nohup python3 -m main -n 2 -d ../../../san2/data/wikidata-history-dumps >> $LOG 2>&1

    # check exit status
    if [ $? -ne 0 ]; then
        echo "main.py crashed at $(date)" >> $LOG
        break
    fi

    # optional: stop after 30 files
    processed=$(wc -l < processed_files.txt)
    if [ "$processed" -ge 3 ]; then
        echo "Reached 3 processed files, stopping." >> $LOG
        break
    fi
done 