#!/bin/bash

LOG="parser_output_2.log"
MAX_FILES=200
BATCH_SIZE=2
count=0

while [ "$count" -lt "$MAX_FILES" ]; do
    remaining=$((MAX_FILES - count))
    this_batch=$BATCH_SIZE

    # if fewer than batch size left, only process what's needed
    if [ "$remaining" -lt "$BATCH_SIZE" ]; then
        this_batch=$remaining
    fi

    echo "Starting a new batch of $this_batch files at $(date)" >> $LOG
    nohup python3 -m main -n "$this_batch" -d ../../../san2/data/wikidata-history-dumps >> $LOG 2>&1

    if [ $? -ne 0 ]; then
        echo "main.py crashed at $(date)" >> $LOG
        break
    fi

    count=$((count + this_batch))
    echo "Processed so far: $count / $MAX_FILES" >> $LOG
done

echo "Done. Processed $count files in total." >> $LOG