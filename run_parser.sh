#!/bin/bash

LOG="logs/parser_output.log"
if [ -z "$1" ]; then
    read -p "Enter total number of files to process: " TOTAL
else
    TOTAL=$1
fi

echo "Starting parser with TOTAL=$TOTAL" >> "$LOG"
while true; do
    echo "Starting a new batch at $(date)" >> $LOG
    echo "PID: $$" >> $LOG

    # Run main.py in the foreground
    python3 -m main >> $LOG 2>&1
    STATUS=$?

    if [ $STATUS -ne 0 ]; then
        echo "main.py crashed with exit code $STATUS at $(date)" >> $LOG
        exit $STATUS
    fi

    # Check if there are still unprocessed files left
    # `main.py` logs processed files into PROCESSED_FILES_PATH
    PROCESSED=$(wc -l < logs/processed_files.txt)

    if [ "$PROCESSED" -ge "$TOTAL" ]; then
        echo "All files processed at $(date)" >> $LOG
        break
    else
        echo "Batch finished, but more files remain. Restarting..." >> $LOG
    fi
done