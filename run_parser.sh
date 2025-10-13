#!/bin/bash

LOG="logs/parser_output.log"

# Ask for target number of files to process
if [ -z "$1" ]; then
    read -p "Enter number of files to process this run: " TOTAL
else
    TOTAL=$1
fi

# Get how many files were already processed before this run started
if [ -f logs/processed_files.txt ]; then
    INITIAL_PROCESSED=$(wc -l < logs/processed_files.txt)
else
    INITIAL_PROCESSED=0
fi

echo "Starting parser — target: $TOTAL new files (already processed before: $INITIAL_PROCESSED)" >> "$LOG"

ACCUMULATED=0  # number of processed files in this run

while true; do
    echo "Starting a new batch at $(date)" >> "$LOG"
    echo "PID: $$" >> "$LOG"

    python3 -m main >> "$LOG" 2>&1
    STATUS=$?

    if [ $STATUS -ne 0 ]; then
        echo "main.py crashed with exit code $STATUS at $(date)" >> "$LOG"
        exit $STATUS
    fi

    # Recount total processed files
    CURRENT_PROCESSED=$(wc -l < logs/processed_files.txt)
    NEW_PROCESSED=$((CURRENT_PROCESSED - INITIAL_PROCESSED - ACCUMULATED))
    ACCUMULATED=$((ACCUMULATED + NEW_PROCESSED))

    echo "Processed $NEW_PROCESSED new files this batch. Accumulated: $ACCUMULATED / $TOTAL" >> "$LOG"

    # Stop if accumulated processed files reaches total
    if [ "$ACCUMULATED" -ge "$TOTAL" ]; then
        echo "Finished. Processed $ACCUMULATED new files (total now: $CURRENT_PROCESSED) at $(date)" >> "$LOG"
        break
    fi

    echo "Batch finished — restarting..." >> "$LOG"
done