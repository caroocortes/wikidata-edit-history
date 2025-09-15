#!/bin/bash

LOG="parser_output.log"
echo "Starting a new batch of files at $(date)" >> $LOG

# Run in background
nohup python3 -m main >> $LOG 2>&1 &

PID=$!
echo "Started main.py with PID $PID" >> $LOG

# Optional: wait for it and check exit status
wait $PID
STATUS=$?
if [ $STATUS -ne 0 ]; then
    echo "main.py crashed with exit code $STATUS at $(date)" >> $LOG
else
    echo "Finished processing all files successfully." >> $LOG
fi