#!/bin/bash

LOG="parser_output.log"
echo "Starting a new batch of files at $(date)" >> $LOG
nohup python3 -m main >> $LOG 2>&1
if [ $? -ne 0 ]; then
    echo "main.py crashed at $(date)" >> $LOG
    break
fi
echo "Finished processing all files." >> $LOG