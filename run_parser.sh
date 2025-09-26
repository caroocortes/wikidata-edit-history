#!/bin/bash

# LOG="logs/parser_output.log"
# echo "Starting a new batch of files at $(date)" >> $LOG

# # Run in background
# nohup python3 -m main >> $LOG 2>&1 &

# PID=$!
# echo "Started main.py with PID $PID" >> $LOG

# # Optional: wait for it and check exit status
# wait $PID
# STATUS=$?
# if [ $STATUS -ne 0 ]; then
#     echo "main.py crashed with exit code $STATUS at $(date)" >> $LOG
# else
#     echo "Finished processing all files successfully." >> $LOG
# fi

#!/bin/bash

LOG="logs/parser_output.log"

while true; do
    echo "Starting a new batch at $(date)" >> $LOG

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
    TOTAL=100

    if [ "$PROCESSED" -ge "$TOTAL" ]; then
        echo "All files processed at $(date)" >> $LOG
        break
    else
        echo "Batch finished, but more files remain. Restarting..." >> $LOG
    fi
done