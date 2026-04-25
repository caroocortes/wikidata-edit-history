#!/bin/bash

LOG_DIR="logs"

NUM_FILES=$1
if [ -z "$NUM_FILES" ]; then
  echo "Usage: run_parser.sh <num_files>"
  exit 1
fi

RUN_DATE=$(date +"%Y%m%d_%H%M%S")

LOG_FILE="${LOG_DIR}/parser_output_${RUN_DATE}_${NUM_FILES}.log"

echo "Starting file processing at $(date)" >> "$LOG_FILE"
echo "PID: $$" >> "$LOG_FILE"
echo "Target files: $NUM_FILES" >> "$LOG_FILE"

python3 -m main -n "$NUM_FILES" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo "main.py crashed with exit code $EXIT_CODE at $(date)" >> "$LOG_FILE"
  exit $EXIT_CODE
fi

echo "Finished file processing at $(date)" >> "$LOG_FILE"
echo "Total files processed in this run: $NUM_FILES" >> "$LOG_FILE"

echo "=========================================="
echo "Finished: $(date)"
echo "Exit code: $EXIT_CODE"
echo "=========================================="

exit $EXIT_CODE