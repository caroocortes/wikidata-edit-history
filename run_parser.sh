#!/bin/bash

LOG_DIR="logs"

NUM_FILES=$1
if [ -z "$NUM_FILES" ]; then
  echo "Usage: run_parser.sh <num_files>"
  exit 1
fi

RUN_DATE=$(date +"%Y%m%d_%H%M%S")
RUN_FOLDER="${LOG_DIR}/run_${RUN_DATE}_files${NUM_FILES}"
mkdir -p "$RUN_FOLDER"

LOG_FILE="${RUN_FOLDER}/parser_output.log"

echo "Starting file processing at $(date)" >> "$LOG_FILE"
echo "PID: $$" >> "$LOG_FILE"
echo "Target files: $NUM_FILES" >> "$LOG_FILE"

python3 -m main --max-files "$NUM_FILES" >> "$LOG_FILE" 2>&1
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

# Show final stats
sacct -j $SLURM_JOB_ID --format=JobID,MaxRSS,Elapsed,State,ExitCode,MaxDiskRead,MaxDiskWrite

# Show monitoring summary
if [ -f "monitor_${SLURM_JOB_ID}.log" ]; then
    echo ""
    echo "Monitoring summary (last 20 lines):"
    tail -20 "monitor_${SLURM_JOB_ID}.log"
fi
exit $EXIT_CODE