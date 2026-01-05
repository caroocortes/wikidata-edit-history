#!/bin/bash
#SBATCH -A sci-naumann
#SBATCH -J run_wd_1000files
#SBATCH --time=2-00:00:00
#SBATCH --mem=2T
#SBATCH -c 80
#SBATCH --partition=cpu-batch
#SBATCH -o logs/test2_%j.out
#SBATCH --mail-type=ALL 
#SBATCH --mail-user=slack:carolina.cortes

echo "Started: $(date) on $(hostname)"

echo "Starting environment setup"
source /sc/home/carolina.cortes/conda3/etc/profile.d/conda.sh
conda activate venv
echo "Environment setup complete"

# Find DB node dynamically
DB_NODE=$(squeue -u $USER --name=wd_db --noheader --format='%N' | head -1)

if [ -z "$DB_NODE" ]; then
    echo "ERROR: Database job 'wd_db' not found!"
    echo "Make sure your database is running first"
    exit 1
fi

echo "Found PostgreSQL on node: $DB_NODE"

# Set DB connection
export DB_HOST=$DB_NODE
export DB_PORT=5433
export DB_NAME=wikidata
export DB_USER=postgres
export DB_PASS=postgres

echo "Connecting to PostgreSQL at ${DB_HOST}:${DB_PORT}"

# Test connection
if ! timeout 5 bash -c "cat < /dev/null > /dev/tcp/${DB_HOST}/5433" 2>/dev/null; then
    echo "ERROR: Cannot reach PostgreSQL at ${DB_HOST}:5433"
    exit 1
fi

echo "✓ Connection to database verified"

# Run parser
NUM_FILES=925
echo "Processing $NUM_FILES files"
bash run_parser.sh "$NUM_FILES"

EXIT_CODE=$?

echo "Job finished: $(date)"
sacct -j $SLURM_JOB_ID --format=JobID,MaxRSS,Elapsed,State

exit $EXIT_CODE