#!/bin/bash

# ==========================================
# Configuration — edit these as needed
# ==========================================
WORK_DIR="/path/to/Wikidata-Toolkit"
JAR_FILE="$WORK_DIR/wdtk-examples/target/wdtk-examples-0.17.1-SNAPSHOT.jar"
MAX_HEAP="140G"
INIT_HEAP="140G"
GC_THREADS=32
CONCURRENT_GC_THREADS=8
# ==========================================

if ! command -v java &> /dev/null; then
    echo "ERROR: Java not found. Please install Java and try again."
    exit 1
fi

if ! command -v mvn &> /dev/null; then
    echo "ERROR: Maven not found. Please install Maven and try again."
    exit 1
fi

echo "=========================================="
echo "Job started at: $(date)"
echo "Running on node: $(hostname)"
echo "Working directory: $(pwd)"
java -version
mvn -version
echo "=========================================="

echo "Building project..."
mvn clean
mvn package -DskipTests

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found at $JAR_FILE"
    exit 1
fi

echo "Found JAR: $JAR_FILE"

JAVA_OPTS="-Xmx$MAX_HEAP -Xms$INIT_HEAP"
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC"
JAVA_OPTS="$JAVA_OPTS -XX:ParallelGCThreads=$GC_THREADS"
JAVA_OPTS="$JAVA_OPTS -XX:ConcGCThreads=$CONCURRENT_GC_THREADS"
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions"
JAVA_OPTS="$JAVA_OPTS -XX:G1NewSizePercent=30"
JAVA_OPTS="$JAVA_OPTS -Xlog:gc*:file=gc.log"

echo "Starting extraction..."
echo "Java options: $JAVA_OPTS"
echo "=========================================="

java $JAVA_OPTS -jar $JAR_FILE
EXIT_CODE=$?

echo "=========================================="
echo "Job finished at: $(date)"
echo "Exit code: $EXIT_CODE"
echo "=========================================="

if [ $EXIT_CODE -eq 0 ]; then
    echo "SUCCESS: Extraction completed successfully!"
else
    echo "ERROR: Extraction failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi