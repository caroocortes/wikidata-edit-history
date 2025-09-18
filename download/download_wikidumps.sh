#!/bin/bash

DOWNLOAD_DIR="/san2/data/wikidata-history-dumps"
# DOWNLOAD_DIR="data/wikidata_dumps_20250601"
URL_LIST="xml_download_links.txt"
LOG_FILE="logs/download.log"

mkdir -p "$DOWNLOAD_DIR"

while IFS= read -r url; do
    # Extract filename from URL
    filename=$(basename "$url")

    # Full path for the output file
    output_path="$DOWNLOAD_DIR/$filename"

    # If the file already exists, skip
    if [ -f "$output_path" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Skipping existing file: $filename" | tee -a "$LOG_FILE"
        continue
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Downloading: $filename" | tee -a "$LOG_FILE"

    start_time=$(date +%s)
    # wget --quiet --continue --tries=3 --timeout=30 --output-document="$output_path" "$url" 2>> "$LOG_FILE"

    aria2c "$url" \
        -x 2 -s 2 \
        --continue=true \
        --timeout=30 \
        --retry-wait=60 \
        --max-tries=3 \
        --summary-interval=60 \
        --log-level=warn \
        --dir="$DOWNLOAD_DIR" \
        --out="$filename"

    end_time=$(date +%s)
    elapsed=$((end_time - start_time))

    if [ -f "$DOWNLOAD_DIR/$filename" ]; then
        size_bytes=$(stat --printf="%s" "$DOWNLOAD_DIR/$filename")
        size_mb=$(awk "BEGIN {printf \"%.2f\", $size_bytes / 1024 / 1024}")
    else
        size_mb="N/A"
    fi

    if [ $? -eq 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Finished downloading: $filename in ${elapsed} seconds (${size_mb} MB)" | tee -a "$LOG_FILE"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR downloading: $filename" | tee -a "$LOG_FILE"
    fi

    # sleep 10 minutes between downloads
    sleep 600
done < "$URL_LIST"