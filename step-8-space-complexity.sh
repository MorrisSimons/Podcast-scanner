#!/usr/bin/env bash

# Script to measure memory usage as we increase --limit for step-8-build-cassandra-indices.py

LIMITS=(50 100 250 500 750 1000 1500 2000 2500 3000)

RESULTS_FILE="space_experiment_results.tsv"

# Detect OS for cross-platform compatibility
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
else
    OS="linux"
fi

echo -e "limit\tmax_rss_kb\toutput_size_bytes" > "$RESULTS_FILE"

# Check if virtual environment should be activated
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="${SCRIPT_DIR}/venv/bin/activate"
if [[ -f "$VENV_PATH" ]]; then
    echo "Activating virtual environment..."
    source "$VENV_PATH"
fi

for LIMIT in "${LIMITS[@]}"; do
    echo "Running for limit=$LIMIT ..."
    OUT_JSON="__hashmap_limit_${LIMIT}.json"
    
    if [[ "$OS" == "macos" ]]; then
        # macOS: /usr/bin/time -l outputs RSS as "XXXX  maximum resident set size" (in bytes)
        # Capture stderr where time outputs its stats
        (/usr/bin/time -l python3 step-8-build-cassandra-indices.py --limit "$LIMIT" --output "$OUT_JSON") 2> __step8_time.tmp
        
        # Extract max RSS from time output (format: "1081344  maximum resident set size")
        MAX_RSS_BYTES=$(grep "maximum resident set size" __step8_time.tmp 2>/dev/null | awk '{print $1}' | head -1)
        if [[ -n "$MAX_RSS_BYTES" && "$MAX_RSS_BYTES" =~ ^[0-9]+$ ]]; then
            MAX_RSS_KB=$((MAX_RSS_BYTES / 1024))
        else
            MAX_RSS_KB="N/A"
        fi
    else
        # Linux: /usr/bin/time -f outputs max RSS in KB
        /usr/bin/time -f "%M" python3 step-8-build-cassandra-indices.py --limit "$LIMIT" --output "$OUT_JSON" 2> __step8_time.tmp
        
        # Extract memory line (should be the last line)
        MAX_RSS_KB=$(tail -n 1 __step8_time.tmp | tr -d '\n')
        if [[ -z "$MAX_RSS_KB" ]]; then
            MAX_RSS_KB="N/A"
        fi
    fi
    
    # Output file size in bytes
    FILESIZE="N/A"
    if [[ -f "$OUT_JSON" ]]; then
        if [[ "$OS" == "macos" ]]; then
            FILESIZE=$(stat -f "%z" "$OUT_JSON")
        else
            FILESIZE=$(stat --format="%s" "$OUT_JSON")
        fi
    fi
    
    # Write to results file
    echo -e "${LIMIT}\t${MAX_RSS_KB}\t${FILESIZE}" >> "$RESULTS_FILE"
    
    # Optionally: keep or clean up JSONs
    # rm -f "$OUT_JSON"
done

rm -f __step8_time.tmp __step8_output.tmp

echo "Results written to $RESULTS_FILE"
