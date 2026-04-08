#!/usr/bin/env bash

set -euo pipefail

INPUT_PATTERN="./histroydata/202603-citibike-tripdata_*.csv"
OUTPUT_DIR="./spark_output_local"

if ls ${INPUT_PATTERN} >/dev/null 2>&1; then
  echo "Running local Spark analysis on ${INPUT_PATTERN}"
else
  echo "No files matched ${INPUT_PATTERN}"
  echo "Update INPUT_PATTERN in testSparkLocal.bash or pass input/output manually:"
  echo "  ./.venv/bin/python spark_analysis.py --input ./your_file.csv --output ./spark_output_local"
  exit 1
fi

./.venv/bin/python spark_analysis.py --input "${INPUT_PATTERN}" --output "${OUTPUT_DIR}"

echo "Local Spark analysis complete. Output written to ${OUTPUT_DIR}"