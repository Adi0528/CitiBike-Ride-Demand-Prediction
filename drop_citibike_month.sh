#!/bin/bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: ./drop_citibike_month.sh YEAR MONTH"
    echo "Example: ./drop_citibike_month.sh 2024 1"
    exit 1
fi

YEAR=$1
RAW_MONTH=$2

if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "❌ Error: YEAR must be a valid 4-digit number, e.g. 2024."
    exit 1
fi

if ! [[ "$RAW_MONTH" =~ ^[0-9]{1,2}$ ]]; then
    echo "❌ Error: MONTH must evaluate as an integer from 1 to 12."
    exit 1
fi

MONTH_NUM=$((10#$RAW_MONTH))

if [ "$MONTH_NUM" -lt 1 ] || [ "$MONTH_NUM" -gt 12 ]; then
    echo "❌ Error: MONTH must evaluate within standard 1 to 12 ranges."
    exit 1
fi

MONTH=$(printf "%02d" "$MONTH_NUM")
TARGET_DIR="./data/incoming"
FILE_PATTERN="${YEAR}${MONTH}-citibike-tripdata_*.csv"

shopt -s nullglob
CHUNK_FILES=("${TARGET_DIR}"/${FILE_PATTERN})
shopt -u nullglob

if [ "${#CHUNK_FILES[@]}" -eq 0 ]; then
    echo "❌ Error: No files matching ${FILE_PATTERN} found inside ${TARGET_DIR}."
    echo "   Did you run download_citibike_data.py for ${YEAR}-${MONTH} yet?"
    exit 1
fi

echo "📦 Found ${#CHUNK_FILES[@]} chunk file(s) for ${YEAR}-${MONTH}."

for CHUNK_PATH in "${CHUNK_FILES[@]}"; do
    CHUNK_FILENAME=$(basename "$CHUNK_PATH")

    if stat -c%s "${CHUNK_PATH}" >/dev/null 2>&1; then
        FILE_SIZE=$(stat -c%s "${CHUNK_PATH}")
    else
        FILE_SIZE=$(stat -f%z "${CHUNK_PATH}")
    fi

    MANIFEST_NAME="${CHUNK_FILENAME%.csv}.manifest.json"

    cat > "${TARGET_DIR}/${MANIFEST_NAME}" <<EOF
{
  "dataset": "citibike",
  "source_filepath": "/opt/airflow/data/incoming/${CHUNK_FILENAME}",
  "source_filename": "${CHUNK_FILENAME}",
  "source_year": ${YEAR},
  "source_month": ${MONTH_NUM},
  "file_size_bytes": ${FILE_SIZE},
  "discovered_at_utc": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

    echo "📡 Handshake manifest generated for ${CHUNK_FILENAME}."
done
