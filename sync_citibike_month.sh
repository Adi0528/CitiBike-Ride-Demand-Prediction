#!/bin/bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: ./sync_citibike_month.sh YEAR MONTH"
    echo "Example: ./sync_citibike_month.sh 2024 5"
    exit 1
fi

YEAR=$1
MONTH=$2
MAX_ATTEMPTS=6
WAIT_SECONDS=25

MONTH_PADDED=$(printf "%02d" "$((10#$MONTH))")
EXPECTED_COUNT=$(ls ./data/incoming/${YEAR}${MONTH_PADDED}-citibike-tripdata_*.csv 2>/dev/null | wc -l | tr -d ' ')

if [ "$EXPECTED_COUNT" -eq 0 ]; then
    echo "❌ No local files found for ${YEAR}-${MONTH}. Did you download this month yet?"
    exit 1
fi

echo "🎯 Target: ${EXPECTED_COUNT} chunk file(s) for ${YEAR}-${MONTH} need to land in Bronze."

for ATTEMPT in $(seq 1 $MAX_ATTEMPTS); do
    echo ""
    echo "=== Attempt ${ATTEMPT} of ${MAX_ATTEMPTS} ==="

    ./drop_citibike_month.sh "$YEAR" "$MONTH"

    echo "⏳ Waiting ${WAIT_SECONDS}s for Airflow to process..."
    sleep $WAIT_SECONDS

    LANDED_COUNT=$(docker exec -i --workdir /opt/airflow jupyter_notebook python -c "
from pyspark.sql import SparkSession
import yaml
with open('/opt/airflow/config/local.yml') as f:
    cfg = yaml.safe_load(f)
catalog = cfg['catalog']['spark_catalog_name']
spark = (SparkSession.builder.appName('SyncCheck')
    .config(f'spark.sql.catalog.{catalog}', 'org.apache.iceberg.spark.SparkCatalog')
    .config(f'spark.sql.catalog.{catalog}.type', cfg['catalog']['type'])
    .config(f'spark.sql.catalog.{catalog}.warehouse', cfg['catalog']['warehouse_path'])
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .getOrCreate())
df = spark.table(f'{catalog}.bronze.citibike_trips_raw')
count = df.filter((df.source_year == ${YEAR}) & (df.source_month == $((10#$MONTH)))).select('source_filename').distinct().count()
print(count)
" 2>/dev/null | tail -1)

    echo "📊 Landed so far: ${LANDED_COUNT} of ${EXPECTED_COUNT} chunk file(s) for ${YEAR}-${MONTH}."

    if [ "$LANDED_COUNT" -eq "$EXPECTED_COUNT" ]; then
        echo "✅ All chunks for ${YEAR}-${MONTH} confirmed landed in Bronze."
        exit 0
    fi
done

echo "⚠️  Still missing chunks for ${YEAR}-${MONTH} after ${MAX_ATTEMPTS} attempts. Manual investigation needed."
exit 1
