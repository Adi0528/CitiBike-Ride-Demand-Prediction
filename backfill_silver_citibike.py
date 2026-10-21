import re
import subprocess
import sys
import yaml
from pyspark.sql import SparkSession

with open("/opt/airflow/config/local.yml", "r") as f:
    cfg = yaml.safe_load(f)
catalog = cfg["catalog"]["spark_catalog_name"]

spark = (SparkSession.builder
    .appName("SilverBackfillCitiBike")
    .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog}.type", cfg["catalog"]["type"])
    .config(f"spark.sql.catalog.{catalog}.warehouse", cfg["catalog"]["warehouse_path"])
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate())

ledger = spark.table(f"{catalog}.audit.citibike_ingestion_ledger").where("status = 'SUCCESS'")
rows = ledger.select("source_filename", "ingestion_run_id").distinct().collect()

print(f"Found {len(rows)} successfully-ingested Bronze file(s) to backfill into Silver.\n")

FILENAME_PATTERN = re.compile(r"^(\d{4})(\d{2})-citibike-tripdata")

success_count = 0
fail_count = 0

for row in rows:
    filename = row["source_filename"]
    run_id = row["ingestion_run_id"]

    match = FILENAME_PATTERN.match(filename)
    if not match:
        print(f"⚠️  Skipping {filename}: could not parse year/month from filename.")
        continue

    year, month = match.group(1), str(int(match.group(2)))

    cmd = [
        sys.executable, "/opt/airflow/jobs/silver_wap_citibike.py",
        "--config", "/opt/airflow/config/local.yml",
        "--source_filename", filename,
        "--ingestion_run_id", run_id,
        "--target_year", year,
        "--target_month", month,
    ]

    print(f"▶️  Processing {filename} (run_id={run_id[:20]}...)")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"   ✅ Done.")
        success_count += 1
    else:
        print(f"   ❌ Failed. stderr:\n{result.stderr[-1000:]}")
        fail_count += 1

print(f"\n=== Backfill complete: {success_count} succeeded, {fail_count} failed, out of {len(rows)} total. ===")
