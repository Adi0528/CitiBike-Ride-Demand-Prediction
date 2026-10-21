import argparse
import sys
import hashlib
import yaml
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, current_timestamp, unix_timestamp

def month_bounds(year: int, month: int) -> tuple[str, str]:
    if year < 2000 or year > 2100: raise ValueError(f"Invalid year tracking parameter: {year}")
    if month < 1 or month > 12: raise ValueError(f"Invalid month tracking parameter: {month}")

    start_date = f"{year}-{month:02}-01 00:00:00"
    if month == 12:
        end_date = f"{year + 1}-01-01 00:00:00"
    else:
        end_date = f"{year}-{month + 1:02}-01 00:00:00"
    return start_date, end_date

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--source_filename", required=True)
    parser.add_argument("--ingestion_run_id", required=True)
    parser.add_argument("--target_year", type=int, required=True)
    parser.add_argument("--target_month", type=int, required=True)
    args = parser.parse_args()

    start_date, end_date = month_bounds(args.target_year, args.target_month)

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    catalog = cfg["catalog"]["spark_catalog_name"]
    safe_run_hash = hashlib.sha256(args.ingestion_run_id.encode("utf-8")).hexdigest()[:16]
    candidate_table_name = f"{catalog}.silver.citibike_candidate_{safe_run_hash}"

    spark = (SparkSession.builder
        .appName("SilverWAPPurificationJob-CitiBike")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", cfg["catalog"]["type"])
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg["catalog"]["warehouse_path"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate())

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.audit")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.quarantine")

    bronze_table = f"{catalog}.bronze.citibike_trips_raw"
    silver_table = f"{catalog}.silver.citibike_trips"
    silver_ledger_table = f"{catalog}.audit.citibike_silver_publish_ledger"
    quarantine_table = f"{catalog}.quarantine.citibike_trips"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
            start_station_id STRING, start_station_name STRING,
            end_station_id STRING, end_station_name STRING,
            start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE,
            member_casual STRING, trip_duration_seconds BIGINT,
            source_filename STRING, source_checksum STRING,
            source_year INT, source_month INT,
            ingestion_run_id STRING, published_at_utc TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(started_at))
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table} (
            ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
            start_station_name STRING, start_station_id STRING, end_station_name STRING,
            end_station_id STRING, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE,
            member_casual STRING, source_filename STRING, source_checksum STRING,
            source_year INT, source_month INT, ingestion_run_id STRING, wap_run_id STRING, quarantined_at TIMESTAMP
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_ledger_table} (
            ingestion_run_id STRING, source_filename STRING, status STRING, verified_at TIMESTAMP
        ) USING iceberg
    """)

    already_published = spark.table(silver_ledger_table).where((col("ingestion_run_id") == args.ingestion_run_id) & (col("status") == "SUCCESS")).count()
    if already_published > 0:
        print(f"Idempotency guard: Silver ledger already has SUCCESS. Skipping.")
        sys.exit(0)

    already_in_silver = spark.table(silver_table).where(col("ingestion_run_id") == args.ingestion_run_id).count()
    if already_in_silver > 0:
        print(f"Idempotency guard: Silver already contains run_id. Re-aligning ledger.")
        ledger_df = spark.createDataFrame([Row(ingestion_run_id=args.ingestion_run_id, source_filename=args.source_filename, status="SUCCESS")]).withColumn("verified_at", current_timestamp())
        ledger_df.writeTo(silver_ledger_table).append()
        sys.exit(0)

    df_raw = spark.table(bronze_table).where(col("ingestion_run_id") == args.ingestion_run_id)

    if df_raw.limit(1).count() == 0:
        print(f"ℹ️ Current run ID matched 0 rows. Checking ledger for original successful execution of: {args.source_filename}")
        bronze_ledger_table = f"{catalog}.audit.citibike_ingestion_ledger"

        orig_run = spark.table(bronze_ledger_table).where(
            (col("source_filename") == args.source_filename) & (col("status") == "SUCCESS")
        ).select("ingestion_run_id").orderBy(col("finished_at").asc()).first()

        if orig_run:
            historical_run_id = orig_run["ingestion_run_id"]
            print(f"🔄 Duplicate file detected. Tracking history back to original historical run ID: {historical_run_id}")

            historical_published = spark.table(silver_ledger_table).where(
                (col("ingestion_run_id") == historical_run_id) & (col("status") == "SUCCESS")
            ).count()

            if historical_published > 0:
                print("🟢 Idempotency Skip: Filename has already completed its processing in Silver. Exiting cleanly.")
                sys.exit(0)

            df_raw = spark.table(bronze_table).where(col("ingestion_run_id") == historical_run_id)
            args.ingestion_run_id = historical_run_id

    if df_raw.limit(1).count() == 0:
        print(f"❌ No Bronze rows found for ingestion_run_id={args.ingestion_run_id}")
        sys.exit(1)

    df_raw = df_raw.withColumn(
        "trip_duration_seconds",
        unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
    )

    quantiles_duration = df_raw.approxQuantile("trip_duration_seconds", [0.999], 0.001)

    if not quantiles_duration:
        print("❌ Quality gate failure: unable to compute duration quantile.")
        sys.exit(1)

    max_duration_seconds = float(quantiles_duration[0])
    min_duration_seconds = 60

    valid_condition = (
        col("ride_id").isNotNull() &
        col("started_at").isNotNull() & col("ended_at").isNotNull() &
        col("start_station_id").isNotNull() & col("end_station_id").isNotNull() &
        (col("ended_at") > col("started_at")) &
        (col("trip_duration_seconds") >= min_duration_seconds) &
        (col("trip_duration_seconds") <= max_duration_seconds) &
        (col("started_at") >= start_date) & (col("started_at") < end_date) &
        col("member_casual").isin(["member", "casual"])
    )

    df_clean = (df_raw.filter(valid_condition).select(
            col("ride_id").cast("string").alias("ride_id"),
            col("rideable_type").cast("string").alias("rideable_type"),
            col("started_at").cast("timestamp").alias("started_at"),
            col("ended_at").cast("timestamp").alias("ended_at"),
            col("start_station_id").cast("string").alias("start_station_id"),
            col("start_station_name").cast("string").alias("start_station_name"),
            col("end_station_id").cast("string").alias("end_station_id"),
            col("end_station_name").cast("string").alias("end_station_name"),
            col("start_lat").cast("double").alias("start_lat"),
            col("start_lng").cast("double").alias("start_lng"),
            col("end_lat").cast("double").alias("end_lat"),
            col("end_lng").cast("double").alias("end_lng"),
            col("member_casual").cast("string").alias("member_casual"),
            col("trip_duration_seconds").cast("bigint").alias("trip_duration_seconds"),
            lit(args.source_filename).cast("string").alias("source_filename"),
            col("source_checksum").cast("string").alias("source_checksum"),
            col("source_year").cast("int").alias("source_year"),
            col("source_month").cast("int").alias("source_month"),
            lit(args.ingestion_run_id).cast("string").alias("ingestion_run_id"),
            current_timestamp().alias("published_at_utc"),
        ))

    spark.sql(f"DROP TABLE IF EXISTS {candidate_table_name}")

    try:
        df_clean.writeTo(candidate_table_name).using("iceberg").create()

        total_count = df_raw.count()
        valid_count = spark.read.table(candidate_table_name).count()
        drop_rate = ((total_count - valid_count) / total_count) * 100 if total_count > 0 else 0.0

        print(f"📊 Quality audit: Total={total_count}, Valid={valid_count}, Dropped={drop_rate:.2f}%")

        if drop_rate > 35.0:
            print(f"❌ WAP-style gate breach: dropped row rate {drop_rate:.2f}% exceeded 35% threshold.")
            sys.exit(1)

        df_quarantine = df_raw.filter(~valid_condition).select(
            col("ride_id").cast("string").alias("ride_id"),
            col("rideable_type").cast("string").alias("rideable_type"),
            col("started_at").cast("timestamp").alias("started_at"),
            col("ended_at").cast("timestamp").alias("ended_at"),
            col("start_station_name").cast("string").alias("start_station_name"),
            col("start_station_id").cast("string").alias("start_station_id"),
            col("end_station_name").cast("string").alias("end_station_name"),
            col("end_station_id").cast("string").alias("end_station_id"),
            col("start_lat").cast("double").alias("start_lat"),
            col("start_lng").cast("double").alias("start_lng"),
            col("end_lat").cast("double").alias("end_lat"),
            col("end_lng").cast("double").alias("end_lng"),
            col("member_casual").cast("string").alias("member_casual"),
            col("source_filename").cast("string").alias("source_filename"),
            col("source_checksum").cast("string").alias("source_checksum"),
            col("source_year").cast("int").alias("source_year"),
            col("source_month").cast("int").alias("source_month"),
            col("ingestion_run_id").cast("string").alias("ingestion_run_id"),
            lit(safe_run_hash).cast("string").alias("wap_run_id"),
            current_timestamp().alias("quarantined_at")
        )
        df_quarantine.writeTo(quarantine_table).append()

        print("🚀 Promoting valid candidate rows into production Silver after audit approval.")
        spark.read.table(candidate_table_name).writeTo(silver_table).append()

        ledger_df = spark.createDataFrame([Row(ingestion_run_id=args.ingestion_run_id, source_filename=args.source_filename, status="SUCCESS")]).withColumn("verified_at", current_timestamp())
        ledger_df.writeTo(silver_ledger_table).append()

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {candidate_table_name}")
        print("🧹 Temporary Candidate staging table purged from metadata registry.")

if __name__ == "__main__":
    main()
