import argparse
import sys
import hashlib
import yaml
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, current_timestamp, col

SOURCE_COLUMN_TYPES = {
    "ride_id": "string", "rideable_type": "string", "started_at": "timestamp",
    "ended_at": "timestamp", "start_station_name": "string", "start_station_id": "string",
    "end_station_name": "string", "end_station_id": "string", "start_lat": "double",
    "start_lng": "double", "end_lat": "double", "end_lng": "double", "member_casual": "string",
}

def validate_year_month(year: int, month: int):
    if year < 2000 or year > 2100: raise ValueError(f"Invalid year parameter configuration: {year}")
    if month < 1 or month > 12: raise ValueError(f"Invalid month parameter configuration: {month}")

def calculate_file_sha256(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--source_filepath", required=True)
    parser.add_argument("--source_filename", required=True)
    parser.add_argument("--source_year", type=int, required=True)
    parser.add_argument("--source_month", type=int, required=True)
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    validate_year_month(args.source_year, args.source_month)

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    catalog = cfg["catalog"]["spark_catalog_name"]
    computed_checksum = calculate_file_sha256(args.source_filepath)
    print(f"🔒 File checksum: {computed_checksum}")

    spark = (SparkSession.builder
        .appName("BronzeIngestionJob-CitiBike")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", cfg["catalog"]["type"])
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg["catalog"]["warehouse_path"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate())

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.audit")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze")

    ledger_table = f"{catalog}.audit.citibike_ingestion_ledger"
    bronze_table = f"{catalog}.bronze.citibike_trips_raw"

    spark.sql(f"CREATE TABLE IF NOT EXISTS {ledger_table} (source_filename STRING, file_checksum STRING, ingestion_run_id STRING, status STRING, finished_at TIMESTAMP) USING iceberg")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table} (
            ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
            start_station_name STRING, start_station_id STRING, end_station_name STRING,
            end_station_id STRING, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE,
            member_casual STRING, source_filename STRING, source_checksum STRING,
            source_year INT, source_month INT, ingestion_run_id STRING, ingested_at_utc TIMESTAMP
        ) USING iceberg PARTITIONED BY (source_year, source_month)
    """)

    dup_ledger = spark.table(ledger_table).where((col("file_checksum") == computed_checksum) & (col("status") == "SUCCESS")).count()
    if dup_ledger > 0:
        print("🟢 Idempotency skip: Filename has already completed its processing in Silver. Exiting cleanly.")
        sys.exit(0)

    dup_table = spark.table(bronze_table).where(col("source_checksum") == computed_checksum).count()
    if dup_table > 0:
        print("Idempotency guard: checksum already present in table. Re-aligning ledger.")
        ledger_df = spark.createDataFrame([Row(source_filename=args.source_filename, file_checksum=computed_checksum, ingestion_run_id=args.run_id, status="SUCCESS")]).withColumn("finished_at", current_timestamp())
        ledger_df.writeTo(ledger_table).append()
        sys.exit(0)

    df_raw = spark.read.option("header", "true").csv(args.source_filepath)

    for required_col, spark_type in SOURCE_COLUMN_TYPES.items():
        if required_col not in df_raw.columns:
            df_raw = df_raw.withColumn(required_col, lit(None).cast(spark_type))

    df_bronze = df_raw.select(
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
        lit(args.source_filename).cast("string").alias("source_filename"),
        lit(computed_checksum).cast("string").alias("source_checksum"),
        lit(args.source_year).cast("int").alias("source_year"),
        lit(args.source_month).cast("int").alias("source_month"),
        lit(args.run_id).cast("string").alias("ingestion_run_id"),
        current_timestamp().alias("ingested_at_utc"),
    )

    df_bronze.writeTo(bronze_table).append()

    ledger_df = spark.createDataFrame([Row(source_filename=args.source_filename, file_checksum=computed_checksum, ingestion_run_id=args.run_id, status="SUCCESS")]).withColumn("finished_at", current_timestamp())
    ledger_df.writeTo(ledger_table).append()
    print("🟢 Bronze Ingestion complete (Citi Bike).")

if __name__ == "__main__":
    main()
