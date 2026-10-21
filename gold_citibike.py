import argparse
import sys
import yaml
from pyspark.sql import SparkSession

def month_bounds(year: int, month: int) -> tuple[str, str]:
    if year < 2000 or year > 2100: raise ValueError(f"Invalid year: {year}")
    if month < 1 or month > 12: raise ValueError(f"Invalid month: {month}")

    start_date = f"{year}-{month:02}-01 00:00:00"
    if month == 12:
        end_date = f"{year + 1}-01-01 00:00:00"
    else:
        end_date = f"{year}-{month + 1:02}-01 00:00:00"
    return start_date, end_date

def load_target_stations(path: str) -> list[str]:
    with open(path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--station_list", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    start_date, end_date = month_bounds(args.year, args.month)
    target_stations = load_target_stations(args.station_list)

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    catalog = cfg["catalog"]["spark_catalog_name"]

    spark = (SparkSession.builder
        .appName("GoldCitiBikeStationHourlyDemand")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", cfg["catalog"]["type"])
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg["catalog"]["warehouse_path"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate())

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold")

    gold_table = f"{catalog}.gold.citibike_station_hourly_demand"
    silver_table = f"{catalog}.silver.citibike_trips"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            target_year INT, target_month INT,
            station_id STRING, hour_ts TIMESTAMP,
            trip_count INT
        ) USING iceberg
    """)

    spark.sql(f"DELETE FROM {gold_table} WHERE target_year = {args.year} AND target_month = {args.month}")

    station_values_sql = ", ".join(f"('{s}')" for s in target_stations)
    station_in_sql = ", ".join(f"'{s}'" for s in target_stations)

    spark.sql(f"""
        INSERT INTO {gold_table}
        WITH time_skeleton AS (
            SELECT explode(sequence(timestamp('{start_date}'), timestamp('{end_date}') - interval '1 hour', interval '1 hour')) AS hour_ts
        ),
        station_skeleton AS (
            SELECT station_id FROM VALUES {station_values_sql} AS t(station_id)
        ),
        dense_grid AS (
            SELECT s.station_id, t.hour_ts FROM station_skeleton s CROSS JOIN time_skeleton t
        ),
        actual_aggregates AS (
            SELECT
                start_station_id AS station_id,
                date_trunc('hour', started_at) AS hour_ts,
                CAST(COUNT(*) AS INT) AS actual_trips
            FROM {silver_table}
            WHERE started_at >= timestamp('{start_date}') AND started_at < timestamp('{end_date}')
              AND start_station_id IN ({station_in_sql})
            GROUP BY 1, 2
        )
        SELECT
            {args.year} AS target_year,
            {args.month} AS target_month,
            g.station_id,
            g.hour_ts,
            CAST(coalesce(a.actual_trips, 0) AS INT) AS trip_count
        FROM dense_grid g
        LEFT JOIN actual_aggregates a
          ON g.station_id = a.station_id AND g.hour_ts = a.hour_ts
    """)

    final_cols = spark.table(gold_table).columns
    forbidden_leakage_fields = ["ended_at", "end_station_id", "trip_duration_seconds", "member_casual", "rideable_type"]

    for forbidden in forbidden_leakage_fields:
        if forbidden in final_cols:
            print(f"❌ Target leakage field detected in ML mart schema: {forbidden}")
            sys.exit(1)

    row_count = spark.table(gold_table).where(f"target_year = {args.year} AND target_month = {args.month}").count()
    print(f"🟢 Gold citibike_station_hourly_demand rebuilt for {args.year}-{args.month:02}. Rows: {row_count}. Leakage check passed.")

if __name__ == "__main__":
    main()
