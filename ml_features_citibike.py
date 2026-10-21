import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def main():
    with open("/opt/airflow/config/local.yml", "r") as f:
        cfg = yaml.safe_load(f)

    catalog = cfg["catalog"]["spark_catalog_name"]

    spark = (SparkSession.builder
        .appName("MLFeatureEngineeringCitiBike")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", cfg["catalog"]["type"])
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg["catalog"]["warehouse_path"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate())

    gold_table = f"{catalog}.gold.citibike_station_hourly_demand"
    features_table = f"{catalog}.gold.citibike_ml_features"

    df = spark.table(gold_table)

    w = Window.partitionBy("station_id").orderBy("hour_ts")

    df_feat = (df
        .withColumn("hour_of_day", F.hour("hour_ts"))
        .withColumn("day_of_week", F.dayofweek("hour_ts"))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]).cast("int"))
        .withColumn("is_rush_hour", F.col("hour_of_day").isin([7, 8, 9, 17, 18, 19]).cast("int"))
        .withColumn("hour_sin", F.sin(F.col("hour_of_day") * (2 * 3.141592653589793 / 24)))
        .withColumn("hour_cos", F.cos(F.col("hour_of_day") * (2 * 3.141592653589793 / 24)))
        .withColumn("lag_1h", F.lag("trip_count", 1).over(w))
        .withColumn("lag_2h", F.lag("trip_count", 2).over(w))
        .withColumn("lag_24h", F.lag("trip_count", 24).over(w))
        .withColumn("lag_168h", F.lag("trip_count", 168).over(w))
        .withColumn("rolling_mean_3h", F.avg("trip_count").over(w.rowsBetween(-3, -1)))
        .withColumn("rolling_mean_24h", F.avg("trip_count").over(w.rowsBetween(-24, -1)))
        .withColumn("rolling_std_24h", F.stddev("trip_count").over(w.rowsBetween(-24, -1)))
        .withColumn("trend_velocity_1h", F.col("lag_1h") - F.col("lag_2h"))
        .withColumn("rolling_delta_24h", F.col("lag_1h") - F.col("rolling_mean_24h"))
    )

    df_final = df_feat.filter(F.col("lag_168h").isNotNull())

    df_final.writeTo(features_table).using("iceberg").createOrReplace()

    total_rows = df_final.count()
    print(f"🟢 ML feature table rebuilt: {features_table}")
    print(f"   Rows with full feature history: {total_rows}")
    print(f"   Columns: {df_final.columns}")

if __name__ == "__main__":
    main()
