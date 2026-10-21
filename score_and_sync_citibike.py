from __future__ import annotations
import os

import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import psycopg

sys.path.insert(0, "/opt/airflow")
from jobs.ml_utils import load_config, create_spark_session, get_models_dir, read_json

LABEL_MAP = {0: "Low", 1: "Normal", 2: "High"}

COST_MATRIX = np.array([
    [0.0,  2.0,  4.0],
    [8.0,  0.0,  2.0],
    [16.0, 8.0,  0.0],
])

FEATURE_COLUMNS = [
    "hour_of_day", "day_of_week", "is_weekend", "is_rush_hour",
    "hour_sin", "hour_cos",
    "lag_1h", "lag_2h", "lag_24h", "lag_168h",
    "rolling_mean_3h", "rolling_mean_24h", "rolling_std_24h",
    "trend_velocity_1h", "rolling_delta_24h",
]

CONN_STRING = f"host=operational_cache_postgres dbname=operational_cache user=cache_user password={os.environ['POSTGRES_CACHE_PASSWORD']} port=5432"

RECENT_HOURS_TO_SCORE = 72


def class_from_count(count: float, q25: float, q75: float) -> int:
    if count < q25:
        return 0
    if count <= q75:
        return 1
    return 2


def penalty(actual_class: int, predicted_class: int) -> float:
    return float(COST_MATRIX[int(actual_class), int(predicted_class)])


def main():
    cfg = load_config("/opt/airflow/config/local.yml")
    models_dir = get_models_dir(cfg)

    metadata = read_json(models_dir / "citibike_champion_metadata.json")
    model = joblib.load(models_dir / "citibike_champion_model.pkl")

    station_id = metadata["station_id"]
    q25 = metadata["q25"]
    q75 = metadata["q75"]
    model_name = metadata["model_name"]
    model_version = metadata["model_version"]

    print(f"Loaded champion model for station_id={station_id} (q25={q25}, q75={q75})")

    spark = create_spark_session(cfg, app_name="CitiBikeScoreAndSync")
    catalog = cfg["catalog"]["spark_catalog_name"]
    features_table = f"{catalog}.gold.citibike_ml_features"

    pdf = (
        spark.table(features_table)
        .filter(f"station_id = '{station_id}'")
        .select(["hour_ts", "trip_count"] + FEATURE_COLUMNS)
        .toPandas()
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )

    recent = pdf.tail(RECENT_HOURS_TO_SCORE).copy()
    print(f"Scoring the most recent {len(recent)} hours for station_id={station_id}")

    X = recent[FEATURE_COLUMNS]
    recent["predicted_class"] = model.predict(X)
    recent["actual_class"] = recent["trip_count"].apply(lambda x: class_from_count(x, q25, q75))
    recent["baseline_predicted_class"] = recent["lag_1h"].apply(lambda x: class_from_count(x, q25, q75))

    recent["model_penalty"] = recent.apply(
        lambda r: penalty(r["actual_class"], r["predicted_class"]), axis=1
    )
    recent["baseline_penalty"] = recent.apply(
        lambda r: penalty(r["actual_class"], r["baseline_predicted_class"]), axis=1
    )

    scored_at = datetime.now(timezone.utc)

    rows = []
    for _, r in recent.iterrows():
        rows.append((
            str(station_id),
            r["hour_ts"].to_pydatetime(),
            scored_at,
            model_name,
            model_version,
            float(r["trip_count"]),
            int(r["actual_class"]),
            LABEL_MAP[int(r["actual_class"])],
            int(r["predicted_class"]),
            LABEL_MAP[int(r["predicted_class"])],
            int(r["baseline_predicted_class"]),
            LABEL_MAP[int(r["baseline_predicted_class"])],
            float(r["model_penalty"]),
            float(r["baseline_penalty"]),
        ))

    print(f"Connecting to Postgres operational cache to upsert {len(rows)} row(s)...")
    conn = psycopg.connect(CONN_STRING)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS citibike_hourly_demand_predictions (
                    station_id VARCHAR,
                    hour_ts TIMESTAMPTZ,
                    scored_at_ts TIMESTAMPTZ,
                    model_name VARCHAR,
                    model_version VARCHAR,
                    actual_trip_count DOUBLE PRECISION,
                    actual_class BIGINT,
                    actual_label VARCHAR,
                    predicted_class BIGINT,
                    predicted_label VARCHAR,
                    baseline_predicted_class BIGINT,
                    baseline_predicted_label VARCHAR,
                    model_penalty DOUBLE PRECISION,
                    baseline_penalty DOUBLE PRECISION,
                    PRIMARY KEY (station_id, hour_ts)
                );
            """)

            upsert_sql = """
                INSERT INTO citibike_hourly_demand_predictions (
                    station_id, hour_ts, scored_at_ts, model_name, model_version,
                    actual_trip_count, actual_class, actual_label,
                    predicted_class, predicted_label,
                    baseline_predicted_class, baseline_predicted_label,
                    model_penalty, baseline_penalty
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (station_id, hour_ts) DO UPDATE SET
                    scored_at_ts = EXCLUDED.scored_at_ts,
                    model_name = EXCLUDED.model_name,
                    model_version = EXCLUDED.model_version,
                    actual_trip_count = EXCLUDED.actual_trip_count,
                    actual_class = EXCLUDED.actual_class,
                    actual_label = EXCLUDED.actual_label,
                    predicted_class = EXCLUDED.predicted_class,
                    predicted_label = EXCLUDED.predicted_label,
                    baseline_predicted_class = EXCLUDED.baseline_predicted_class,
                    baseline_predicted_label = EXCLUDED.baseline_predicted_label,
                    model_penalty = EXCLUDED.model_penalty,
                    baseline_penalty = EXCLUDED.baseline_penalty;
            """
            cur.executemany(upsert_sql, rows)
        conn.commit()
        print(f"🟢 Synced {len(rows)} prediction rows to Postgres operational cache.")
    except Exception as e:
        conn.rollback()
        print(f"❌ DATABASE ERROR: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
