from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import optuna
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

sys.path.insert(0, "/opt/airflow")
from jobs.ml_utils import load_config, create_spark_session, get_models_dir, write_json

optuna.logging.set_verbosity(optuna.logging.WARNING)

FEATURE_COLUMNS = [
    "hour_of_day", "day_of_week", "is_weekend", "is_rush_hour",
    "hour_sin", "hour_cos",
    "lag_1h", "lag_2h", "lag_24h", "lag_168h",
    "rolling_mean_3h", "rolling_mean_24h", "rolling_std_24h",
    "trend_velocity_1h", "rolling_delta_24h",
]

COST_MATRIX = np.array([
    [0.0,  2.0,  4.0],
    [8.0,  0.0,  2.0],
    [16.0, 8.0,  0.0],
])


def class_from_count(count: float, q25: float, q75: float) -> int:
    if count < q25:
        return 0
    if count <= q75:
        return 1
    return 2


def mean_penalty(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(COST_MATRIX[y_true.astype(int), y_pred.astype(int)].mean())


def build_pipeline(params: dict) -> Pipeline:
    clf = LGBMClassifier(random_state=42, verbosity=-1, **params)
    return Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("model", clf),
    ])


def tune_with_optuna(X_train, y_train, X_val, y_val, n_trials: int) -> dict:
    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 300),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 8, 64),
        }
        model = build_pipeline(params)
        model.fit(X_train, y_train)
        preds = model.predict(X_val)
        return mean_penalty(y_val.values, preds)

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    return study.best_params


def main():
    cfg = load_config("/opt/airflow/config/local.yml")
    spark = create_spark_session(cfg, app_name="CitiBikeModelTraining")

    catalog = cfg["catalog"]["spark_catalog_name"]
    features_table = f"{catalog}.gold.citibike_ml_features"

    df = spark.table(features_table)

    print("Identifying highest-volume station for this training pass...")
    top_station_row = (
        df.groupBy("station_id")
        .sum("trip_count")
        .orderBy("sum(trip_count)", ascending=False)
        .first()
    )
    station_id = top_station_row["station_id"]
    print(f"Training on station_id={station_id} (highest total volume in the dataset)")

    pdf = (
        df.filter(df.station_id == station_id)
        .select(["hour_ts", "trip_count"] + FEATURE_COLUMNS)
        .toPandas()
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )

    n = len(pdf)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)
    train_df = pdf.iloc[:train_end].copy()
    val_df = pdf.iloc[train_end:val_end].copy()
    test_df = pdf.iloc[val_end:].copy()
    print(f"Train/Val/Test sizes: {len(train_df)}/{len(val_df)}/{len(test_df)}")

    q25 = float(train_df["trip_count"].quantile(0.25))
    q75 = float(train_df["trip_count"].quantile(0.75))
    print(f"Demand-tier thresholds learned from TRAIN only: q25={q25}, q75={q75}")

    for d in (train_df, val_df, test_df):
        d["demand_class"] = d["trip_count"].apply(lambda x: class_from_count(x, q25, q75))
        d["lag_1h_class"] = d["lag_1h"].apply(lambda x: class_from_count(x, q25, q75))

    X_train, y_train = train_df[FEATURE_COLUMNS], train_df["demand_class"]
    X_val, y_val = val_df[FEATURE_COLUMNS], val_df["demand_class"]
    X_test, y_test = test_df[FEATURE_COLUMNS], test_df["demand_class"]

    naive_test_preds = test_df["lag_1h_class"].values
    naive_test_penalty = mean_penalty(y_test.values, naive_test_preds)
    print(f"\n📊 Naive baseline test penalty (avg $ per hour): {naive_test_penalty:.4f}")

    print("\nRunning Optuna hyperparameter search (minimizing operational penalty)...")
    best_params = tune_with_optuna(X_train, y_train, X_val, y_val, n_trials=20)
    print(f"Best params found: {best_params}")

    X_train_final = pd.concat([X_train, X_val], ignore_index=True)
    y_train_final = pd.concat([y_train, y_val], ignore_index=True)

    final_model = build_pipeline(best_params)
    final_model.fit(X_train_final, y_train_final)

    model_test_preds = final_model.predict(X_test)
    model_test_penalty = mean_penalty(y_test.values, model_test_preds)
    print(f"📊 Cost-sensitive model test penalty (avg $ per hour): {model_test_penalty:.4f}")

    pct_improvement = ((naive_test_penalty - model_test_penalty) / naive_test_penalty) * 100 if naive_test_penalty > 0 else 0.0
    print(f"\n🏆 Operational cost reduction vs naive baseline: {pct_improvement:.1f}%")

    models_dir = get_models_dir(cfg)
    models_dir.mkdir(parents=True, exist_ok=True)

    model_path = models_dir / "citibike_champion_model.pkl"
    joblib.dump(final_model, model_path)

    metadata = {
        "station_id": station_id,
        "model_name": "LightGBM",
        "model_version": "citibike-v1-cost-sensitive",
        "penalty_version": "asymmetric-v1-8-2",
        "best_params": best_params,
        "q25": q25,
        "q75": q75,
        "features": FEATURE_COLUMNS,
        "naive_test_penalty": naive_test_penalty,
        "model_test_penalty": model_test_penalty,
        "pct_cost_reduction_vs_naive": pct_improvement,
        "train_rows": len(train_df),
        "val_rows": len(val_df),
        "test_rows": len(test_df),
    }
    write_json(models_dir / "citibike_champion_metadata.json", metadata)

    print(f"\n🟢 Model + metadata exported to {models_dir}")


if __name__ == "__main__":
    main()
