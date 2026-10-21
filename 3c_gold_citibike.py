import sys
import subprocess
from datetime import datetime
from airflow.sdk import dag, task, Asset
from airflow.sdk.exceptions import AirflowFailException

SILVER_CITIBIKE_DATA = Asset(
    name="warehouse/silver/citibike_trips",
    uri="iceberg://silver.citibike_trips",
)

@dag(
    dag_id="3c_gold_citibike",
    start_date=datetime(2026, 4, 1),
    schedule=[SILVER_CITIBIKE_DATA],
    catchup=False,
    max_active_runs=1,
)
def gold_citibike_dag():

    @task(task_id="run_citibike_gold_aggregation")
    def run_citibike_gold_aggregation(**context):
        events = context.get("triggering_asset_events", {}).get(SILVER_CITIBIKE_DATA, [])
        if not events:
            raise AirflowFailException("Missing upstream Silver citibike asset event metadata.")

        lineage = events[0].extra

        cmd = [
            sys.executable, "/opt/airflow/jobs/gold_citibike.py",
            "--config", "/opt/airflow/config/local.yml",
            "--station_list", "/opt/airflow/config/top50_station_ids.txt",
            "--year", str(lineage["source_year"]),
            "--month", str(lineage["source_month"]),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout)

        if result.returncode != 0:
            raise AirflowFailException(f"Citi Bike Gold aggregation failed:\n{result.stderr}")

    run_citibike_gold_aggregation()

gold_citibike_dag()
