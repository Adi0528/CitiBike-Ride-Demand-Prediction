import sys
import logging
import subprocess
from datetime import datetime
from airflow.sdk import dag, task, Asset
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.exceptions import AirflowSkipException

BRONZE_CITIBIKE_DATA = Asset(
    name="warehouse/bronze/citibike_trips_raw",
    uri="iceberg://bronze.citibike_trips_raw",
)
SILVER_CITIBIKE_DATA = Asset(
    name="warehouse/silver/citibike_trips",
    uri="iceberg://silver.citibike_trips",
)

@dag(
    dag_id="2b_silver_wap_purification_citibike",
    start_date=datetime(2026, 4, 1),
    schedule=[BRONZE_CITIBIKE_DATA],
    catchup=False,
    max_active_runs=1,
)
def silver_wap_purification_citibike_dag():

    @task(task_id="invoke_silver_wap_job_citibike", outlets=[SILVER_CITIBIKE_DATA])
    def invoke_silver_wap_job_citibike(**context):
        log = logging.getLogger("airflow.task")

        events = context.get("triggering_asset_events", {}).get(BRONZE_CITIBIKE_DATA, [])
        if not events:
            raise AirflowFailException("Missing upstream Bronze citibike asset event metadata.")

        lineage = dict(events[0].extra)

        cmd = [
            sys.executable, "/opt/airflow/jobs/silver_wap_citibike.py",
            "--config", "/opt/airflow/config/local.yml",
            "--source_filename", str(lineage["source_filename"]),
            "--ingestion_run_id", str(lineage["ingestion_run_id"]),
            "--target_year", str(lineage["source_year"]),
            "--target_month", str(lineage["source_month"]),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        log.info("Silver Spark job stdout:\n%s", result.stdout)

        if result.returncode != 0:
            log.error("Silver Spark job stderr:\n%s", result.stderr)
            raise AirflowFailException(f"Silver WAP-style job failed with exit code {result.returncode}")

        if "Idempotency Skip" in result.stdout:
            log.info("🛑 Spark job detected a duplicate drop. Short-circuiting Airflow context to prevent downstream triggers.")
            raise AirflowSkipException("Skipping downstream updates because this data file has already been processed.")

        context["outlet_events"][SILVER_CITIBIKE_DATA].extra = lineage

    invoke_silver_wap_job_citibike()

silver_wap_purification_citibike_dag()
