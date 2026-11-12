"""Airflow DAG: one run per month, each run loads that month into bronze.

Backfill a range with:
    airflow dags backfill tlc_bronze_ingest -s 2024-01-01 -e 2024-12-31
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

DATASET = os.environ.get("TLC_DATASET", "yellow")
VOLUME_ROOT = os.environ.get("BRONZE_VOLUME_ROOT", "/Volumes/main/lakehouse/bronze")


@dag(
    dag_id="tlc_bronze_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=True,
    # One month at a time keeps the backfill gentle on the TLC bucket.
    max_active_runs=1,
    default_args={
        # The TLC publishes about two months late, so a run may need to wait for its file.
        "retries": 5,
        "retry_delay": timedelta(hours=6),
    },
    tags=["bronze", "tlc"],
)
def tlc_bronze_ingest():
    @task
    def load(data_interval_start=None) -> dict:
        from dataclasses import asdict

        from lakehouse.ingest.download import HttpDownloader
        from lakehouse.ingest.loader import load_month
        from lakehouse.ingest.storage import DatabricksVolumeStorage

        month = data_interval_start.date().replace(day=1)
        result = load_month(DATASET, month, DatabricksVolumeStorage(VOLUME_ROOT), HttpDownloader())
        print(f"{month:%Y-%m} {result.status}, rows={result.record.row_count:,}")
        return asdict(result.record)

    load()


tlc_bronze_ingest()
