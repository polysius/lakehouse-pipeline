"""Airflow DAG: one run per month loads that month into bronze, then builds silver.

Backfill a range with:
    airflow dags backfill tlc_pipeline -s 2024-01-01 -e 2024-12-31

A batch that fails the quality gate fails the build_silver task without retries. The task log
holds the alert text, and the batch waits in the quarantine_trips table for review.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

DATASET = os.environ.get("TLC_DATASET", "yellow")
VOLUME_ROOT = os.environ.get("BRONZE_VOLUME_ROOT", "/Volumes/workspace/lakehouse/bronze")
SILVER_SCHEMA = os.environ.get("SILVER_SCHEMA", "workspace.lakehouse")

log = logging.getLogger(__name__)


def alert_on_failure(context: dict) -> None:
    """Runs when a task fails for good, after any retries. The log line is the alert."""
    task_instance = context["task_instance"]
    log.error(
        "ALERT: task %s in %s failed for run %s: %s",
        task_instance.task_id,
        task_instance.dag_id,
        context["run_id"],
        context.get("exception"),
    )


@dag(
    dag_id="tlc_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=True,
    # One month at a time keeps the backfill gentle on the TLC bucket and on Databricks.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": alert_on_failure,
    },
    tags=["bronze", "silver", "tlc"],
)
def tlc_pipeline():
    @task(
        # The TLC publishes about two months late. A run for an unpublished month retries for
        # about 30 hours, then fails. Clear the failed run once the file is published.
        retries=5,
        retry_delay=timedelta(hours=6),
    )
    def load_bronze(data_interval_start=None) -> dict:
        from dataclasses import asdict

        from lakehouse.ingest.download import HttpDownloader
        from lakehouse.ingest.loader import load_month
        from lakehouse.ingest.storage import DatabricksVolumeStorage

        month = data_interval_start.date().replace(day=1)
        result = load_month(DATASET, month, DatabricksVolumeStorage(VOLUME_ROOT), HttpDownloader())
        log.info("%s %s, rows=%s", f"{month:%Y-%m}", result.status, f"{result.record.row_count:,}")
        return asdict(result.record)

    @task
    def build_silver(data_interval_start=None) -> dict:
        from databricks.connect import DatabricksSession

        from lakehouse.quality.alert import format_alert
        from lakehouse.quality.gate import QualityGateFailed
        from lakehouse.silver.job import run_month

        month = data_interval_start.date().replace(day=1)
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
        try:
            return run_month(spark, DATASET, month, VOLUME_ROOT, SILVER_SCHEMA)
        except QualityGateFailed as failure:
            alert = format_alert(failure.result, f"{SILVER_SCHEMA}.quarantine_trips")
            # Retrying cannot fix bad data, so fail the task for good.
            raise AirflowFailException(alert) from failure

    load_bronze() >> build_silver()


tlc_pipeline()
