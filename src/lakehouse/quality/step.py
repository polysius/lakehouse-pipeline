"""Run the quality gate inside the silver job: pull the batch, record the verdict, hold failures."""

from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from lakehouse.quality import gate
from lakehouse.quality.thresholds import HISTORY_MONTHS

QUALITY_RUNS_SCHEMA = StructType(
    [
        StructField("source_month", StringType()),
        StructField("passed", BooleanType()),
        StructField("row_count", LongType()),
        StructField("failed_checks", StringType()),
        StructField("checked_at", StringType()),
    ]
)


def _recent_passed_row_counts(spark: SparkSession, schema: str, month: date) -> list[int]:
    """Row counts of the most recent months before this one that passed the gate."""
    table = f"{schema}.quality_runs"
    if not spark.catalog.tableExists(table):
        return []
    rows = (
        spark.table(table)
        .where(F.col("passed") & (F.col("source_month") < f"{month:%Y-%m}"))
        .orderBy(F.col("source_month").desc())
        .limit(HISTORY_MONTHS)
        .collect()
    )
    return [row["row_count"] for row in rows]


def _record_run(spark: SparkSession, schema: str, result: gate.GateResult, overwrite_where):
    failed = "; ".join(f"{check.name}: {check.detail}" for check in result.failed_checks)
    row = {
        "source_month": result.source_month,
        "passed": result.passed,
        "row_count": result.row_count,
        "failed_checks": failed,
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    log = spark.createDataFrame([row], QUALITY_RUNS_SCHEMA)
    overwrite_where(log, f"{schema}.quality_runs", f"source_month = '{result.source_month}'")


def check_batch(
    spark: SparkSession, schema: str, month: date, flagged: DataFrame, overwrite_where
) -> gate.GateResult:
    """Gate one month. A failed batch is held in quarantine_trips and raises QualityGateFailed.

    A batch that passes also clears any rows an earlier failed run left in quarantine.
    """
    frame = flagged.select(*gate.GATE_COLUMNS).toPandas()
    history = _recent_passed_row_counts(spark, schema, month)
    result = gate.evaluate_batch(frame, month, history)
    _record_run(spark, schema, result, overwrite_where)

    quarantine_table = f"{schema}.quarantine_trips"
    if result.passed:
        if spark.catalog.tableExists(quarantine_table):
            spark.sql(f"DELETE FROM {quarantine_table} WHERE source_month = '{result.source_month}'")
        return result

    reasons = "; ".join(check.name for check in result.failed_checks)
    held = flagged.withColumn("quarantine_reason", F.lit(reasons))
    overwrite_where(held, quarantine_table, f"source_month = '{result.source_month}'")
    raise gate.QualityGateFailed(result)
