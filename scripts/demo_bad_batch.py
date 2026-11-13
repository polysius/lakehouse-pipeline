"""Plant a bad batch in a sandbox schema and show the quality gate quarantining it.

    python scripts/demo_bad_batch.py setup      # sandbox schema, volume, three good months
    python scripts/demo_bad_batch.py plant      # replace April with a corrupted, truncated file
    python scripts/demo_bad_batch.py report     # what the gate decided and what was held back

After `plant`, run the pipeline for April against the sandbox:
    SILVER_SCHEMA=workspace.lakehouse_demo \\
    python -m lakehouse.silver.job --start 2024-04 --end 2024-04 \\
        --bronze-root /Volumes/workspace/lakehouse_demo/bronze --schema workspace.lakehouse_demo

The sandbox keeps the demo away from the real silver tables.
"""

import sys

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from lakehouse.ingest import tlc
from lakehouse.silver.job import run_month

REAL_BRONZE = "/Volumes/workspace/lakehouse/bronze"
DEMO_SCHEMA = "workspace.lakehouse_demo"
DEMO_BRONZE = "/Volumes/workspace/lakehouse_demo/bronze"
GOOD_MONTHS = ("2024-01", "2024-02", "2024-03")
PLANTED_MONTH = "2024-04"
KEEP_FRACTION = 0.05  # a truncated delivery: only 5% of the rows arrive


def bronze_path(root: str, month: str) -> str:
    return f"{root}/{tlc.bronze_key('yellow', tlc.parse_month(month))}"


def copy_file(spark, month: str) -> None:
    """Copy a real month into the sandbox volume."""
    spark.read.parquet(bronze_path(REAL_BRONZE, month)).coalesce(1).write.mode("overwrite").parquet(
        bronze_path(DEMO_BRONZE, month)
    )


def setup(spark) -> None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.lakehouse_demo")
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.lakehouse_demo.bronze")
    # The zone lookup is a single CSV file that the silver job joins to.
    spark.read.csv(f"{REAL_BRONZE}/reference/taxi_zone_lookup.csv", header=True).coalesce(
        1
    ).write.mode("overwrite").option("header", True).csv(f"{DEMO_BRONZE}/reference/taxi_zone_lookup.csv")
    for month in GOOD_MONTHS:
        copy_file(spark, month)
        run_month(spark, "yellow", tlc.parse_month(month), DEMO_BRONZE, DEMO_SCHEMA)
        print(f"{month} passed the gate")


def plant(spark) -> None:
    """Write a corrupted April: few rows, negative fares, stray dates, unknown payment codes."""
    real = spark.read.parquet(bronze_path(REAL_BRONZE, PLANTED_MONTH)).sample(KEEP_FRACTION, seed=7)
    roll = F.rand(seed=11)
    corrupted = (
        real.withColumn(
            "fare_amount",
            F.when(roll < 0.20, -F.abs(F.col("fare_amount"))).otherwise(F.col("fare_amount")),
        )
        .withColumn(
            "tpep_pickup_datetime",
            F.when(roll.between(0.20, 0.30), F.lit("2002-12-31 01:00:00").cast("timestamp_ntz"))
            .otherwise(F.col("tpep_pickup_datetime")),
        )
        .withColumn(
            "payment_type",
            F.when(roll.between(0.30, 0.45), F.lit(99).cast("long")).otherwise(F.col("payment_type")),
        )
    )
    corrupted.coalesce(1).write.mode("overwrite").parquet(bronze_path(DEMO_BRONZE, PLANTED_MONTH))
    print(f"planted {PLANTED_MONTH}: about {KEEP_FRACTION:.0%} of the rows, with corrupted values")


def report(spark) -> None:
    for name in ("quality_runs", "quarantine_trips", "silver_trips", "silver_run_log"):
        table = f"{DEMO_SCHEMA}.{name}"
        print(f"\n{table}")
        if name == "quarantine_trips":
            spark.table(table).groupBy("source_month", "quarantine_reason").count().show(truncate=False)
        elif name == "silver_trips":
            spark.table(table).groupBy("source_month").count().orderBy("source_month").show()
        else:
            spark.table(table).orderBy("source_month").show(truncate=False)


if __name__ == "__main__":
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    {"setup": setup, "plant": plant, "report": report}[sys.argv[1]](spark)
