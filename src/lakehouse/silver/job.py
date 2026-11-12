"""Build the silver tables from bronze, one month at a time.

Run from WSL with DATABRICKS_HOST and DATABRICKS_TOKEN set:
    python -m lakehouse.silver.job --start 2024-01 --end 2024-12

Each month is written with replaceWhere, so rerunning a month replaces its rows
instead of adding a second copy.
"""

import argparse
import time
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from lakehouse.ingest import tlc
from lakehouse.ingest.reference import ZONE_LOOKUP_KEY
from lakehouse.silver import transform

DEFAULT_BRONZE_ROOT = "/Volumes/workspace/lakehouse/bronze"
DEFAULT_SCHEMA = "workspace.lakehouse"

RUN_LOG_SCHEMA = StructType(
    [
        StructField("source_month", StringType()),
        StructField("bronze_rows", LongType()),
        StructField("rejected_rows", LongType()),
        StructField("duplicate_rows", LongType()),
        StructField("silver_rows", LongType()),
        StructField("seconds", IntegerType()),
    ]
)


def overwrite_where(df: DataFrame, table: str, predicate: str, partition_by: str | None = None):
    """Replace only the rows matching predicate. Creates the table on first use."""
    writer = df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(table)


def run_month(
    spark: SparkSession, dataset: str, month: date, bronze_root: str, schema: str
) -> dict:
    started = time.time()
    source_month = f"{month:%Y-%m}"
    silver_table = f"{schema}.silver_trips"
    rejected_table = f"{schema}.silver_trips_rejected"

    bronze = spark.read.parquet(f"{bronze_root}/{tlc.bronze_key(dataset, month)}")
    zones = transform.prepare_zones(
        spark.read.csv(f"{bronze_root}/{ZONE_LOOKUP_KEY}", header=True)
    )

    flagged = transform.flag_rejects(transform.standardise(bronze, source_month))
    good, rejected = transform.split_rejects(flagged)

    silver = transform.add_pickup_date(
        transform.add_zones(transform.add_labels(transform.deduplicate(good)), zones)
    )

    month_start, month_end = month.isoformat(), tlc.next_month(month).isoformat()
    overwrite_where(
        silver,
        silver_table,
        f"pickup_date >= '{month_start}' AND pickup_date < '{month_end}'",
        partition_by="pickup_date",
    )
    overwrite_where(rejected, rejected_table, f"source_month = '{source_month}'")

    # Count from the written tables, so the log reflects what is stored.
    silver_rows = (
        spark.table(silver_table)
        .where(f"pickup_date >= '{month_start}' AND pickup_date < '{month_end}'")
        .count()
    )
    rejected_rows = spark.table(rejected_table).where(f"source_month = '{source_month}'").count()
    bronze_rows = bronze.count()
    row = {
        "source_month": source_month,
        "bronze_rows": bronze_rows,
        "rejected_rows": rejected_rows,
        "duplicate_rows": bronze_rows - rejected_rows - silver_rows,
        "silver_rows": silver_rows,
        "seconds": int(time.time() - started),
    }
    log = spark.createDataFrame([row], RUN_LOG_SCHEMA)
    overwrite_where(log, f"{schema}.silver_run_log", f"source_month = '{source_month}'")
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="yellow", choices=tlc.DATASETS)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--bronze-root", default=DEFAULT_BRONZE_ROOT)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    args = parser.parse_args()

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    for month in tlc.months_between(tlc.parse_month(args.start), tlc.parse_month(args.end)):
        row = run_month(spark, args.dataset, month, args.bronze_root, args.schema)
        print(
            f"{row['source_month']} bronze={row['bronze_rows']:,} rejected={row['rejected_rows']:,} "
            f"duplicates={row['duplicate_rows']:,} silver={row['silver_rows']:,} "
            f"{row['seconds']}s"
        )


if __name__ == "__main__":
    main()
