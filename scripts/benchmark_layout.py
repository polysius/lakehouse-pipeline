"""Compare table layouts on a fixed set of queries.

    python scripts/benchmark_layout.py --build     # create the layout variants once
    python scripts/benchmark_layout.py --repeats 5 # time the queries, print markdown

Variants, all copies of silver_trips except the last:
    flat          no partitioning, no optimisation
    partitioned   partitioned by pickup_date
    liquid        liquid clustering on (pickup_date, pu_location_id)
    zorder        silver_trips itself: partitioned by pickup_date, OPTIMIZE ZORDER BY pu_location_id

Timing notes: variants run interleaved so cache warm-up affects them evenly. Each query carries
a predicate that is always true but changes every repeat, so a repeat cannot be answered from
the result cache. Queries aggregate and collect rows, because count(*) on Delta can be answered
from the log without reading data.
"""

import argparse
import statistics
import time

from databricks.connect import DatabricksSession

SCHEMA = "workspace.lakehouse"
SOURCE = f"{SCHEMA}.silver_trips"
VARIANTS = {
    "flat": f"{SCHEMA}.bench_flat",
    "partitioned": f"{SCHEMA}.bench_partitioned",
    "liquid": f"{SCHEMA}.bench_liquid",
    "zorder": SOURCE,
}

# {table} and {noise} are filled in per run. fare_amount is never negative in silver,
# so "fare_amount > -N" keeps every row and skips no files.
QUERIES = {
    "one day": (
        "SELECT sum(total_amount), avg(tip_amount) FROM {table} "
        "WHERE pickup_date = '2024-06-15' AND fare_amount > -{noise}"
    ),
    "one week by zone": (
        "SELECT pu_zone, sum(total_amount) AS revenue FROM {table} "
        "WHERE pickup_date BETWEEN '2024-03-04' AND '2024-03-10' AND fare_amount > -{noise} "
        "GROUP BY pu_zone ORDER BY revenue DESC LIMIT 10"
    ),
    "one zone, one month": (
        "SELECT sum(total_amount), avg(trip_distance) FROM {table} "
        "WHERE pu_location_id = 132 AND pickup_date BETWEEN '2024-07-01' AND '2024-07-31' "
        "AND fare_amount > -{noise}"
    ),
    "one zone, full year": (
        "SELECT sum(total_amount), avg(trip_distance) FROM {table} "
        "WHERE pu_location_id = 132 AND fare_amount > -{noise}"
    ),
    "full year (control)": (
        "SELECT month(pickup_date) AS m, avg(fare_amount) FROM {table} "
        "WHERE fare_amount > -{noise} GROUP BY month(pickup_date) ORDER BY m"
    ),
}


def build_variants(spark) -> None:
    for name in ("flat", "partitioned", "liquid"):
        spark.sql(f"DROP TABLE IF EXISTS {VARIANTS[name]}")
    spark.sql(f"CREATE TABLE {VARIANTS['flat']} AS SELECT * FROM {SOURCE}")
    spark.sql(
        f"CREATE TABLE {VARIANTS['partitioned']} PARTITIONED BY (pickup_date) "
        f"AS SELECT * FROM {SOURCE}"
    )
    spark.sql(
        f"CREATE TABLE {VARIANTS['liquid']} CLUSTER BY (pickup_date, pu_location_id) "
        f"AS SELECT * FROM {SOURCE}"
    )
    spark.sql(f"OPTIMIZE {VARIANTS['liquid']}")
    spark.sql(f"OPTIMIZE {SOURCE} ZORDER BY (pu_location_id)")


def table_layout(spark, table: str) -> tuple[int, float]:
    detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
    return detail["numFiles"], detail["sizeInBytes"] / 1e6


def time_query(spark, template: str, table: str, noise: int) -> float:
    start = time.perf_counter()
    spark.sql(template.format(table=table, noise=noise)).collect()
    return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()

    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    if args.build:
        build_variants(spark)

    print("| Variant | Files | Size (MB) |\n|---|--:|--:|")
    for name, table in VARIANTS.items():
        files, size = table_layout(spark, table)
        print(f"| {name} | {files:,} | {size:,.0f} |")

    print("\n| Query | " + " | ".join(f"{v} first / median (s)" for v in VARIANTS) + " |")
    print("|---|" + "--:|" * len(VARIANTS))
    for query_name, template in QUERIES.items():
        timings = {variant: [] for variant in VARIANTS}
        for repeat in range(args.repeats):
            for variant, table in VARIANTS.items():  # interleaved on purpose
                # A new noise value per repeat means the same table never sees the same SQL twice.
                timings[variant].append(time_query(spark, template, table, noise=1 + repeat))
        cells = [f"{t[0]:.1f} / {statistics.median(t):.1f}" for t in timings.values()]
        print(f"| {query_name} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
