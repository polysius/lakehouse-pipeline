"""Export the small gold tables to CSV, so another project can load them into Postgres.

    python scripts/export_gold.py --out data/gold_export

Needs DATABRICKS_HOST, DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN, and `databricks-sql-connector`
(installed with dbt-databricks). The 39.7M row fact table is left out on purpose. The metric
tables and dimensions hold everything the text-to-SQL benchmark asks about.
"""

import argparse
import csv
import os
from pathlib import Path

from databricks import sql

TABLES = (
    "metric_daily_trips",
    "metric_borough_revenue",
    "metric_tip_rate",
    "metric_hourly_demand",
    "metric_top_zones",
    "dim_zone",
    "dim_date",
    "dim_payment_type",
    "dim_rate_code",
    "dim_vendor",
)


def export_table(cursor, table: str, out_dir: Path) -> int:
    cursor.execute(f"select * from workspace.gold.{table}")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    with open(out_dir / f"{table}.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/gold_export")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    host = os.environ["DATABRICKS_HOST"].strip().removeprefix("https://").rstrip("/")
    with (
        sql.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"].strip(),
            access_token=os.environ["DATABRICKS_TOKEN"].strip(),
        ) as connection,
        connection.cursor() as cursor,
    ):
        for table in TABLES:
            print(f"{table}: {export_table(cursor, table, out_dir):,} rows")


if __name__ == "__main__":
    main()
