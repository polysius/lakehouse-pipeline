"""Write a Tableau workbook with one worksheet per gold metric table.

    python scripts/make_tableau.py                          # tableau/lakehouse.twb, placeholder host
    python scripts/make_tableau.py --local                  # tableau/local/lakehouse.twb, real host

The committed workbook carries a placeholder host so no workspace address is published. With
--local the host and warehouse path come from DATABRICKS_HOST and DATABRICKS_HTTP_PATH, and the
output goes to a gitignored folder. Tableau asks for the access token when it opens the file.

This file was written without Tableau available, so it has not been opened in Tableau.
"""

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import quoteattr

PLACEHOLDER_HOST = "YOUR-WORKSPACE.cloud.databricks.com"
PLACEHOLDER_HTTP_PATH = "/sql/1.0/warehouses/YOUR-WAREHOUSE-ID"
CATALOG, SCHEMA = "workspace", "gold"


@dataclass(frozen=True)
class Sheet:
    """One worksheet. `columns` goes on the columns shelf and `rows` on the rows shelf."""

    table: str
    title: str
    columns: str
    rows: str
    mark: str
    fields: tuple[tuple[str, str], ...]  # (column name, Tableau datatype)


SHEETS = (
    Sheet(
        "metric_daily_trips",
        "Daily trips and revenue",
        "pickup_date",
        "revenue",
        "line",
        (("pickup_date", "date"), ("trips", "integer"), ("revenue", "real")),
    ),
    Sheet(
        "metric_borough_revenue",
        "Revenue by borough",
        "month",
        "revenue",
        "bar",
        (("month", "date"), ("borough", "string"), ("revenue", "real")),
    ),
    Sheet(
        "metric_tip_rate",
        "Tip rate by payment type",
        "payment_type_name",
        "avg_tip_rate",
        "bar",
        (("payment_type_name", "string"), ("avg_tip_rate", "real")),
    ),
    Sheet(
        "metric_hourly_demand",
        "Demand by weekday and hour",
        "pickup_hour",
        "day_name",
        "square",
        (("pickup_hour", "integer"), ("day_name", "string"), ("trips", "integer")),
    ),
    Sheet(
        "metric_top_zones",
        "Top 15 pickup zones",
        "revenue",
        "zone",
        "bar",
        (("zone", "string"), ("revenue", "real")),
    ),
)


def datasource_xml(sheet: Sheet, host: str, http_path: str) -> str:
    connection = (
        f"<connection class='databricks' server={quoteattr(host)} port='443' "
        f"http-path={quoteattr(http_path)} dbname={quoteattr(CATALOG)} "
        f"schema={quoteattr(SCHEMA)} authentication='auth-pass' "
        "sslmode='require' username='token' />"
    )
    columns = "".join(
        f"<column datatype='{kind}' name='[{name}]' role="
        f"'{'measure' if kind in ('integer', 'real') else 'dimension'}' "
        f"type='{'quantitative' if kind in ('integer', 'real') else 'nominal'}' />"
        for name, kind in sheet.fields
    )
    return (
        f"<datasource caption={quoteattr(sheet.table)} inline='true' name='{sheet.table}'>"
        f"<connection class='federated'><named-connections>"
        f"<named-connection caption='Databricks' name='databricks.{sheet.table}'>{connection}"
        f"</named-connection></named-connections>"
        f"<relation connection='databricks.{sheet.table}' name={quoteattr(sheet.table)} "
        f"table='[{SCHEMA}].[{sheet.table}]' type='table' /></connection>{columns}</datasource>"
    )


def worksheet_xml(sheet: Sheet) -> str:
    source = f"<datasource name='{sheet.table}' />"
    dependencies = (
        f"<datasource-dependencies datasource='{sheet.table}'>"
        + "".join(
            f"<column datatype='{kind}' name='[{name}]' role="
            f"'{'measure' if kind in ('integer', 'real') else 'dimension'}' />"
            for name, kind in sheet.fields
        )
        + "</datasource-dependencies>"
    )
    return (
        f"<worksheet name={quoteattr(sheet.title)}><table>"
        f"<view><datasources>{source}</datasources>{dependencies}</view>"
        f"<style /><panes><pane selection-relaxation-option='selection-relaxation-allow'>"
        f"<view>{source}</view><mark class='{sheet.mark.title()}' /></pane></panes>"
        f"<rows>[{sheet.table}].[{sheet.rows}]</rows>"
        f"<cols>[{sheet.table}].[{sheet.columns}]</cols></table></worksheet>"
    )


def workbook_xml(host: str, http_path: str) -> str:
    sources = "".join(datasource_xml(sheet, host, http_path) for sheet in SHEETS)
    sheets = "".join(worksheet_xml(sheet) for sheet in SHEETS)
    return (
        "<?xml version='1.0' encoding='utf-8' ?>\n"
        "<workbook source-build='2024.1.0 (20241.24.0312.1013)' source-platform='win' "
        "version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>"
        f"<datasources>{sources}</datasources><worksheets>{sheets}</worksheets></workbook>\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="use the host from the environment")
    args = parser.parse_args()

    if args.local:
        host = os.environ["DATABRICKS_HOST"].strip().removeprefix("https://").rstrip("/")
        http_path = os.environ["DATABRICKS_HTTP_PATH"].strip()
        output = Path("tableau/local/lakehouse.twb")
    else:
        host, http_path, output = (
            PLACEHOLDER_HOST,
            PLACEHOLDER_HTTP_PATH,
            Path("tableau/lakehouse.twb"),
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(workbook_xml(host, http_path), encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
