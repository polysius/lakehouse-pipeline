"""Names and locations for NYC TLC trip record files."""

from datetime import date

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATASETS = ("yellow", "green", "fhv", "fhvhv")


def parse_month(text: str) -> date:
    """Turn '2024-03' into the first day of that month."""
    year, month = text.split("-")
    return date(int(year), int(month), 1)


def months_between(start: date, end: date) -> list[date]:
    """Every month from start to end, both included."""
    months = []
    current = start
    while current <= end:
        months.append(current)
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)
    return months


def file_name(dataset: str, month: date) -> str:
    if dataset not in DATASETS:
        raise ValueError(f"Unknown dataset {dataset!r}, expected one of {DATASETS}")
    return f"{dataset}_tripdata_{month:%Y-%m}.parquet"


def source_url(dataset: str, month: date) -> str:
    return f"{BASE_URL}/{file_name(dataset, month)}"


def bronze_key(dataset: str, month: date) -> str:
    """Storage path of the raw file. Partitioned by month so a rerun overwrites in place."""
    return f"tlc/{dataset}/year={month:%Y}/month={month:%m}/{file_name(dataset, month)}"


def manifest_key(dataset: str, month: date) -> str:
    """Storage path of the record describing one load."""
    return f"_manifests/tlc/{dataset}/{month:%Y-%m}.json"
