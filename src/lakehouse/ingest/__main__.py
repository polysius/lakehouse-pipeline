"""Command line backfill: python -m lakehouse.ingest --start 2024-01 --end 2024-12."""

import argparse
from pathlib import Path

from lakehouse.ingest import tlc
from lakehouse.ingest.download import HttpDownloader, SourceNotPublishedError
from lakehouse.ingest.loader import load_month
from lakehouse.ingest.storage import DatabricksVolumeStorage, LocalStorage


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="yellow", choices=tlc.DATASETS)
    parser.add_argument("--start", required=True, help="First month, e.g. 2024-01")
    parser.add_argument("--end", required=True, help="Last month, e.g. 2024-12")
    parser.add_argument("--storage", choices=["local", "databricks"], default="local")
    parser.add_argument("--root", default="data", help="Local directory or volume path")
    args = parser.parse_args()

    if args.storage == "local":
        storage = LocalStorage(Path(args.root))
    else:
        storage = DatabricksVolumeStorage(args.root)
    downloader = HttpDownloader()

    months = tlc.months_between(tlc.parse_month(args.start), tlc.parse_month(args.end))
    for month in months:
        try:
            result = load_month(args.dataset, month, storage, downloader)
        except SourceNotPublishedError as error:
            print(f"{month:%Y-%m} not published yet: {error}")
            continue
        print(f"{month:%Y-%m} {result.status:7} rows={result.record.row_count:>10,}")


if __name__ == "__main__":
    main()
