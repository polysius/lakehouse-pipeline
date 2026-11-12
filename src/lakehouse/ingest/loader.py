"""Idempotent load of one month of trip records into bronze."""

import hashlib
import json
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from lakehouse.ingest import tlc
from lakehouse.ingest.download import Downloader
from lakehouse.ingest.storage import Storage


@dataclass(frozen=True)
class LoadRecord:
    """What one load wrote. Stored next to the data as the audit trail."""

    dataset: str
    month: str
    source_url: str
    bronze_key: str
    sha256: str
    size_bytes: int
    row_count: int
    loaded_at: str


@dataclass(frozen=True)
class LoadResult:
    status: str  # "loaded" or "skipped"
    record: LoadRecord


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_month(dataset: str, month: date, storage: Storage, downloader: Downloader) -> LoadResult:
    """Download one month into bronze unless a complete load already exists.

    A load is complete when both the data file and its manifest exist. The manifest is
    written last, so a crash between the two steps leaves the month to be redone.
    The data key depends only on dataset and month, so a redo overwrites rather than duplicates.
    """
    data_key = tlc.bronze_key(dataset, month)
    manifest_key = tlc.manifest_key(dataset, month)

    if storage.exists(data_key) and storage.exists(manifest_key):
        record = LoadRecord(**json.loads(storage.read_text(manifest_key)))
        return LoadResult("skipped", record)

    url = tlc.source_url(dataset, month)
    with tempfile.TemporaryDirectory() as scratch:
        local_file = Path(scratch) / tlc.file_name(dataset, month)
        downloader.fetch(url, local_file)
        record = LoadRecord(
            dataset=dataset,
            month=f"{month:%Y-%m}",
            source_url=url,
            bronze_key=data_key,
            sha256=sha256_of(local_file),
            size_bytes=local_file.stat().st_size,
            row_count=pq.ParquetFile(local_file).metadata.num_rows,
            loaded_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        storage.upload(local_file, data_key)

    storage.write_text(manifest_key, json.dumps(asdict(record), indent=2))
    return LoadResult("loaded", record)
