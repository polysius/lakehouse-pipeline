from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse.ingest import tlc
from lakehouse.ingest.download import SourceNotPublishedError
from lakehouse.ingest.loader import load_month, sha256_of
from lakehouse.ingest.storage import LocalStorage


class FakeDownloader:
    """Writes a small parquet file per URL and counts how often it is asked."""

    def __init__(self, rows_per_file: int = 5, unpublished: tuple[str, ...] = ()):
        self.rows_per_file = rows_per_file
        self.unpublished = unpublished
        self.calls = 0

    def fetch(self, url: str, destination: Path) -> None:
        if any(marker in url for marker in self.unpublished):
            raise SourceNotPublishedError(url)
        self.calls += 1
        table = pa.table({"trip_id": list(range(self.rows_per_file))})
        pq.write_table(table, destination)


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "bronze")


def test_first_load_writes_data_and_manifest(storage):
    result = load_month("yellow", date(2024, 1, 1), storage, FakeDownloader(rows_per_file=7))

    assert result.status == "loaded"
    assert result.record.row_count == 7
    assert storage.exists(tlc.bronze_key("yellow", date(2024, 1, 1)))
    assert storage.exists(tlc.manifest_key("yellow", date(2024, 1, 1)))


def test_manifest_checksum_matches_stored_file(storage, tmp_path):
    result = load_month("yellow", date(2024, 1, 1), storage, FakeDownloader())

    stored_file = storage.root / result.record.bronze_key
    assert result.record.sha256 == sha256_of(stored_file)
    assert result.record.size_bytes == stored_file.stat().st_size


def test_rerun_skips_and_downloads_nothing(storage):
    downloader = FakeDownloader()
    load_month("yellow", date(2024, 1, 1), storage, downloader)

    second = load_month("yellow", date(2024, 1, 1), storage, downloader)

    assert second.status == "skipped"
    assert downloader.calls == 1


def test_backfill_twelve_months_twice_does_not_duplicate(storage):
    downloader = FakeDownloader()
    months = tlc.months_between(date(2024, 1, 1), date(2024, 12, 1))

    first = [load_month("yellow", month, storage, downloader).status for month in months]
    second = [load_month("yellow", month, storage, downloader).status for month in months]

    assert first == ["loaded"] * 12
    assert second == ["skipped"] * 12
    data_files = list((storage.root / "tlc").rglob("*.parquet"))
    assert len(data_files) == 12


def test_crash_before_manifest_is_redone_without_duplicates(storage):
    month = date(2024, 1, 1)
    # Simulate a crash after the data upload and before the manifest write.
    load_month("yellow", month, storage, FakeDownloader())
    (storage.root / tlc.manifest_key("yellow", month)).unlink()

    result = load_month("yellow", month, storage, FakeDownloader())

    assert result.status == "loaded"
    assert len(list((storage.root / "tlc").rglob("*.parquet"))) == 1


def test_corrupt_manifest_is_redone(storage):
    month = date(2024, 1, 1)
    load_month("yellow", month, storage, FakeDownloader())
    (storage.root / tlc.manifest_key("yellow", month)).write_text('{"dataset": "yel')

    result = load_month("yellow", month, storage, FakeDownloader())

    assert result.status == "loaded"
    assert result.record.row_count == 5


def test_unpublished_month_raises_and_writes_nothing(storage):
    downloader = FakeDownloader(unpublished=("2024-01",))

    with pytest.raises(SourceNotPublishedError):
        load_month("yellow", date(2024, 1, 1), storage, downloader)

    assert not storage.root.exists()
