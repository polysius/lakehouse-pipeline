"""Reference files that are not monthly, such as the taxi zone lookup."""

import json
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.ingest.download import Downloader
from lakehouse.ingest.loader import sha256_of
from lakehouse.ingest.storage import Storage

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_LOOKUP_KEY = "reference/taxi_zone_lookup.csv"
ZONE_LOOKUP_MANIFEST_KEY = "_manifests/reference/taxi_zone_lookup.json"


@dataclass(frozen=True)
class ReferenceRecord:
    source_url: str
    bronze_key: str
    sha256: str
    size_bytes: int
    row_count: int
    loaded_at: str


def load_zone_lookup(storage: Storage, downloader: Downloader) -> str:
    """Download the zone lookup once. Returns 'loaded' or 'skipped'."""
    if storage.exists(ZONE_LOOKUP_KEY) and storage.exists(ZONE_LOOKUP_MANIFEST_KEY):
        return "skipped"

    with tempfile.TemporaryDirectory() as scratch:
        local_file = Path(scratch) / "taxi_zone_lookup.csv"
        downloader.fetch(ZONE_LOOKUP_URL, local_file)
        lines = local_file.read_text().splitlines()
        record = ReferenceRecord(
            source_url=ZONE_LOOKUP_URL,
            bronze_key=ZONE_LOOKUP_KEY,
            sha256=sha256_of(local_file),
            size_bytes=local_file.stat().st_size,
            row_count=len(lines) - 1,  # the first line is the header
            loaded_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        storage.upload(local_file, ZONE_LOOKUP_KEY)

    storage.write_text(ZONE_LOOKUP_MANIFEST_KEY, json.dumps(asdict(record), indent=2))
    return "loaded"
