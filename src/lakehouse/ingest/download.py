"""Fetching source files over HTTP."""

from pathlib import Path
from typing import Protocol

import requests


class SourceNotPublishedError(Exception):
    """The TLC has not released this month yet. Retrying later may succeed."""


class Downloader(Protocol):
    def fetch(self, url: str, destination: Path) -> None: ...


class HttpDownloader:
    def __init__(self, timeout_seconds: int = 60, chunk_bytes: int = 1024 * 1024):
        self.timeout_seconds = timeout_seconds
        self.chunk_bytes = chunk_bytes

    def fetch(self, url: str, destination: Path) -> None:
        # The TLC bucket answers 403 or 404 for months that are not published.
        with requests.get(url, stream=True, timeout=self.timeout_seconds) as response:
            if response.status_code in (403, 404):
                raise SourceNotPublishedError(f"{url} returned {response.status_code}")
            response.raise_for_status()
            with open(destination, "wb") as handle:
                handle.writelines(response.iter_content(self.chunk_bytes))
