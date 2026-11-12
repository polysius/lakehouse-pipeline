"""Where bronze files live. Local disk for tests and WSL, a Databricks volume for real runs."""

import io
import shutil
from abc import ABC, abstractmethod
from pathlib import Path


class Storage(ABC):
    """Keys are relative paths such as 'tlc/yellow/year=2024/month=01/file.parquet'."""

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def upload(self, local_path: Path, key: str) -> None:
        """Copy a local file to key, replacing anything already there."""

    @abstractmethod
    def read_text(self, key: str) -> str: ...

    @abstractmethod
    def write_text(self, key: str, text: str) -> None: ...


class LocalStorage(Storage):
    def __init__(self, root: Path):
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def upload(self, local_path: Path, key: str) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Copy to a temporary name first so a crash never leaves a half-written file at the key.
        temporary = target.with_name(target.name + ".part")
        shutil.copyfile(local_path, temporary)
        temporary.replace(target)

    def read_text(self, key: str) -> str:
        return self._path(key).read_text()

    def write_text(self, key: str, text: str) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)


class DatabricksVolumeStorage(Storage):
    """Unity Catalog volume reached through the Databricks Files API.

    Authentication comes from the standard DATABRICKS_HOST and DATABRICKS_TOKEN
    environment variables, which the SDK reads on its own.
    """

    def __init__(self, volume_root: str, client=None):
        if client is None:
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient()
        self.volume_root = volume_root.rstrip("/")
        self.files = client.files

    def _path(self, key: str) -> str:
        return f"{self.volume_root}/{key}"

    def exists(self, key: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self.files.get_metadata(self._path(key))
        except NotFound:
            return False
        return True

    def upload(self, local_path: Path, key: str) -> None:
        with open(local_path, "rb") as handle:
            self.files.upload(self._path(key), handle, overwrite=True)

    def read_text(self, key: str) -> str:
        return self.files.download(self._path(key)).contents.read().decode()

    def write_text(self, key: str, text: str) -> None:
        self.files.upload(self._path(key), io.BytesIO(text.encode()), overwrite=True)
