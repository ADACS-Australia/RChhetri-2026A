from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import time
import shutil

import boto3


@dataclass(frozen=True)
class DirectorySnapshot:
    """A point-in-time snapshot of an entry directory's contents.
    Used to detect when a directory has stopped changing and is safe to receive.
    """

    file_count: int
    "Number of files present in the directory"

    total_size: int
    "Combined size of all files in bytes"


class DataSource(ABC):
    """Abstract base class for entry data sources.

    Handles quiescence detection — an entry is only considered ready
    once its file count and total size have been unchanged for stability_check.
    Subclasses implement the source-specific primitives: list, snapshot, receive.

    """

    def __init__(self):
        self._snapshots: dict[str, tuple[DirectorySnapshot, float]] = {}

    @abstractmethod
    def list_entries(self) -> list[str]:
        """Return all entry names currently visible in the source.

        :return: List of entry names (directory/prefix names, not full paths)
        """
        ...

    @abstractmethod
    def snapshot(self, entry_name: str) -> DirectorySnapshot:
        """Return a point-in-time snapshot of the given entry's contents.

        :param entry_name: The entry name to snapshot
        :return: A DirectorySnapshot with the current file count and total size
        """
        ...

    @abstractmethod
    def receive(self, entry_name: str, destination: Path) -> Path:
        """Move or download an entry into the local staging directory.

        :param entry_name: The entry name to receive
        :param destination: The local directory to receive the entry into
        :return: The path to the received entry directory
        """
        ...

    def get_ready_entries(self, stability_check: int = 60) -> list[str]:
        """Return entries that have been stable for at least stability_check seconds.

        On each call, snapshots all visible entries and compares against the
        previous snapshot. If unchanged, checks whether enough time has passed.
        If changed, resets the stability timer.

        :param stability_check: How long a directory must be unchanged before it
            is considered ready for ingestion (seconds)
        :return: List of entry names that are ready to be received
        """
        now = time.time()
        ready = []

        for entry_name in self.list_entries():
            current = self.snapshot(entry_name)
            if entry_name in self._snapshots:
                last_snapshot, stable_since = self._snapshots[entry_name]
                if current == last_snapshot:
                    if now - stable_since >= stability_check:
                        ready.append(entry_name)
                        del self._snapshots[entry_name]
                else:
                    self._snapshots[entry_name] = (current, now)
            else:
                self._snapshots[entry_name] = (current, now)

        return ready

    @classmethod
    def from_str(cls, source: str) -> "DataSource":
        """Constructor method that creates the appropriate DataSource object based on
        a string input."""
        if source.startswith("local://"):
            path = Path(source.removeprefix("local://"))
            return LocalDataSource(watch_dir=path)
        elif source.startswith("s3://"):
            # s3://bucket/prefix/
            without_scheme = source.removeprefix("s3://")
            bucket, _, prefix = without_scheme.partition("/")
            return S3DataSource(bucket=bucket, prefix=prefix)
        else:
            raise ValueError(
                f"Unrecognised source URI '{source}'. " "Expected 'local:///path/to/dir' or 's3://bucket/prefix/'"
            )


class LocalDataSource(DataSource):
    """Watches a local directory for entry subdirectories.

    :param watch_dir: The directory to watch for incoming entry directories
    """

    def __init__(self, watch_dir: Path):
        super().__init__()
        self.watch_dir = watch_dir

    def list_entries(self) -> list[str]:
        return [e.name for e in self.watch_dir.iterdir() if e.is_dir()]

    def snapshot(self, entry_name: str) -> DirectorySnapshot:
        obs_dir = self.watch_dir / entry_name
        files = [f for f in obs_dir.rglob("*") if f.is_file()]
        return DirectorySnapshot(
            file_count=len(files),
            total_size=sum(f.stat().st_size for f in files),
        )

    def receive(self, entry_name: str, destination: Path) -> Path:
        source = self.watch_dir / entry_name
        dest = destination / entry_name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.move(str(source), str(dest))
        return dest


class S3DataSource(DataSource):
    """Watches an S3 bucket prefix for entry directories.

    :param bucket: The S3 bucket name
    :param prefix: The key prefix to watch, e.g. 'incoming/'. Trailing slash
        is added automatically if missing
    """

    def __init__(self, bucket: str, prefix: str):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.s3 = boto3.client("s3")

    def list_entries(self) -> list[str]:
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        return list({obj["Key"].removeprefix(self.prefix).split("/")[0] for obj in response.get("Contents", [])})

    def snapshot(self, entry_name: str) -> DirectorySnapshot:
        prefix = f"{self.prefix}{entry_name}/"
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        objects = response.get("Contents", [])
        return DirectorySnapshot(
            file_count=len(objects),
            total_size=sum(o["Size"] for o in objects),
        )

    def receive(self, entry_name: str, destination: Path) -> Path:
        dest = destination / entry_name
        dest.mkdir(parents=True, exist_ok=True)
        prefix = f"{self.prefix}{entry_name}/"
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            filename = key.removeprefix(prefix)
            self.s3.download_file(self.bucket, key, str(dest / filename))
        return dest
