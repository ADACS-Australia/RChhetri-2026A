from pathlib import Path

from pydantic import field_validator

from needle.config.base import NeedleModel
from needle.lib.datasource import DataSource


class DataConfig(NeedleModel):
    """Configuration for data ingestion and staging."""

    source: str
    "Data source URI. Use 'local:///path/to/dir' or 's3://bucket/prefix/'"

    staging_dir: Path
    "Local directory where received entries are staged before processing"

    stability_check: int = 60
    "How long an entry must be unchanged before it is considered ready (seconds)"

    @property
    def data_source(self) -> DataSource:
        """Construct a DataSource from the source URI.
        :return: A LocalDataSource or S3DataSource depending on the URI scheme
        """
        return DataSource.from_str(self.source)

    @field_validator("source")
    @classmethod
    def valid_source(cls, v: str) -> str:
        if not v.startswith("s3://"):
            path = Path(v.removeprefix("local://"))
            if not path.is_absolute():
                raise ValueError(f"Local source path must be absolute, got '{path}'")
        return v
