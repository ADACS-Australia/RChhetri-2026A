from pathlib import Path

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
        return DataSource.from_string(self.source)
