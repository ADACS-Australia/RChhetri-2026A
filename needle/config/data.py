from pathlib import Path

from pydantic import field_validator

from needle.config.base import NeedleModel


class DataConfig(NeedleModel):
    """Configuration for data ingestion and staging."""

    source: str | Path = Path.home() / "observations"
    "Data source URI. Use 'local:///path/to/dir' or /path/to/dir for local or 's3://bucket/prefix/' for S3"

    staging_dir: Path = Path.home() / "needle_data"
    "Local directory where received entries are staged before processing"

    stability_check: int = 60
    "How long an entry must be unchanged before it is considered ready (seconds)"

    @field_validator("source")
    @classmethod
    def valid_source(cls, v: str) -> str:
        if not v.startswith("s3://"):
            path = Path(v.removeprefix("local://"))
            if not path.is_absolute():
                raise ValueError(f"Local source path must be absolute, got '{path}'")
        return v
