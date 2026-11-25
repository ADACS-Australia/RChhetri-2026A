from pathlib import Path

from needle.config.base import NeedleModel


class DataConfig(NeedleModel):
    """Configuration for data ingestion and staging."""

    work_dir: Path = Path.home() / "needle_data"
    "Local directory where received entries are sent for processing"

    download_dir: Path = Path.home() / "needle_data_download"
    "Local where downloads are received before processing"

    casa_dir: Path = Path.home() / "casa"
    "The location to store CASA data, including measures data and logs"
