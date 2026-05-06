from pathlib import Path

from needle.config.base import NeedleModel


class WatcherConfig(NeedleModel):
    """Flow-level configuration"""

    watch_dir: Path
    "Directory to watch for incoming subdirectories"

    staging_dir: Path
    "Directory where stable subdirectories are moved before the pipeline runs"

    sentinel_file: str = "done.txt"
    "Sentinel filename that signals a directory is complete and ready"

    pipeline_deployment: str = "needle-pipeline"
    "Prefect deployment to trigger"

    poll_interval_seconds: int = 600
    "How often the watcher flow runs (seconds)"
