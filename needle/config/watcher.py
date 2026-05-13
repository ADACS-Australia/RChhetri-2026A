from pathlib import Path
from typing import Literal, Optional

from needle.config.base import NeedleModel


class WatcherConfig(NeedleModel):
    """Configuration for the watcher."""

    poll_interval: int = 30
    "How often to poll the source in seconds"

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    "Log level for the watcher"

    log_file: Optional[Path] = None
    "File to write logs to"
