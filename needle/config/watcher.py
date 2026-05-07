from needle.config.base import NeedleModel


class WatcherConfig(NeedleModel):
    """Configuration for the watcher."""

    poll_interval: int = 30
    "How often to poll the source in seconds"
