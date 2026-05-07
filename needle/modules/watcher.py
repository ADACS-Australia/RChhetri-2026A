import logging
import time

from needle.config.watcher import WatcherConfig
from needle.config.data import DataConfig
from needle.lib.events import emit_observation_ready

logger = logging.getLogger(__name__)

WATCHER_RESOURCE_ID = "needle.watcher"


def watch(watcher_cfg: WatcherConfig, data_cfg: DataConfig):
    """Poll a data source for ready entries and emit Prefect events.

    Runs indefinitely, checking for ready entries on each poll. When an
    entry is ready a Prefect event is emitted for the courier to handle.

    :param watcher_cfg: Watcher configuration
    :param data_cfg: data configuration
    """

    logger.info("Watching for ready entries...")
    while True:
        for entry_name in data_cfg.data_source.get_ready_entries(data_cfg.stability_check):
            event = emit_observation_ready(entry_name=entry_name, resource_id=WATCHER_RESOURCE_ID)
            if not event:
                logger.warning(f"Error emitting observation-ready event for entry: {entry_name}")
            logger.info(f"Emitted event for {entry_name}: {event.model_dump()}")
        time.sleep(watcher_cfg.poll_interval)
