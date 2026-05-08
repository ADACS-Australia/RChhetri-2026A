import time

from needle.config.watcher import WatcherConfig
from needle.config.data import DataConfig
from needle.lib.datasource import DataSource
from needle.lib.events import emit_observation_ready
from needle.lib.logging import setup_watcher_logger


WATCHER_RESOURCE_ID = "needle.watcher"


def watch(watcher_cfg: WatcherConfig, data_cfg: DataConfig):
    """Poll a data source for ready entries and emit Prefect events.

    Runs indefinitely, checking for ready entries on each poll. When an
    entry is ready a Prefect event is emitted for the courier to handle.

    :param watcher_cfg: Watcher configuration
    :param data_cfg: data configuration
    """
    logger = setup_watcher_logger(log_file=watcher_cfg.log_file, level=watcher_cfg.log_level)

    logger.info("Watching for ready entries...")
    data_source = DataSource.from_str(data_cfg.source)
    while True:
        logger.debug(f"Scanning source: {data_cfg.source}")
        ready_entries = data_source.get_ready_entries(data_cfg.stability_check)
        logger.debug(f"Data Source state: {data_source.state}")

        for entry_name in ready_entries:
            event = emit_observation_ready(entry_name=entry_name, resource_id=WATCHER_RESOURCE_ID)
            if not event:
                logger.warning(f"Error emitting observation-ready event for entry: {entry_name}")
            data_source.mark_received(entry_name)  # Mark received so it isn't emitted again until it disappears
            logger.info(f"Emitted event for {entry_name}: {event.model_dump()}")

        if data_source.received:
            logger.info(f"The following entries are awaiting removal: {data_source.received}")

        time.sleep(watcher_cfg.poll_interval)
