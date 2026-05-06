import shutil
from pathlib import Path

from prefect.events import emit_event
from prefect import flow, task, get_run_logger

from needle.config.watcher import WatcherConfig


@task
def find_ready_directories(watch_dir: Path, sentinel_file: str) -> list[Path]:
    """Return subdirectories of watch_dir that contain the sentinel file."""
    logger = get_run_logger()
    if not watch_dir.exists():
        logger.warning(f"Watch directory does not exist: {watch_dir}")
        return []

    ready = []
    for entry in watch_dir.iterdir():
        if entry.is_dir() and (entry / sentinel_file).exists():
            ready.append(entry)
            logger.info(f"Found ready directory: {entry.name}")

    return ready


@task
def move_to_staging(source_dir: Path, staging_dir: Path) -> Path:
    """Move source_dir into staging_dir. Returns the new path."""
    logger = get_run_logger()
    staging_dir.mkdir(parents=True, exist_ok=True)
    destination = staging_dir / source_dir.name

    # If a previous run left a same-named directory in staging, remove it first
    if destination.exists():
        logger.warning(f"Destination already exists, removing: {destination}")
        shutil.rmtree(destination)

    shutil.move(str(source_dir), str(destination))
    logger.info(f"Moved {source_dir} → {destination}")
    return destination


@flow(name="watcher", log_prints=True)
def watcher_flow(cfg: WatcherConfig):
    ready_dirs = find_ready_directories(cfg.watch_dir, cfg.sentinel_file)
    triggered = []

    for source_dir in ready_dirs:
        staged_dir = move_to_staging(source_dir, cfg.staging_dir)
        emit_event(
            event="needle-pipeline.files.ready",
            resource={"prefect.resource.id": "landing-zone.watcher"},
            payload={"data_dir": str(staged_dir), "obs_name": source_dir.name},
        )
        triggered.append(source_dir.name)

    if triggered:
        print(f"Triggered {len(triggered)} pipeline run(s): {triggered}")
    else:
        print("No ready directories found this poll.")
