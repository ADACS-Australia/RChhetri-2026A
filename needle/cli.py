import argparse
import logging
from pathlib import Path
import sys
import threading
import time
from typing import Literal
import yaml

from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
from prefect_dask import DaskTaskRunner

from needle.config.base import NeedleModel
from needle.config.container import ContainerConfig
from needle.config.pipeline import NeedleConfig
from needle.flows.pipeline import needle_pipeline
from needle.flows.courier import courier_flow, COURIER_RESOURCE_ID
from needle.lib.events import OBSERVATION_READY_EVENT, OBSERVATION_STAGED_EVENT
from needle.lib.flow import CONTAINER_DATA_DIR
from needle.lib.slurm_cluster import SifSLURMCluster
from needle.modules.watcher import watch, WATCHER_RESOURCE_ID

logger = logging.getLogger(__name__)


class Env(NeedleModel):
    """Helper for environment variables to pass to set for flow runtime"""

    PREFECT_API_URL: str = "http://localhost:4200/api"
    "The URL of the prefect API"
    PREFECT_LOGGING_EXTRA_LOGGERS: str = "needle"
    "Required for needle logging in prefect UI"
    PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    "Log level for needle logging"
    PREFECT_RESULTS_PERSIST_BY_DEFAULT: Literal["true", "false"] = "false"
    "Whether to cache results by default"
    PREFECT_LOCAL_STORAGE_PATH: str = f"{CONTAINER_DATA_DIR}/prefect_cache"
    "Location to store the cache if caching is enabled"


def _load_slurm_task_runner(cluster_cfg_path: Path) -> DaskTaskRunner:
    if not cluster_cfg_path.exists():
        raise FileNotFoundError(f"Cluster config not found: {cluster_cfg_path}")
    with open(cluster_cfg_path) as f:
        cfg = yaml.safe_load(f)

    min_workers: int = cfg.pop("min_workers", 1)
    max_workers: int = cfg.pop("max_workers", 1)
    dashboard_port: int = cfg.pop("dashboard_port", 8787)
    cfg["scheduler_options"] = {"dashboard_address": f":{dashboard_port}"}

    # Build container config if sif_path is present, otherwise SifSLURMCluster is a no-op
    container_data = cfg.pop("container", None)
    cfg["container_cfg"] = ContainerConfig(**container_data) if container_data else None

    logger.info(
        f"Building SifSLURMCluster from {cluster_cfg_path} " f"(min_workers={min_workers}, max_workers={max_workers})"
    )
    return DaskTaskRunner(
        cluster_class=SifSLURMCluster,
        cluster_kwargs=cfg,
        adapt_kwargs={"minimum": min_workers, "maximum": max_workers},
    )


def _load_local_task_runner(max_workers: int) -> DaskTaskRunner:
    """Original local Dask cluster task runner (Docker mode)."""
    return DaskTaskRunner(cluster_kwargs={"n_workers": max_workers, "threads_per_worker": 1})


def _parse_pipeline(parser: argparse.ArgumentParser) -> argparse.Namespace:
    parser.add_argument(
        "--cluster-cfg",
        "--cluster_cfg",
        dest="cluster_cfg",
        default=None,
        help=(
            "Path to a cluster.yaml file. When provided, tasks run on a SLURM cluster via dask-jobqueue. "
            "When omitted, a local Dask cluster is created and used."
        ),
        required=False,
    )
    return parser.parse_args()


def run():
    desc = """Runs the Needle Pipeline now.
    Expects a .needle.yaml to be in the user home. See setup_env.sh for setup help."""
    args = _parse_pipeline(argparse.ArgumentParser(description=desc))
    cfg = NeedleConfig.get_config()

    if args.cluster_cfg:
        cluster_cfg_path = Path(args.cluster_cfg)
        task_runner = _load_slurm_task_runner(cluster_cfg_path)
        print(f"Using SLURM task runner from {cluster_cfg_path}")
    else:
        print("Using local environment for task runs")
        task_runner = _load_local_task_runner(cfg.flow.max_workers)

    needle_pipeline.with_options(
        task_runner=task_runner,
        result_storage=cfg.flow.data_dir / Path("prefect_cache", persist_result=False),
    )(cfg=cfg)


def _watch_and_restart(watcher_cfg, data_cfg):
    """Run watch(), restarting on failure after restart_delay seconds."""
    while True:
        try:
            watch(watcher_cfg, data_cfg)
        except Exception as e:
            logger.error(f"Watcher crashed: {e} — restarting in 30s", exc_info=True)
            time.sleep(30)


def needle_serve():
    desc = """Starts the Watcher, which polls the source directory for observations.
    Serves the Courier and Needle Pipeline to the Prefect Server.
    Expects a .needle.yaml to be in the user home. See setup_env.sh for setup help."""
    args = _parse_pipeline(argparse.ArgumentParser(description=desc))

    cfg = NeedleConfig.get_config()
    if args.cluster_cfg:
        task_runner = _load_slurm_task_runner(Path(args.cluster_cfg))
        print(f"Using SLURM task runner from {args.cluster_cfg}")
    else:
        task_runner = _load_local_task_runner(cfg.flow.max_workers)
        print("Using local environment for task runs")

    # Start watcher in background thread
    watcher_thread = threading.Thread(target=_watch_and_restart, args=(cfg.watcher, cfg.data), daemon=True)
    watcher_thread.start()
    print(f"Watcher started — source: {cfg.data.source}, polling every {cfg.watcher.poll_interval}s")

    # We cannot use prefect's serve() function to serve multiple flows as it ignores the configured taskrunner
    # Serve courier in background thread
    courier_thread = threading.Thread(
        target=courier_flow.serve,
        kwargs={
            "name": "needle-courier",
            "parameters": {"data_cfg": cfg.data.to_kwargs()},
            "triggers": [
                DeploymentEventTrigger(
                    name="observation-ready-trigger",
                    enabled=True,
                    expect={OBSERVATION_READY_EVENT},
                    match={"prefect.resource.id": WATCHER_RESOURCE_ID},
                    parameters={"entry_name": "{{ event.payload.entry_name }}"},
                    flow_run_name="courier-{{ event.payload.entry_name }}",
                )
            ],
        },
        daemon=True,
    )
    courier_thread.start()
    print("Courier deployment started")

    # Serve pipeline on main thread (blocks)
    needle_pipeline.with_options(
        task_runner=task_runner,
        result_storage=cfg.data.staging_dir / Path("prefect_cache"),
        persist_result=False,
    ).serve(
        name="needle-pipeline",
        parameters={"cfg": cfg.to_kwargs()},
        triggers=[
            DeploymentEventTrigger(
                name="observation-staged-trigger",
                enabled=True,
                expect={OBSERVATION_STAGED_EVENT},
                match={"prefect.resource.id": COURIER_RESOURCE_ID},
                parameters={"work_dir": "{{ event.payload.staged_dir }}"},
            )
        ],
    )


def validate_config():
    desc = """Validates a needle pipeline YAML config file. Optionally pretty-prints to stdout."""
    parser = argparse.ArgumentParser(description=desc)
    cfg_default = Path.home() / Path(".needle.yaml")
    parser.add_argument(
        "-c",
        "--cfg",
        default=cfg_default,
        help=f"Path to the config YAML file (default: {cfg_default})",
    )
    parser.add_argument(
        "-p",
        "--pretty_print",
        action="store_true",
        help="Whether to pretty print the config",
    )
    args = parser.parse_args()
    path = Path(args.cfg)
    if not path.exists():
        print(f"ERROR: File not found: {path}")
        sys.exit(1)

    valid = NeedleConfig.validate(path=path)
    if not valid:
        return
    if args.pretty_print:
        print()
        NeedleConfig.load(path=path).pretty_print()
