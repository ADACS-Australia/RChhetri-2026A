import argparse
import logging
from pathlib import Path
import sys
import threading
import time

from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
from prefect_dask import DaskTaskRunner

from needle.config.cluster import ClusterConfig
from needle.config.pipeline import NeedleConfig
from needle.flows.pipeline import needle_pipeline
from needle.flows.courier import courier_flow, COURIER_RESOURCE_ID
from needle.lib.events import OBSERVATION_READY_EVENT, OBSERVATION_STAGED_EVENT
from needle.lib.logging import setup_logging
from needle.modules.watcher import watch, WATCHER_RESOURCE_ID

logger = logging.getLogger("needle-cli")


def _setup_cli_logging(level: str = "INFO"):
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)


def _load_task_runner(args: argparse.Namespace, cfg: NeedleConfig) -> DaskTaskRunner:
    mode = args.mode

    if mode is None:
        mode = "cluster" if (Path.home() / ".needle_cluster.yaml").exists() else "local"

    if mode == "cluster":
        cluster_cfg = ClusterConfig.get_config()
        logger.info(f"Using {cluster_cfg.type} cluster")
        return cluster_cfg.to_task_runner()

    logger.info("Using local environment for task runs")
    return DaskTaskRunner(cluster_kwargs={"n_workers": cfg.flow.max_workers, "threads_per_worker": 1})


def _parse_pipeline(parser: argparse.ArgumentParser) -> argparse.Namespace:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--cluster",
        dest="mode",
        action="store_const",
        const="cluster",
        help="Run using a cluster configured with ~/.needle_cluster.yaml",
    )
    group.add_argument(
        "--local",
        dest="mode",
        action="store_const",
        const="local",
        help="Run locally without any cluster or container",
    )
    parser.add_argument(
        "--log-level",
        "--log_level",
        dest="log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )
    return parser.parse_args()


def run():
    desc = """Runs the Needle Pipeline now.
    Expects a .needle.yaml to be in the user home. See setup_env.sh for setup help."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--work-dir",
        help="The location of the calibration and target observation pairs. Should be an existing directory",
        type=str,
        required=True,
    )
    args = _parse_pipeline(parser)
    _setup_cli_logging(args.log_level)
    setup_logging(args.log_level)
    cfg = NeedleConfig.get_config()

    if not Path(args.work_dir).exists():
        raise NotADirectoryError(f"Could not find work directory: {args.work_dir}")

    needle_pipeline.with_options(task_runner=_load_task_runner(args, cfg))(cfg=cfg, work_dir=args.work_dir)


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
    _setup_cli_logging(args.log_level)
    setup_logging(args.log_level)
    cfg = NeedleConfig.get_config()

    # Start watcher in background thread
    watcher_thread = threading.Thread(target=_watch_and_restart, args=(cfg.watcher, cfg.data), daemon=True)
    watcher_thread.start()
    logger.info(f"Watcher started — source: {cfg.data.source}, polling every {cfg.watcher.poll_interval}s")

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
    logger.info("Courier deployment started")

    # Serve pipeline on main thread (blocks)
    needle_pipeline.with_options(
        task_runner=_load_task_runner(args, cfg),
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
        logger.error(f"ERROR: File not found: {path}")
        sys.exit(1)

    valid = NeedleConfig.validate(path=path)
    if not valid:
        return
    if args.pretty_print:
        print()
        NeedleConfig.load(path=path).pretty_print()
