import argparse
import logging
from pathlib import Path
import threading
import time
from typing import Literal, Optional
import yaml

from dask_jobqueue import SLURMCluster
from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
from prefect_dask import DaskTaskRunner

from needle.config.pipeline import NeedleConfig
from needle.config.base import NeedleModel
from needle.flows.pipeline import needle_pipeline
from needle.flows.courier import courier_flow, COURIER_RESOURCE_ID
from needle.lib.events import OBSERVATION_READY_EVENT, OBSERVATION_STAGED_EVENT
from needle.lib.flow import CONTAINER_DATA_DIR
from needle.lib.logging import setup_logging
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
    PREFECT_RESULTS_PERSIST_BY_DEFAULT: Literal["true", "false"] = "true"
    "Whether to cache results by default"
    PREFECT_LOCAL_STORAGE_PATH: str = f"{CONTAINER_DATA_DIR}/prefect_cache"
    "Location to store the cache if caching is enabled"


def _load_slurm_task_runner(cluster_cfg_path: Path) -> DaskTaskRunner:
    """
    Parse a cluster.yaml file into a DaskTaskRunner backed by a SLURMCluster.

    Keys min_workers and max_workers control adaptive scaling and are not
    passed to SLURMCluster directly — all other keys are forwarded as-is.
    """
    if not cluster_cfg_path.exists():
        raise FileNotFoundError(f"Cluster config not found: {cluster_cfg_path}")

    with open(cluster_cfg_path) as f:
        cfg = yaml.safe_load(f)

    # Pull out scaling params — these are not SLURMCluster constructor args
    min_workers: int = cfg.pop("min_workers", 1)
    max_workers: int = cfg.pop("max_workers", 4)
    dashboard_port: int = cfg.pop("dashboard_port", 8787)

    # Inject dashboard address into scheduler options
    cfg["scheduler_options"] = {"dashboard_address": f":{dashboard_port}"}

    logger.info(
        f"Building SLURMCluster from {cluster_cfg_path} " f"(min_workers={min_workers}, max_workers={max_workers})"
    )
    return DaskTaskRunner(
        cluster_class=SLURMCluster,
        cluster_kwargs=cfg,
        adapt_kwargs={"minimum": min_workers, "maximum": max_workers},
    )


def _load_slurm_deploy_kwargs(cfg: NeedleConfig) -> dict:
    return dict(
        name="needle-pipeline",
        work_pool_name="needle-pool-slurm",
        parameters={"cfg": cfg.model_dump()},
        push=False,
        build=False,
    )


def _load_local_task_runner(max_workers: int) -> DaskTaskRunner:
    """Original local Dask cluster task runner (Docker mode)."""
    return DaskTaskRunner(cluster_kwargs={"n_workers": max_workers, "threads_per_worker": 1})


def _load_local_deploy_kwargs(cfg: NeedleConfig, env: Env) -> dict:
    return dict(
        name="needle-pipeline",
        work_pool_name="needle-pool",
        image="needle:latest",
        job_variables={
            "volumes": [
                f"{cfg.flow.data_dir}:{CONTAINER_DATA_DIR}",
                "casa_data:/opt/needle/.casa/data",
            ],
            "image_pull_policy": "Never",
            "auto_remove": False,
            "networks": ["needle-network"],
            "env": env.model_dump(),
            "container_create_kwargs": {"shm_size": cfg.flow.shm_size},
        },
        parameters={"cfg": cfg.model_dump()},
        push=False,
        build=False,
    )


def _parse_pipeline(parser: Optional[argparse.ArgumentParser] = None) -> argparse.Namespace:
    if not parser:
        parser = argparse.ArgumentParser(
            "Runs the Needle Pipeline. Expects a .needle.yaml to be in the user home. See setup_env.sh for setup help."
        )
    parser.add_argument(
        "--cluster-cfg",
        "--cluster_cfg",
        dest="cluster_cfg",
        default=None,
        help=(
            "Path to a cluster.yaml file. When provided, tasks run on a SLURM cluster via dask-jobqueue."
            "When omitted, a local Dask cluster is created and used."
        ),
        required=False,
    )
    return parser.parse_args()


def run():
    """Run the pipeline locally, now"""
    logger = setup_logging()
    args = _parse_pipeline()
    cfg = NeedleConfig.get_config()

    if args.cluster_cfg:
        cluster_cfg_path = Path(args.cluster_cfg)
        task_runner = _load_slurm_task_runner(cluster_cfg_path)
        logger.info(f"Using SLURM task runner from {cluster_cfg_path}")
    else:
        logger.info("Using local environment for task runs")
        task_runner = _load_local_task_runner(cfg.flow.max_workers)

    needle_pipeline.with_options(
        task_runner=task_runner,
        result_storage=cfg.flow.data_dir / Path("prefect_cache", persist_result=True),
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
    """Serve the pipeline as a deployment to a server"""
    args = _parse_pipeline()
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
        persist_result=True,
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
                flow_run_name="pipeline-{{ event.payload.entry_name }}",
            )
        ],
    )


def deploy():
    """Create a pipeline deployment using a container and a worker"""
    parser = argparse.ArgumentParser("Deploys the Needle Pipeline.")
    deploy_args = parser.add_argument_group("Deploy Args")
    Env.add_to_parser(deploy_args)
    args = _parse_pipeline()
    cfg = NeedleConfig.get_config()

    if args.cluster_cfg:
        raise NotImplementedError("Deploying to a cluster is currently unsupported")
        # cluster_cfg_path = Path(args.cluster_cfg)
        # task_runner = _load_slurm_task_runner(cluster_cfg_path)
        # print(f"Using SLURM task runner from {cluster_cfg_path}")
        # kwargs = _load_slurm_deploy_kwargs(cfg)
    else:
        task_runner = _load_local_task_runner(cfg.flow.max_workers)
        print("Using local Dask task runner (Docker mode)")
        env = Env(args.deploy_args.to_kwargs())
        kwargs = _load_local_deploy_kwargs(cfg, env)

    print(f"Deploy kwargs: {kwargs}")
    needle_pipeline.with_options(task_runner=task_runner).deploy(**kwargs)
