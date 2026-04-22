import os
import argparse
from pathlib import Path
from typing import Literal
import yaml

from dask_jobqueue import SLURMCluster
from prefect_dask import DaskTaskRunner
from pydantic import BaseModel, ValidationError

from needle.flows.pipeline import needle_pipeline
from needle.lib.flow import CONTAINER_DATA_DIR
from needle.lib.config import get_config
from needle.config.pipeline import PipelineConfig


class Env(BaseModel):
    """Helper for environment variables to pass to set for flow runtime"""

    PREFECT_LOGGING_EXTRA_LOGGERS: str = "needle"
    PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    # PREFECT_RESULTS_PERSIST_BY_DEFAULT: Literal["true", "false"] = "true"
    PREFECT_API_URL: str = "http://localhost:4200/api"


def _check_prefect_env_variables():
    unset = []
    for v in Env().model_dump().keys():
        unset.append(v)
    if unset:
        raise EnvironmentError(f"Env varaibles {unset} are not set. Please add it to your environment")
    try:
        Env(
            PREFECT_API_URL=os.environ["PREFECT_API_URL"],
            PREFECT_LOGGING_EXTRA_LOGGERS=os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"],
            PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL=os.environ["PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL"],
            # PREFECT_RESULTS_PERSIST_BY_DEFAULT=os.environ["PREFECT_RESULTS_PERSIST_BY_DEFAULT"],
        )
    except ValidationError as e:
        print("Invalid type given to Env model. Have you incorrectly set an environment variable?")
        raise (e)


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

    print(f"Building SLURMCluster from {cluster_cfg_path} " f"(min_workers={min_workers}, max_workers={max_workers})")
    return DaskTaskRunner(
        cluster_class=SLURMCluster,
        cluster_kwargs=cfg,
        adapt_kwargs={"minimum": min_workers, "maximum": max_workers},
    )


def _load_slurm_deploy_kwargs(cfg: PipelineConfig) -> dict:
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


def _load_local_deploy_kwargs(cfg: PipelineConfig) -> dict:
    env = Env(
        PREFECT_LOCAL_STORAGE_PATH=f"{CONTAINER_DATA_DIR}/prefect_cache",
        PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL=cfg.flow.log_level,
        PREFECT_API_URL=cfg.flow.prefect_api_url,
    )
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


def _parse_pipeline() -> argparse.Namespace:
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
    args = _parse_pipeline()
    # cfg_path cannot be overridden. It must be static since CASA's config.py relies on it for configuration.
    cfg = get_config()

    # Set environment in for local runtime
    # env = Env(PREFECT_API_URL=cfg.flow.prefect_api_url, PREFECT_LOGGING_EXTRA_LOGGERS=cfg.flow.log_level)
    # for k, v in env.model_dump().items():
    #     if v is not None:
    #         os.environ[k] = v

    _check_prefect_env_variables()
    if args.cluster_cfg:
        cluster_cfg_path = Path(args.cluster_cfg)
        task_runner = _load_slurm_task_runner(cluster_cfg_path)
        print(f"Using SLURM task runner from {cluster_cfg_path}")
    else:
        print("Using local environment for task runs")
        task_runner = _load_local_task_runner(cfg.flow.max_workers)

    needle_pipeline.with_options(
        task_runner=task_runner,
        result_storage=cfg.flow.data_dir / Path("prefect_cache", persist_result=True),
    )(cfg=cfg)


def deploy():
    """Create a pipeline deployment using a container and a worker"""
    args = _parse_pipeline()
    cfg = get_config()

    if args.cluster_cfg:
        cluster_cfg_path = Path(args.cluster_cfg)
        task_runner = _load_slurm_task_runner(cluster_cfg_path)
        print(f"Using SLURM task runner from {cluster_cfg_path}")
        kwargs = _load_slurm_deploy_kwargs(cfg)
    else:
        task_runner = _load_local_task_runner(cfg.flow.max_workers)
        print("Using local Dask task runner (Docker mode)")
        kwargs = _load_local_deploy_kwargs(cfg)

    print(f"Deploy kwargs: {kwargs}")
    needle_pipeline.with_options(task_runner=task_runner).deploy(**kwargs)
