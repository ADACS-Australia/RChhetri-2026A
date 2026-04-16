import argparse
from pathlib import Path

import yaml
from dask_jobqueue import SLURMCluster
from prefect_dask import DaskTaskRunner

from needle.flows.pipeline import needle_pipeline
from needle.lib.flow import CONTAINER_DATA_DIR
from needle.config.pipeline import PipelineConfig


def load_slurm_task_runner(cluster_cfg_path: Path) -> DaskTaskRunner:
    """
    Parse a cluster.yaml file into a DaskTaskRunner backed by a SLURMCluster.

    Keys min_workers and max_workers control adaptive scaling and are not
    passed to SLURMCluster directly — all other keys are forwarded as-is.
    """
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


def load_slurm_deploy_kwargs(cfg: PipelineConfig) -> dict:
    return dict(
        name="needle-pipeline",
        work_pool_name="needle-pool-slurm",
        parameters={"cfg": cfg.model_dump()},
        push=False,
        build=False,
    )


def load_local_task_runner(max_workers: int) -> DaskTaskRunner:
    """Original local Dask cluster task runner (Docker mode)."""
    return DaskTaskRunner(cluster_kwargs={"n_workers": max_workers, "threads_per_worker": 1})


def load_local_deploy_kwargs(cfg: PipelineConfig) -> dict:
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
            "env": {
                "PREFECT_LOGGING_EXTRA_LOGGERS": "needle",
                "PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL": cfg.flow.log_level,
                "PREFECT_RESULTS_PERSIST_BY_DEFAULT": "true",
                "PREFECT_LOCAL_STORAGE_PATH": f"{CONTAINER_DATA_DIR}/prefect-cache",
            },
            "container_create_kwargs": {"shm_size": cfg.flow.shm_size},
        },
        parameters={"cfg": cfg.model_dump()},
        push=False,
        build=False,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cfg-file",
        "--cfg_file",
        dest="cfg_file",
        default="needle.yaml",
    )
    parser.add_argument(
        "--cluster-cfg",
        "--cluster_cfg",
        dest="cluster_cfg",
        default=None,
        help=(
            "Path to a cluster.yaml file. When provided, tasks run on a "
            "SLURM cluster via dask-jobqueue. When omitted, the original "
            "local Docker Dask cluster is used."
        ),
    )
    args = parser.parse_args()

    if not args.cfg_file:
        print("Missing required argument: --cfg-file")
        raise SystemExit(1)
    try:
        cfg = PipelineConfig.from_yaml(Path(args.cfg_file))
    except ValueError as e:
        print(e)
        raise SystemExit(1)

    if args.cluster_cfg:
        cluster_cfg_path = Path(args.cluster_cfg)
        if not cluster_cfg_path.exists():
            print(f"Cluster config not found: {cluster_cfg_path}")
            raise SystemExit(1)
        task_runner = load_slurm_task_runner(cluster_cfg_path)
        print(f"Using SLURM task runner from {cluster_cfg_path}")
        kwargs = load_slurm_deploy_kwargs(cfg)
    else:
        task_runner = load_local_task_runner(cfg.flow.max_workers)
        print("Using local Dask task runner (Docker mode)")
        kwargs = load_local_deploy_kwargs(cfg)

    print(f"Deploy kwargs: {kwargs}")
    needle_pipeline.with_options(task_runner=task_runner).deploy(**kwargs)


if __name__ == "__main__":
    main()
