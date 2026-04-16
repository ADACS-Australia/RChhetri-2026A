import argparse
import os
from pathlib import Path

from prefect_dask import DaskTaskRunner

from needle.config.pipeline import PipelineConfig


def load_local_task_runner(max_workers: int) -> DaskTaskRunner:
    """Original local Dask cluster task runner (Docker mode)."""
    return DaskTaskRunner(cluster_kwargs={"n_workers": max_workers, "threads_per_worker": 1})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cfg-file",
        "--cfg_file",
        dest="cfg_file",
        default="needle.yaml",
    )
    parser.add_argument("--prefect-url", dest="prefect_url", default="http://localhost:4200/api")
    args = parser.parse_args()

    if not args.cfg_file:
        print("Missing required argument: --cfg-file")
        raise SystemExit(1)
    try:
        cfg = PipelineConfig.from_yaml(Path(args.cfg_file))
    except ValueError as e:
        print(e)
        raise SystemExit(1)

    os.environ["PREFECT_API_URL"] = args.prefect_url
    os.environ["PREFECT_RESULTS_PERSIST_BY_DEFAULT"] = "true"
    os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] = "needle"
    os.environ["PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL"] = cfg.flow.log_level

    from needle.flows.pipeline import needle_pipeline

    task_runner = load_local_task_runner(cfg.flow.max_workers)
    print("Using local Dask task runner (Docker mode)")
    needle_pipeline.with_options(task_runner=task_runner)(cfg=cfg)


if __name__ == "__main__":
    main()
