import argparse
import logging
from pathlib import Path
import sys

from prefect.task_runners import ProcessPoolTaskRunner

from needle.flows.pipeline import needle_pipeline
from needle.lib.flow import CONTAINER_DATA_DIR
from needle.models.base import NeedleValidationError
from needle.models.pipeline import PipelineConfig

logger = logging.getLogger(__name__)


def main():
    peek_parser = argparse.ArgumentParser()
    peek_parser.add_argument("--cfg-file", "--cfg_file", dest="cfg_file", default="needle.yaml")
    partial, _ = peek_parser.parse_known_args(sys.argv[1:])
    if not partial.cfg_file:
        print("Missing required argument: --cfg-file")
        raise SystemExit(1)
    try:
        cfg = PipelineConfig.from_yaml(Path(partial.cfg_file))
    except NeedleValidationError as e:
        print(e)
        raise SystemExit(1)

    needle_pipeline.with_options(task_runner=ProcessPoolTaskRunner(max_workers=cfg.flow.max_workers)).deploy(
        name="needle-pipeline",
        work_pool_name="needle-pool",
        image="needle:latest",
        job_variables={
            "volumes": [f"{cfg.flow.local_data_dir}:{CONTAINER_DATA_DIR}", "casa_data:/opt/needle/.casa/data"],
            "image_pull_policy": "Never",
            "auto_remove": False,  # Useful for debugging
            "networks": ["needle-network"],
            "env": {
                "PREFECT_LOGGING_EXTRA_LOGGERS": "needle",
                "PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL": cfg.flow.log_level,
                "PREFECT_RESULTS_PERSIST_BY_DEFAULT": "true",  # Enable Caching
                "PREFECT_LOCAL_STORAGE_PATH": f"{CONTAINER_DATA_DIR}/prefect-cache",  # File caching location
            },
            "container_create_kwargs": {"shm_size": cfg.flow.shm_size},  # Required for BANE runs
        },
        parameters={"cfg": cfg.model_dump()},
        push=False,
        build=False,
    )


if __name__ == "__main__":
    main()
