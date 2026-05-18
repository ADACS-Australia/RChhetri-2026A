from pathlib import Path
from typing import Tuple

from prefect import task

from needle.config.base import ContainerConfig
from needle.config.beam import MSBeamPair
from needle.lib.logging import setup_logging
from needle.modules.inspect_ms import InspectMSContext, inspect_ms


@task()
def inspect_pair_task(
    ms_pair: MSBeamPair, runtime: ContainerConfig = None, log_level: str = "INFO"
) -> Tuple[Path, Path]:
    """Inspects a pair measurement sets. Outputs the metadata to a json file for each ms"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    cal_info = inspect_ms(InspectMSContext(runtime=runtime, ms=ms_pair.cal, log_level=log_level))
    tgt_info = inspect_ms(InspectMSContext(runtime=runtime, ms=ms_pair.tgt, log_level=log_level))
    return (cal_info.to_json(), tgt_info.to_json())


@task()
def inspect_ms_task(ms: Path, runtime: ContainerConfig = None, log_level: str = "INFO") -> Path:
    """Inspects a measurement set. Outputs the metadata to a json file for each ms"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ms_info = inspect_ms(InspectMSContext(runtime=runtime, ms=ms, log_level=log_level))
    return ms_info.to_json()
