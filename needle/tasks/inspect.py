from pathlib import Path
from typing import Tuple

from prefect import task

from needle.config.pipeline import MSBeamPair
from needle.modules.inspect_ms import MSInfo
from needle.lib.logging import setup_logging


@task()
def inspect_pair_task(ms_pair: MSBeamPair, log_level: str = "INFO") -> Tuple[Path, Path]:
    """Inspects a pair measurement sets. Outputs the metadata to a json file for each ms"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    cal_ms = MSInfo(ms_pair.cal)
    tgt_ms = MSInfo(ms_pair.tgt)
    return (cal_ms.to_json(), tgt_ms.to_json())


@task()
def inspect_ms_task(ms: Path, log_level: str = "INFO") -> Path:
    """Inspects a measurement set. Outputs the metadata to a json file for each ms"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    return MSInfo(ms).to_json()
