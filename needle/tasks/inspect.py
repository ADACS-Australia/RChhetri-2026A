from pathlib import Path
from typing import Tuple

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE

from needle.config.beam import MSBeamPair
from needle.lib.logging import setup_logging
from needle.modules.inspect import InspectMSContext, MSInfo, inspect_ms


@task(cache_policy=NO_CACHE)
def inspect_pair_task(client: Client, ms_pair: MSBeamPair, log_level: str = "INFO") -> Tuple[Path, Path]:
    """Inspects a pair of measurement sets. Outputs the metadata to a json file for each ms"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    cal_info: MSInfo
    tgt_info: MSInfo
    cal_info, tgt_info = client.gather(
        client.map(inspect_ms, (InspectMSContext(ms=ms_pair.cal), InspectMSContext(ms=ms_pair.tgt)))
    )
    return (cal_info.to_json(), tgt_info.to_json())


@task()
def inspect_ms_task(ms: Path, log_level: str = "INFO") -> Path:
    """Inspects a measurement set. Outputs the metadata to a json file for each ms"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ms_info = inspect_ms(InspectMSContext(ms=ms))
    return ms_info.to_json()
