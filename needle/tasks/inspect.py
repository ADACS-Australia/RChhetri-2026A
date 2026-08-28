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
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    f_cal_info = client.submit(inspect_ms, InspectMSContext(ms=ms_pair.cal))
    f_tgt_info = client.submit(inspect_ms, InspectMSContext(ms=ms_pair.tgt))
    cal_info: MSInfo
    tgt_info: MSInfo
    cal_info, tgt_info = client.gather((f_cal_info, f_tgt_info))

    f_cal_path = client.submit(cal_info.to_json)
    f_tgt_path = client.submit(tgt_info.to_json)
    return (f_cal_path.result(), f_tgt_path.result())


@task(cache_policy=NO_CACHE)
def inspect_ms_task(client: Client, ms: Path, log_level: str = "INFO") -> Path:
    """Inspects a measurement set. Outputs the metadata to a json file for each ms"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ms_info: MSInfo = client.submit(inspect_ms, InspectMSContext(ms=ms)).result()
    return client.submit(ms_info.to_json).result()
