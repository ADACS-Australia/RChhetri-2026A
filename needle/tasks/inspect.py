from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE

from needle.config.calibrate import CalibrationSolution
from needle.lib.logging import setup_logging
from needle.modules.inspect import InspectMSContext, MSInfo, inspect_ms


@task(cache_policy=NO_CACHE)
def inspect_ms_task(client: Client, ms: Path | CalibrationSolution, log_level: str = "INFO") -> Path | None:
    """Inspects a measurement set. Outputs the metadata to a json file for each ms"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    if isinstance(ms, CalibrationSolution):  # No-op
        return

    ms_info: MSInfo = client.submit(inspect_ms, InspectMSContext(ms=ms)).result()
    return client.submit(ms_info.to_json).result()
