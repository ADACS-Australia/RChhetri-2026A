from prefect import task

from distributed import Client
from prefect.cache_policies import NO_CACHE

from needle.config.beam import BeamPair, MSBeamPair
from needle.modules.convert import convert_to_ms, ConvertContext
from needle.lib.logging import setup_logging


@task(cache_policy=NO_CACHE)
def convert_beam_pair_task(client: Client, pair: BeamPair, log_level: str = "INFO") -> MSBeamPair:
    """Convert a raw beam pair to measurement sets.
    Uses existing .ms files if already present, otherwise converts.
    Will not attempt to operate on calibration solutions.
    Also creates the beam directory and puts the measurements sets in there.
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = ConvertContext(input=pair.tgt)
    logger.info(f"Creating measurement set from {pair.tgt}")
    f_tgt_ms = client.submit(convert_to_ms, ctx)

    ctx = ConvertContext(input=pair.cal)
    logger.info(f"Creating measurement set from {pair.cal}")
    f_cal_ms = client.submit(convert_to_ms, ctx)
    tgt_ms, cal = client.gather([f_tgt_ms, f_cal_ms])

    return BeamPair(beam=pair.beam, tgt=tgt_ms, cal=cal)
