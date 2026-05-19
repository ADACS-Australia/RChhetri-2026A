from typing import Optional

from prefect import task

from needle.config.container import ContainerConfig
from needle.config.beam import BeamPair, MSBeamPair
from needle.modules.convert import convert_to_ms, ConvertContext
from needle.lib.logging import setup_logging


@task()
def convert_beam_pair_task(
    pair: BeamPair, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO"
) -> MSBeamPair:
    """Convert a raw beam pair to measurement sets.
    Uses existing .ms files if already present, otherwise converts.
    Also creates the beam directory and puts the measurements sets in there.
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = ConvertContext(runtime=runtime, input=pair.tgt)
    logger.info(f"Creating measurement set from {pair.tgt}")
    tgt_ms = convert_to_ms(ctx)

    ctx = ConvertContext(runtime=runtime, input=pair.cal)
    logger.info(f"Creating measurement set from {pair.cal}")
    cal_ms = convert_to_ms(ctx)

    return MSBeamPair(beam=pair.beam, tgt=tgt_ms, cal=cal_ms)
