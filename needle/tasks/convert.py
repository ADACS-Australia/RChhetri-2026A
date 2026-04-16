from prefect import task
from typing import Optional

from needle.config.pipeline import ApptainerConfig
from needle.config.pipeline import BeamPair, MSBeamPair
from needle.modules.convert import convert_to_ms, ConvertContext
from needle.lib.logging import setup_logging


@task()
def convert_beam_pair_task(
    pair: BeamPair, runtime: Optional[ApptainerConfig] = None, log_level: str = "INFO"
) -> MSBeamPair:
    """Convert a raw beam pair to measurement sets.
    Uses existing .ms files if already present, otherwise converts.
    Also creates the beam directory and puts the measurements sets in there.
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    beam_dir = pair.setup_beam_dir()  # Set up the working directory for this beam

    ctx = ConvertContext(runtime=runtime, input=pair.tgt, output_dir=beam_dir)
    logger.info(f"Creating measurement set from {pair.tgt}")
    tgt_ms = convert_to_ms(ctx)

    ctx = ConvertContext(runtime=runtime, input=pair.cal, output_dir=beam_dir)
    logger.info(f"Creating measurement set from {pair.cal}")
    cal_ms = convert_to_ms(ctx)

    return MSBeamPair(beam=pair.beam, tgt=tgt_ms, cal=cal_ms, parent_dir=pair.parent_dir)
