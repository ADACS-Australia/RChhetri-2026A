from pathlib import Path
from prefect import task

from distributed import Client
from prefect.cache_policies import NO_CACHE

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution
from needle.modules.convert import convert_to_ms, ConvertContext
from needle.lib.logging import beam_logger


@task(cache_policy=NO_CACHE, task_run_name="convert_beam{pair.beam}")
def convert_beam_pair_task(client: Client, pair: BeamPair, log_level: str = "INFO", **kwargs) -> BeamPair:
    """Convert a raw beam pair to measurement sets.
    Uses existing .ms files if already present, otherwise converts.
    Will not attempt to operate on calibration solutions.
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(pair.tgt.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        f_tgt = None
        tgt = pair.tgt
        ctx = ConvertContext(input=pair.tgt)
        if pair.tgt.suffix == ".ms":
            logger.info(f"No need to run conversion. Already in desired format: {pair.tgt}")
        else:
            logger.info(f"Creating measurement set from {pair.tgt}")
            f_tgt = client.submit(convert_to_ms, ctx)

        f_cal = None
        cal = pair.cal
        if isinstance(pair.cal, CalibrationSolution) or pair.cal.suffix == ".ms":
            logger.info(f"No need to run conversion. Already in desired format: {pair.cal}")
        else:
            ctx = ConvertContext(input=pair.cal)
            logger.info(f"Creating measurement set from {pair.cal}")
            f_cal = client.submit(convert_to_ms, ctx)
        if f_tgt:
            tgt = f_tgt.result()
            logger.info(f"Completed measurement set conversion for {pair.tgt.name}")
        if f_cal:
            cal = f_cal.result()
            logger.info(f"Completed measurement set conversion for {pair.cal.name}")

    return BeamPair(beam=pair.beam, name=pair.name, tgt=tgt, cal=cal)


@task(cache_policy=NO_CACHE, task_run_name="convert_{path.stem}")
def convert_task(
    client: Client, input: Path | CalibrationSolution, log_level: str = "INFO", **kwargs
) -> Path | CalibrationSolution:
    """Convert an observation file to a measurement set.
    Will not attempt to operate on calibration solutions.
    Also creates the beam directory and puts the measurements sets in there.
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(input.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        if isinstance(input, CalibrationSolution) or input.suffix == ".ms":
            logger.info(f"No need to run conversion. Already in desired format: {input}")
            return input
        logger.info(f"Creating measurement set from {input}")
        ctx = ConvertContext(input=input)
        result = client.submit(convert_to_ms, ctx).result()
        logger.info(f"Completed measurement set conversion for {input.name}")

    return result
