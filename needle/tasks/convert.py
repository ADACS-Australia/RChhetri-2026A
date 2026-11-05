from pathlib import Path
from prefect import task

from distributed import Client
from prefect.cache_policies import NO_CACHE

from needle.config.beam import BeamInput, BeamPair, CalibratedBeam
from needle.config.calibrate import CalibrationSolution
from needle.modules.convert import convert_to_ms, ConvertContext
from needle.lib.names import NeedleNames
from needle.lib.logging import beam_logger


@task(cache_policy=NO_CACHE, task_run_name="convert_beam{beam.beam}")
def convert_beam_task(client: Client, beam: BeamInput, log_level: str = "INFO", **kwargs) -> BeamInput:
    """Convert a raw beam beam to measurement sets.
    Uses existing .ms files if already present, otherwise converts.
    Will not attempt to operate on calibration solutions.
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(NeedleNames(path=beam.path).beam_dir, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
        if isinstance(beam, CalibratedBeam):
            logger.info(f"No need to run conversion. Already in desired format: {beam.path}")
            return beam

        f_tgt = None
        tgt = beam.tgt
        ctx = ConvertContext(input=beam.tgt)
        if beam.tgt.suffix == ".ms":
            logger.info(f"No need to run conversion. Already in desired format: {beam.tgt}")
        else:
            logger.info(f"Creating measurement set from {beam.tgt}")
            f_tgt = client.submit(convert_to_ms, ctx)

        f_cal = None
        cal = beam.cal
        if isinstance(beam.cal, CalibrationSolution) or beam.cal.suffix == ".ms":
            logger.info(f"No need to run conversion. Already in desired format: {beam.cal}")
        else:
            ctx = ConvertContext(input=beam.cal)
            logger.info(f"Creating measurement set from {beam.cal}")
            f_cal = client.submit(convert_to_ms, ctx)
        if f_tgt:
            tgt = f_tgt.result()
            logger.info(f"Completed measurement set conversion for {beam.tgt.name}")
        if f_cal:
            cal = f_cal.result()
            logger.info(f"Completed measurement set conversion for {beam.cal.name}")

    return BeamPair(beam=beam.beam, name=beam.name, tgt=tgt, cal=cal)


@task(cache_policy=NO_CACHE, task_run_name="convert_{input.stem}")
def convert_task(
    client: Client, input: Path | CalibrationSolution, log_level: str = "INFO", **kwargs
) -> Path | CalibrationSolution:
    """Convert an observation file to a measurement set.
    Will not attempt to operate on calibration solutions.
    Also creates the beam directory and puts the measurements sets in there.
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(NeedleNames(path=input).beam_dir, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        if isinstance(input, CalibrationSolution) or input.suffix == ".ms":
            logger.info(f"No need to run conversion. Already in desired format: {input}")
            return input
        logger.info(f"Creating measurement set from {input}")
        ctx = ConvertContext(input=input)
        result = client.submit(convert_to_ms, ctx).result()
        logger.info(f"Completed measurement set conversion for {input.name}")

    return result
