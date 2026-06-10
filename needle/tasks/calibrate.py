from pathlib import Path

from prefect import task

from needle.config.calibrate import CalibrateConfig
from needle.config.beam import MSBeamPair
from needle.modules.calibrate import calibrate_observation, CalibrateContext, CalibrateOutput
from needle.lib.logging import setup_logging


@task()
def calibrate_task(cal: Path, tgt: Path, cfg: CalibrateConfig, log_level: str = "INFO") -> CalibrateOutput:
    """Calibrates a target using a calibrator source. Returns the calibrated measurement set"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CalibrateContext(cfg=cfg, cal=cal, tgt=tgt)
    return calibrate_observation(ctx)


@task()
def calibrate_pair_task(ms_pair: MSBeamPair, cfg: CalibrateConfig, log_level: str = "INFO") -> CalibrateOutput:
    """Calibrates a target using its associated calibrator source. Returns the calibrated measurement set"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CalibrateContext(cfg=cfg, cal=ms_pair.cal, tgt=ms_pair.tgt)
    return calibrate_observation(ctx)
