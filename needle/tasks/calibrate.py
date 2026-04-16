from pathlib import Path
from typing import Optional

from prefect import task

from needle.config.pipeline import ContainerConfig
from needle.config.calibrate import CalibrateConfig
from needle.config.pipeline import MSBeamPair
from needle.modules.calibrate import calibrate_observation, CalibrateContext
from needle.lib.flow import CACHE_STRATEGY, CACHE_EXPIRATION
from needle.lib.logging import setup_logging


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def calibrate_task(
    cal: Path, tgt: Path, cfg: CalibrateConfig, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO"
) -> Path:
    """Calibrates a target using a calibrator source. Returns the calibrated measurement set"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CalibrateContext(runtime=runtime, cfg=cfg, cal=cal, tgt=tgt)
    return calibrate_observation(ctx)


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def calibrate_pair_task(
    ms_pair: MSBeamPair, cfg: CalibrateConfig, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO"
) -> Path:
    """Calibrates a target using a calibrator source. Returns the calibrated measurement set"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CalibrateContext(runtime=runtime, cfg=cfg, cal=ms_pair.cal, tgt=ms_pair.tgt)
    return calibrate_observation(ctx)
