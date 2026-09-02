from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE

from needle.config.calibrate import SolveCalibrationConfig, ApplyCalibrationConfig, CalInput, CalibrationSolution
from needle.modules.calibrate import (
    SolveCalibrationContext,
    ApplyCalibrationContext,
    apply_calibration,
    solve_calibration,
)
from needle.lib.logging import setup_logging


@task(cache_policy=NO_CACHE)
def solve_calibration_task(
    client: Client, cfg: SolveCalibrationConfig, cal: CalInput, log_level: str = "INFO"
) -> CalibrationSolution:
    """Solves for a calibration solution. If the provided cal is already solved, this is a no-op"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    if isinstance(cal, Path):
        ctx = SolveCalibrationContext(cfg=cfg, cal=cal)
        cal = client.submit(solve_calibration, ctx)
    return cal


@task(cache_policy=NO_CACHE)
def apply_calibration_task(
    client: Client, cfg: ApplyCalibrationConfig, cal: CalibrationSolution, tgt: Path, log_level: str = "INFO"
) -> Path:
    """Applies an existing calibrator solution to the target. Returns the calibrated target measurement set."""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = ApplyCalibrationContext(cfg=cfg, cal=cal, tgt=tgt)
    return client.submit(apply_calibration, ctx)
