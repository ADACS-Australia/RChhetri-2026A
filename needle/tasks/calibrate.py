from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import task_run

from needle.config.beam import CalibratedBeam
from needle.config.calibrate import SolveCalibrationConfig, ApplyCalibrationConfig, CalInput, CalibrationSolution
from needle.lib.dask import submit_in_beam
from needle.lib.logging import beam_logger
from needle.lib.names import NeedleNames
from needle.modules.calibrate import (
    SolveCalibrationContext,
    ApplyCalibrationContext,
    apply_calibration,
    solve_calibration,
)


@task(cache_policy=NO_CACHE, task_run_name="solve_calibration")
def solve_calibration_task(
    client: Client,
    cfg: SolveCalibrationConfig,
    cal: CalInput | None,
    log_level: str = "INFO",
    overwrite=False,
    **kwargs,
) -> CalibrationSolution | None:
    """Solves for a calibration solution. If the provided cal is already solved, this is a no-op"""
    if cal is None:
        return None
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(NeedleNames(path=cal).beam_dir, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        if not isinstance(cal, CalibrationSolution):
            ctx = SolveCalibrationContext(cfg=cfg, cal=cal)
            if ctx.output.exists() and overwrite is False:
                logger.info(f"Found existing solution: {ctx.output}\nWill not reprocess.")
                return ctx.output
            elif NeedleNames(path=cal).calibrated_image_path.exists and overwrite is False:
                logger.info(
                    f"Found existing calibrated image: {NeedleNames(path=cal).calibrated_image_path}\nWill not reprocess."
                )
                return ctx.output

            logger.info(f"Beginning calibration solution calculation for calibrator: {cal.name}")
            cal = submit_in_beam(client, solve_calibration, ctx).result()
            logger.info(f"Completed calibration solution calculation: {cal.bpcal.name} and {cal.gcal.name}")
    return cal


@task(cache_policy=NO_CACHE, task_run_name="apply_calibration")
def apply_calibration_task(
    client: Client,
    cfg: ApplyCalibrationConfig,
    cal: CalibrationSolution | None,
    tgt: Path,
    log_level: str = "INFO",
    overwrite=False,
    **kwargs,
) -> Path:
    """Applies an existing calibrator solution to the target. Returns the calibrated target measurement set."""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(NeedleNames(path=tgt).beam_dir, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
        if cal is None:
            logger.info(f"No calibration solution provided, will assume already calibrated target: {tgt}")
            return tgt

        ctx = ApplyCalibrationContext(cfg=cfg, cal=cal, tgt=tgt)
        if ctx.output_path.exists() and overwrite is False:  # Do not re-process
            logger.info(f"Found existing solution: {ctx.output_path}\nWill not reprocess.")
            return ctx.output_path

        logger.info(
            f"Beginning calibration application for calibrator solutions: {cal.bpcal.name} and {cal.gcal.name} to target: {tgt.name}"
        )
        result = submit_in_beam(client, apply_calibration, ctx).result()
        logger.info(
            f"""Completed calibration application for calibrator solutions: {cal.bpcal.name} and {cal.gcal.name} to target: {tgt.name}
        Calibrated solution name: {result.name}"""
        )
    return result
