from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE


from needle.lib.logging import setup_logging
from needle.modules.diagnostics import (
    MSDiagnosticsContext,
    MSDiagnosticsOutput,
    CalDiagnosticsOutput,
    CalDiagnosticsContext,
    run_diagnostics,
)
from needle.config.calibrate import CalibrationSolution


@task(cache_policy=NO_CACHE)
def ms_diagnostics_task(
    client: Client,
    ms: Path | CalibrationSolution,
    log_level: str = "INFO",
) -> MSDiagnosticsOutput:
    """Runs diagnostics on a measurement set"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    if isinstance(ms, CalibrationSolution):  # No-op
        return None

    ctx = MSDiagnosticsContext(ms=ms, output_dir=ms.parent / "diagnostics")
    return client.submit(run_diagnostics, ctx).result()


@task(cache_policy=NO_CACHE)
def cal_diagnostics_task(
    client: Client,
    cal: CalibrationSolution,
    log_level: str = "INFO",
) -> CalDiagnosticsOutput:
    """Runs diagnostics on a calibration solution"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CalDiagnosticsContext(solution=cal, output_dir=cal.gcal.parent / "diagnostics")
    return client.submit(run_diagnostics, ctx).result()
