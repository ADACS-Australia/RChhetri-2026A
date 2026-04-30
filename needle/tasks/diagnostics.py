from pathlib import Path
from typing import Optional

from prefect import task

from needle.config.pipeline import ContainerConfig
from needle.lib.flow import CACHE_STRATEGY, CACHE_EXPIRATION
from needle.lib.logging import setup_logging
from needle.modules.diagnostics import DiagnosticsContext, DiagnosticsOutput, diagnostics
from needle.modules.calibrate import CalibrateOutput


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def diagnostics_task(
    ms: Path,
    gcal: Optional[Path] = None,
    bpcal: Optional[Path] = None,
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
) -> DiagnosticsOutput:
    """Runs diagnostics on a measurement set. Optionally takes calibration tables to diagnose"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = DiagnosticsContext(
        ms=ms, gcal=gcal, bpcal=bpcal, runtime=runtime, log_level=log_level, output_dir=ms.parent / "diagnostics"
    )
    return diagnostics(ctx)


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def diagnostics_cal_output_task(
    cal_output: CalibrateOutput,
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
) -> DiagnosticsOutput:
    """Runs diagnostics on a calibration output object."""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = DiagnosticsContext(
        ms=cal_output.tgt,
        gcal=cal_output.gcal,
        bpcal=cal_output.bpcal,
        runtime=runtime,
        log_level=log_level,
        output_dir=cal_output.tgt.parent / "diagnostics",
    )
    return diagnostics(ctx)
