from pathlib import Path
from typing import Optional

from prefect import task

from needle.lib.logging import setup_logging
from needle.modules.diagnostics import DiagnosticsContext, DiagnosticsOutput, diagnostics
from needle.modules.calibrate import CalibrateOutput


@task()
def diagnostics_task(
    ms: Path,
    gcal: Optional[Path] = None,
    bpcal: Optional[Path] = None,
    log_level: str = "INFO",
) -> DiagnosticsOutput:
    """Runs diagnostics on a measurement set. Optionally takes calibration tables to diagnose"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = DiagnosticsContext(ms=ms, gcal=gcal, bpcal=bpcal, output_dir=ms.parent / "diagnostics")
    return diagnostics(ctx)


@task()
def diagnostics_cal_output_task(
    cal_output: CalibrateOutput,
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
        output_dir=cal_output.tgt.parent / "diagnostics",
    )
    return diagnostics(ctx)
