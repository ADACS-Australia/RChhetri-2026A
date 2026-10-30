import json
from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE


from needle.config.beam import BeamPair
from needle.lib.beam import find_beam_dir
from needle.lib.dask import submit_in_beam
from needle.lib.exceptions import BeamUnusable
from needle.lib.logging import beam_logger
from needle.modules.diagnostics import (
    MSDiagnosticsContext,
    MSDiagnosticsOutput,
    CalDiagnosticsOutput,
    CalDiagnosticsContext,
    run_diagnostics,
)
from needle.config.calibrate import CalibrationSolution

DIAGNOSTICS_DIRNAME = "diagnostics"
FLAG_PCT_THRESH = 90


def _flagged_pct(client: Client, ms: Path, logger) -> float:
    ctx = MSDiagnosticsContext(ms=ms, output_dir=ms.parent / DIAGNOSTICS_DIRNAME)
    if ctx.output.exists:  # drop the call parens/adjust if exists is a method
        logger.info(f"Found existing diagnostics at {ctx.output.antenna_amp_stats_plot.parent}. Will not recreate.")
        output = ctx.output
    else:
        output = submit_in_beam(client, run_diagnostics, ctx).result()
    with open(output.flag_summary_data) as f:
        return float(json.load(f)["overall_flagged_pct"])


@task(cache_policy=NO_CACHE, task_run_name="pre_cal_diagnostics_beam{beam.beam}")
def pre_cal_diagnostics(
    client: Client,
    beam: BeamPair,
    log_level: str = "INFO",
    flag_pct_thresh: float = FLAG_PCT_THRESH,
    **kwargs,
) -> BeamPair:
    """Runs diagnostics on a beam. Intended for use before any calibration is attempted.
    Checks for poor quality (overly flagged) observations. If past the threshold, will raise an error.
    Always runs, regardless of overwrite flag.

    :raises BeamUnusable: Raised if the fraction of flagged data is past the threshold for either of the cal or tgt observations
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(find_beam_dir(beam.tgt), log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        to_check = []
        if not isinstance(beam.cal, CalibrationSolution):  # nothing to check if it's a solution
            to_check.append(("Calibrator", beam.cal))
        to_check.append(("Target", beam.tgt))

        for label, ms in to_check:
            logger.info(f"Performing pre-calibration diagnostics for {ms.name}")
            pct = _flagged_pct(client, ms, logger)
            if pct > flag_pct_thresh:
                raise BeamUnusable(f"{label} is {pct:.1f}% flagged. Beam {beam.beam} is unusable.")
            logger.info(f"Completed pre-calibration diagnostics for {ms.name}")

    return beam


@task(cache_policy=NO_CACHE, task_run_name="ms_diagnostics_{ms.stem}")
def ms_diagnostics_task(
    client: Client, ms: Path | CalibrationSolution, log_level: str = "INFO", overwrite: bool = False, **kwargs
) -> MSDiagnosticsOutput:
    """Runs diagnostics on a measurement set"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(find_beam_dir(ms), log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        if isinstance(ms, CalibrationSolution):  # No-op
            return None

        ctx = MSDiagnosticsContext(ms=ms, output_dir=ms.parent / DIAGNOSTICS_DIRNAME)
        if ctx.output.exists and not overwrite:
            logger.info(f"Found existing diagnostics at {ctx.output.antenna_amp_stats_plot.parent}. Will not recreate.")
            return ctx.output
        logger.info(f"Running diagnostics on {ms.name}")
        result = submit_in_beam(client, run_diagnostics, ctx).result()
        logger.info(f"Completed diagnostics on {ms.name}")
    return result


@task(cache_policy=NO_CACHE, task_run_name="cal_diagnostics_{cal.stem}")
def cal_diagnostics_task(
    client: Client, cal: CalibrationSolution, log_level: str = "INFO", overwrite: bool = False, **kwargs
) -> CalDiagnosticsOutput:
    """Runs diagnostics on a calibration solution"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(find_beam_dir(cal), log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        ctx = CalDiagnosticsContext(solution=cal, output_dir=cal.gcal.parent / DIAGNOSTICS_DIRNAME)
        if ctx.output.exists and not overwrite:
            logger.info(f"Found existing diagnostics at {ctx.output.gain_caltable_plot.parent}. Will not recreate.")
            return ctx.output
        logger.info(f"Beginning diagnostics on {cal.bpcal.name} and {cal.gcal.name}")
        result = submit_in_beam(client, run_diagnostics, ctx).result()
        logger.info(f"Completed diagnostics on {cal.bpcal.name} and {cal.gcal.name}")
    return result
