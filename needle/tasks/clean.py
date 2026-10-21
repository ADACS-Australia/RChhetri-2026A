import os
from pathlib import Path
from typing import Any, Optional

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE


from needle.config.clean import WSCleanConfig
from needle.lib.dask import submit_in_beam
from needle.lib.logging import beam_logger
from needle.modules.clean import run_clean, WSCleanContext, WSCleanIntervalOutput


@task(cache_policy=NO_CACHE, task_run_name="interval_clean_{ms.stem}_{interval[0]}_{interval[1]}")
def interval_clean_task(
    client: Client,
    ms: Path,
    cfg: WSCleanConfig,
    mask: Optional[Path],
    interval: tuple[int, int],
    log_level: str = "INFO",
    overwrite: bool = False,
    dependencies: Optional[Any] = None,
    **kwargs,
) -> WSCleanIntervalOutput:
    """Cleans a single time interval slice of a measurement set.

    :raises FileNotFoundError: Raised when the number of expected image files does not match the amount found
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(ms.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
        _ = dependencies

        output_dir = Path(f"{ms.with_suffix('')}_interval")
        ctx = WSCleanContext(cfg=cfg, ms=ms, fits_mask=mask, interval=interval, output_dir=output_dir)
        if overwrite is False and ctx.output.exists():
            logger.info(
                f"Interval {interval[0]}->{interval[1]} clean already complete at {output_dir}\nWill not reprocess"
            )
            return ctx.output

        logger.info(f"Beginning cleaning of interval {interval[0]}->{interval[1]} of {ms}")
        wsclean_output: WSCleanIntervalOutput = submit_in_beam(client, run_clean, ctx).result()

        n_expected = interval[1] - interval[0]
        if not len(wsclean_output.image) == n_expected:
            raise FileNotFoundError(
                f"Expected number of image files ({n_expected}) do not match actual count ({len(wsclean_output.image)})"
            )
        for f in wsclean_output.psf + wsclean_output.dirty + wsclean_output.residual + wsclean_output.model:
            os.remove(f)

        # Remap the interval images so that they're named nicely
        wsclean_output.remap()
        logger.info(f"Completed interval cleaning task on inerval {interval[0]}->{interval[1]} for {ms.name}")
    return wsclean_output


@task(cache_policy=NO_CACHE, task_run_name="clean_{ms.stem}")
def clean_task(
    client: Client,
    ms: Path,
    cfg: WSCleanConfig,
    mask: Optional[Path] = None,
    log_level: str = "INFO",
    overwrite: bool = False,
    **kwargs,
) -> Path:
    """Perform a clean on a measurement set with an optional mask input. Return the fits image path.
    Expects only one image output.

    :raises RuntimeError: Raised if there is not exactly one image output"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(ms.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        ctx = WSCleanContext(cfg=cfg, ms=ms, fits_mask=mask)
        if not cfg.subtract_model and ctx.output.image and overwrite is False:
            logger.info(f"Found existing output: image: {ctx.output.image}\nWill not recreate")
            return ctx.output.image[0]

        logger.info(f"Beginning clean task for {ms.name}")
        wsclean_output = submit_in_beam(client, run_clean, ctx).result()
        if not cfg.subtract_model and len(wsclean_output.image) != 1:
            raise RuntimeError(f"Unexpected number wsclean outputs: {wsclean_output}")

        logger.info(f"Completed clean task for {ms.name}")
    return wsclean_output.image[0]


@task(cache_policy=NO_CACHE, task_run_name="predict_{ms.stem}")
def predict_task(
    client: Client,
    ms: Path,
    cfg: WSCleanConfig,
    log_level: str = "INFO",
    dependencies: Optional[Any] = None,
    overwrite: bool = False,
    **kwargs,
) -> Path:
    """Fills the MODEL_DATA column of the measurement set.
    Expects a run_clean to have been done with the provided config already to generate the -model.fits file.
    Mostly the same as clean_task but also generates a model column and doesn't return the wsclean output.
    Model column will not be overwritten if it exists and overwrite is false.
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(ms.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
        _ = dependencies

        logger.info(f"Beginning model prediction task for {ms.name}")
        ctx = WSCleanContext(cfg=cfg, ms=ms, predict=True)
        submit_in_beam(client, run_clean, ctx, overwrite_predictions=overwrite).result()
        logger.info(f"Completed model prediction task for {ms.name}")
    return ms
