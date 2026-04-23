import os
from pathlib import Path
from typing import Any, Optional

from prefect import task

from needle.config.pipeline import ContainerConfig
from needle.lib.flow import CACHE_STRATEGY, CACHE_EXPIRATION
from needle.lib.logging import setup_logging
from needle.config.clean import WSCleanConfig
from needle.modules.clean import run_clean, WSCleanContext
from needle.modules.inspect_ms import inspect_ms


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def interval_clean_task(
    ms: Path,
    cfg: WSCleanConfig,
    mask: Optional[Path],
    interval: tuple[int, int],
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
    wait_for_: Optional[Any] = None,
) -> list[Path]:
    """Cleans a single time interval slice of a measurement set."""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
    _ = wait_for_

    output_dir = Path(f"{ms.with_suffix('')}_interval")
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Cleaning interval {interval} of {ms}")
    ctx = WSCleanContext(
        runtime=runtime,
        cfg=cfg,
        ms=ms,
        fits_mask=mask,
        interval=interval,
        output_dir=output_dir,
    )
    wsclean_output = run_clean(ctx)
    if not wsclean_output.image:
        raise FileNotFoundError(f"Expected image file from wsclean '{wsclean_output.image}' does not exist")

    for f in wsclean_output.psf + wsclean_output.dirty + wsclean_output.residual + wsclean_output.model:
        os.remove(f)

    # Remap the interval images so that they're named nicely
    return wsclean_output.remap_interval_images(interval_start=interval[0])


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def clean_task(
    ms: Path,
    cfg: WSCleanConfig,
    mask: Optional[Path] = None,
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
) -> Path:
    """Perform a clean on a measurement set with an optional mask input. Return the fits image path"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = WSCleanContext(runtime=runtime, cfg=cfg, ms=ms, fits_mask=mask)
    wsclean_output = run_clean(ctx)

    if len(wsclean_output.image) > 1:
        raise RuntimeError(f"More than one output from wsclean. Please investigate. Found {wsclean_output.image}")
    if len(wsclean_output.image) < 1:
        raise RuntimeError(f"No image output from wsclean. Search prefix: {wsclean_output.prefix} ")

    return wsclean_output.image[0]


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def predict_task(
    ms: Path,
    cfg: WSCleanConfig,
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
    wait_for_: Optional[Any] = None,
) -> Path:
    """Fills the MODEL_DATA column of the measurement set.
    Expects a run_clean to have been done with the provided config already to generate the -model.fits file.
    Mostly the same as clean_task but checks for MODEL_DATA and doesn't return the wsclean output."""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
    _ = wait_for_

    ctx = WSCleanContext(runtime=runtime, cfg=cfg, ms=ms, predict=True)
    if not len(ctx.output.model) == 1:
        raise RuntimeError(f"Unexcpected number of output model files found with prefix: {ctx.name}")
    if not ctx.output.model[0].exists():
        raise RuntimeError(f"Expected output model to exist but cannot find with prefix: {ctx.name}")

    run_clean(ctx)
    inspect = inspect_ms(ms)
    if "MODEL_DATA" not in inspect.data_columns:
        raise RuntimeError(f"Coluld not find MODEL_DATA column in {ms} after wsclean predict")

    return ms
