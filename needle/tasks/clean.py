import os
from pathlib import Path
from typing import Any, Optional

from prefect import task

from needle.config.pipeline import ContainerConfig
from needle.lib.logging import setup_logging
from needle.config.clean import WSCleanConfig
from needle.modules.clean import run_clean, WSCleanContext


@task()
def interval_clean_task(
    ms: Path,
    cfg: WSCleanConfig,
    mask: Optional[Path],
    interval: tuple[int, int],
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
    wait_for_: Optional[Any] = None,
) -> list[Path]:
    """Cleans a single time interval slice of a measurement set.

    :raises FileNotFoundError: Raised when the number of expected image files does not match the amount found
    """
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
    n_expected = interval[1] - interval[0]
    if not len(wsclean_output.image) == n_expected:
        raise FileNotFoundError(
            f"Expected number of image files ({n_expected}) do not match actual count ({len(wsclean_output.image)})"
        )

    for f in wsclean_output.psf + wsclean_output.dirty + wsclean_output.residual + wsclean_output.model:
        os.remove(f)

    # Remap the interval images so that they're named nicely
    return wsclean_output.remap_interval_images(interval_start=interval[0])


@task()
def clean_task(
    ms: Path,
    cfg: WSCleanConfig,
    mask: Optional[Path] = None,
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
) -> Path:
    """Perform a clean on a measurement set with an optional mask input. Return the fits image path.
    Expects only one image output.

    :raises RuntimeError: Raised if there is not exactly one image output"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = WSCleanContext(runtime=runtime, cfg=cfg, ms=ms, fits_mask=mask)
    wsclean_output = run_clean(ctx)

    if len(wsclean_output.image) != 1:
        raise RuntimeError(f"Unexpected number of wsclean image outputs: {wsclean_output.image}")

    return wsclean_output.image[0]


@task()
def predict_task(
    ms: Path,
    cfg: WSCleanConfig,
    runtime: Optional[ContainerConfig] = None,
    log_level: str = "INFO",
    wait_for_: Optional[Any] = None,
) -> Path:
    """Fills the MODEL_DATA column of the measurement set.
    Expects a run_clean to have been done with the provided config already to generate the -model.fits file.
    Mostly the same as clean_task but doesn't return the wsclean output.
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
    _ = wait_for_

    ctx = WSCleanContext(runtime=runtime, cfg=cfg, ms=ms, predict=True)
    run_clean(ctx)
    return ms
