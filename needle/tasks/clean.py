import os
from pathlib import Path
from typing import Optional

from prefect import task

from needle.models.clean import WSCleanConfig, WSCleanContext
from needle.modules.clean import run_clean, interval_clean
from needle.lib.flow import CACHE_STRATEGY, CACHE_EXPIRATION
from needle.lib.logging import setup_logging


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def interval_clean_task(ms: Path, cfg: WSCleanConfig, mask: Optional[Path], log_level: str = "INFO") -> list[Path]:
    """Cleans on each interval in a measurement set. Keeps the -image.fits but removes everything else."""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
    wsclean_outputs = interval_clean(ms, cfg, mask=mask)

    images = []
    for out in wsclean_outputs:
        for file in (out.image, out.residual, out.dirty, out.model, out.psf):
            if not file.exists:
                raise FileNotFoundError(f"Expected file output from wsclean '{file}' does not exist")
        os.remove(out.residual)
        os.remove(out.dirty)
        os.remove(out.model)
        os.remove(out.psf)
        images.append(out)

    return images


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def clean_task(ms: Path, cfg: WSCleanConfig, mask: Optional[Path] = None, log_level: str = "INFO") -> Path:
    """Perform a clean on a measurement set with an optional mask input. Return the fits image path"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = WSCleanContext(cfg=cfg, ms=ms, fits_mask=mask)
    wsclean_output = run_clean(ctx)
    for file in (
        wsclean_output.image,
        wsclean_output.psf,
        wsclean_output.residual,
        wsclean_output.dirty,
        wsclean_output.model,
    ):
        if not file.exists:
            raise FileNotFoundError(f"Expected file output from wsclean '{file}' does not exist")

    return wsclean_output.image
