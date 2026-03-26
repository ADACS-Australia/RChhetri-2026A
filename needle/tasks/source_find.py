from pathlib import Path
from typing import Tuple

from prefect import task

from needle.lib.logging import setup_logging
from needle.lib.flow import CACHE_EXPIRATION, CACHE_STRATEGY
from needle.modules.source_find import run_aegean, run_bane, squeeze_fits
from needle.models.source_find import SourceFindConfig, SourceFindOutput


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def squeeze_fits_task(fits_path: Path, log_level: str = "INFO") -> Path:
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    return squeeze_fits(fits_path)


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def run_bane_task(fits_path: Path, cores: int = 1, log_level: str = "INFO") -> Tuple[Path, Path]:
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    return run_bane(fits_path, cores)  # bkg, rms


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def run_aegean_task(
    fits_path: Path, bkg_rms: Tuple[Path, Path], cfg: SourceFindConfig, log_level: str = "INFO"
) -> SourceFindOutput:
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    return run_aegean(fits_path, *bkg_rms, cfg)


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def source_find_task(fits_path: Path, cfg: SourceFindConfig, log_level: str = "INFO") -> list[SourceFindOutput]:
    """Find sources in fits images. Returns a path to a json of sources"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    fits_path = squeeze_fits(fits_path)
    bkg, rms = run_bane(fits_path, cfg.cores)  # bkg, rms
    output = run_aegean(fits_path, bkg, rms, cfg)

    # Check that all expected files exist
    for file in (
        output.bkg_path,
        output.rms_path,
        output.sources_txt,
        output.sources_json,
    ):
        if not file.exists():
            raise FileNotFoundError(f"Expected file output from source_find '{file}' does not exist")

    return output.sources_json
