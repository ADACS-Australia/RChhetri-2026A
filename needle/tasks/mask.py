from pathlib import Path

from prefect import task

from needle.config.mask import CreateMaskConfig
from needle.modules.mask import create_mask, CreateMaskContext
from needle.lib.logging import setup_logging


@task()
def create_mask_task(
    sources_json: Path,
    fits_image: Path,
    cfg: CreateMaskConfig,
    log_level: str = "INFO",
) -> Path:
    """Creates a fits mask using a fits image as reference. Returns the path to the mask.

    :raises FileNotFoundError: Raised if the mask file is not found after running the mask module
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CreateMaskContext(cfg=cfg, image=fits_image, sources=sources_json)
    output = create_mask(ctx)
    if not output.mask.exists():
        raise FileNotFoundError(f"Expected file output from source_find '{output.mask}' does not exist")
    return output.mask
