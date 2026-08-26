from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE

from needle.config.mask import CreateMaskConfig
from needle.modules.mask import create_mask, CreateMaskContext
from needle.lib.logging import setup_logging


@task(cache_policy=NO_CACHE)
def create_mask_task(
    client: Client,
    sources_json: Path,
    fits_image: Path,
    cfg: CreateMaskConfig,
    log_level: str = "INFO",
) -> Path:
    """Creates a fits mask using a fits image as reference. Returns the path to the mask.

    :raises FileNotFoundError: Raised if the mask file is not found after running the mask module
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    output = client.submit(create_mask, CreateMaskContext(cfg=cfg, image=fits_image, sources=sources_json)).result()
    if not output.mask.exists():
        raise FileNotFoundError(f"Expected file output from source_find '{output.mask}' does not exist")
    return output.mask
