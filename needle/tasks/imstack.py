from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE

from needle.config.imstack import ImageStackConfig
from needle.lib.beam import find_beam_dir
from needle.lib.dask import submit_in_beam
from needle.lib.logging import beam_logger
from needle.modules.imstack import ImageStackContext, create_image_stack


@task(cache_policy=NO_CACHE, task_run_name="create_image_stack_{obsid}")
def create_image_stack_task(
    client: Client,
    cfg: ImageStackConfig,
    input_dir: Path,
    obsid: str,
    log_level: str = "INFO",
    overwrite: bool = False,
    **kwargs,
) -> Path:
    """Builds an HDF5 image stack from the WSClean FITS images in a directory.

    :raises FileNotFoundError: Raised if the output file is not found after building the stack
    :return: The path to the HDF5 image stack
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(find_beam_dir(input_dir), log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        ctx = ImageStackContext(cfg=cfg, obsid=obsid, input_dir=input_dir, overwrite=overwrite)
        if not overwrite and ctx.output.exists():
            logger.info(f"Found existing image stack at {ctx.output}\nWill not recreate")
            return ctx.output

        logger.info(f"Beginning image stack creation for {obsid}")
        output = submit_in_beam(client, create_image_stack, ctx).result()
        if not output.exists():
            raise FileNotFoundError(f"Expected image stack '{output}' does not exist")
        logger.info(f"Completed image stack creation for {obsid}")

    return output
