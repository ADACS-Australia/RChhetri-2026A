from pathlib import Path

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE


from needle.lib.aegean import AegeanSourceList
from needle.lib.logging import beam_logger
from needle.modules.source_find import source_find, SourceFindContext
from needle.config.source_find import SourceFindConfig


@task(cache_policy=NO_CACHE, task_run_name="source_find_{fits_image.stem}")
def source_find_task(
    client: Client, fits_image: Path, cfg: SourceFindConfig, log_level: str = "INFO", overwrite: bool = False, **kwargs
) -> Path:
    """Find sources in fits images. Returns a path to a json of sources

    :raises FileNotFoundError: Raised if the source find file is not found after running the source finder
    """
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(fits_image.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))
        ctx = SourceFindContext(cfg=cfg, image=fits_image)
        if overwrite is False and ctx.output.exists():
            logger.info(f"Sources file exists at {ctx.output.sources_txt}\nWill not recalculate.")
            output = ctx.output
        else:
            logger.info(f"Beginning source-finding for {fits_image}")
            output = client.submit(source_find, ctx).result()
            if not output.sources_txt.exists():
                raise FileNotFoundError(f"Expected file output from source_find '{output.sources_txt}' does not exist")
            logger.info(f"Completed source-finding for {fits_image}")

        # Convert the catalog to a more workable json format
        output_json = output.sources_txt.with_suffix(".json")
        if overwrite is False and output_json.exists():
            logger.info(f"JSON file exists at {output_json}\nWill not recalculate.")
        else:
            source_list = AegeanSourceList.from_txt_catalog(output.sources_txt)
            source_list.to_json(output_json)

    return output_json
