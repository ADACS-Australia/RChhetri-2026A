from pathlib import Path
from typing import Optional

from prefect import task

from needle.config.pipeline import ContainerConfig
from needle.lib.aegean import AegeanSourceList
from needle.lib.logging import setup_logging
from needle.lib.flow import CACHE_EXPIRATION, CACHE_STRATEGY
from needle.modules.source_find import source_find, SourceFindContext
from needle.config.source_find import SourceFindConfig


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def source_find_task(
    fits_path: Path, cfg: SourceFindConfig, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO"
) -> Path:
    """Find sources in fits images. Returns a path to a json of sources"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = SourceFindContext(runtime=runtime, cfg=cfg, image=fits_path)
    output = source_find(ctx)
    if not output.sources_txt.exists():
        raise FileNotFoundError(f"Expected file output from source_find '{output.sources_txt}' does not exist")

    # Convert the catalog to a more workable json format
    source_list = AegeanSourceList.from_txt_catalog(output.sources_txt)
    output_json = output.sources_txt.with_suffix(".json")
    source_list.to_json(output_json)

    return output_json
