from pathlib import Path
from typing import Optional

from prefect import task

from needle.lib.logging import setup_logging
from needle.config.container import ContainerConfig
from needle.modules.casa_data import download_casa_rundata, CasaDataUpdateContext


@task
def update_casa_data(data_path: Path, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO") -> None:
    """Updates the casa dataset. ONLY run this in serial

    :param data_path: The path to the casa dataset
    :param runtime: The container runtime to use to update the dataset
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    ctx = CasaDataUpdateContext(runtime=runtime, casa_data_path=data_path)
    download_casa_rundata(ctx)
    return
