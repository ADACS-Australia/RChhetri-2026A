import logging
from pathlib import Path

import click

from needle.config.base import needle_module_args
from needle.config.container import ContainerConfig
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class CasaDataUpdateContext(SubprocessExecContext):
    """Context for downloading CASA measures data"""

    casa_data_path: Path
    "Path to download the CASA measures data to"

    @property
    def cmd(self) -> list[list[str]]:
        """Checks if the casa data directory is initiated, if not, will run pull_data. If it is, it'll run update.
        If the directory exists but doesn't have a readme, this is unexpected so we don't know what to do."""
        readme_path = self.casa_data_path / "readme.txt"
        is_empty = not self.casa_data_path.exists() or not any(self.casa_data_path.iterdir())

        if readme_path.exists():
            func_call = f"casaconfig.data_update(path='{self.casa_data_path}')"
        elif is_empty:
            func_call = f"casaconfig.pull_data(path='{self.casa_data_path}')"
        else:
            raise RuntimeError(
                f"{self.casa_data_path} is non-empty but has no readme.txt — "
                "cannot safely determine whether to pull or update CASA data."
            )
        return [["mkdir", "-p", self.casa_data_path], ["python", "-c", f"import casaconfig; {func_call}"]]


def download_casa_rundata(ctx: CasaDataUpdateContext) -> None:
    """Download CASA measures data if not already present.

    :param ctx: Casa data update context object
    """
    procs = ctx.execute()
    for p in procs:
        if p.stdout:
            logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()

    readme_path = ctx.casa_data_path / "readme.txt"
    if readme_path.exists():
        logger.info("CASA measures data successfully populated.")
    else:
        logger.warning("data_update completed but readme.txt still not found.")


@needle_module_args(ContainerConfig, name="casadata", help="A module for updating the casa data")
@click.option(
    "--data_path",
    "-d",
    type=click.Path(file_okay=False, exists=True, path_type=Path),
    required=True,
    help="The path to the casa data directory",
)
def entrypoint(cfg: ContainerConfig, data_path: Path):
    ctx = CasaDataUpdateContext(runtime=cfg, casa_data_path=data_path)
    download_casa_rundata(ctx)


if __name__ == "__main__":
    entrypoint()
