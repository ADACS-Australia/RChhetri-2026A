from argparse import ArgumentParser
import logging
from pathlib import Path

from needle.lib.logging import setup_logging
from needle.config.container import ContainerConfig
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class CasaDataUpdateContext(SubprocessExecContext):
    """Context for downloading CASA measures data"""

    casa_data_path: Path
    "Path to download the CASA measures data to"

    @property
    def cmd(self) -> list[list[str]]:
        return [["python", "-c", f"import casaconfig; casaconfig.data_update(path='{self.casa_data_path}')"]]


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


def main():
    desc = """A module for updating the casa run data appropriately"""
    parser = ArgumentParser(description=desc)
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )
    parser.add_argument("data_path", type=str, help="The path to the casa data directory")

    ContainerConfig.add_to_parser(parser)

    args = parser.parse_args()
    setup_logging(args.log_level)

    ctx = CasaDataUpdateContext(runtime=ContainerConfig.from_namespace(args), casa_data_path=args.data_path)
    download_casa_rundata(ctx)


if __name__ == "__main__":
    main()
