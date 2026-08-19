"""
Converts a .uvfits or .mir file to a measurement set (.ms)
"""

import logging
from pathlib import Path
from typing import Optional

import click

from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class ConvertContext(SubprocessExecContext):
    input: Path
    "The path to the file to convert to a measurement set"

    output_dir: Optional[Path] = None
    "The directory to output the converted file to"

    @property
    def output(self) -> Path:
        if self.output_dir:
            return self.output_dir / self.input.with_suffix(".ms").name
        return self.input.with_suffix(".ms")

    @property
    def cmd(self) -> list[list[str]]:
        """
        Returns the relevant command to convert the input to a measurement set. If already a .ms, will put it in the
        appropriate directory if it's not already there.

        :raises Exception: Raised if the input file type is not supported
        """
        match self.input.suffix:
            case ".uvfits":
                expr = f"from casatasks import importuvfits; importuvfits(fitsfile='{self.input}', vis='{self.output}')"
            case ".mir":
                expr = f"from casatasks import importmiriad; importmiriad(mirfile='{self.input}', vis='{self.output}')"
            case ".ms":
                if self.output_dir:  # Copy to the provided output directory
                    if not self.output.exists():
                        logger.info(f"Copying {self.input} -> {self.output}")
                        expr = f"from casatools import table; tb=table(); tb.open('{self.input}'); tb.copy(newtablename='{self.output}', deep=True)"
                    else:
                        logger.info(f"MS already exists at {self.output}, skipping copy")
                        return [[]]
                else:
                    logger.info(f"Input {self.input} is already an MS and no output_dir provided, skipping")
                    return [[]]
            case _:
                raise Exception(f"Unsupported file type: {self.input.suffix}")

        return [["python3", "-c", expr]]  # execute() expects a list of lists


def convert_to_ms(ctx: ConvertContext) -> Path:
    """Converts a valid observation file to a measurement set

    :param ctx: The CovertContext object
    :return: The written measurement set path
    """
    if ctx.output.exists():
        logger.warning(f"Expected output file '{ctx.output}' already exists. Will not overwrite")
        return ctx.output

    logger.info("Executing conversion to MS")
    ctx.log_cmd()
    procs = ctx.execute()
    for p in procs:
        logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()

    logger.info(f"Conversion complete. Output at {ctx.output}")
    return ctx.output


@click.command(name="convert", help="Converts a .uvfits or .mir file to a measurement set (.ms)")
@click.argument(
    "input",
    type=click.Path(dir_okay=True, file_okay=True, exists=True, path_type=Path),
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=False, path_type=Path),
    default=None,
    help="Directory to write the output MS to",
)
def entrypoint(input: Path, output_dir: Optional[Path] = None):
    ctx = ConvertContext(input=input, output_dir=output_dir)
    convert_to_ms(ctx)


if __name__ == "__main__":
    entrypoint()
