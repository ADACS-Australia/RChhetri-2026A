"""
Runs WSClean on a calibrated measurement set.
Supports both shallow and deep cleaning via WSCleanConfig subclasses.
"""

from argparse import ArgumentParser, Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from pathlib import Path
import subprocess
from typing import Optional

from needle.lib.logging import setup_logging
from needle.models.clean import (
    WSCleanConfig,
    WSCleanOutput,
    WSCleanContext,
    ShallowCleanConfig,
    DeepCleanConfig,
)
from needle.modules.inspect_ms import inspect_ms

logger = logging.getLogger(__name__)


def interval_clean(ms: Path, cfg: WSCleanConfig, mask: Optional[Path] = None) -> list[WSCleanOutput]:
    """Cleans on each interval in a measurement set

    :param ms: The measurement set to clean
    :param cfg: The WSCleanConfig object
    :param mask: The path to the fits mask to use
    :return: List of the WSCleanOutput objects
    """
    # Number of intervals - the number of imaging instances we will run
    info = inspect_ms(ms)
    logger.debug(f"time info: {info.time}")
    logger.debug(f"n_integrations value: {info.time.n_integrations}")
    intervals = info.time.n_integrations

    # Ouptut the fits files to here
    output_dir = Path(f"{ms.with_suffix("")}_interval")
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Creating a series of images over {info.time.n_integrations} integrations")

    futures = []
    wsclean_outputs = []
    with ThreadPoolExecutor() as executor:
        for i in range(0, intervals - 1):
            ctx = WSCleanContext(cfg=cfg, ms=ms, fits_mask=mask, interval=(i, i), output_dir=output_dir)
            futures.append(executor.submit(run_clean, ctx))

        for future in as_completed(futures):
            wsclean_outputs.append(future.result())

    return wsclean_outputs


def run_clean(ctx: WSCleanContext) -> WSCleanOutput:
    """Run WSClean on a measurement set.

    Builds and executes the WSClean command for the given cfg. Both
    shallow and deep cleans are handled by passing the appropriate
    WSCleanConfig subclass. Output images are written with the given
    name prefix.

    :param ctx: WSClean run context object
    :raises RuntimeError: If WSClean exits with a non-zero return code
    :returns: Path to the output dirty/cleaned image
    """
    logger.info(f"Running WSClean on {ctx.ms}")

    result = subprocess.run(ctx.cmd, capture_output=True, text=True)

    if result.stdout:
        logger.debug(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"WSClean failed with return code {result.returncode}:\n{result.stderr}\n\n{result.stdout}")

    logger.info(f"WSClean complete, output image: {ctx.output.image}")

    assert ctx.output.image.exists(), "image fits does not exist"
    assert ctx.output.dirty.exists(), "dirty fits does not exist"
    assert ctx.output.model.exists(), "model fits does not exist"
    assert ctx.output.residual.exists(), "residual fits does not exist"
    return ctx.output


def _parse(parser: ArgumentParser) -> Namespace:
    parser.add_argument("--ms", type=Path, required=True, help="The path to the measurement set to perform a clean on")
    parser.add_argument(
        "--mask", type=Path, required=False, help="The path to the fits mask to use if utilising a masked clean"
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )
    return parser.parse_args()


def shallow():
    """Shallow clean preset configuration"""
    parser = ShallowCleanConfig.add_to_parser(
        ArgumentParser("Run WSClean on a measurement set with shallow-clean presets")
    )
    args = _parse(parser)
    setup_logging(args.log_level)

    run_clean(WSCleanContext(cfg=ShallowCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask))


def deep():
    """Deep clean preset configuration"""
    parser = DeepCleanConfig.add_to_parser(ArgumentParser("Run WSClean on a measurement set with deep-clean presets"))
    args = _parse(parser)
    setup_logging(args.log_level)

    run_clean(WSCleanContext(cfg=DeepCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask))


def main():
    """Generic preset configuration"""
    parser = WSCleanConfig.add_to_parser(ArgumentParser("Run WSClean on a measurement set"))
    args = _parse(parser)
    setup_logging(args.log_level)

    run_clean(WSCleanContext(cfg=WSCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask))


if __name__ == "__main__":
    main()
