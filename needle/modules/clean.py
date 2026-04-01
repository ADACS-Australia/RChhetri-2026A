"""
Runs WSClean on a calibrated measurement set.
Supports both shallow and deep cleaning via WSCleanConfig subclasses.
"""

from argparse import ArgumentParser, Namespace
import logging
from pathlib import Path
import subprocess

from needle.lib.logging import setup_logging
from needle.models.clean import (
    WSCleanConfig,
    WSCleanOutput,
    WSCleanContext,
    ShallowCleanConfig,
    DeepCleanConfig,
    ModelSubtractCleanConfig,
)

logger = logging.getLogger(__name__)


def run_clean(ctx: WSCleanContext) -> WSCleanOutput:
    """Run WSClean on a measurement set.

    Builds and executes the WSClean command for the given cfg. Both
    shallow and deep cleans are handled by passing the appropriate
    WSCleanConfig subclass. Output images are written with the given
    name prefix.

    :param ctx: WSClean run context object
    :raises RuntimeError: If WSClean exits with a non-zero return code
    :returns: The wsclean output object
    """
    logger.info(f"Running WSClean on {ctx.ms}")
    logger.debug(f"{" ".join(str(c) for c in ctx.cmd)}")  # Log the wsclean command

    result = subprocess.run(ctx.cmd, capture_output=True, text=True)

    if result.stdout:
        logger.debug(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"WSClean failed with return code {result.returncode}:\n{result.stderr}\n\n{result.stdout}")

    logger.info(f"WSClean complete, output image: {ctx.output.image}")

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


def model_subtract():
    """Create the MODEL_DATA column"""
    parser = ModelSubtractCleanConfig.add_to_parser(
        ArgumentParser("Run WSClean on a measurement set with model-subtract presets")
    )
    args = _parse(parser)
    setup_logging(args.log_level)

    ctx = WSCleanContext(cfg=ModelSubtractCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask)
    run_clean(ctx)


def shallow():
    """Shallow clean preset configuration"""
    parser = ShallowCleanConfig.add_to_parser(
        ArgumentParser("Run WSClean on a measurement set with shallow-clean presets")
    )
    args = _parse(parser)
    setup_logging(args.log_level)

    ctx = WSCleanContext(cfg=ShallowCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask)
    run_clean(ctx)


def deep():
    """Deep clean preset configuration"""
    parser = DeepCleanConfig.add_to_parser(ArgumentParser("Run WSClean on a measurement set with deep-clean presets"))
    args = _parse(parser)
    setup_logging(args.log_level)

    ctx = WSCleanContext(cfg=DeepCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask)
    run_clean(ctx)


def main():
    """Generic preset configuration"""
    parser = WSCleanConfig.add_to_parser(ArgumentParser("Run WSClean on a measurement set"))
    args = _parse(parser)
    setup_logging(args.log_level)

    ctx = WSCleanContext(cfg=WSCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask)
    model_predict(ctx)


if __name__ == "__main__":
    main()
