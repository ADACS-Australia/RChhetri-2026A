"""
Wraps the flagging utility in CASA. Allows for multi-step flags. Order of flags is chosen by flag.py
"""

from argparse import ArgumentParser
import logging
import warnings
from pathlib import Path

with warnings.catch_warnings():
    warnings.simplefilter("ignore", SyntaxWarning)
    from casatasks import flagdata


from needle.models.flag import FlagStepConfig, FlagConfig, FlagContext
from needle.lib.logging import setup_logging

logger = logging.getLogger(__name__)


def apply_flag_step(ms: Path, step: FlagStepConfig) -> None:
    """Applies a single flag operation with CASA"""

    logger.info(f"Applying: {step}")
    logger.debug(f"flagdata kwargs: {step._flagdata_kwargs}")
    assert ms.exists(), f"Measurement set does not exist: {ms}"
    flagdata(vis=str(ms), **step._flagdata_kwargs)


def flag_observation(ctx: FlagContext):
    """Flags an observation using the given configuration

    :param ctx: The flag context object
    :param cfg: The flagging configuration
    :raises TypeError: Raised if something other than a measurement set is provided
    :raises ValueError: Raised if there are no configured steps to flag
    """
    steps = [
        ctx.cfg.quack,
        ctx.cfg.clip,
        ctx.cfg.tfcrop,
        ctx.cfg.rflag,
        ctx.cfg.extend,
        ctx.cfg.manual,
    ]
    active_steps = [s for s in steps if s is not None]
    if not active_steps:
        raise ValueError("No flagging steps configured")

    logger.info(f"Flagging measurement set: {ctx.ms}")
    for step in active_steps:
        apply_flag_step(ctx.ms, step)

    logger.info("Flagging complete")


def main():
    parser = ArgumentParser("Flag a measurement set")
    parser = FlagConfig.add_to_parser(ArgumentParser())
    parser.add_argument("--ms", type=Path, required=True, help="The path to the measurement set")
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    flag_observation(FlagContext(cfg=FlagConfig.from_namespace(args), ms=args.ms))


if __name__ == "__main__":
    main()
