"""
Wraps the flagging utility in CASA. Allows for multi-step flags. Order of flags is chosen by flag.py
"""

from argparse import ArgumentParser
import logging
from pathlib import Path

from pydantic import field_validator

from needle.config.base import ContainerConfig
from needle.config.flag import FlagConfig, FlagStepConfig
from needle.lib.logging import setup_logging
from needle.lib.validate import validate_path_ms
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class FlagContext(SubprocessExecContext):
    cfg: FlagConfig
    "Static configuration values"

    ms: Path
    "The path to the measurement set to flag"

    @field_validator("ms")
    @classmethod
    def _valid_image(cls, ms) -> Path:
        validate_path_ms(ms)
        return ms

    def _flagdata_cmd(self, step: FlagStepConfig) -> list[str]:
        """Constructs a single casa flagdata command for the given step"""
        # expr = f"flagdata(vis='{self.ms}', {step._flagdata_kwargs})"
        # return ["casa", "--nogui", "-c", expr]
        expr = f"from casatasks import flagdata; flagdata(vis='{self.ms}', {step._flagdata_kwargs})"
        return ["python3", "-c", expr]

    @property
    def cmd(self) -> list[list[str]]:
        """Returns a list of flagdata commands for each active flagging step, in order"""
        steps = [
            self.cfg.quack,
            self.cfg.clip,
            self.cfg.tfcrop,
            self.cfg.rflag,
            self.cfg.extend,
            self.cfg.manual,
        ]
        active_steps = [s for s in steps if s is not None and s.enabled]
        if not active_steps:
            raise ValueError("No flagging steps configured")
        return [self._flagdata_cmd(s) for s in active_steps]


def flag_observation(ctx: FlagContext) -> None:
    """Flags an observation using the given configuration

    :param ctx: The flag context object
    :raises ValueError: Raised if there are no configured steps to flag
    """
    logger.info(f"Flagging measurement set: {ctx.ms}")
    ctx.log_cmd()
    procs = ctx.execute()
    for p in procs:
        if p.stdout:
            logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()
    logger.info("Flagging complete")


def main():
    parser = ArgumentParser("Flag a measurement set")
    FlagConfig.add_to_parser(parser)
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )

    container_group = parser.add_argument_group(title="Container Arguments")
    ContainerConfig.add_to_parser(container_group)

    required = parser.add_argument_group(title="Required Arguments")
    required.add_argument("--ms", type=Path, required=True, help="The path to the measurement set")
    args = parser.parse_args()
    setup_logging(args.log_level)

    runtime = None
    if args.image:
        runtime = ContainerConfig.from_namespace(args)

    ctx = FlagContext(cfg=FlagConfig.from_namespace(args), ms=args.ms, runtime=runtime)
    flag_observation(ctx)


if __name__ == "__main__":
    main()
