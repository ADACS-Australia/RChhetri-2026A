"""
Flags a measurement set.
Effectively wraps the flagging utility in CASA.
Allows for multi-step flags. Order of flags is chosen by flag.py

Typer version, kept for comparison against the real (Click) flag.py.
"""

import logging
from pathlib import Path

import click
from pydantic import field_validator

from needle.config.base import needle_module_args
from needle.config.flag import FlagConfig, FlagStepConfig
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
        expr = f"from casatasks import flagdata; flagdata(vis='{self.ms}', {step._flagdata_kwargs})"
        return ["python3", "-c", expr]

    @property
    def cmd(self) -> list[list[str]]:
        """Returns a list of flagdata commands for each active flagging step, in order

        :raises ValueError: Raised if there are no configured flag steps
        """
        steps = [
            self.cfg.quack,
            self.cfg.clip,
            self.cfg.tfcrop,
            self.cfg.rflag,
            self.cfg.extend,
            self.cfg.manual,
        ]
        # Flag options are disabled by default
        active_steps = [s for s in steps if s is not None]
        if not active_steps:
            raise ValueError("No flagging steps configured")
        return [self._flagdata_cmd(s) for s in active_steps]


def flag_observation(ctx: FlagContext) -> None:
    """Flags an observation using the given configuration

    :param ctx: The flag context object
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


@needle_module_args(FlagConfig, name="flag", help="Flag a measurement set using CASA's flagging utilities.")
@click.option(
    "--ms",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the measurement set",
)
def entrypoint(cfg: FlagConfig, ms: Path):
    ctx = FlagContext(cfg=cfg, ms=ms)
    flag_observation(ctx)


if __name__ == "__main__":
    entrypoint()
