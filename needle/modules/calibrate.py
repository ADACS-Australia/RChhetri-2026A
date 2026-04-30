"""
Wraps CASA calibration tasks: setjy, bandpass, gaincal, applycal, split.
Each step is an explicit function call — no strategy abstraction.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path
import shutil

from pydantic import field_validator

from needle.config.base import ContainerConfig, NeedleModel
from needle.config.calibrate import CalibrateConfig
from needle.lib.logging import setup_logging
from needle.lib.validate import validate_path_ms
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class CalibrateOutput(NeedleModel):
    tgt: Path
    "Path to the calibrated target measurement set"
    gcal: Path
    "Path to the gain calibration table"
    bpcal: Path
    "Path to the bandpass calibration table"


class CalibrateContext(SubprocessExecContext):
    cfg: CalibrateConfig
    "Static config values"
    cal: Path
    "The calibrator measurement set"
    tgt: Path
    "The target measurement set"

    @field_validator("cal")
    @classmethod
    def _valid_cal(cls, cal: Path) -> Path:
        validate_path_ms(cal)
        return cal

    @field_validator("tgt")
    @classmethod
    def _valid_tgt(cls, tgt: Path) -> Path:
        validate_path_ms(tgt)
        return tgt

    @property
    def _bpcal_path(self) -> Path:
        "Path to the bandpass calibration table"
        return self.cal.with_suffix(".bpcal")

    @property
    def _gcal_path(self) -> Path:
        "Path to the gain calibration table"
        return self.cal.with_suffix(".gcal")

    @property
    def _calibrated_tgt_path(self) -> Path:
        "Path to the calibrated target measurement set produced by split"
        return self.tgt.parent / f"{self.tgt.stem}_calibrated.ms"

    def _python_cmd(self, expr: str) -> list[str]:
        "Wraps a Python expression as a python3 -c command"
        return ["python3", "-c", expr]

    def _setjy_cmd(self) -> list[str]:
        "Constructs the setjy command"
        kwargs = self.cfg.setjy.to_kwargs()
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        return self._python_cmd(f"from casatasks import setjy; setjy(vis='{self.cal}', {kwargs_str})")

    def _bandpass_cmd(self) -> list[str]:
        "Constructs the bandpass command"
        kwargs = self.cfg.bandpass.to_kwargs()
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        return self._python_cmd(
            f"from casatasks import bandpass; bandpass(vis='{self.cal}', caltable='{self._bpcal_path}', {kwargs_str})"
        )

    def _gaincal_cmd(self) -> list[str]:
        "Constructs the gaincal command"
        kwargs = self.cfg.gaincal.to_kwargs()
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        return self._python_cmd(
            f"from casatasks import gaincal; gaincal(vis='{self.cal}', caltable='{self._gcal_path}', "
            f"gaintable=['{self._bpcal_path}'], {kwargs_str})"
        )

    def _applycal_cmd(self) -> list[str]:
        "Constructs the applycal command"
        kwargs = self.cfg.applycal.to_kwargs()
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        return self._python_cmd(
            f"from casatasks import applycal; applycal(vis='{self.tgt}', "
            f"gaintable=['{self._bpcal_path}', '{self._gcal_path}'], {kwargs_str})"
        )

    def _split_cmd(self) -> list[str]:
        "Constructs the split command"
        kwargs = self.cfg.split.to_kwargs()
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        return self._python_cmd(
            f"from casatasks import split; split(vis='{self.tgt}', outputvis='{self._calibrated_tgt_path}', {kwargs_str})"
        )

    @property
    def cmd(self) -> list[list[str]]:
        """Returns the full sequence of calibration commands in order:
        setjy -> bandpass -> gaincal -> applycal -> split"""
        return [
            self._setjy_cmd(),
            self._bandpass_cmd(),
            self._gaincal_cmd(),
            self._applycal_cmd(),
            self._split_cmd(),
        ]


def calibrate_observation(ctx: CalibrateContext) -> CalibrateOutput:
    """Calibrate an observation using the given configuration.

    Runs the configured calibration steps in the correct order: setjy,
    bandpass, gaincal, applycal, then split. Calibration tables are
    automatically passed forward to subsequent steps that need them.

    :param ctx: The calibrate context object
    :returns: The CalibrateOutput object containing the calibration outputs
    """
    logger.info(f"Running calibration on source {ctx.tgt} using calibrator {ctx.cal}")
    if ctx._calibrated_tgt_path.exists():
        logger.info(f"Removing existing calibrated target: {ctx._calibrated_tgt_path}")
        shutil.rmtree(ctx._calibrated_tgt_path)

    ctx.log_cmd()
    procs = ctx.execute()
    for p in procs:
        logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()
    logger.info(f"Calibration complete. Written to {ctx._calibrated_tgt_path}")
    return CalibrateOutput(tgt=ctx._calibrated_tgt_path, gcal=ctx._gcal_path, bpcal=ctx._bpcal_path)


def main():
    parser = CalibrateConfig.add_to_parser(
        ArgumentParser("Calibrate a target measurement set using a calibrator measurement set.")
    )

    container_group = parser.add_argument_group(title="Container Arguments")
    ContainerConfig.add_to_parser(container_group)
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )
    required = parser.add_argument_group(title="Required Arguments")
    required.add_argument("--cal", type=Path, required=True, help="The path to the calibrator measurement set")
    required.add_argument("--tgt", type=Path, required=True, help="The path to the target measurement set")
    args = parser.parse_args()
    setup_logging(args.log_level)

    runtime = None
    if args.image:
        runtime = ContainerConfig.from_namespace(args)

    ctx = CalibrateContext(
        cfg=CalibrateConfig.from_namespace(args),
        cal=args.cal,
        tgt=args.tgt,
        runtime=runtime,
    )
    calibrate_observation(ctx)


if __name__ == "__main__":
    main()
