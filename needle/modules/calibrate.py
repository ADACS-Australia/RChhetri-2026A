"""
Determines calibration solutions and applies them to a target source.
Effectively wraps CASA calibration tasks: setjy, bandpass, gaincal, applycal, split.
"""

import logging
from pathlib import Path
from typing import Tuple
import shutil

from pydantic import field_validator
import click

from needle.config.base import needle_module_args
from needle.config.calibrate import SolveCalibrationConfig, ApplyCalibrationConfig, CalibrateConfig, CalibrationSolution
from needle.lib.validate import validate_path_ms
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class SolveCalibrationContext(SubprocessExecContext):
    """Context class for calibration solution"""

    cfg: SolveCalibrationConfig
    "Static config values"
    cal: Path
    "The calibrator observation measurement set"

    @field_validator("cal")
    @classmethod
    def _valid_cal(cls, cal: Path) -> Path:
        validate_path_ms(cal)
        return cal

    @property
    def bpcal_path(self) -> Path:
        "Path to the bandpass calibration solution table"
        return self.cal.with_suffix(".bpcal")

    @property
    def gcal_path(self) -> Path:
        "Path to the gain calibration solution table"
        return self.cal.with_suffix(".gcal")

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

    @property
    def cmd(self) -> list[list[str]]:
        """Returns the full sequence of calibration commands in order:
        setjy -> bandpass -> gaincal"""
        return [
            self._setjy_cmd(),
            self._bandpass_cmd(),
            self._gaincal_cmd(),
        ]


class ApplyCalibrationContext(SubprocessExecContext):
    """Context class for applying a calibration solution"""

    cfg: ApplyCalibrationConfig
    "Static config values"
    cal: CalibrationSolution
    "The calibration solutions (bpcal and gcal)"
    tgt: Path
    "The target measurement set"

    @field_validator("tgt")
    @classmethod
    def _valid_tgt(cls, tgt: Path) -> Path:
        validate_path_ms(tgt)
        return tgt

    @property
    def _calibrated_tgt_path(self) -> Path:
        "Path to the calibrated target measurement set produced by split"
        return self.tgt.parent / f"{self.tgt.stem}_calibrated.ms"

    def _python_cmd(self, expr: str) -> list[str]:
        "Wraps a Python expression as a python3 -c command"
        return ["python3", "-c", expr]

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
            self._applycal_cmd(),
            self._split_cmd(),
        ]


def solve_calibration(ctx: SolveCalibrationContext) -> CalibrationSolution:
    """Calibrate an observation using the given configuration.

    Runs the configured calibration steps in the correct order: setjy, bandpass, gaincal.
    Calibration tables are automatically passed forward to subsequent steps that need them.

    :param ctx: The solve calibrate context object
    :returns: The CalibrationSolution object containing the calibration solutions
    """
    logger.info(f"Running calibration on source {ctx.tgt} using calibrator {ctx.cal.path}")
    if ctx.gcal_path.exists():
        logger.info(f"Removing existing gcal soluton: {ctx.gcal_path}")
        shutil.rmtree(ctx.gcal_path)
    if ctx.bpcal.exists():
        logger.info(f"Removing existing bpcal solution: {ctx.bpcal_path}")
        shutil.rmtree(ctx.bpcal_path)

    ctx.log_cmd()
    procs = ctx.execute()
    for p in procs:
        logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()
    logger.info(f"Calibration complete. Written to {ctx.gcal_path}, {ctx.bpcal_path}")
    return CalibrationSolution(gcal=ctx.gcal_path, bpcal=ctx.bpcal_path)


def apply_calibration(ctx: ApplyCalibrationContext) -> Path:
    """Apply a set of calibration solutions to an observation

    Runs the configured calibration steps in the correct order: applycal, split.

    :param ctx: The apply calibrate context object
    :returns: The path to the calibrated source
    """
    logger.info(f"Running calibration on source {ctx.tgt} using calibrator {ctx.cal.path}")
    if ctx._calibrated_tgt_path.exists():
        logger.info(f"Removing existing observation: {ctx._calibrated_tgt_path}")
        shutil.rmtree(ctx._calibrated_tgt_path)

    ctx.log_cmd()
    procs = ctx.execute()
    for p in procs:
        logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()
    logger.info(f"Calibration application complete. Written to {ctx._calibrated_tgt_path}")
    return ctx._calibrated_tgt_path


@click.option(
    "--cal",
    "-c",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the calibrator measurement set",
)
def _solve(cfg: SolveCalibrationConfig, cal: Path):
    ctx = SolveCalibrationContext(cfg=cfg, cal=cal)
    solve_calibration(ctx)


_solve_cmd = needle_module_args(
    SolveCalibrationConfig,
    name="solve",
    help="Determine the calibration solutions to a target measurement set using a calibrator measurement set.",
)(_solve)


@click.option(
    "--bpcal",
    "-b",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the bandpass calibration solution",
)
@click.option(
    "--gcal",
    "-g",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the gain calibration solution",
)
@click.option(
    "--tgt",
    "-t",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the target measurement set",
)
def _apply(cfg: ApplyCalibrationConfig, bpcal: Path, gcal: Path, tgt: Path):
    ctx = ApplyCalibrationContext(cfg=cfg, cal=CalibrationSolution(gcal=gcal, bpcal=bpcal), tgt=tgt)
    apply_calibration(ctx)


_apply_cmd = needle_module_args(
    ApplyCalibrationConfig,
    name="apply",
    help="Apply the calibation solutions to a target measurement set.",
)(_apply)


@click.option(
    "--cal",
    "-c",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the calibrator measurement set",
)
@click.option(
    "--tgt",
    "-t",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="The path to the target measurement set",
)
def _both(cfg: CalibrateConfig, cal: Path, tgt: Path) -> Tuple[CalibrationSolution, Path]:
    solve_cfg = SolveCalibrationConfig(setjy=cfg.setjy, bandpass=cfg.bandpass, gaincal=cfg.gaincal)
    apply_cfg = ApplyCalibrationConfig(applycal=cfg.applycal, setjy=cfg.setjy)
    ctx = SolveCalibrationContext(cfg=solve_cfg, cal=cal, tgt=tgt)
    sol = solve_calibration(ctx)
    ctx = ApplyCalibrationContext(cfg=apply_cfg, cal=sol, tgt=tgt)
    return sol, apply_calibration(ctx)


_both_cmd = needle_module_args(
    CalibrateConfig,
    name="both",
    help="Determine the calibration solutions and then apply them to the target observation",
)(_both)

entrypoint = click.Group(
    name="calibrate",
    help="""Calibration steps on a measurement set

    solve :: Solve for the calibration solutions
    apply :: Apply a set of calibration solutions
    both :: Solve for the calibration solutions and then apply them
    """,
)
for _cmd in (_solve_cmd, _apply_cmd, _both_cmd):
    entrypoint.add_command(_cmd)

if __name__ == "__main__":
    entrypoint()
