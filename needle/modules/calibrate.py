"""
Wraps CASA calibration tasks: setjy, bandpass, gaincal, applycal, split.
Each step is an explicit function call — no strategy abstraction.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path
import shutil
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore", SyntaxWarning)
    from casatasks import setjy, bandpass, gaincal, applycal, split

from needle.models.calibrate import (
    CalibrateConfig,
    CalibrateContext,
    SetjyConfig,
    BandpassConfig,
    GaincalConfig,
    ApplycalConfig,
    SplitConfig,
)
from needle.lib.logging import setup_logging

logger = logging.getLogger(__name__)


def calibrated_target_name(ms: Path) -> Path:
    """The name of the file after running split()"""
    return ms.parent / f"{ms.stem}_calibrated.ms"


def run_setjy(ms: Path, cfg: SetjyConfig) -> None:
    """Set the flux density scale for a calibrator source.

    Sets the flux density of a known calibrator source in the MODEL_DATA column
    of the measurement set, using a standard flux density scale. This should be
    run before bandpass or gain calibration.

    :param ms: The measurement set to operate on
    :param cfg: Setjy configuration
    """
    logger.info(f"Running setjy on {ms}")
    setjy(vis=str(ms), **cfg.to_kwargs())


def run_bandpass(ms: Path, cfg: BandpassConfig) -> Path:
    """Derive a bandpass calibration solution.

    Solves for the frequency-dependent gain of each antenna across the band.
    Should be run after setjy and before gaincal.

    :param ms: The measurement set to operate on
    :param cfg: Bandpass configuration
    :returns: Path to the bandpass calibration table
    """
    caltable = ms.with_suffix(".bpcal")
    logger.info(f"Running bandpass on {ms} -> {caltable}")
    bandpass(vis=str(ms), caltable=str(caltable), **cfg.to_kwargs())
    if not caltable.exists():
        raise RuntimeError(f"Bandpass failed to produce calibration table: {caltable}")
    return caltable


def run_gaincal(ms: Path, cfg: GaincalConfig, apply_tables: list[Path] | None = None) -> Path:
    """Derive a gain calibration solution.

    Solves for the time-dependent complex gains of each antenna. Can solve for
    amplitude, phase, or both depending on calmode. Pre-applies any provided
    calibration tables (e.g. bandpass) during solving.

    :param ms: The measurement set to operate on
    :param cfg: Gaincal configuration
    :param apply_tables: Calibration tables to pre-apply during solving
    :returns: Path to the gain calibration table
    """
    caltable = ms.with_suffix(".gcal")
    logger.info(f"Running gaincal on {ms} -> {caltable}")
    gaincal(
        vis=str(ms),
        caltable=str(caltable),
        gaintable=[str(t) for t in apply_tables] if apply_tables else [],
        **cfg.to_kwargs(),
    )
    if not caltable.exists():
        raise RuntimeError(f"Gaincal failed to produce calibration table: {caltable}")
    return caltable


def run_applycal(ms: Path, cfg: ApplycalConfig, apply_tables: list[Path]) -> None:
    """Apply calibration solutions to the measurement set.

    Applies the provided calibration tables to the target data, writing
    corrected visibilities to the CORRECTED_DATA column.

    :param ms: The measurement set to operate on
    :param cfg: Applycal configuration
    :param apply_tables: Calibration tables to apply
    """
    logger.info(f"Running applycal on {ms} with tables: {apply_tables}")
    applycal(
        vis=str(ms),
        gaintable=[str(t) for t in apply_tables],
        **cfg.to_kwargs(),
    )


def run_split(ms: Path, cfg: SplitConfig) -> Path:
    """Split out the calibrated data into a new measurement set.

    Extracts the corrected data column into a new, lighter measurement set.
    This is typically the final step in calibration, producing the output
    that will be passed to imaging.

    :param ms: The measurement set to split
    :param cfg: Split configuration
    :returns: Path to the newly created measurement set
    """
    output = calibrated_target_name(ms)
    if output.exists():
        # CASA raises an error if the target already exists
        shutil.rmtree(output)
    logger.info(f"Running split on {ms} -> {output}")
    split(vis=str(ms), outputvis=str(output), **cfg.to_kwargs())
    return output


def calibrate_observation(ctx: CalibrateContext) -> Path:
    """Calibrate an observation using the given configuration.

    Runs the cfgured calibration steps in the correct order: setjy,
    bandpass, gaincal, applycal, then split. Each step derives its own
    output path from the input ms. Calibration tables are automatically
    passed forward to subsequent steps that need them.

    :param ctx: The calibrate context object
    :raises TypeError: If the input file is not a measurement set
    :raises ValueError: If no calibration steps are cfgured
    :returns: Path to the calibrated measurement set
    """
    logger.info(f"Running calibration on source {ctx.tgt} using calibrator {ctx.cal}")
    run_setjy(ctx.cal, ctx.cfg.setjy)

    apply_tables: list[Path] = []
    apply_tables.append(run_bandpass(ctx.cal, ctx.cfg.bandpass))
    apply_tables.append(run_gaincal(ctx.cal, ctx.cfg.gaincal, apply_tables))
    run_applycal(ctx.tgt, ctx.cfg.applycal, apply_tables)

    calibrated_tgt = run_split(ctx.tgt, ctx.cfg.split)
    logger.info(f"Calibration complete. Written to {calibrated_tgt}")

    return calibrated_tgt


def main():
    parser = CalibrateConfig.add_to_parser(
        ArgumentParser("Calibrate a target measurement set using a calibrator measurement set.")
    )
    parser.add_argument("--cal", type=Path, required=True, help="The path to the calibrator measurement set")
    parser.add_argument("--tgt", type=Path, required=True, help="The path to the target measurement set")
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

    calibrate_observation(CalibrateContext(cfg=CalibrateConfig.from_namespace(args), ms=args.ms))


if __name__ == "__main__":
    main()
