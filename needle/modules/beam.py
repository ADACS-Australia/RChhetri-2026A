"""
Handles finding and grouping calibrator and target observations into their respective BeamPairs
"""

import logging
from pathlib import Path
import re

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution, CalInput

logger = logging.getLogger(__name__)

TGT_PATTERN = r"(?!cal_)(?P<name>.+)_beam(?P<beam>\d{2})\.(uvfits|mir|ms)"
CAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.(uvfits|mir|ms)"
BPCAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.bpcal"
GCAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.gcal"


def find_beam_pairs(
    search_dir: Path,
    tgt_pattern: str = TGT_PATTERN,
    cal_pattern: str = CAL_PATTERN,
    bpcal_pattern=BPCAL_PATTERN,
    gcal_pattern=GCAL_PATTERN,
) -> list[BeamPair]:
    """Match targets and calibrators, prioritising calibration solutions, by beam number within a staged observation
    directory.

    :param search_dir: The directory to search for beam pairs
    :param tgt_pattern: The regex pattern to use for target sources
    :param cal_pattern: The regex pattern to use for calibrator sources
    :param bpcal_pattern: The regex pattern to use for bandpass calibration solutions
    :param gcal_pattern: The regex pattern to use for gain calibration solutions
    """
    targets = {m.group("beam"): path for path in search_dir.iterdir() if (m := re.match(tgt_pattern, path.name))}
    calibrators = {m.group("beam"): path for path in search_dir.iterdir() if (m := re.match(cal_pattern, path.name))}
    bpcals = {m.group("beam"): path for path in search_dir.iterdir() if (m := re.match(bpcal_pattern, path.name))}
    gcals = {m.group("beam"): path for path in search_dir.iterdir() if (m := re.match(gcal_pattern, path.name))}

    solved_beams = bpcals.keys() & gcals.keys()
    bp_only = bpcals.keys() - gcals.keys()
    gc_only = gcals.keys() - bpcals.keys()
    if bp_only:
        logger.warning(f".bpcal with no matching .gcal for beams: {bp_only}")
    if gc_only:
        logger.warning(f".gcal with no matching .bpcal for beams: {gc_only}")

    overlap = calibrators.keys() & solved_beams
    if overlap:
        logger.warning(f"Beams with both a calibrator observation and a solution, preferring solution: {overlap}")

    cal_inputs: dict[str, CalInput] = {
        **{b: Path(p) for b, p in calibrators.items()},
        **{b: CalibrationSolution(bpcal=bpcals[b], gcal=gcals[b]) for b in solved_beams},  # solutions win
    }

    matched = targets.keys() & cal_inputs.keys()
    unmatched_targets = targets.keys() - matched
    unmatched_calibrators = cal_inputs.keys() - matched

    if unmatched_targets:
        logger.warning(f"Targets with no calibrator match for beams: {unmatched_targets}")
    if unmatched_calibrators:
        logger.warning(f"Calibrators with no target match for beams: {unmatched_calibrators}")
    if not matched:
        logger.debug(f"Failed to find beam pairs with patterns: \ntgt: {tgt_pattern} \ncal: {cal_pattern}")
        logger.debug(f"Looked in directory and found (unmatched) files: {list(search_dir.iterdir())}")

    return [BeamPair(beam=beam, tgt=targets[beam], cal=cal_inputs[beam]) for beam in sorted(matched)]
