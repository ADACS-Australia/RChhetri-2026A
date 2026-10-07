"""
Handles finding and grouping calibrator and target observations into their respective BeamPairs
"""

import logging
from pathlib import Path
import re

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution, CalInput

logger = logging.getLogger(__name__)

TGT_PATTERN = r"(?!cal_)(?P<name>.+)_beam(?P<beam>\d{2})\.(uvfits|mir|ms)$"
CAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.(uvfits|mir|ms)$"
BPCAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.bpcal$"
GCAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.gcal$"
BEAM_DIR_PATTERN = r"beam\d{2}$"

# Priority of formats to use. Best last.
FORMAT_PRIORITY = (".mir", ".uvfits" ".ms")


def _sort_key(path: Path) -> tuple[int, str]:
    """Order by format preference, then name so ties are deterministic.

    Suffixes not in the list (.bpcal, .gcal) get -1; they never compete, so it makes no difference.
    """
    rank = FORMAT_PRIORITY.index(path.suffix) if path.suffix in FORMAT_PRIORITY else -1
    return rank, path.name


def _index(directories: list[Path], pattern: str) -> dict[str, Path]:
    """Map beam number -> path for entries matching `pattern` across `directories`.

    Later directories win on duplicates, with a warning.
    """

    found: dict[str, Path] = {}
    for directory in directories:
        for path in sorted(directory.iterdir(), key=_sort_key):
            if m := re.match(pattern, path.name):
                beam = m.group("beam")
                if beam in found:
                    if found[beam].parent == directory:
                        logger.info(
                            f"Beam {beam} has multiple formats in {directory}, using {path.name} over {found[beam].name}"
                        )
                    else:
                        logger.warning(f"Beam {beam} found in both {found[beam].parent} and {directory}, using {path}")
                found[beam] = path
    return found


def find_beam_pairs(
    search_dir: Path,
    tgt_pattern: str = TGT_PATTERN,
    cal_pattern: str = CAL_PATTERN,
    bpcal_pattern: str = BPCAL_PATTERN,
    gcal_pattern: str = GCAL_PATTERN,
    beam_dir_pattern: str = BEAM_DIR_PATTERN,
) -> list[BeamPair]:
    """Match targets and calibrators, prioritising calibration solutions, by beam number within a staged observation
    directory.

    :param search_dir: The directory to search for beam pairs
    :param tgt_pattern: The regex pattern to use for target sources
    :param cal_pattern: The regex pattern to use for calibrator sources
    :param bpcal_pattern: The regex pattern to use for bandpass calibration solutions
    :param gcal_pattern: The regex pattern to use for gain calibration solutions
    :param beam_dir_pattern: The regex pattern to use for finding pre-existing beam directories
    :param format_priority: Priority list
    """
    # Beam dirs go last so already-staged files win. Identify them by name, as .ms/.mir are directories too.
    beam_dirs = sorted(p for p in search_dir.iterdir() if p.is_dir() and re.fullmatch(beam_dir_pattern, p.name))
    dirs = [search_dir, *beam_dirs]

    targets = _index(dirs, tgt_pattern)
    calibrators = _index(dirs, cal_pattern)
    bpcals = _index(dirs, bpcal_pattern)
    gcals = _index(dirs, gcal_pattern)

    # Seaerch for targets, calibrators and calibrator solutions. A solution needs a .gcal and a .bpcal
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

    # One calibration input per beam; the second dict overrides the first
    cal_inputs: dict[str, CalInput] = {
        **{b: Path(p) for b, p in calibrators.items()},
        **{b: CalibrationSolution(bpcal=bpcals[b], gcal=gcals[b]) for b in solved_beams},  # solutions win
    }

    # Only beams with both a target and calibration input (observation or solution) are usable
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
