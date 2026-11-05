"""
Finds and groups the observations in a staging directory into per-beam inputs.

Each beam resolves to one of:

- a `CalibratedBeam`: an already calibrated measurement set, ready to use, or
- a `BeamPair`: a target matched with its calibrator observation or calibration solution.

A calibrated measurement set supersedes any raw target/calibrator files for the same beam.
"""

import logging
import re
from pathlib import Path

from needle.config.beam import BeamInput, BeamPair, CalibratedBeam
from needle.config.calibrate import CalibrationSolution, CalInput

logger = logging.getLogger(__name__)

TGT_PATTERN = r"(?!cal_)(?P<name>.+)_beam(?P<beam>\d{2})\.(uvfits|mir|ms)$"
CAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.(uvfits|mir|ms)$"
BPCAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.bpcal$"
GCAL_PATTERN = r"cal_beam(?P<beam>\d{2})\.gcal$"
CALIBRATED_PATTERN = r"(?P<name>.+)_beam(?P<beam>\d{2})_calibrated\.ms$"
BEAM_DIR_PATTERN = r"beam\d{2}$"

# Priority of formats to use. Best last.
FORMAT_PRIORITY = (".mir", ".uvfits", ".ms")


## Indexing helpers ##


def _observation_name(path: Path, pattern: str) -> str:
    """Extract the observation name from a path using the pattern's `name` group."""
    m = re.match(pattern, path.name)
    if m is None or "name" not in m.groupdict():
        raise ValueError(f"Pattern {pattern!r} must match {path.name!r} and define a 'name' group")
    return m.group("name")


def _sort_key(path: Path) -> tuple[int, str]:
    """Order by format preference, then name so ties are deterministic.

    Suffixes not in the list (.bpcal, .gcal) get -1; they never compete, so it makes no difference.
    """
    rank = FORMAT_PRIORITY.index(path.suffix) if path.suffix in FORMAT_PRIORITY else -1
    return rank, path.name


def _index(directories: list[Path], pattern: str) -> dict[str, Path]:
    """Map beam number -> path for entries matching `pattern` across `directories`.

    Within a directory the preferred format wins. Later directories win over earlier ones, with a warning.
    """
    found: dict[str, Path] = {}
    for directory in directories:
        for path in sorted(directory.iterdir(), key=_sort_key):
            m = re.match(pattern, path.name)
            if m is None:
                continue

            beam = m.group("beam")
            if (existing := found.get(beam)) is not None:
                if existing.parent == directory:
                    logger.info(
                        f"Beam {beam} has multiple formats in {directory}, using {path.name} over {existing.name}"
                    )
                else:
                    logger.warning(f"Beam {beam} found in both {existing.parent} and {directory}, using {path}")
            found[beam] = path
    return found


def _find_beam_dirs(search_dir: Path, pattern: str) -> list[Path]:
    """Find pre-existing beam directories. Identified by name, as .ms/.mir are directories too."""
    return sorted(p for p in search_dir.iterdir() if p.is_dir() and re.fullmatch(pattern, p.name))


def _without(index: dict[str, Path], beams: set[str]) -> dict[str, Path]:
    """Return a copy of `index` with the given beams removed."""
    return {beam: path for beam, path in index.items() if beam not in beams}


## Matching helpers ##


def _build_cal_inputs(
    calibrators: dict[str, Path], bpcals: dict[str, Path], gcals: dict[str, Path]
) -> dict[str, CalInput]:
    """Choose one calibration input per beam. A solution (needs both .bpcal and .gcal) beats a calibrator observation."""
    solved = bpcals.keys() & gcals.keys()

    if bp_only := bpcals.keys() - gcals.keys():
        logger.warning(f".bpcal with no matching .gcal for beams: {sorted(bp_only)}")
    if gc_only := gcals.keys() - bpcals.keys():
        logger.warning(f".gcal with no matching .bpcal for beams: {sorted(gc_only)}")
    if overlap := calibrators.keys() & solved:
        logger.warning(
            f"Beams with both a calibrator observation and a solution, preferring solution: {sorted(overlap)}"
        )

    cal_inputs: dict[str, CalInput] = dict(calibrators)
    cal_inputs.update({beam: CalibrationSolution(bpcal=bpcals[beam], gcal=gcals[beam]) for beam in solved})
    return cal_inputs


def _pair_beams(targets: dict[str, Path], cal_inputs: dict[str, CalInput], tgt_pattern: str) -> list[BeamPair]:
    """Pair targets with calibration inputs by beam, warning about anything left over."""
    matched = targets.keys() & cal_inputs.keys()

    if unmatched_targets := targets.keys() - matched:
        logger.warning(f"Targets with no calibrator match for beams: {sorted(unmatched_targets)}")
    if unmatched_cals := cal_inputs.keys() - matched:
        logger.warning(f"Calibrators with no target match for beams: {sorted(unmatched_cals)}")

    return [
        BeamPair(beam=beam, name=_observation_name(targets[beam], tgt_pattern), tgt=targets[beam], cal=cal_inputs[beam])
        for beam in sorted(matched)
    ]


## Public API ##


def find_beams(
    search_dir: Path,
    tgt_pattern: str = TGT_PATTERN,
    cal_pattern: str = CAL_PATTERN,
    bpcal_pattern: str = BPCAL_PATTERN,
    gcal_pattern: str = GCAL_PATTERN,
    calibrated_pattern: str = CALIBRATED_PATTERN,
    beam_dir_pattern: str = BEAM_DIR_PATTERN,
) -> list[BeamInput]:
    """Find the inputs for each beam within a staged observation directory.

    Beams with an already calibrated measurement set are returned as `CalibratedBeam`s and need nothing else. The
    remaining beams are returned as `BeamPair`s, matching targets and calibrators and preferring calibration solutions
    over calibrator observations. Results are sorted by beam number.

    :param search_dir: The directory to search
    :param tgt_pattern: The regex pattern to use for target sources
    :param cal_pattern: The regex pattern to use for calibrator sources
    :param bpcal_pattern: The regex pattern to use for bandpass calibration solutions
    :param gcal_pattern: The regex pattern to use for gain calibration solutions
    :param calibrated_pattern: The regex pattern to use for already calibrated measurement sets
    :param beam_dir_pattern: The regex pattern to use for finding pre-existing beam directories
    """
    # Beam dirs go last so already-staged files win
    dirs = [search_dir, *_find_beam_dirs(search_dir, beam_dir_pattern)]

    calibrated = _index(dirs, calibrated_pattern)
    targets = _index(dirs, tgt_pattern)
    calibrators = _index(dirs, cal_pattern)
    bpcals = _index(dirs, bpcal_pattern)
    gcals = _index(dirs, gcal_pattern)

    # A calibrated MS makes the raw inputs for that beam redundant, so drop them before matching. This also avoids
    # spurious "unmatched" warnings for beams that are already done.
    done = set(calibrated)
    if superseded := done & (targets.keys() | calibrators.keys() | bpcals.keys() | gcals.keys()):
        logger.info(f"Beams with a calibrated MS, ignoring raw target/calibrator inputs: {sorted(superseded)}")

    cal_inputs = _build_cal_inputs(_without(calibrators, done), _without(bpcals, done), _without(gcals, done))
    pairs = _pair_beams(_without(targets, done), cal_inputs, tgt_pattern)

    ready = [
        CalibratedBeam(beam=beam, name=_observation_name(path, calibrated_pattern), ms=path)
        for beam, path in calibrated.items()
    ]

    results: list[BeamInput] = sorted([*ready, *pairs], key=lambda b: b.beam)

    if not results:
        logger.debug(
            f"Failed to find any beams with patterns:\n"
            f"  tgt: {tgt_pattern}\n  cal: {cal_pattern}\n  calibrated: {calibrated_pattern}"
        )
        logger.debug(f"Looked in {search_dir} and found (unmatched) files: {list(search_dir.iterdir())}")

    return results
