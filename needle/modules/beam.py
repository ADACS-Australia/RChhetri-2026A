import logging
from pathlib import Path
import re

from needle.config.beam import BeamPair

logger = logging.getLogger(__name__)


def find_beam_pairs(
    search_dir: Path,
    tgt_pattern: str = r"(?!cal_)(?P<name>.+)_beam(?P<beam>\d{2})\.(uvfits|mir|ms)",
    cal_pattern: str = r"cal_beam(?P<beam>\d{2})\.(uvfits|mir|ms)",
) -> list[BeamPair]:
    """Match targets and calibrators by beam number within a staged observation directory.

    :param search_dir: The directory to search for beam pairs
    :param tgt_pattern: The regex pattern to use for target sources
    :param cal_pattern: The regex pattern to use for calibrator sources
    """
    targets = {m.group("beam"): path for path in search_dir.iterdir() if (m := re.match(tgt_pattern, path.name))}
    calibrators = {m.group("beam"): path for path in search_dir.iterdir() if (m := re.match(cal_pattern, path.name))}

    matched = targets.keys() & calibrators.keys()
    unmatched_targets = targets.keys() - matched
    unmatched_calibrators = calibrators.keys() - matched

    if unmatched_targets:
        logger.warning(f"Targets with no calibrator match for beams: {unmatched_targets}")
    if unmatched_calibrators:
        logger.warning(f"Calibrators with no target match for beams: {unmatched_calibrators}")
    if not matched:
        logger.debug(f"Failed to find beam pairs with patterns: \n{tgt_pattern} \n{cal_pattern}")
        logger.debug(f"Looked in directory and found files: {list(search_dir.iterdir())}")

    return [
        BeamPair(beam=beam, tgt=targets[beam], cal=calibrators[beam], parent_dir=search_dir / f"beam{beam}")
        for beam in sorted(matched)
    ]
