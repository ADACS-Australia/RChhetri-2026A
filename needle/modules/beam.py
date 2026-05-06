import logging
from pathlib import Path
import re

from needle.config.beam import BeamPair

logger = logging.getLogger(__name__)


def find_beam_pairs(search_dir: Path, tgt_pattern: str, cal_pattern: str) -> list[BeamPair]:
    """Match targets and calibrators by beam number within a staged observation directory."""
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
        raise ValueError(f"No matching beam pairs found in {search_dir}")

    return [
        BeamPair(beam=beam, tgt=targets[beam], cal=calibrators[beam], parent_dir=search_dir / f"beam{beam}")
        for beam in sorted(matched)
    ]
