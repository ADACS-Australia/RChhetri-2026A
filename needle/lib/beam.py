from pathlib import Path
import re

from needle.config.calibrate import CalInput, CalibrationSolution


def find_beam_dir(path: Path | CalInput | str) -> Path:
    """Finds the beam directory, given a full path that includes it"""
    if isinstance(path, str):
        path = Path(path)
    if isinstance(path, CalibrationSolution):
        path = path.gcal
    p = path.resolve()
    for parent in [p, *p.parents]:
        if re.fullmatch(r"beam\d{1,2}", parent.name):
            return parent
    raise ValueError(f"No beamNN directory found in {path}")
