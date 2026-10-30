from pathlib import Path
import re


def find_beam_dir(path: Path | str) -> Path:
    """Finds the beam directory, given a full path that includes it"""
    p = Path(path).resolve()
    for parent in [p, *p.parents]:
        if re.fullmatch(r"beam\d{1,2}", parent.name):
            return parent
    raise ValueError(f"No beamNN directory found in {path}")
