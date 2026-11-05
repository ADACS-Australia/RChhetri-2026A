import logging
from pathlib import Path

from needle.config.base import NeedleModel
from needle.config.calibrate import CalInput

logger = logging.getLogger(__name__)


def _move(path: Path, new_dir: Path) -> Path:
    dest = new_dir / path.name
    if path == dest:  # already staged
        return path
    if dest.exists():
        raise FileExistsError(f"Cannot move {path} -> {dest}: destination already exists")
    path.rename(dest)
    return dest


class BeamPair(NeedleModel):
    """A matched target/calibrator pair belonging to the same beam."""

    beam: str
    "Beam identifier e.g. '00'"

    name: str
    "Name of the observation e.g. 'SB123456'"

    tgt: Path
    "Path to the target input file"

    cal: CalInput
    "The calibrator observation or solution"

    @property
    def path(self) -> Path:
        "Main data file for this beam"
        return self.tgt

    def move_files(self, new_dir):
        "Move the tgt and cal to a new directory"
        # Do not make parents! This can lead to issues if multiple processes attempt to create the parent concurrently
        new_dir.mkdir(parents=False, exist_ok=True, mode=0o770)
        self.move_tgt(new_dir)
        self.move_cal(new_dir)

    def move_tgt(self, new_dir: Path):
        "Moves the target to a new location"
        new_dir.mkdir(parents=False, exist_ok=True, mode=0o770)
        self.tgt = _move(self.tgt, new_dir)

    def move_cal(self, new_dir: Path):
        "Moves the calibrator to a new location"
        new_dir.mkdir(parents=False, exist_ok=True, mode=0o770)
        if isinstance(self.cal, Path):
            self.cal = _move(self.cal, new_dir)
        else:
            self.cal.bpcal = _move(self.cal.bpcal, new_dir)
            self.cal.gcal = _move(self.cal.gcal, new_dir)


class CalibratedBeam(NeedleModel):
    """A calibrated target"""

    beam: str
    "Beam identifier e.g. '00'"

    name: str
    "Name of the observation e.g. 'SB123456'"

    ms: Path
    "Path to the calibrated measurement set"

    @property
    def path(self) -> Path:
        "Main data file for this beam"
        return self.ms

    def move_files(self, new_dir: Path):
        "Move the calibrated MS to a new directory"
        # Do not make parents! This can lead to issues if multiple processes attempt to create the parent concurrently
        new_dir.mkdir(parents=False, exist_ok=True, mode=0o770)
        self.ms = _move(self.ms, new_dir)


BeamInput = BeamPair | CalibratedBeam
