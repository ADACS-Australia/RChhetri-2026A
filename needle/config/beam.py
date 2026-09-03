import logging
from pathlib import Path

from needle.config.base import NeedleModel
from needle.config.calibrate import CalInput

logger = logging.getLogger(__name__)


class BeamPair(NeedleModel):
    """A matched target/calibrator pair belonging to the same beam."""

    beam: str
    "Beam identifier e.g. '00'"

    tgt: Path
    "Path to the target input file"

    cal: CalInput
    "The calibrator observation or solution"

    def move_files(self, new_dir):
        "Move the tgt and cal to a new directory"
        # Do not make parents! This can lead to issues if multiple processes attempt to create the parent concurrently
        new_dir.mkdir(parents=False, exist_ok=True)
        self.move_tgt(new_dir)
        self.move_cal(new_dir)

    def move_tgt(self, new_dir: Path):
        "Moves the target to a new location"
        new_dir.mkdir(parents=False, exist_ok=True)
        new_path = new_dir / self.tgt.name
        self.tgt.rename(new_path)
        self.tgt = new_path

    def move_cal(self, new_dir: Path):
        "Moves the calibrator to a new location"
        new_dir.mkdir(parents=False, exist_ok=True)
        if isinstance(self.cal, Path):
            new_path = new_dir / self.cal.name
            self.cal.rename(new_path)
            self.cal = new_path
        else:  # CalibrationSolution
            new_path = new_dir / self.cal.bpcal.name
            self.cal.bpcal.rename(new_path)
            self.cal.bpcal = new_path
            new_path = new_dir / self.cal.gcal.name
            self.cal.gcal.rename(new_path)
            self.cal.gcal = new_path
