import logging
from pathlib import Path

from pydantic import model_validator

from needle.config.base import NeedleModel

logger = logging.getLogger(__name__)


class BeamPair(NeedleModel):
    """A matched target/calibrator pair belonging to the same beam."""

    beam: str
    "Beam identifier e.g. '00'"

    tgt: Path
    "Path to the target input file"

    cal: Path
    "Path to the calibrator input file"

    def move_files(self, new_dir):
        "Move the tgt and cal to a new directory"
        # Do not make parents! This can lead to issues if multiple processes attempt to create the parent concurrently
        new_dir.mkdir(parents=False, exist_ok=True)
        self.move_tgt(new_dir)
        self.move_cal(new_dir)

    def move_tgt(self, new_dir: Path):
        "Moves the target to a new location"
        new_dir.mkdir(parents=False, exist_ok=True)
        new_path = new_dir / self.tgt.stem
        self.tgt.rename(new_path)
        self.tgt = new_path

    def move_cal(self, new_dir: Path):
        "Moves the calibrator to a new location"
        new_dir.mkdir(parents=False, exist_ok=True)
        new_path = new_dir / self.cal.stem
        self.cal.rename(new_path)
        self.cal = new_path


class MSBeamPair(BeamPair):
    """A beam pair where both files are guaranteed to be measurement sets."""

    @model_validator(mode="after")
    def validate_ms_suffixes(self):
        for field, path in [("target", self.tgt), ("calibrator", self.cal)]:
            if path.suffix != ".ms":
                raise ValueError(f"{field} must be a measurement set, got {path}")
        return self

    @model_validator(mode="after")
    def validate_exists(self):
        if not self.tgt.exists():
            raise ValueError(f"{self.tgt} does not exist. Cannot construct model.")
        if not self.cal.exists():
            raise ValueError(f"{self.cal} does not exist. Cannot construct model.")
        return self
