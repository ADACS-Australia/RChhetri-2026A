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

    parent_dir: Path
    "Path to the directory to put this BeamPair into"

    @property
    def beam_dir(self) -> Path:
        "Working directory for this beam"
        return self.parent_dir / f"beam{self.beam}"

    def setup_beam_dir(self) -> Path:
        "Create the working directory for the beam"
        # Do not make parents! This can lead to issues if multiple processes attempt to create the parent concurrently
        self.beam_dir.mkdir(parents=False, exist_ok=True)
        return self.beam_dir


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
