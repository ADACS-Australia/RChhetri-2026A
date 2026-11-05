from pathlib import Path
from pydantic import BaseModel
import re

from needle.config.calibrate import CalInput, CalibrationSolution


class NeedleNames(BaseModel):
    """Class for finding and generating names and paths of things from an input path"""

    path: Path | CalInput | str
    "Path to either the beam directory or anything in it"

    @property
    def beam_dir(self) -> Path:
        """Returns the directory of the beam."""
        return self._find_beam_dir()

    @property
    def observation_dir(self) -> Path:
        """Returns the name of the observation."""
        return self.beam_dir.parent

    @property
    def calibrated_image_path(self) -> Path:
        """Returns the name of the calibrated image."""
        obs_name = self.observation_dir.name
        beam_name = self.beam_dir.name
        return self.beam_dir / f"{obs_name}_{beam_name}_calibrated.ms"

    def _find_beam_dir(self) -> Path:
        """Finds the beam directory, provided self.path includes it"""
        path = self.path
        if isinstance(self.path, str):
            path = Path(self.path)
        elif isinstance(self.path, CalibrationSolution):
            path = self.path.gcal
        p = path.resolve()
        for parent in [p, *p.parents]:
            if re.fullmatch(r"beam\d{1,2}", parent.name):
                return parent
        raise ValueError(f"No beamNN directory found in {path}")
