import logging
from pathlib import Path
import re
from typing import Optional
import yaml

from pydantic import ValidationError, model_validator, field_validator

from needle.models.base import NeedleModel
from needle.models.calibrate import CalibrateConfig
from needle.models.clean import ShallowCleanConfig, DeepCleanConfig, IntervalCleanConfig
from needle.models.flag import FlagConfig
from needle.models.mask import CreateMaskConfig
from needle.models.source_find import SourceFindConfig
from needle.lib.flow import CONTAINER_DATA_DIR, BEAMS_DIR

logger = logging.getLogger(__name__)


class BeamPair(NeedleModel):
    """A matched target/calibrator pair belonging to the same beam."""

    beam: str
    "Beam identifier e.g. '00'"

    tgt: Path
    "Path to the target input file — any supported format"

    cal: Path
    "Path to the calibrator input file — any supported format"

    @property
    def beam_dir(self) -> Path:
        "Working directory for this beam"
        return BEAMS_DIR / f"beam{self.beam}"

    def setup_beam_dir(self) -> Path:
        "Create the working directory for the beam"
        # Do not make parents! This can lead to issues if multiple processes attempt to create the parent concurrently
        self.beam_dir.mkdir(parents=False, exist_ok=True)
        return self.beam_dir


class MSBeamPair(BeamPair):
    """A beam pair where both files are guaranteed to be measurement sets."""

    tgt: Path
    "Path to the target measurement set"

    cal: Path
    "Path to the calibrator measurement set"

    @model_validator(mode="after")
    def validate_ms_suffixes(self):
        for field, path in [("target", self.tgt), ("calibrator", self.cal)]:
            if path.suffix != ".ms":
                raise ValueError(f"{field} must be a measurement set, got {path}")
        return self

    @model_validator(mode="after")
    def validate_exists(self):
        if not self.tgt.exists():
            raise ValidationError(f"{self.tgt} does not exist. Cannot construct model.")
        if not self.cal.exists():
            raise ValidationError(f"{self.cal} does not exist. Cannot construct model.")
        return self


class PipelineFlowConfig(NeedleModel):
    """Flow-level configuration"""

    tgt_pattern: str
    "Path to the target Input file. Can be one of .mir, .uvfits or .ms"

    cal_pattern: str
    "Path to the calibator Input file. Can be one of .mir, .uvfits or .ms"

    local_data_dir: Path
    "Local path containing the input data files"

    overwrite: bool
    "Whether to overwrite any existing data"

    shm_size: str = "2gb"
    "Size of /dev/shm in the runtime container"

    log_level: str = "INFO"
    "Logging level"

    max_workers: Optional[int] = None
    "Maximum number of worker processes for concurrent task execution"

    @field_validator("log_level")
    @classmethod
    def valid_log_level(cls, v: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}, got '{v}'")
        return upper  # normalise to uppercase

    @property
    def beam_pairs(self) -> list[BeamPair]:
        "Match targets and calibrators by beam number"
        targets = {
            m.group("beam"): path
            for path in CONTAINER_DATA_DIR.iterdir()
            if (m := re.match(self.tgt_pattern, path.name))
        }
        calibrators = {
            m.group("beam"): path
            for path in CONTAINER_DATA_DIR.iterdir()
            if (m := re.match(self.cal_pattern, path.name))
        }

        matched = targets.keys() & calibrators.keys()
        unmatched_targets = targets.keys() - matched
        unmatched_calibrators = calibrators.keys() - matched

        if unmatched_targets:
            logger.warning(f"Targets with no calibrator match for beams: {unmatched_targets}")
        if unmatched_calibrators:
            logger.warning(f"Calibrators with no target match for beams: {unmatched_calibrators}")
        if not matched:
            raise ValueError(f"No matching beam pairs found in {CONTAINER_DATA_DIR}")

        return [BeamPair(beam=beam, tgt=targets[beam], cal=calibrators[beam]) for beam in sorted(matched)]


class PipelineConfig(NeedleModel):
    """The top-level config model, merges the flow config and the task cfgs"""

    flow: PipelineFlowConfig
    "flow-level configuration — file discovery and beam matching"

    flag: FlagConfig
    "Flagging config"

    calibrate: CalibrateConfig
    "Calibration config"

    shallow_clean: ShallowCleanConfig
    "Shallow clean config"

    source_find: SourceFindConfig
    "Source find config"

    create_mask: CreateMaskConfig
    "Mask creation config"

    deep_clean: DeepCleanConfig
    "Deep clean config"

    interval_clean: IntervalCleanConfig
    "Deep clean config"

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        with open(path) as f:
            merged = yaml.safe_load(f)
        try:
            return cls.model_validate(merged)
        except ValidationError as e:
            missing = [err["loc"][0] for err in e.errors() if err["type"] == "missing"]
            if missing:
                fields = ", ".join(f"'{f}'" for f in missing)
                raise ValueError(f"Config file {path} is missing required section(s): {fields}") from e
            raise
