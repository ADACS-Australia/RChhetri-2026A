import logging
from pathlib import Path
from typing import Optional, Literal
import yaml

from pydantic import ValidationError

from needle.config.base import ContainerConfig, NeedleModel
from needle.config.calibrate import CalibrateConfig
from needle.config.clean import ShallowCleanConfig, DeepCleanConfig, IntervalCleanConfig, ModelSubtractCleanConfig
from needle.config.data import DataConfig
from needle.config.flag import FlagConfig
from needle.config.mask import CreateMaskConfig
from needle.config.source_find import SourceFindConfig
from needle.config.watcher import WatcherConfig

logger = logging.getLogger(__name__)


class PipelineFlowConfig(NeedleModel):
    """Flow-level configuration"""

    tgt_pattern: str = r"(?!cal_)(?P<name>.+)_beam(?P<beam>\d{2})\.(uvfits|mir|ms)"
    "Pattern pointing to the target input files. Can be one of .mir, .uvfits or .ms"

    cal_pattern: str = r"cal_beam(?P<beam>\d{2})\.(uvfits|mir|ms)"
    "Path to the calibator Input file. Can be one of .mir, .uvfits or .ms"

    overwrite: bool = True
    "Whether to overwrite any existing data"

    shm_size: str = "2gb"
    "Size of /dev/shm in the runtime container"

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    "Logging level"

    max_workers: Optional[int] = None
    "Maximum number of worker processes for concurrent task execution"

    runtime: Optional[ContainerConfig] = None
    "Runtime information. An optional ContainerConfig. None is interpreted as the local runtime."

    interval_tasks: int = 1
    "The number of tasks to split the interval cleaning into per beam"


class NeedleConfig(NeedleModel):
    """The top-level config model, merges the flow config and the task cfgs"""

    flow: PipelineFlowConfig
    "flow-level configuration for the pipeline"

    data: DataConfig
    "Config for the data specifics"

    watcher: WatcherConfig
    "Config for the Watcher"

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

    model_subtract: ModelSubtractCleanConfig
    "Deep clean config"

    interval_clean: IntervalCleanConfig
    "Deep clean config"

    @classmethod
    def from_yaml(cls, path: Path) -> "NeedleConfig":
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

    @classmethod
    def get_config(
        cls,
    ) -> "NeedleConfig":
        """Attempts to load the pipeline config from the expected location"""
        # cfg_path cannot be overridden. It must be static since CASA's config.py relies on it for configuration.
        cfg_path = Path.home() / Path(".needle.yaml")
        if not cfg_path.exists():
            raise FileNotFoundError(f"Expected file {cfg_path} does not exist. See setup_env.sh for assistance")
        return NeedleConfig.from_yaml(cfg_path)
