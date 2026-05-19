import logging
from pathlib import Path
from typing import Optional, Literal
import yaml

from pydantic import ValidationError

from needle.config.base import NeedleModel
from needle.config.container import ContainerConfig
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

    flow: PipelineFlowConfig = PipelineFlowConfig()
    "Flow-level configuration for the pipeline"

    data: DataConfig = DataConfig()
    "Config for the data specifics"

    watcher: WatcherConfig = WatcherConfig()
    "Config for the Watcher"

    flag: FlagConfig = FlagConfig()
    "Flagging config"

    calibrate: CalibrateConfig = CalibrateConfig()
    "Calibration config"

    shallow_clean: ShallowCleanConfig = ShallowCleanConfig()
    "Shallow clean config"

    source_find: SourceFindConfig = SourceFindConfig()
    "Source find config"

    create_mask: CreateMaskConfig = CreateMaskConfig()
    "Mask creation config"

    deep_clean: DeepCleanConfig = DeepCleanConfig()
    "Deep clean config"

    model_subtract: ModelSubtractCleanConfig = ModelSubtractCleanConfig()
    "Deep clean config"

    interval_clean: IntervalCleanConfig = IntervalCleanConfig()
    "Deep clean config"

    @classmethod
    def from_yaml(cls, path: Path) -> "NeedleConfig":
        """Constructs the object from a .yaml file

        :param path: Path to the .yaml config file
        :raises ValueErorr: Raised if the config is not valid
        :returns: The NeedleConfig object constructed from the .yaml file
        """
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
    def get_config(cls) -> "NeedleConfig":
        """Attempts to load the pipeline config from the expected location

        :raises FileNotFoundError: Raised if the config file is not found in the expected location
        :returns: The NeedleConfig object constructed from the .yaml file
        """
        # cfg_path cannot be overridden. It must be static since CASA's config.py relies on it for configuration.
        cfg_path = Path.home() / Path(".needle.yaml")
        if not cfg_path.exists():
            raise FileNotFoundError(f"Expected file {cfg_path} does not exist. See setup_env.sh for assistance")
        return NeedleConfig.from_yaml(cfg_path)
