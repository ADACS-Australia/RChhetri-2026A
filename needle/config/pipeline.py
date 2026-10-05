import logging
import os
from pathlib import Path
import traceback
from typing import Optional, Literal
import yaml

from pydantic import ValidationError, model_validator

from needle.config.base import NeedleModel
from needle.config.calibrate import SolveCalibrationConfig, ApplyCalibrationConfig
from needle.config.clean import ShallowCleanConfig, DeepCleanConfig, IntervalCleanConfig, ModelSubtractCleanConfig
from needle.config.data import DataConfig
from needle.config.flag import FlagConfig
from needle.config.mask import CreateMaskConfig
from needle.config.source_find import SourceFindConfig
from needle.config.watcher import WatcherConfig

logger = logging.getLogger(__name__)


class ConfigLoadError(Exception):
    """The config source couldn't be read into a dict of sections."""


class PipelineFlowConfig(NeedleModel):
    """Flow-level configuration"""

    overwrite: bool = True
    "Whether to overwrite any existing data"

    shm_size: str = "2gb"
    "Size of /dev/shm in the runtime container"

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    "Logging level"

    max_threads: Optional[int] = None
    "Maximum number of thread worker processes for concurrent task execution"

    interval_tasks: int = 1
    "The number of tasks to split the interval cleaning into per beam"

    skip_to_deep_clean: bool = False
    "Skip shallow clean and source finding by using WSClean's auto-masking to determine source locations. Requires deep_clean.auto_mask to be set."


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

    calibrate_solve: SolveCalibrationConfig = SolveCalibrationConfig()
    "Calibration solution finding config"

    calibrate_apply: ApplyCalibrationConfig = ApplyCalibrationConfig()
    "Calibration solution application config"

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

    @model_validator(mode="after")
    def _check_skip_to_deep_clean(self) -> "NeedleConfig":
        """If the skip_to_deep_clean flag is set or unset in the PipelineFlowConfig, it should also be set appropriately
        in the deep_clean_config."""
        if self.flow.skip_to_deep_clean:
            if self.deep_clean.auto_mask is None or self.interval_clean.auto_mask is None:
                raise ValueError(
                    "deep_clean.auto_mask and interval_clean.auto_mask must be set for flow.skip_to_deep_clean to be set"
                )
            if self.deep_clean.auto_threshold is None or self.interval_clean.auto_threshold is None:
                raise ValueError(
                    "deep_clean.auto_threshold and interval_clean.auto_threshold must be set for flow.skip_to_deep_clean to be set"
                )
        elif self.deep_clean.auto_mask:
            logging.warning("deep_clean.auto_mask is set but flow.skip_to_deep_clean is not. Consider turning it on")
        elif self.interval_clean.auto_mask:
            logging.warning(
                "interval_clean.auto_mask is set but flow.skip_to_deep_clean is not. Consider turning it on"
            )
        return self

    @classmethod
    def load(cls, source: Path | str | dict) -> "NeedleConfig":
        """Constructs the object from a .yaml path or dictionary

        :param source: Path to the .yaml config file, or a dictionary of config data
        :raises ValueErorr: Raised if the config is not valid
        :returns: The NeedleConfig object constructed from the .yaml file
        """
        if isinstance(source, dict):
            data = source
        else:
            with open(Path(source)) as f:
                data = yaml.safe_load(f)
        try:
            return cls.model_validate(data)
        except ValidationError as e:
            missing = [err["loc"][0] for err in e.errors() if err["type"] == "missing"]
            if missing:
                fields = ", ".join(f"'{f}'" for f in missing)
                raise ValueError(f"Config is missing required section(s): {fields}") from e
            raise

    @classmethod
    def get_config(cls) -> "NeedleConfig":
        """Attempts to load the pipeline config from the expected location

        :raises FileNotFoundError: Raised if the config file is not found in the expected location
        :returns: The NeedleConfig object constructed from the .yaml file
        """
        # cfg_path cannot be overridden. It must be static since CASA's config.py relies on it for configuration.
        cfg_path = Path(os.environ.get("NEEDLE_CONFIG", Path.home() / Path(".needle.yaml")))
        if not cfg_path.exists():
            raise FileNotFoundError(f"Expected file {cfg_path} does not exist. See setup_env.sh for assistance")
        return NeedleConfig.load(cfg_path)

    @classmethod
    def validate(cls, source: str | Path | dict, quiet: bool = False, full_traceback: bool = False) -> bool:
        BOLD = "\033[1m"
        RED = "\033[91m"
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        RESET = "\033[0m"

        def emit(msg: str, fmt: str = ""):
            if not quiet:
                print(f"{fmt}{msg}{RESET}")

        def clean_msg(msg: str) -> str:
            # Pydantic prefixes ValueErrors raised in validators
            return msg.removeprefix("Value error, ")

        def emit_validation_errors(exc: ValidationError, indent: str = "  "):
            for err in exc.errors():
                loc = " -> ".join(str(i) for i in err["loc"])
                prefix = f"{loc}: " if loc else ""
                emit(f"{indent}• {BOLD}{prefix}{RESET}{RED}{clean_msg(err['msg'])}", RED)

        emit("\n--- Config Validation ---", BOLD)
        # Try to read in the config file
        try:
            raw = cls._read_raw(source)
        except ConfigLoadError as e:
            emit(f"  ✗ {e}\n", RED)
            if full_traceback:
                emit(traceback.format_exc(), RED)
            return False

        errors = {}
        validated = {}

        for f, field_info in cls.model_fields.items():
            section_type = field_info.annotation
            section_data = raw.get(f)
            try:
                validated[f] = section_type.model_validate(section_data or {})
            except ValidationError as e:
                errors[f] = e

        for f in cls.model_fields:
            if f in validated:
                emit(f"  ✓ {f}: {type(validated[f]).__name__}", GREEN)
            elif f in errors:
                emit(f"  ✗ {f}: FAILED", RED)

        if errors:
            emit(f"\n{len(errors)} section(s) failed validation:\n", BOLD)
            for f, exc in errors.items():
                emit(f"[{f}]", YELLOW)
                emit_validation_errors(exc)
                emit("")
            return False

        emit("\nAll sections validated OK. Checking cross-section rules...\n", BOLD)
        try:
            cls.load(raw)
            emit("  ✓ Full config loaded successfully", GREEN)
            return True
        except ValidationError as e:
            emit("  ✗ Config failed cross-section validation:\n", RED)
            emit_validation_errors(e, indent="    ")
            if full_traceback:
                emit(f"\n{traceback.format_exc()}", RED)
            emit("")
        except Exception as e:
            emit(f"  ✗ Unexpected error loading config: {e}", RED)
            if full_traceback:
                emit(f"\n{traceback.format_exc()}", RED)

        return False

    @staticmethod
    def _read_raw(source: str | Path | dict) -> dict:
        """Reads the raw config dict from a path or passes a dict straight through.

        :raises ConfigLoadError: with a user-friendly message if the source can't be loaded
        """
        if isinstance(source, dict):
            return source

        path = Path(source)
        try:
            data = yaml.safe_load(path.read_text())
        except (OSError, UnicodeDecodeError) as e:
            reason = getattr(e, "strerror", None) or str(e)
            raise ConfigLoadError(f"Could not read config file '{path}':\n    {reason}") from e
        except yaml.YAMLError as e:
            msg = f"'{path}' is not valid YAML:\n"
            mark = getattr(e, "problem_mark", None)
            if mark is not None:
                # PyYAML marks are 0-indexed
                msg += f"\n    Line {mark.line + 1}, column {mark.column + 1}: {getattr(e, 'problem', e)}"
                if snippet := mark.get_snippet():
                    msg += f"\n\n{snippet}"
            else:
                msg += f"\n    {e}"
            raise ConfigLoadError(msg) from e

        if data is None:
            return {}  # empty file: let section validation report what's missing
        if not isinstance(data, dict):
            raise ConfigLoadError(
                f"'{path}' must contain a mapping of config sections at the top level, "
                f"but found a {type(data).__name__}."
            )
        return data
