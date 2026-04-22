from pathlib import Path
from needle.config.pipeline import PipelineConfig


def get_config() -> PipelineConfig:
    """Attempts to load the pipeline config from the expected location"""
    cfg_path = Path.home() / Path(".needle.yaml")
    if not cfg_path.exists():
        raise FileNotFoundError(f"Expected file {cfg_path} does not exist. See setup_env.sh for assistance")
    return PipelineConfig.from_yaml(cfg_path)
