from pathlib import Path
from needle.config.pipeline import NeedleConfig


def get_config() -> NeedleConfig:
    """Attempts to load the pipeline config from the expected location"""
    # cfg_path cannot be overridden. It must be static since CASA's config.py relies on it for configuration.
    cfg_path = Path.home() / Path(".needle.yaml")
    if not cfg_path.exists():
        raise FileNotFoundError(f"Expected file {cfg_path} does not exist. See setup_env.sh for assistance")
    return NeedleConfig.from_yaml(cfg_path)
