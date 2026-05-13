import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from needle.config.pipeline import PipelineFlowConfig, NeedleConfig


def test_pipeline_flow_config_invalid_log_level():
    """Test validation of log level in PipelineFlowConfig."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="Input should be 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'"):
        PipelineFlowConfig(overwrite=False, log_level="INVALID")


def test_get_config_no_file():
    """Test error handling when the needle config file is missing."""
    # Mock Path.home to return a specific directory
    mock_home = Path("/mock/home")

    with patch("pathlib.Path.home", return_value=mock_home):
        # We also need to ensure exists() returns False for this path
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Expected file /mock/home/.needle.yaml does not exist"):
                NeedleConfig.get_config()


def test_get_config_success():
    """Test successful loading of the needle pipeline configuration."""
    mock_home = Path("/mock/home")
    mock_pipeline_config = MagicMock(spec=NeedleConfig)

    with patch("pathlib.Path.home", return_value=mock_home):
        with patch("pathlib.Path.exists", return_value=True):
            with patch("needle.config.pipeline.NeedleConfig.from_yaml", return_value=mock_pipeline_config):
                config = NeedleConfig.get_config()
                assert config == mock_pipeline_config


def test_get_config_integration(tmp_path):
    """Test get_config with a real YAML file and minimal mock."""
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    cfg_path = mock_home / ".needle.yaml"
    
    # Create a minimal valid config
    config_data = {
        "flow": {"overwrite": False},
        "data": {"source": "local:///tmp/data", "staging_dir": "/tmp/staged"},
        "watcher": {},
        "flag": {},
        "calibrate": {
            "setjy": {},
            "bandpass": {},
            "gaincal": {},
            "applycal": {},
            "split": {}
        },
        "shallow_clean": {"niter": 100},
        "source_find": {},
        "create_mask": {},
        "deep_clean": {"niter": 1000},
        "model_subtract": {},
        "interval_clean": {"niter": 500}
    }
    
    import yaml
    with open(cfg_path, "w") as f:
        yaml.dump(config_data, f)
        
    with patch("pathlib.Path.home", return_value=mock_home):
        # We don't mock exists() here, we want it to use the real one on the mock_home
        config = NeedleConfig.get_config()
        
    assert isinstance(config, NeedleConfig)
    assert config.flow.overwrite is False
    assert config.data.source == "local:///tmp/data"
    # Even though we provided {}, SetjyConfig should have its defaults
    assert config.calibrate.setjy.standard == "Perley-Butler 2017"
