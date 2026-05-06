import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from needle.lib.config import get_config
from needle.config.pipeline import NeedleConfig


def test_get_config_no_file():
    """Test error handling when the needle config file is missing."""
    # Mock Path.home to return a specific directory
    mock_home = Path("/mock/home")

    with patch("pathlib.Path.home", return_value=mock_home):
        # We also need to ensure exists() returns False for this path
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Expected file /mock/home/.needle.yaml does not exist"):
                get_config()


def test_get_config_success():
    """Test successful loading of the needle pipeline configuration."""
    mock_home = Path("/mock/home")
    mock_pipeline_config = MagicMock(spec=NeedleConfig)

    with patch("pathlib.Path.home", return_value=mock_home):
        with patch("pathlib.Path.exists", return_value=True):
            with patch("needle.config.pipeline.NeedleConfig.from_yaml", return_value=mock_pipeline_config):
                config = get_config()
                assert config == mock_pipeline_config
