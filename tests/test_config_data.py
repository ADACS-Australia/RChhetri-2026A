import pytest
from pydantic import ValidationError
from needle.config.data import DataConfig


def test_data_config_valid_local():
    """Test DataConfig with valid local path."""
    cfg = DataConfig(source="local:///tmp/data", staging_dir="/tmp/staged")
    assert cfg.source == "local:///tmp/data"


def test_data_config_valid_s3():
    """Test DataConfig with valid S3 path."""
    cfg = DataConfig(source="s3://bucket/prefix", staging_dir="/tmp/staged")
    assert cfg.source == "s3://bucket/prefix"


def test_data_config_invalid_local_relative():
    """Test DataConfig with invalid relative local path."""
    with pytest.raises(ValidationError, match="Local source path must be absolute"):
        DataConfig(source="local://./data", staging_dir="/tmp/staged")


def test_data_config_no_scheme_absolute():
    """Test DataConfig with absolute path but no scheme (should be treated as local)."""
    cfg = DataConfig(source="/tmp/data", staging_dir="/tmp/staged")
    assert cfg.source == "/tmp/data"
