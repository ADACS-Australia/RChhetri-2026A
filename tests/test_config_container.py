import pytest
from pathlib import Path
from needle.config.container import ContainerConfig
from pydantic import ValidationError


def test_container_config_validation(tmp_path):
    """Test validation logic for ContainerConfig."""
    image_path = tmp_path / "test.sif"
    image_path.touch()

    # Valid config
    config = ContainerConfig(image=image_path)
    assert config.image == image_path

    # Invalid suffix
    bad_suffix = tmp_path / "test.img"
    bad_suffix.touch()
    with pytest.raises(ValidationError, match="Expected a .sif file"):
        ContainerConfig(image=bad_suffix)

    # Non-existent file
    with pytest.raises(ValidationError, match="Container image not found"):
        ContainerConfig(image=tmp_path / "missing.sif")


def test_container_config_to_args(tmp_path):
    """Test converting ContainerConfig to command line arguments."""
    image_path = tmp_path / "test.sif"
    image_path.touch()

    config = ContainerConfig(
        image=image_path, binds=[Path("/tmp")], env={"KEY": "VALUE"}, writable=True, type="apptainer"
    )
    args = config.to_args()
    assert "apptainer" in args
    assert "exec" in args
    assert "--writable" in args
    assert "--bind" in args
    assert "/tmp" in args
    assert "--env" in args
    assert "KEY=VALUE" in args
    assert str(image_path) in args
