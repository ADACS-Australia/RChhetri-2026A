import pytest
from pathlib import Path
from argparse import ArgumentParser
from needle.config.base import NeedleModel, ContainerConfig
from pydantic import ValidationError


class MockModel(NeedleModel):
    name: str
    value: int = 10
    tags: list[str] = []


def test_needle_model_to_kwargs():
    """Test converting NeedleModel to keyword arguments."""
    model = MockModel(name="test", value=20)
    kwargs = model.to_kwargs()
    assert kwargs == {"name": "test", "value": 20, "tags": []}


def test_needle_model_str():
    """Test string representation of NeedleModel."""
    model = MockModel(name="test", value=20)
    assert "MockModel" in str(model)
    assert "name=test" in str(model)
    assert "value=20" in str(model)


def test_needle_model_add_to_parser():
    """Test adding NeedleModel fields to an ArgumentParser."""
    parser = ArgumentParser()
    MockModel.add_to_parser(parser)
    args = parser.parse_args(["--name", "test", "--value", "30", "--tags", "a", "--tags", "b"])
    assert args.name == "test"
    assert args.value == 30
    assert args.tags == ["a", "b"]


def test_needle_model_from_namespace():
    """Test creating NeedleModel from an ArgumentParser namespace."""
    parser = ArgumentParser()
    MockModel.add_to_parser(parser)
    args = parser.parse_args(["--name", "test", "--value", "30"])
    model = MockModel.from_namespace(args)
    assert model.name == "test"
    assert model.value == 30


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
