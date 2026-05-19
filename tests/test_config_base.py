from argparse import ArgumentParser
from needle.config.base import NeedleModel


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
