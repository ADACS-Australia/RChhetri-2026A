from pathlib import Path
from typing import Literal, Optional

import click
from click.testing import CliRunner

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


class Nested(NeedleModel):
    interval: int = 5
    "an interval, nested one level deep"


class DeepNested(NeedleModel):
    inner: Nested = Nested()
    "nested-inside-nested, to check multi-level dot -> __ mangling"


class Sample(NeedleModel):
    name: str
    "a required string"
    count: int = 1
    "a plain int with a default"
    active: bool = False
    "a boolean flag"
    mode: Literal["a", "b", "c"] = "a"
    "a constrained choice"
    tags: list[str] = []
    "a repeatable string list"
    mapping: Optional[dict[str, str]] = None
    "a repeatable KEY=VALUE dict"
    output: Optional[Path] = None
    "an optional path, to check Optional[X] unwrapping"
    nested: Nested = Nested()
    "a nested NeedleModel, one level deep"
    deep: DeepNested = DeepNested()
    "a nested NeedleModel, two levels deep"


def _params_by_name(params: list[click.Parameter]) -> dict[str, click.Parameter]:
    return {p.name: p for p in params}


def test_param_names_are_valid_identifiers_with_dotted_flag_text():
    """Test a dotted field path produces an appropriately formatted argument"""
    params = _params_by_name(Sample.to_click_params())
    for p in params.values():
        assert p.name.isidentifier(), f"param name {p.name!r} is not a valid identifier"
    assert params["nested__interval"].opts == ["--nested.interval"]
    assert params["deep__inner__interval"].opts == ["--deep.inner.interval"]


def test_special_field_shapes_map_to_the_right_click_constructs():
    """One test per non-trivial branch: bool -> flag pair, Literal -> Choice,
    list -> repeatable, dict -> repeatable KEY=VALUE, Optional[X] -> unwrapped to X."""
    params = _params_by_name(Sample.to_click_params())

    bool_p = params["active"]
    assert bool_p.opts == ["--active"] and bool_p.secondary_opts == ["--no-active"]

    choice_p = params["mode"]
    assert isinstance(choice_p.type, click.Choice)
    assert set(choice_p.type.choices) == {"a", "b", "c"}

    list_p = params["tags"]
    assert list_p.multiple is True

    dict_p = params["mapping"]
    assert dict_p.multiple is True

    optional_p = params["output"]
    assert optional_p.default is None  # unwrapped Optional[Path], didn't choke on the Union


def test_from_kwargs_reconstructs_every_field_shape():
    cfg = Sample.from_kwargs(
        {
            "name": "obs1",
            "count": 9,
            "active": True,
            "mode": "b",
            "tags": ("x", "y"),
            "mapping": (("a", "1"), ("b", "2")),
            "nested__interval": 42,
            "deep__inner__interval": 7,
        }
    )
    assert (cfg.name, cfg.count, cfg.active, cfg.mode) == ("obs1", 9, True, "b")
    assert cfg.tags == ["x", "y"]
    assert cfg.mapping == {"a": "1", "b": "2"}
    assert cfg.nested.interval == 42
    assert cfg.deep.inner.interval == 7


def test_from_kwargs_falls_back_to_model_defaults_when_absent():
    cfg = Sample.from_kwargs({"name": "obs1"})
    assert cfg.count == 1
    assert cfg.tags == []
    assert cfg.mapping == {}
    assert cfg.nested.interval == 5


def test_full_command_round_trip_with_every_field_overridden():
    """The level that actually would have caught our real bugs: build a real click.Command
    from to_click_params() and invoke it through Click's own parser, not just inspect the
    generated Option objects in isolation."""
    captured = {}

    @click.command()
    def cmd(**kwargs):
        captured.update(kwargs)

    cmd.params = Sample.to_click_params()
    runner = CliRunner()
    result = runner.invoke(
        cmd,
        [
            "--name",
            "obs1",
            "--count",
            "9",
            "--active",
            "--mode",
            "b",
            "--tags",
            "x",
            "--tags",
            "y",
            "--mapping",
            "a=1",
            "--nested.interval",
            "42",
            "--deep.inner.interval",
            "7",
        ],
    )
    assert result.exit_code == 0, result.output

    cfg = Sample.from_kwargs(captured)
    assert cfg.count == 9 and cfg.active is True and cfg.mode == "b"
    assert cfg.tags == ["x", "y"]
    assert cfg.mapping == {"a": "1"}
    assert cfg.nested.interval == 42
    assert cfg.deep.inner.interval == 7
