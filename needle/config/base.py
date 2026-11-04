from enum import Enum
from pathlib import Path
import types
from typing import Literal, Union, get_args, get_origin
import yaml

import click
from pydantic import BaseModel, ConfigDict
from pydantic_core import PydanticUndefinedType

from needle.lib.constants import GREEN, BOLD, YELLOW, CYAN, FMT_RST, DIM
from needle.lib.logging import setup_logging


def _kv_pair(value: str) -> tuple[str, str]:
    """Parses a 'KEY=VALUE' string into a (key, value) tuple, for dict-typed fields."""
    key, _, val = value.partition("=")
    return key, val


def _is_model_annotation(ann) -> bool:
    """True if a type annotation is, or contains (e.g. `Foo | None`, `Optional[Foo]`), a pydantic model."""
    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return True
    return any(_is_model_annotation(a) for a in get_args(ann))


class NeedleModel(BaseModel):
    """Base class for all models in the project."""

    model_config = ConfigDict(
        use_attribute_docstrings=True,
    )

    @classmethod
    def from_yaml(cls, path: Path) -> "NeedleModel":
        "Load this cfg from a YAML file"
        with open(path) as f:
            return cls.model_validate(yaml.safe_load(f))

    def __str__(self) -> str:
        fields = " :: ".join(f"{k}={v}" for k, v in self.model_dump().items())
        return f"{self.__class__.__name__} :: {fields}"

    def to_kwargs(self) -> dict:
        "Serialise this model to a flat dict of kwargs for passing to tasks"
        return self.model_dump(exclude_none=True)

    def pformat(self, indent: int = 0, color: bool = True, mark_changed: bool = True) -> str:
        """Returns a human-readable, tree-style rendering of this model and all nested models.

        :param indent: Starting indentation level (in units of 2 spaces)
        :param color: Whether to emit ANSI colour codes
        :param mark_changed: Highlight values that differ from the field default
        """
        c = (lambda code, s: f"{code}{s}{FMT_RST}") if color else (lambda code, s: s)
        lines: list[str] = []

        def fmt_scalar(v) -> str:
            return v.name if isinstance(v, Enum) else repr(v)

        def render(value, prefix: str, key: str, is_last: bool, changed: bool, is_section: bool) -> None:
            connector = "└── " if is_last else "├── "
            branch = c(DIM, prefix + connector)
            child_prefix = prefix + ("    " if is_last else "│   ")

            if isinstance(value, NeedleModel):
                lines.append(f"{branch}{c(BOLD + CYAN, key)}:")
                render_children(value, child_prefix)
            elif isinstance(value, dict) and value:
                lines.append(f"{branch}{c(BOLD, key)}:")
                items = list(value.items())
                for i, (k, v) in enumerate(items):
                    render(v, child_prefix, str(k), i == len(items) - 1, False, False)
            elif isinstance(value, (list, tuple)) and any(isinstance(v, NeedleModel) for v in value):
                lines.append(f"{branch}{c(BOLD, key)}:")
                for i, v in enumerate(value):
                    render(v, child_prefix, f"[{i}]", i == len(value) - 1, False, False)
            else:
                key_style = BOLD + CYAN if is_section else BOLD
                val = c(YELLOW if changed else GREEN, fmt_scalar(value))
                lines.append(f"{branch}{c(key_style, key)}: {val}")

        def render_children(model: "NeedleModel", prefix: str) -> None:
            fields = list(type(model).model_fields.items())
            for i, (name, info) in enumerate(fields):
                value = getattr(model, name)
                is_section = _is_model_annotation(info.annotation)
                changed = False
                if mark_changed and not isinstance(value, BaseModel):
                    try:
                        changed = value != info.get_default(call_default_factory=True)
                    except Exception:
                        changed = False
                render(value, prefix, name, i == len(fields) - 1, changed, is_section)

        render_children(self, "  " * indent)
        return "\n".join(lines)

    def pprint(self, **kwargs) -> None:
        print(self.pformat(**kwargs))

    @classmethod
    def to_click_params(cls, prefix: str = "") -> list[click.Parameter]:
        """Build click.Option objects for this model's fields, with dot-notation names for nested models."""
        params: list[click.Parameter] = []
        for field_name, field_info in cls.model_fields.items():
            opt_name = field_name if not prefix else f"{prefix}.{field_name}"
            py_name = opt_name.replace(".", "__")
            flag = f"--{opt_name}"
            annotation = field_info.annotation
            help_text = field_info.description or ""
            default = None if isinstance(field_info.default, PydanticUndefinedType) else field_info.default

            # Unwrap Optional[X] → X
            origin = get_origin(annotation)
            if origin is Union or isinstance(annotation, types.UnionType):
                non_none = [a for a in get_args(annotation) if a is not type(None)]
                annotation = non_none[0] if non_none else annotation

            # Recurse into nested NeedleModel subclasses
            if isinstance(annotation, type) and issubclass(annotation, NeedleModel):
                params.extend(annotation.to_click_params(prefix=opt_name))
                continue

            origin = get_origin(annotation)
            if origin is list:
                inner_type = get_args(annotation)[0]
                params.append(
                    click.Option(
                        [flag, py_name],
                        type=inner_type,
                        multiple=True,
                        default=(),
                        help=f"{help_text} (repeatable)",
                        show_default=True,
                    )
                )
            elif origin is dict:
                params.append(
                    click.Option(
                        [flag, py_name],
                        type=_kv_pair,
                        multiple=True,
                        metavar="KEY=VALUE",
                        help=f"{help_text} (repeatable, KEY=VALUE)",
                        show_default=True,
                    )
                )
            elif annotation is bool:
                if "." in opt_name:
                    prefix_part, leaf = opt_name.rsplit(".", 1)
                    on_flag = f"--{opt_name}"
                    off_flag = f"--{prefix_part}.no-{leaf}"
                else:
                    on_flag = f"--{opt_name}"
                    off_flag = f"--no-{opt_name}"

                params.append(
                    click.Option([f"{on_flag}/{off_flag}", py_name], default=default, help=help_text, show_default=True)
                )
            elif get_origin(annotation) is Literal:
                choices = get_args(annotation)
                params.append(
                    click.Option(
                        [flag, py_name],
                        type=click.Choice([str(c) for c in choices]),
                        default=default,
                        help=help_text,
                        show_default=True,
                    )
                )
            else:
                params.append(
                    click.Option([flag, py_name], type=annotation, default=default, help=help_text, show_default=True)
                )
        return params

    @classmethod
    def from_kwargs(cls, flat: dict) -> "NeedleModel":
        """Construct this model from a flat dict keyed by the '__'-joined names produced by
        to_click_params() (dots -> "__", to keep them valid Click/Python identifiers) — the
        click analog of from_namespace."""
        kwargs = {}
        for field_name, field_info in cls.model_fields.items():
            annotation = field_info.annotation

            origin = get_origin(annotation)
            if origin is Union or isinstance(annotation, types.UnionType):
                non_none = [a for a in get_args(annotation) if a is not type(None)]
                annotation = non_none[0] if non_none else annotation

            if get_origin(annotation) is dict:
                raw = flat.get(field_name, ())
                kwargs[field_name] = dict(raw) if raw else {}
                continue

            if get_origin(annotation) is list:
                raw = flat.get(field_name, ())
                if raw:  # an empty repeatable option should fall back to the model default
                    kwargs[field_name] = list(raw)
                continue

            # Recurse into nested NeedleModel subclasses
            if isinstance(annotation, type) and issubclass(annotation, NeedleModel):
                prefix = f"{field_name}__"
                sub_flat = {k[len(prefix) :]: v for k, v in flat.items() if k.startswith(prefix)}
                kwargs[field_name] = annotation.from_kwargs(sub_flat)
                continue

            if field_name in flat:
                kwargs[field_name] = flat[field_name]

        return cls(**kwargs)


_log_level_option = click.Option(
    ["--log-level", "--log_level", "-l"],
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="The minimum threshold logging level",
)


def needle_module_args(model_cls: type[NeedleModel], *, name: str, help: str):
    """Decorator. Wraps a function `fn(cfg: model_cls, **extras)` into a click.Command.

    Options for model_cls's pydantic fields are generated automatically (via
    NeedleModel.to_click_params). Anything else the command needs — stack ordinary
    @click.option()/@click.argument() decorators directly on the function, same as you would
    for any plain Click command; this decorator picks them up via Click's own
    __click_params__ mechanism (the same one @click.command() itself uses).

    Usage:

        @pydantic_command(FlagConfig, name="flag", help="Flag a measurement set...")
        @click.option("--ms", type=click.Path(exists=True, path_type=Path), required=True,
                      help="The path to the measurement set")
        def command(cfg: FlagConfig, ms: Path):
            ...
    """
    model_params = model_cls.to_click_params()

    def decorator(fn):
        # Click appends params from stacked decorators here, in reverse (closest-to-function-
        # first) order — reverse it back so options display in the order they were declared.
        extra_params = list(reversed(getattr(fn, "__click_params__", [])))
        extra_names = {p.name for p in extra_params}

        def callback(**kwargs):
            setup_logging(kwargs.pop("log_level"))
            extra_kwargs = {k: v for k, v in kwargs.items() if k in extra_names}
            model_kwargs = {k: v for k, v in kwargs.items() if k not in extra_names}
            cfg = model_cls.from_kwargs(model_kwargs)
            return fn(cfg=cfg, **extra_kwargs)

        return click.Command(
            name=name,
            help=help,
            params=[*extra_params, *model_params, _log_level_option],
            callback=callback,
        )

    return decorator
