from pathlib import Path
import types
from typing import Literal, Union, get_args, get_origin
import yaml

import click
from pydantic import BaseModel, ConfigDict
from pydantic_core import PydanticUndefinedType

from needle.lib.logging import setup_logging


def _kv_pair(value: str) -> tuple[str, str]:
    """Parses a 'KEY=VALUE' string into a (key, value) tuple, for dict-typed fields."""
    key, _, val = value.partition("=")
    return key, val


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

    def pretty_print(self, _last_levels: tuple[bool, ...] = ()) -> None:
        """Pretty prints the model, including any nested models"""
        if not _last_levels:
            print(f"[{type(self).__name__}]")
        fields = list(type(self).model_fields.keys())
        for i, field_name in enumerate(fields):
            field_val = getattr(self, field_name)
            is_last = i == len(fields) - 1
            pad = "".join("    " if last else "│   " for last in _last_levels)

            if isinstance(field_val, NeedleModel):
                connector = "└── " if is_last else "├── "
                if not _last_levels:  # Don't pad the final level
                    print(f"{pad}│")
                print(f"{pad}{connector}{field_name}: [{type(field_val).__name__}]")
                field_val.pretty_print(_last_levels=(*_last_levels, is_last))

            else:
                connector = "└── " if is_last else "│   "
                print(f"{pad}{connector}{field_name}: {field_val}")

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
                kwargs[field_name] = list(raw) if raw else []
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
