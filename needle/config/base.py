from argparse import ArgumentParser, Namespace, _ArgumentGroup
from enum import Enum
from pathlib import Path
import types
from typing import Literal, Optional, Union, get_args, get_origin
import yaml

from pydantic import BaseModel, ConfigDict, field_validator


class NeedleModuleName(str, Enum):
    FLAG = "flag"
    CALIBRATE = "calibrate"
    SHALLOW_CLEAN = "shallow_clean"
    SOURCE_FIND = "source_find"
    CREATE_MASK = "create_mask"
    DEEP_CLEAN = "deep_clean"
    SUBTRACT_MODEL_CLEAN = "subtract_model"
    INTERVAL_CLEAN = "interval_clean"


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

    @classmethod
    def add_to_parser(cls, parser: ArgumentParser | _ArgumentGroup, prefix: str = ""):
        "Add this model's fields to an argument parser, with dot-notation for nested models"
        for field_name, field_info in cls.model_fields.items():
            arg_name = f"--{field_name}" if not prefix else f"--{prefix}.{field_name}"
            annotation = field_info.annotation
            help_text = field_info.description or ""
            default = field_info.default

            # Unwrap Optional[X] → X, handling both Union[X, None] and X | None
            origin = get_origin(annotation)
            if origin is Union or isinstance(annotation, types.UnionType):
                non_none = [a for a in get_args(annotation) if a is not type(None)]
                annotation = non_none[0] if non_none else annotation

            # Recurse into nested NeedleModel subclasses
            if isinstance(annotation, type) and issubclass(annotation, NeedleModel):
                annotation.add_to_parser(parser, prefix=f"{field_name}" if not prefix else f"{prefix}.{field_name}")
                continue

            # Handle list types e.g. list[Path], list[str]
            origin = get_origin(annotation)
            if origin is list:
                inner_type = get_args(annotation)[0]
                parser.add_argument(
                    arg_name,
                    type=inner_type,
                    action="append",
                    default=[],
                    help=f"{help_text} (default: {default})",
                )
                continue

            if origin is dict:
                parser.add_argument(
                    arg_name,
                    type=lambda s: s.split("=", 1),
                    action="append",
                    default=[],
                    metavar="KEY=VALUE",
                    help=f"{help_text} (default: {default})",
                )
                continue

            if annotation is bool:
                parser.add_argument(
                    arg_name,
                    type=lambda x: x.lower() not in ("false", "0", "no"),
                    default=default,
                    metavar="BOOL",
                    help=f"{help_text} (default: {default})",
                )
            else:
                parser.add_argument(
                    arg_name,
                    type=annotation,
                    default=default,
                    help=f"{help_text} (default: {default})",
                )
        return parser

    @classmethod
    def from_namespace(cls, namespace: Namespace) -> "NeedleModel":
        "Construct this model from an argparse Namespace, handling nested dot-notation fields"
        kwargs = {}
        flat = vars(namespace)

        for field_name, field_info in cls.model_fields.items():
            annotation = field_info.annotation

            # Unwrap Optional[X] → X
            origin = get_origin(annotation)
            if origin is Union or isinstance(annotation, types.UnionType):
                non_none = [a for a in get_args(annotation) if a is not type(None)]
                annotation = non_none[0] if non_none else annotation

            # Recurse into nested NeedleModel subclasses
            if isinstance(annotation, type) and issubclass(annotation, NeedleModel):
                # Extract only the keys relevant to this sub-model
                prefix = f"{field_name}."
                sub_namespace = Namespace(**{k[len(prefix) :]: v for k, v in flat.items() if k.startswith(prefix)})
                kwargs[field_name] = annotation.from_namespace(sub_namespace)
            else:
                if field_name in flat:
                    kwargs[field_name] = flat[field_name]

        return cls(**kwargs)


class ContainerConfig(NeedleModel):
    image: Path
    "Path to the image (.sif) file"
    binds: Optional[list[Path]] = None
    "Host paths to bind mount into the container"
    env: Optional[dict[str, str]] = None
    "Environment variables to set inside the container"
    writable: bool = False
    "Mount the container as writable (--writable)"
    type: Literal["apptainer", "singularity"] = "apptainer"
    "The container executor"

    @field_validator("image")
    @classmethod
    def _valid_image(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Container image not found: {v}")
        if v.suffix != ".sif":
            raise ValueError(f"Expected a .sif file, got: {v.suffix}")
        return v

    def to_args(self) -> list[str]:
        """Converts the config to apptainer/singularity exec arguments"""
        args = [self.type, "exec"]
        if self.writable:
            args.append("--writable")
        if self.binds:
            for b in self.binds:
                args += ["--bind", str(b)]
        if self.env:
            for k, v in self.env.items():
                args += ["--env", f"{k}={v}"]
        args.append(str(self.image))
        return args
