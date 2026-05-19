from pathlib import Path
from typing import Literal, Optional

from pydantic import field_validator

from needle.config.base import NeedleModel


class ContainerConfig(NeedleModel):
    """Configuration for a (.sif) container to use for runtime execution"""

    image: Path
    "Path to the image (.sif) file. Image must exist."

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
