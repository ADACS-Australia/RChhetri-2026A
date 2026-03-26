from pathlib import Path
from typing import ClassVar

from pydantic import field_validator

from needle.lib.aegean import AegeanSourceList
from needle.lib.validate import validate_path_fits
from needle.models.base import NeedleModel, NeedleModuleName


class CreateMaskOutput(NeedleModel):
    """Class to encompass the outputs of"""

    prefix: Path
    "The prefix path for wsclean to output to"

    @property
    def mask(self) -> Path:
        return Path(str(self.prefix) + "-clean_mask").with_suffix(".fits")


class CreateMaskConfig(NeedleModel):
    """Config for the mask creation step"""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.CREATE_MASK

    padding: float = 5.0
    "The padding around each source"


class CreateMaskContext(NeedleModel):

    cfg: CreateMaskConfig
    "Static configuration for mask creation"

    image: Path
    "Path to the fits image. The intended mask target - used for size reference"

    sources: Path | AegeanSourceList
    "The source list. Also accepts a .json of the sources"

    @field_validator("sources")
    @classmethod
    def _valid_sources(cls, s) -> AegeanSourceList:
        """Convert .json to AegeanSourceList if necessary"""
        if isinstance(s, AegeanSourceList):
            return s
        return AegeanSourceList.from_json(s)

    @field_validator("image")
    @classmethod
    def _valid_image(cls, im) -> Path:
        validate_path_fits(im)
        return im
