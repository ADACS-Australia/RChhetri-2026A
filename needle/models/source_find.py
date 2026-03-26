from typing import Optional, ClassVar

from pydantic import field_validator
from pathlib import Path
from needle.models.base import NeedleModel, NeedleModuleName
from needle.lib.validate import validate_path_fits


class SourceFindOutput(NeedleModel):
    """Class to encompass the outputs of WSClean"""

    prefix: Path
    "The prefix path. Will be used for naming output files."

    @property
    def bkg_path(self) -> Path:
        return Path(str(self.prefix) + "_bkg").with_suffix(".fits")

    @property
    def rms_path(self) -> Path:
        return Path(str(self.prefix) + "_rms").with_suffix(".fits")

    @property
    def sources_txt(self) -> Path:
        return Path(str(self.prefix) + "-sources").with_suffix(".txt")

    @property
    def sources_json(self) -> Path:
        return Path(str(self.prefix) + "-sources").with_suffix(".json")


class SourceFindConfig(NeedleModel):
    """Config for running Aegean source finding"""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SOURCE_FIND

    innerclip: int = 7
    "Inner clip threshold SNR"

    outerclip: int = 6
    "Outer clip threshold SNR"

    max_summits: Optional[int] = None
    "Fit up to this many components to each island"

    cores: int = 1
    "Number of CPU cores to use for source finding"


class SourceFindContext(NeedleModel):
    """Context object for the Source Finding module"""

    cfg: SourceFindConfig
    "Static configuration for the source finding module"

    image: Path
    "Fits image to source-find"

    @field_validator("image")
    @classmethod
    def valid_image(cls, im: Path) -> Path:
        validate_path_fits(im)
        return im
