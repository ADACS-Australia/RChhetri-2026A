import logging
from pathlib import Path
from typing import ClassVar

from needle.config.base import NeedleModel, NeedleModuleName

logger = logging.getLogger(__name__)


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
