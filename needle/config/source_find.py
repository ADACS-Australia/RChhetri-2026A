from typing import Optional, ClassVar

from needle.config.base import NeedleModel, NeedleModuleName


class SourceFindConfig(NeedleModel):
    """Config for running BANE + Aegean source finding"""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SOURCE_FIND

    innerclip: int = 7
    "Inner clip threshold SNR"

    outerclip: int = 6
    "Outer clip threshold SNR"

    max_summits: Optional[int] = None
    "Fit up to this many components to each island"

    cores: int = 1
    "Number of CPU cores to use for BANE"
