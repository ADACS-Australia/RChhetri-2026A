from typing import ClassVar, Literal

from pydantic import model_validator, Field

from needle.config.base import NeedleModel, NeedleModuleName


class WSCleanConfig(NeedleModel):
    """Base configuration for WSClean imaging."""

    size: int = 2048
    "Image size in pixels (square)"

    scale: str = "10asec"
    "Pixel scale e.g. 10asec, 2arcmin"

    niter: int = 10000
    "Maximum number of clean iterations"

    pol: str = "XX"
    "Polarisation to image"

    data_column: str = "DATA"
    "Data column to image: DATA or CORRECTED_DATA"

    auto_threshold: float | None = None
    "Auto-threshold in S/N"

    auto_mask: float | None = None
    "Auto-mask in S/N"

    minuv_l: float | None = None
    "Minimum UV distance in lambda"

    weight: Literal["natural", "uniform", "briggs"] = "briggs"
    "The weighting to use"

    robust: float = 0.5
    "For briggs weighting, the robustness to use -1<=r<=1"

    tag: str = ""
    "A tag to append to the output file names as an identifier"

    subtract_model: bool = False
    "Subtract the model from the data column"

    @model_validator(mode="after")
    def _valid_robust(self) -> "WSCleanConfig":
        if self.weight == "briggs":
            if self.robust <= -1 or self.robust >= 1:
                raise ValueError(f"Robustness should be >=-1 and <=1. Got {self.robust}")
        return self

    @model_validator(mode="after")
    def _valid_thresholds(self) -> "WSCleanConfig":
        """This is enforced by wsclean"""
        if self.auto_threshold and self.auto_mask:
            if self.auto_mask < self.auto_threshold:
                raise ValueError("The auto-masking threshold must be greater than the auto-threshold")
        return self


### Some common use-cases with sensible defaults


class ShallowCleanConfig(WSCleanConfig):
    """Shallow clean without masking — used for initial imaging and mask generation.
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SHALLOW_CLEAN

    tag: str = Field("shallow", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(10000, description=WSCleanConfig.model_fields["niter"].description)


class DeepCleanConfig(WSCleanConfig):
    """Deep clean with masking — used for final imaging.
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.DEEP_CLEAN

    tag: str = Field("deep", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(50000, description=WSCleanConfig.model_fields["niter"].description)
    minuv_l: float | None = Field(300.0, description=WSCleanConfig.model_fields["minuv_l"].description)
    auto_threshold: float | None = Field(0.5, description=WSCleanConfig.model_fields["auto_threshold"].description)


class ModelSubtractCleanConfig(WSCleanConfig):
    """Model subtraction 'clean'. Doesn't do any actual cleaning by default, just subtracts MODEL_DATA from DATA>
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SUBTRACT_MODEL_CLEAN

    tag: str = Field("subtract_model", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(0, description=WSCleanConfig.model_fields["niter"].description)
    subtract_model: bool = Field(True, description=WSCleanConfig.model_fields["subtract_model"].description)


class IntervalCleanConfig(WSCleanConfig):
    """Interval cleaning configuration - many snapshots over the length of an obs.
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.INTERVAL_CLEAN

    tag: str = Field("intervals", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(300, description=WSCleanConfig.model_fields["niter"].description)
    minuv_l: float | None = Field(300.0, description=WSCleanConfig.model_fields["minuv_l"].description)
    auto_threshold: float | None = Field(0.5, description=WSCleanConfig.model_fields["auto_threshold"].description)
