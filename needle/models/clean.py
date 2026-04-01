from glob import glob
from pathlib import Path
from typing import ClassVar, Literal

from pydantic import field_validator, model_validator, Field

from needle.lib.validate import validate_path_fits, validate_path_ms
from needle.models.base import NeedleModel, NeedleModuleName


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
    "Auto-threshold multiplier"

    auto_mask: float | None = None
    "Auto-mask multiplier"

    minuv_l: float | None = None
    "Minimum UV distance in lambda"

    weight: Literal["natural", "uniform", "briggs"] = "uniform"
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
            if self.robust < -1 or self.robust > 1:
                raise ValueError(f"Robustness should be >=-1 and <=1. Got {self.robust}")
        return self


class ShallowCleanConfig(WSCleanConfig):
    """Shallow clean without masking — used for initial imaging and mask generation.
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SHALLOW_CLEAN

    tag: str = Field("shallow", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(10000, description=WSCleanConfig.model_fields["niter"].description)
    minuv_l: float | None = Field(None, description=WSCleanConfig.model_fields["minuv_l"].description)


class DeepCleanConfig(WSCleanConfig):
    """Deep clean with masking — used for final imaging.
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.DEEP_CLEAN

    tag: str = Field("deep", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(50000, description=WSCleanConfig.model_fields["niter"].description)
    minuv_l: float | None = Field(300.0, description=WSCleanConfig.model_fields["minuv_l"].description)
    auto_threshold: float | None = Field(0.5, description=WSCleanConfig.model_fields["auto_threshold"].description)
    auto_mask: float | None = Field(3.0, description=WSCleanConfig.model_fields["auto_mask"].description)


class ModelSubtractCleanConfig(WSCleanConfig):
    """Model subtraction 'clean'. Doesn't do any actual cleaning by default, just subtracts MODEL_DATA from DATA>
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SUBTRACT_MODEL_CLEAN

    tag: str = Field("subtract_model", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(0, description=WSCleanConfig.model_fields["niter"].description)
    minuv_l: float | None = Field(None, description=WSCleanConfig.model_fields["minuv_l"].description)
    auto_threshold: float | None = Field(None, description=WSCleanConfig.model_fields["auto_threshold"].description)
    auto_mask: float | None = Field(None, description=WSCleanConfig.model_fields["auto_mask"].description)


class IntervalCleanConfig(WSCleanConfig):
    """Interval cleaning configuration - many snapshots over the length of an obs.
    This is essentially the same as its parent class, but with sensible defaults."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.INTERVAL_CLEAN

    tag: str = Field("intervals-out", description=WSCleanConfig.model_fields["tag"].description)
    niter: int = Field(300, description=WSCleanConfig.model_fields["niter"].description)
    minuv_l: float | None = Field(300.0, description=WSCleanConfig.model_fields["minuv_l"].description)
    auto_threshold: float | None = Field(3.9e-3, description=WSCleanConfig.model_fields["auto_threshold"].description)
    auto_mask: float | None = Field(3.0, description=WSCleanConfig.model_fields["auto_mask"].description)


class WSCleanOutput(NeedleModel):
    """Class to encompass the outputs of WSClean. Uses glob to find expected files using the name prefix"""

    prefix: Path
    "The prefix path for wsclean outputs. Should be the -name of the context object"

    @property
    def image(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-image.fits")]

    @property
    def psf(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-psf.fits")]

    @property
    def dirty(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-dirty.fits")]

    @property
    def model(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-model.fits")]

    @property
    def residual(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-residual.fits")]


class WSCleanContext(NeedleModel):
    """The full runtime context required for running WSClean"""

    cfg: WSCleanConfig
    "Static configuration for WSClean module"

    ms: Path
    "Path to the measurement set to clean"

    fits_mask: Path | None = None
    "Path to a FITS mask"

    intervals_out: int | None = None
    "The number of snapshots to image. Default (None) will not use this flag"

    predict: bool = False
    "Predict visibilities - this will create a MODEL_DATA column in the ms"

    output_dir: Path | None = None
    "A directory to output the resulting files to. Default (None) is ms directory."

    @field_validator("ms")
    @classmethod
    def _valid_ms(cls, ms) -> Path:
        validate_path_ms(ms)
        return ms

    @field_validator("fits_mask")
    @classmethod
    def _valid_fits_mask(cls, msk) -> Path | None:
        if msk:
            validate_path_fits(msk)
        return msk

    @property
    def name(self) -> str:
        """The 'name' input to wsclean.
        Can use the output variable to write to a different directory
        """
        # ms path is /path/to/m_set.ms
        # /path/to/m_set
        name = self.ms.with_suffix("")
        if self.output_dir:
            # /path/to/out_dir/m_set
            name = str(self.output_dir / Path(self.ms.name).with_suffix(""))
        if self.cfg.tag:
            # /path/to/m_set_tag
            name = f"{name}_{self.cfg.tag}"
        return name

    @property
    def cmd(self) -> list[str]:
        """Constructs the full WSClean command as a list of strings suitable for passing to subprocess.
        Optional parameters are only included if set on the cfg.
        """

        cmd = [
            "wsclean",
            "-name",
            self.name,
            "-size",
            str(self.cfg.size),
            str(self.cfg.size),
            "-scale",
            self.cfg.scale,
            "-niter",
            str(self.cfg.niter),
            "-pol",
            self.cfg.pol,
            "-data-column",
            self.cfg.data_column,
            "-weight",
            self.cfg.weight,
        ]

        if self.cfg.weight == "briggs":  # Add robustness if using briggs weighting
            cmd += ["-robust", str(self.robust)]
        if self.intervals_out is not None:
            cmd += ["-intervals-out", str(self.intervals_out)]
        if self.cfg.auto_threshold is not None:
            cmd += ["-auto-threshold", str(self.cfg.auto_threshold)]
        if self.cfg.auto_mask is not None:
            cmd += ["-auto-mask", str(self.cfg.auto_mask)]
        if self.fits_mask is not None:
            cmd += ["-fits-mask", str(self.fits_mask)]
        if self.cfg.minuv_l is not None:
            cmd += ["-minuv-l", str(self.cfg.minuv_l)]
        if self.cfg.subtract_model:
            cmd += ["-subtract-model"]

        # Override the command if predict flag is used. No other cleaning is relevant when using this flag.
        if self.predict:
            cmd = ["wsclean", "-name", self.name, "-predict"]

        cmd.append(str(self.ms))
        return cmd

    @property
    def output(self) -> WSCleanOutput:
        """The WSCleanOutput object - the expected outputs from running the cmd"""
        return WSCleanOutput(prefix=self.name)
