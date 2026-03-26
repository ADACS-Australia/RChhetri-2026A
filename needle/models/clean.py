from pathlib import Path
from typing import ClassVar, Tuple, Literal

from pydantic import field_validator, model_validator

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

    @model_validator(mode="after")
    def _valid_robust(self) -> "WSCleanConfig":
        if self.weight == "briggs":
            if self.robust < -1 or self.robust > 1:
                raise ValueError(f"Robustness should be >=-1 and <=1. Got {self.robust}")
        return self


class ShallowCleanConfig(WSCleanConfig):
    """Shallow clean without masking — used for initial imaging and mask generation."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.SHALLOW_CLEAN

    niter: int = 10000
    "Maximum number of clean iterations"

    minuv_l: float | None = None
    "Minimum UV distance in lambda — omit to skip"

    tag: str = "shallow"
    "A tag to append to the output file names as an identifier"


class DeepCleanConfig(WSCleanConfig):
    """Deep clean with masking — used for final imaging."""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.DEEP_CLEAN

    niter: int = 500000
    "Maximum number of clean iterations"

    auto_threshold: float = 0.5
    "Auto-threshold multiplier"

    auto_mask: float = 3.0
    "Auto-mask multiplier"

    minuv_l: float = 300.0
    "Minimum UV distance in lambda"

    tag: str = "deep"
    "A tag to append to the output file names as an identifier"


class IntervalCleanConfig(WSCleanConfig):
    """Interval cleaning configuration - many snapshots over the length of an obs"""

    module: ClassVar[NeedleModuleName] = NeedleModuleName.INTERVAL_CLEAN

    niter: int = 300
    "Maximum number of clean iterations"

    auto_threshold: float = 3.9e-3
    "Auto-threshold multiplier"

    auto_mask: float = 3.0
    "Auto-mask multiplier"

    minuv_l: float = 300.0
    "Minimum UV distance in lambda"

    tag: str = "interval"
    "A tag to append to the output file names as an identifier"


class WSCleanOutput(NeedleModel):
    """Class to encompass the outputs of WSClean"""

    prefix: Path
    "The prefix path for wsclean to output to"

    @property
    def image(self) -> Path:
        return Path(str(self.prefix) + "-image").with_suffix(".fits")

    @property
    def psf(self) -> Path:
        return Path(str(self.prefix) + "-psf").with_suffix(".fits")

    @property
    def dirty(self) -> Path:
        return Path(str(self.prefix) + "-dirty").with_suffix(".fits")

    @property
    def model(self) -> Path:
        return Path(str(self.prefix) + "-model").with_suffix(".fits")

    @property
    def residual(self) -> Path:
        return Path(str(self.prefix) + "-residual").with_suffix(".fits")


class WSCleanContext(NeedleModel):
    """The full runtime context required for running WSClean"""

    cfg: WSCleanConfig
    "Static configuration for WSClean module"

    ms: Path
    "Path to the measurement set to clean"

    fits_mask: Path | None = None
    "Path to a FITS mask"

    interval: Tuple[int, int] | None = None
    "The intervals/integrations to image. Default (None) is all"

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
        """
        The 'name' input to wsclean.
        Can use the output variable to write to a different directory
        Will automatically add the interval to the name if cleaning over an interval
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
        if self.interval:
            # /path/to/m_set_i0-1
            name = f"{name}_i{self.interval[0]}-{self.interval[1]}"
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
        if self.interval is not None:
            cmd += ["-interval", str(self.interval[0]), str(self.interval[1])]
        if self.cfg.auto_threshold is not None:
            cmd += ["-auto-threshold", str(self.cfg.auto_threshold)]
        if self.cfg.auto_mask is not None:
            cmd += ["-auto-mask", str(self.cfg.auto_mask)]
        if self.fits_mask is not None:
            cmd += ["-fits-mask", str(self.fits_mask)]
        if self.cfg.minuv_l is not None:
            cmd += ["-minuv-l", str(self.cfg.minuv_l)]

        cmd.append(str(self.ms))
        return cmd

    @property
    def output(self) -> WSCleanOutput:
        """The WSCleanOutput object - the expected outputs from running the cmd"""
        return WSCleanOutput(prefix=self.name)
