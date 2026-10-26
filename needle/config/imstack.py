from typing import Literal

from pydantic import field_validator, model_validator

from needle.config.base import NeedleModel


class ImageStackConfig(NeedleModel):
    """Configuration for building an HDF5 image stack from WSClean FITS images."""

    suffixes: list[str] = ["image"]
    "WSClean image suffixes to store e.g. ['image', 'dirty', 'model']"

    pols: list[str] = ["XX"]
    "Polarisations to store e.g. ['XX', 'YY']"

    filename: str = "{stem}-t{time:04d}-{suffix}.fits"
    "Filename template for unbanded data. May use stem, obsid, prefix, band, time, pol and suffix"

    filename_band: str = "{stem_band}-t{time:04d}-{suffix}.fits"
    "Filename template for banded data. May use stem_band in addition to the unbanded fields"

    pb_filename: str = "{stem}-{pol}-beam.fits"
    "Primary beam filename template for unbanded data"

    pb_filename_band: str = "{stem_band}-{pol}-beam.fits"
    "Primary beam filename template for banded data"

    hdu: int = 0
    "FITS HDU index to read"

    stamp_size: int = 16
    "HDF5 chunk size in pixels. The image size must be divisible by this"

    pb_threshold: float = 0.1
    "Fraction of the maximum primary beam below which data is set to NaN"

    skip_beam: bool = False
    "Do not read or store primary beam information"

    n_channels: int = 1
    "Number of channels per image. Only one is supported"

    dtype: Literal["float16", "float32", "float64"] = "float16"
    "Data type used to store the image data"

    cache_size_mb: int = 1024
    "HDF5 metadata cache size in MB"

    start: int = 0
    "Index of the first timestep"

    time_interval: float = 1.0
    "Seconds between consecutive timesteps. Stored as TIME_INTERVAL"

    bands: list[str] | None = None
    "Contiguous frequency bands to store. None stores a single unbanded group"

    prefix: str = ""
    "Name prefix inserted between the observation id and the timestep token"

    allow_missing: bool = False
    "Truncate at the first missing timestep instead of raising"

    old_wcs_timesteps: bool = False
    "Read WSCTIMES and WSCTIMEE from the FITS headers instead of using absolute timestep indices"

    @field_validator("suffixes")
    @classmethod
    def _valid_suffixes(cls, v: list[str]) -> list[str]:
        reserved = {"beam", "header", "timestamp", "timestep_start", "timestep_stop"}
        if not v:
            raise ValueError("Must contain at least one suffix")
        if len(v) != len(set(v)):
            raise ValueError(f"Duplicate suffixes: {v}")
        if clash := reserved & set(v):
            raise ValueError(f"Suffixes clash with reserved dataset names: {clash}")
        return v

    @field_validator("pols")
    @classmethod
    def _valid_pols(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("Must contain at least one polarisation")
        if len(v) != len(set(v)):
            raise ValueError(f"Duplicate pols: {v}")
        return v

    @field_validator("start")
    @classmethod
    def _valid_start(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"start must be non-negative, got {v}")
        return v

    @field_validator("time_interval")
    @classmethod
    def _valid_time_interval(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"time_interval must be positive, got {v}")
        return v

    @field_validator("bands", mode="before")
    @classmethod
    def _normalise_bands(cls, v) -> list[str] | None:
        if not v:
            return None
        bands = list(v)
        if len(bands) != len(set(bands)):
            raise ValueError(f"Duplicate bands: {bands}")
        return bands

    @model_validator(mode="after")
    def _valid_shape(self) -> "ImageStackConfig":
        if self.hdu < 0:
            raise ValueError(f"hdu must be non-negative, got {self.hdu}")
        if self.stamp_size < 1:
            raise ValueError(f"stamp_size must be positive, got {self.stamp_size}")
        if not 0 <= self.pb_threshold <= 1:
            raise ValueError(f"pb_threshold must be between 0 and 1, got {self.pb_threshold}")
        if self.n_channels != 1:
            raise ValueError(f"Only a single channel is supported, got {self.n_channels}")
        if self.cache_size_mb < 1:
            raise ValueError(f"cache_size_mb must be positive, got {self.cache_size_mb}")
        return self
