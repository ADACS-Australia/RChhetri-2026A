from typing import Literal

from pydantic import field_validator

from needle.config.base import NeedleModel


class ImageStackConfig(NeedleModel):
    """Configuration for building an HDF5 image stack from a directory of FITS images."""

    hdu: int = 0
    "FITS HDU index to read"

    stamp_size: int = 16
    "Minimum HDF5 chunk size in pixels. Chunks below 128 are enlarged to 128 unless the image is smaller"

    time_interval: float = 1.0
    "Seconds between consecutive timesteps. Stored as TIME_INTERVAL"

    dtype: Literal["float16", "float32", "float64"] = "float16"
    "Data type used to store the image data"

    cache_size_mb: int = 1024
    "HDF5 metadata cache size in MB"

    @field_validator("hdu")
    @classmethod
    def _valid_hdu(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"hdu must be non-negative, got {v}")
        return v

    @field_validator("stamp_size")
    @classmethod
    def _valid_stamp_size(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"stamp_size must be positive, got {v}")
        return v

    @field_validator("time_interval")
    @classmethod
    def _valid_time_interval(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"time_interval must be positive, got {v}")
        return v

    @field_validator("cache_size_mb")
    @classmethod
    def _valid_cache_size_mb(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"cache_size_mb must be positive, got {v}")
        return v
