from abc import abstractmethod
from pathlib import Path
from typing import ClassVar

from pydantic import field_validator, model_validator

from needle.lib.validate import validate_path_ms
from needle.models.base import NeedleModel, NeedleModuleName


class FlagStepConfig(NeedleModel):
    """Base class for all flagging step cfgs."""

    @property
    @abstractmethod
    def _flagdata_kwargs(self) -> dict:
        """Keyword arguments passed directly to CASA's flagdata."""
        pass


class QuackConfig(FlagStepConfig):
    interval: float = 10.0
    "Quack interval in seconds"

    mode: str = "beg"
    "Quack mode: beg, end, tail, all"

    @field_validator("mode")
    @classmethod
    def valid_mode(cls, v):
        allowed = {"beg", "end", "tail", "all"}
        if v not in allowed:
            raise ValueError(f"mode must be one of {allowed}")
        return v

    @property
    def _flagdata_kwargs(self) -> dict:
        return dict(mode="quack", quackinterval=self.interval, quackmode=self.mode)


class ClipConfig(FlagStepConfig):
    min_amp: float = 0.0
    "Minimum amplitude to keep"

    max_amp: float = 100.0
    "Maximum amplitude to keep"

    clip_zeros: bool = True
    "Clip zero values"

    @model_validator(mode="after")
    def check_range(self):
        if self.min_amp >= self.max_amp:
            raise ValueError(f"min_amp ({self.min_amp}) must be less than max_amp ({self.max_amp})")
        return self

    @property
    def _flagdata_kwargs(self) -> dict:
        return dict(
            mode="clip", clipzeros=self.clip_zeros, clipminmax=[self.min_amp, self.max_amp], correlation="ABS_ALL"
        )


class TfCropConfig(FlagStepConfig):
    time_cutoff: float = 4.0
    "Time sigma cutoff"

    freq_cutoff: float = 4.0
    "Frequency sigma cutoff"

    max_npieces: int = 7
    "Max number of pieces for fitting"

    flag_dimension: str = "freqtime"
    "Dimension to flag: time, freq, or freqtime"

    extend_flags: bool = False
    "Extend flags after tfcrop"

    @property
    def _flagdata_kwargs(self) -> dict:
        return dict(
            mode="tfcrop",
            datacolumn="data",
            timecutoff=self.time_cutoff,
            freqcutoff=self.freq_cutoff,
            maxnpieces=self.max_npieces,
            flagdimension=self.flag_dimension,
            extendflags=self.extend_flags,
        )


class RFlagConfig(FlagStepConfig):
    time_devscale: float = 5.0
    "Time deviation scale factor"

    freq_devscale: float = 5.0
    "Frequency deviation scale factor"

    winsize: int = 3
    "Window size for rms estimation"

    @property
    def _flagdata_kwargs(self) -> dict:
        return dict(
            mode="rflag",
            datacolumn="data",
            timedevscale=self.time_devscale,
            freqdevscale=self.freq_devscale,
            winsize=self.winsize,
        )


class ExtendConfig(FlagStepConfig):
    grow_time: float = 80.0
    "Percentage of timesteps flagged to extend to all"

    grow_freq: float = 80.0
    "Percentage of channels flagged to extend to all"

    extend_pols: bool = True
    "Extend flags to all polarisations"

    @property
    def _flagdata_kwargs(self) -> dict:
        return dict(mode="extend", growtime=self.grow_time, growfreq=self.grow_freq, extendpols=self.extend_pols)


class ManualConfig(FlagStepConfig):
    spw: str = ""
    "Spectral window selection"

    antenna: str = ""
    "Antenna selection"

    timerange: str = ""
    "Time range selection"

    correlation: str = ""
    "Correlation selection"

    @property
    def _flagdata_kwargs(self) -> dict:
        return dict(
            mode="manual", spw=self.spw, antenna=self.antenna, timerange=self.timerange, correlation=self.correlation
        )


class FlagConfig(NeedleModel):
    # TODO: Add more validators for all of the sub-cfgs
    module: ClassVar[NeedleModuleName] = NeedleModuleName.FLAG

    backup: bool = False
    "Whether to make a backup before flagging the measurement set"

    quack: QuackConfig | None = None
    "Quack flagging step — omit to skip"

    clip: ClipConfig | None = None
    "Clip flagging step — omit to skip"

    tfcrop: TfCropConfig | None = None
    "TFCrop flagging step — omit to skip"

    rflag: RFlagConfig | None = None
    "RFlag flagging step — omit to skip"

    extend: ExtendConfig | None = None
    "Extend flagging step — omit to skip"

    manual: ManualConfig | None = None
    "Manual flagging step — omit to skip"


class FlagContext(NeedleModel):
    cfg: FlagConfig
    "Static configuration values"

    ms: Path
    "The path to the measurement set to flag"

    @field_validator("ms")
    @classmethod
    def _valid_image(cls, ms) -> Path:
        validate_path_ms(ms)
        return ms
