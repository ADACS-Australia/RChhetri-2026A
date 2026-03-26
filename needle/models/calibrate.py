from pathlib import Path
from pydantic import field_validator
from typing import ClassVar

from needle.models.base import NeedleModel, NeedleModuleName
from needle.lib.validate import validate_path_ms


class SetjyConfig(NeedleModel):
    field: str = ""
    "Field to set flux density for"

    spw: str = ""
    "Spectral window selection"

    standard: str = "Perley-Butler 2017"
    "Flux density standard"

    model: str = ""
    "Model image to use"

    scalebychan: bool = True
    "Scale flux density by channel"


class BandpassConfig(NeedleModel):
    field: str = ""
    "Field to use for bandpass calibration"

    spw: str = ""
    "Spectral window selection"

    solint: str = "inf"
    "Solution interval"

    combine: str = "scan"
    "Data axes to combine for solving"

    refant: str = ""
    "Reference antenna"

    solnorm: bool = False
    "Normalise the bandpass solution"

    minsnr: float = 3.0
    "The min SNR to accept solutions for"


class GaincalConfig(NeedleModel):
    field: str = ""
    "Field to use for gain calibration"

    spw: str = ""
    "Spectral window selection"

    solint: str = "inf"
    "Solution interval"

    combine: str = ""
    "Data axes to combine for solving"

    refant: str = ""
    "Reference antenna"

    calmode: str = "ap"
    "Calibration mode: ap (amp+phase), a (amp only), p (phase only)"


class ApplycalConfig(NeedleModel):
    field: str = ""
    "Field to apply calibration to"

    spw: str = ""
    "Spectral window selection"

    interp: str = "linear"
    "Interpolation method"

    calwt: bool = False
    "Apply calibration weights"


class SplitConfig(NeedleModel):
    field: str = ""
    "Field to split out"

    spw: str = ""
    "Spectral window selection"

    datacolumn: str = "corrected"
    "Data column to split: corrected, data, model"

    keepflags: bool = False
    "Keep flagged data in output"


class CalibrateConfig(NeedleModel):
    module: ClassVar[NeedleModuleName] = NeedleModuleName.CALIBRATE

    setjy: SetjyConfig
    "Set flux density scale"

    bandpass: BandpassConfig
    "Bandpass calibration"

    gaincal: GaincalConfig
    "Gain calibration"

    applycal: ApplycalConfig
    "Apply calibration solutions"

    split: SplitConfig
    "Split out calibrated data"


class CalibrateContext(NeedleModel):
    cfg: CalibrateConfig
    "Static config values"

    cal: Path
    "The calibator measurement set"

    tgt: Path
    "The target measurement set"

    @field_validator("cal")
    @classmethod
    def _valid_cal(cls, cal: Path) -> Path:
        validate_path_ms(cal)
        return cal

    @field_validator("tgt")
    @classmethod
    def _valid_tgt(cls, tgt: Path) -> Path:
        validate_path_ms(tgt)
        return tgt
