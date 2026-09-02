from pathlib import Path
import logging

from pydantic import model_validator

from needle.config.base import NeedleModel

logger = logging.getLogger(__name__)


class CalibrationSolution(NeedleModel):
    gcal: Path
    "Path to the .gcal directory"
    bpcal: Path
    "Path to the .bpcal directory"

    @model_validator(mode="after")
    def validate_suffixes(self):
        if self.gcal.suffix != ".gcal":
            raise ValueError(f"Gain calibration solution expected (.gcal), got {self.gcal}")
        if self.bpcal.suffix != ".bpcal":
            raise ValueError(f"Bandpass calibration solution expected (.bpcal), got {self.bpcal}")
        return self


CalInput = Path | CalibrationSolution


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


class SolveCalibrationConfig(NeedleModel):
    setjy: SetjyConfig = SetjyConfig()
    "Set flux density scale"

    bandpass: BandpassConfig = BandpassConfig()
    "Bandpass calibration"

    gaincal: GaincalConfig = GaincalConfig()
    "Gain calibration"


class ApplyCalibrationConfig(NeedleModel):
    applycal: ApplycalConfig = ApplycalConfig()
    "Apply calibration solutions"

    split: SplitConfig = SplitConfig()
    "Split out calibrated data"


class CalibrateConfig(NeedleModel):
    """An amalgomation of Solve and Apply"""

    setjy: SetjyConfig = SetjyConfig()
    "Set flux density scale"

    bandpass: BandpassConfig = BandpassConfig()
    "Bandpass calibration"

    gaincal: GaincalConfig = GaincalConfig()
    "Gain calibration"

    applycal: ApplycalConfig = ApplycalConfig()
    "Apply calibration solutions"

    split: SplitConfig = SplitConfig()
    "Split out calibrated data"
