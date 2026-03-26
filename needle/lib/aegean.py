from pathlib import Path
import numpy as np
import json

from AegeanTools.models import ComponentSource
from pydantic import field_validator

from needle.models.base import NeedleModel


class AegeanSource(NeedleModel):
    """A radio source component detected by Aegean. Converts directly from the Aegean ComponentSource object."""

    island: int
    "Island index"

    source: int
    "Source index within island"

    background: float | None
    "Background flux density (Jy/beam)"

    local_rms: float | None
    "Local RMS noise (Jy/beam)"

    ra: float
    "Right ascension (deg)"

    err_ra: float | None
    "Uncertainty in right ascension (deg)"

    dec: float
    "Declination (deg)"

    err_dec: float | None
    "Uncertainty in declination (deg)"

    peak_flux: float | None
    "Peak flux density (Jy/beam)"

    err_peak_flux: float | None
    "Uncertainty in peak flux density (Jy/beam)"

    int_flux: float | None
    "Integrated flux density (Jy)"

    err_int_flux: float | None
    "Uncertainty in integrated flux density (Jy)"

    a: float | None
    "Semi-major axis FWHM (arcsec)"

    err_a: float | None
    "Uncertainty in semi-major axis (arcsec)"

    b: float | None
    "Semi-minor axis FWHM (arcsec)"

    err_b: float | None
    "Uncertainty in semi-minor axis (arcsec)"

    pa: float | None
    "Position angle, east of north (deg)"

    err_pa: float | None
    "Uncertainty in position angle (deg)"

    flags: str
    "Source flags as zero-padded 7-bit binary string, e.g. '0000001'"

    residual_mean: float | None
    "Mean of fit residual (Jy/beam)"

    residual_std: float | None
    "Std dev of fit residual (Jy/beam)"

    uuid: str
    "Unique identifier for this source"

    psf_a: float | None
    "PSF semi-major axis at source location (deg)"

    psf_b: float | None
    "PSF semi-minor axis at source location (deg)"

    psf_pa: float | None
    "PSF position angle at source location (deg)"

    @field_validator("*", mode="before")
    @classmethod
    def sanitise_floats(cls, v):
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        return v

    @classmethod
    def from_component(cls, src: ComponentSource) -> "AegeanSource":
        data = {
            name: f"{int(getattr(src, 'flags')):07b}" if name == "flags" else getattr(src, name)
            for name in ComponentSource.names
            if name not in ("ra_str", "dec_str", "units")
        }
        return cls(**data)


class AegeanSourceList(NeedleModel):
    """A wrapper for a list of sources from the Aegean source finder"""

    sources: list[AegeanSource]
    "The sources"

    @classmethod
    def from_json(cls, path: Path | str) -> "AegeanSourceList":
        if not path.suffix == ".json":
            raise ValueError(f"Expected .json, got file: {path}")
        return cls(sources=[AegeanSource.model_validate(r) for r in json.loads(Path(path).read_text())])

    @classmethod
    def from_component_list(cls, srcs: list[ComponentSource]) -> "AegeanSourceList":
        return cls(sources=[AegeanSource.from_component(src) for src in srcs])

    def to_json(self, path: Path | str) -> None:
        path = Path(path)
        path.write_text(json.dumps([r.model_dump() for r in self.sources], indent=2))
