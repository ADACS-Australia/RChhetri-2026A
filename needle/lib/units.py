import numpy as np

from astropy.time import Time


def mjd_s_to_utc(mjd_s: float) -> str:
    return Time(mjd_s / 86400.0, format="mjd", scale="utc").iso


def rad_to_deg(rad: float) -> float:
    return float(np.degrees(rad))
