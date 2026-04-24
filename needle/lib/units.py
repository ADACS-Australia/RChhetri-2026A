import numpy as np

from casatools import quanta


def mjd_s_to_utc(mjd_s: float) -> str:
    qa = quanta()
    return qa.time(qa.quantity(mjd_s, "s"), form="ymd")[0]


def rad_to_deg(rad: float) -> float:
    return float(np.degrees(rad))
