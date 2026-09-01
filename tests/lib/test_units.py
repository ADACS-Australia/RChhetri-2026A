import numpy as np
from needle.lib.units import mjd_s_to_utc, rad_to_deg


def test_mjd_s_to_utc():
    """Test conversion of MJD seconds to UTC ISO string."""
    # 0 MJD in seconds is 1858-11-17 00:00:00
    assert mjd_s_to_utc(0) == "1858-11-17 00:00:00.000"

    # 59000 MJD in seconds
    mjd = 59000
    mjd_s = mjd * 86400.0
    assert mjd_s_to_utc(mjd_s).startswith("2020-05-31")


def test_rad_to_deg():
    """Test conversion of radians to degrees."""
    assert rad_to_deg(0) == 0.0
    assert rad_to_deg(np.pi) == 180.0
    assert rad_to_deg(np.pi / 2) == 90.0
    assert rad_to_deg(2 * np.pi) == 360.0
