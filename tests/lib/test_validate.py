import pytest
from pathlib import Path
from needle.lib.validate import validate_path_ms, validate_path_fits


def test_validate_path_ms():
    """Test validation of Measurement Set file paths."""
    validate_path_ms(Path("test.ms"))

    with pytest.raises(TypeError, match="Expected a measurement set"):
        validate_path_ms(Path("test.fits"))

    with pytest.raises(TypeError, match="Expected a measurement set"):
        validate_path_ms(Path("test"))


def test_validate_path_fits():
    """Test validation of FITS file paths."""
    validate_path_fits(Path("test.fits"))

    with pytest.raises(TypeError, match="Expected a fits file"):
        validate_path_fits(Path("test.ms"))

    with pytest.raises(TypeError, match="Expected a fits file"):
        validate_path_fits(Path("test"))
