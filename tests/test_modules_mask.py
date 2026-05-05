import pytest
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock, patch
from needle.modules.mask import CreateMaskContext, generate_mask_array
from needle.config.mask import CreateMaskConfig
from needle.lib.aegean import AegeanSourceList


@pytest.fixture
def mock_fits(tmp_path):
    f = tmp_path / "test.fits"
    f.touch()
    return f


@patch("needle.lib.aegean.AegeanSourceList.from_json")
def test_create_mask_context_validation(mock_from_json, mock_fits):
    """Test validation of CreateMaskContext with provided configuration and image."""
    mock_from_json.return_value = MagicMock(spec=AegeanSourceList)
    cfg = CreateMaskConfig(padding=1.0)
    CreateMaskContext(cfg=cfg, image=mock_fits, sources=Path("sources.json"))
    assert mock_from_json.called


@patch("needle.modules.mask.generate_mask_array")
@patch("astropy.io.fits.writeto")
def test_create_mask_execute(mock_writeto, mock_generate, mock_fits):
    """Test the execution of the mask creation process."""
    from astropy.io import fits

    # Return real numpy array and header to avoid formatting issues in logging
    mock_generate.return_value = (np.zeros((10, 10)), fits.Header())
    sources = MagicMock(spec=AegeanSourceList)
    cfg = CreateMaskConfig(padding=1.0)
    ctx = CreateMaskContext(cfg=cfg, image=mock_fits, sources=sources)

    output = ctx.execute()
    # Correct expected path based on CreateMaskOutput implementation
    expected_path = Path(str(mock_fits.with_suffix("")) + "-clean_mask").with_suffix(".fits")
    assert output.mask == expected_path
    assert mock_generate.called
    assert mock_writeto.called


@patch("astropy.io.fits.open")
def test_generate_mask_array(mock_fits_open):
    """Test the generation of a mask array from a source list and FITS image."""
    from astropy.io import fits

    # Setup mock HDU with a more complete header for WCS
    mock_hdu = MagicMock()
    header_dict = {
        "CTYPE1": "RA---TAN",
        "CTYPE2": "DEC--TAN",
        "CRVAL1": 10.0,
        "CRVAL2": 20.0,
        "CRPIX1": 50.0,
        "CRPIX2": 50.0,
        "CDELT1": -0.01,
        "CDELT2": 0.01,
        "NAXIS1": 100,
        "NAXIS2": 100,
    }
    real_header = fits.Header(header_dict)
    mock_hdu.header = real_header
    mock_hdu.data.shape = (100, 100)
    mock_hdul = [mock_hdu]
    mock_fits_open.return_value.__enter__.return_value = mock_hdul

    source_list = MagicMock(spec=AegeanSourceList)
    source = MagicMock()
    source.ra, source.dec, source.a = 10.0, 20.0, 10.0  # 10 arcsec
    source_list.sources = [source]

    # We need to patch WCS to avoid complex setup
    with patch("needle.modules.mask.WCS") as mock_wcs:
        mock_wcs_inst = mock_wcs.return_value
        mock_wcs_inst.all_world2pix.return_value = [[50, 50]]
        mock_wcs_inst.pixel_scale_matrix = MagicMock()
        mock_wcs_inst.pixel_scale_matrix.__getitem__.return_value.__getitem__.return_value = 0.01

        mask, header = generate_mask_array(source_list, Path("test.fits"), padding=1.0)

        assert mask.shape == (100, 100)
        assert header["NAXIS"] == 2
