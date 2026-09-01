import pytest
from unittest.mock import MagicMock, patch
from needle.modules.source_find import SourceFindContext, squeeze_fits
from needle.config.source_find import SourceFindConfig


@pytest.fixture
def mock_fits(tmp_path):
    f = tmp_path / "test.fits"
    f.touch()
    return f


@patch("needle.modules.source_find.squeeze_fits")
@patch("needle.modules.source_find.validate_path_fits")
def test_source_find_context(_, mock_squeeze, mock_fits):
    """Test SourceFindContext initialization and command generation."""
    mock_squeeze.return_value = mock_fits
    cfg = SourceFindConfig(cores=2)
    ctx = SourceFindContext(cfg=cfg, image=mock_fits)

    assert ctx.output.prefix == mock_fits.with_suffix("")
    assert ctx.output.bkg_path == mock_fits.with_suffix("").parent / (mock_fits.stem + "_bkg.fits")

    cmds = ctx.cmd
    assert len(cmds) == 2
    assert "BANE" in cmds[0]
    assert "aegean" in cmds[1]


@patch("astropy.io.fits.open")
def test_squeeze_fits_2d(mock_fits_open, mock_fits):
    """Test that squeeze_fits does nothing to a 2D FITS image."""
    mock_hdu = MagicMock()
    mock_hdu.data.ndim = 2
    mock_hdul = [mock_hdu]
    mock_fits_open.return_value.__enter__.return_value = mock_hdul

    result = squeeze_fits(mock_fits)
    assert result == mock_fits


@patch("astropy.io.fits.open")
@patch("astropy.io.fits.writeto")
def test_squeeze_fits_4d(mock_writeto, mock_fits_open, tmp_path):
    """Test that squeeze_fits correctly reduces a 4D FITS image to 2D."""
    mock_fits = tmp_path / "test.fits"
    mock_fits.touch()

    mock_hdu = MagicMock()
    mock_hdu.data.ndim = 4
    mock_hdu.data.shape = (1, 1, 100, 100)
    # Mocking header with a remove method
    mock_header = MagicMock()
    mock_header.__getitem__.side_effect = lambda x: {
        "NAXIS": 4,
        "NAXIS1": 100,
        "NAXIS2": 100,
        "NAXIS3": 1,
        "NAXIS4": 1,
    }.get(x)
    mock_hdu.header = mock_header

    mock_hdul = [mock_hdu]
    mock_fits_open.return_value.__enter__.return_value = mock_hdul

    result = squeeze_fits(mock_fits)
    assert result == tmp_path / "test-2d.fits"
    assert mock_writeto.called
    assert mock_header.remove.called
