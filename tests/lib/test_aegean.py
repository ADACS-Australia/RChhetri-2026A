import numpy as np
import pytest
from unittest.mock import MagicMock
from needle.lib.aegean import AegeanSource, AegeanSourceList
from AegeanTools.models import ComponentSource


def test_aegean_source_sanitise_floats():
    """Test sanitisation of NaN and inf values in AegeanSource."""
    # Test that NaN and inf are converted to None
    source = AegeanSource(island=1, source=1, background=np.nan, local_rms=np.inf, ra=10.0, dec=-20.0, flags="0000000")
    assert source.background is None
    assert source.local_rms is None
    assert source.ra == 10.0
    assert source.dec == -20.0


def test_aegean_source_from_component():
    """Test creating AegeanSource from an Aegean ComponentSource object."""
    mock_src = MagicMock(spec=ComponentSource)
    mock_src.island = 1
    mock_src.source = 2
    mock_src.background = 0.1
    mock_src.local_rms = 0.01
    mock_src.ra = 123.4
    mock_src.err_ra = 0.01
    mock_src.dec = -45.6
    mock_src.err_dec = 0.01
    mock_src.peak_flux = 1.0
    mock_src.err_peak_flux = 0.1
    mock_src.int_flux = 1.2
    mock_src.err_int_flux = 0.15
    mock_src.a = 10.0
    mock_src.err_a = 0.5
    mock_src.b = 8.0
    mock_src.err_b = 0.4
    mock_src.pa = 45.0
    mock_src.err_pa = 2.0
    mock_src.flags = 1
    mock_src.residual_mean = 0.001
    mock_src.residual_std = 0.01
    mock_src.uuid = "some-uuid"
    mock_src.psf_a = 0.002
    mock_src.psf_b = 0.002
    mock_src.psf_pa = 0.0

    source = AegeanSource.from_component(mock_src)

    assert source.island == 1
    assert source.source == 2
    assert source.flags == "0000001"
    assert source.ra == 123.4
    assert source.uuid == "some-uuid"


def test_aegean_source_list_json(tmp_path):
    """Test AegeanSourceList JSON serialization and deserialization."""
    source = AegeanSource(island=1, source=1, background=0.1, local_rms=0.01, ra=10.0, dec=-20.0, flags="0000000")
    source_list = AegeanSourceList(sources=[source])

    json_path = tmp_path / "sources.json"
    source_list.to_json(json_path)

    assert json_path.exists()

    loaded_list = AegeanSourceList.from_json(json_path)
    assert len(loaded_list.sources) == 1
    assert loaded_list.sources[0].island == 1
    assert loaded_list.sources[0].ra == 10.0


def test_aegean_source_list_from_txt_catalog(tmp_path):
    """Test creating AegeanSourceList from a text catalog file."""
    catalog_content = """# island,source background local_rms ra_str dec_str ra err_ra dec err_dec peak_flux err_peak_flux int_flux err_int_flux a err_a b err_b pa err_pa flags
(0001,00) 0.1 0.01 10:00:00 -20:00:00 150.0 0.01 -20.0 0.01 1.0 0.1 1.2 0.15 10.0 0.5 8.0 0.4 45.0 2.0 1
"""
    catalog_path = tmp_path / "catalog.txt"
    catalog_path.write_text(catalog_content)

    source_list = AegeanSourceList.from_txt_catalog(catalog_path)

    assert len(source_list.sources) == 1
    src = source_list.sources[0]
    assert src.island == 1
    assert src.source == 0
    assert src.ra == 150.0
    assert src.dec == -20.0
    assert src.flags == "0000001"


def test_aegean_source_list_from_json_invalid_extension():
    """Test that from_json raises ValueError for non-JSON file extensions."""
    with pytest.raises(ValueError, match="Expected .json"):
        AegeanSourceList.from_json("test.txt")
