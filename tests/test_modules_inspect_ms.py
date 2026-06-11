import pytest
import json
import numpy as np
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch
from needle.modules.inspect import (
    MSInfo,
    TimeInfo,
    FrequencyInfo,
    BaselineInfo,
    PolarisationInfo,
    FieldInfo,
    InspectMSContext,
    inspect_ms,
)
from needle.config.container import ContainerConfig


@pytest.fixture
def mock_ms(tmp_path):
    ms = tmp_path / "test.ms"
    ms.mkdir()
    return ms


def test_ms_info_from_json(tmp_path):
    """Test loading MSInfo data from a JSON file."""
    json_path = tmp_path / "test_inspect.json"
    data = {
        "ms": "test.ms",
        "time": {
            "start_utc": "2023-01-01",
            "end_utc": "2023-01-01",
            "start_mjd_s": 0.0,
            "end_mjd_s": 100.0,
            "integration_time_s": 1.0,
            "n_integrations": 100,
            "total_duration_s": 100.0,
            "total_duration_min": 1.6667,
        },
        "frequency": {
            "n_spw": 1,
            "spw_centre_hz": [1e9],
            "spw_width_hz": [1e6],
            "spw_n_channels": [1024],
            "spw_freq_min_hz": [0.9e9],
            "spw_freq_max_hz": [1.1e9],
            "centre_wavelength_m": 0.3,
        },
        "baselines": {
            "n_antennas": 2,
            "antenna_names": ["A1", "A2"],
            "antenna_positions_m": [[0, 0, 0], [1, 1, 1]],
            "n_baselines": 1,
            "uv_min_lambda": 1.0,
            "uv_max_lambda": 10.0,
            "uv_min_m": 1.0,
            "uv_max_m": 10.0,
        },
        "polarisation": {"polarisations": ["I"], "n_pols": 1},
        "fields": {"n_fields": 1, "field_names": ["F1"], "phase_centres_deg": [{"ra_deg": 0.0, "dec_deg": 0.0}]},
        "data_columns": {"DATA": [1, 1024, 1]},
    }
    with open(json_path, "w") as f:
        json.dump(data, f)

    info = MSInfo.from_json(json_path)
    assert info.ms == Path("test.ms")
    assert info.time.n_integrations == 100
    assert info.frequency.n_spw == 1


@patch("needle.modules.inspect.open_table")
def test_read_time(mock_open_table, mock_ms):
    """Test reading time information from a Measurement Set table."""
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.return_value = np.array([50000.0, 50001.0, 50002.0])  # MJD seconds

    info = MSInfo(ms=mock_ms)
    t = info._read_time()

    assert isinstance(t, TimeInfo)
    assert t.n_integrations == 3
    assert t.integration_time_s == 1.0
    assert t.total_duration_s == 3.0


@patch("needle.modules.inspect.open_table")
def test_read_frequency(mock_open_table, mock_ms):
    """Test reading frequency information from a Measurement Set table."""
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.nrows.return_value = 1
    mock_tb.getcell.side_effect = [np.array([1e9, 1.1e9]), np.array([0.1e9, 0.1e9])]  # CHAN_FREQ  # CHAN_WIDTH

    info = MSInfo(ms=mock_ms)
    f = info._read_frequency()

    assert isinstance(f, FrequencyInfo)
    assert f.n_spw == 1
    assert f.spw_centre_hz[0] == 1.05e9


@patch("needle.modules.inspect.open_table")
def test_read_baselines(mock_open_table, mock_ms):
    """Test reading baseline information from a Measurement Set table."""
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.side_effect = [
        np.array(["A1", "A2"]),  # antenna_names
        np.array([[0, 0, 0], [10, 10, 10]]),  # antenna_positions
        np.array([[100, 200], [300, 400], [500, 600]]),  # UVW (3, n_rows)
    ]

    with patch("needle.modules.inspect.MSInfo.frequency", new_callable=MagicMock) as mock_freq:
        mock_freq.centre_wavelength_m = 0.2
        info = MSInfo(ms=mock_ms)
        b = info._read_baselines()

        assert isinstance(b, BaselineInfo)
        assert b.n_antennas == 2
        assert b.n_baselines == 1
        assert b.uv_max_m > 0


@patch("needle.modules.inspect.open_table")
def test_read_polarisation(mock_open_table, mock_ms):
    """Test reading polarisation information from a Measurement Set table."""
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcell.return_value = np.array([1, 2, 3, 4])  # I, Q, U, V

    info = MSInfo(ms=mock_ms)
    p = info._read_polarisation()

    assert isinstance(p, PolarisationInfo)
    assert p.polarisations == ["I", "Q", "U", "V"]


@patch("needle.modules.inspect.open_table")
def test_read_fields(mock_open_table, mock_ms):
    """Test reading field information from a Measurement Set table."""
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.side_effect = [["FIELD1"], np.zeros((2, 1, 1))]  # Names  # PHASE_DIR (2, 1, n_fields)

    info = MSInfo(ms=mock_ms)
    f = info._read_fields()

    assert isinstance(f, FieldInfo)
    assert f.n_fields == 1
    assert f.field_names == ["FIELD1"]


@patch("needle.modules.inspect.open_table")
def test_read_data_columns(mock_open_table, mock_ms):
    """Test reading available data columns from a Measurement Set table."""
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.colnames.return_value = ["TIME", "DATA", "CORRECTED_DATA"]
    mock_tb.getcell.return_value = np.zeros((10, 10))

    info = MSInfo(ms=mock_ms)
    cols = info._read_data_columns()

    assert "DATA" in cols
    assert "CORRECTED_DATA" in cols
    assert "TIME" not in cols


def test_inspect_ms_context(mock_ms):
    """Test InspectMSContext validation."""
    ctx = InspectMSContext(ms=mock_ms)
    assert ctx.ms == mock_ms
    assert ctx.output_dir is None


def test_inspect_ms_local(mock_ms):
    """Test local execution of the inspect_ms function."""
    ctx = InspectMSContext(ms=mock_ms)
    with patch("needle.modules.inspect.MSInfo") as mock_ms_info_cls:
        result = inspect_ms(ctx)
        mock_ms_info_cls.assert_called_once_with(ms=mock_ms, output_dir=None)
        assert result == mock_ms_info_cls.return_value
