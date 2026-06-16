import pytest
import numpy as np
from pathlib import Path
from unittest.mock import patch
from needle.modules.diagnostics import MSDiagnostics, DiagnosticsOutput

@pytest.fixture
def mock_ms(tmp_path):
    ms = tmp_path / "test.ms"
    ms.mkdir()
    return ms

def test_diagnostics_output_all_files():
    out = DiagnosticsOutput(antenna_amp_stats_plot=Path("test.png"))
    assert Path("test.png") in out.all_files

@patch("needle.modules.diagnostics.validate_path_ms")
def test_ms_diagnostics_init(mock_validate, mock_ms):
    diag = MSDiagnostics(ms=mock_ms, gcal=None, bpcal=None, output_dir=None)
    assert diag.ms == mock_ms
    assert diag.output_dir == mock_ms.parent

@patch("needle.modules.diagnostics.validate_path_ms")
def test_ms_diagnostics_output_paths(mock_validate, mock_ms):
    diag = MSDiagnostics(ms=mock_ms, gcal=None, bpcal=None, output_dir=mock_ms.parent)
    assert diag.amp_phase_vs_time_plot == mock_ms.parent / "test_amp_phase_vs_time.png"
    assert diag.flag_summary_data == mock_ms.parent / "test_flag_summary.json"

@patch("needle.modules.diagnostics.open_table")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_active_antenna_indices(mock_validate, mock_open_table, mock_ms):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.side_effect = [
        np.array([0, 1, 0]), # ANTENNA1
        np.array([1, 0, 1])  # ANTENNA2
    ]
    
    diag = MSDiagnostics(ms=mock_ms, gcal=None, bpcal=None, output_dir=None)
    indices = diag._active_antenna_indices
    assert indices == [0, 1]
