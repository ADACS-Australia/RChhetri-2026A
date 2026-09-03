import json
import numpy as np
import pytest
from pathlib import Path
from unittest.mock import patch

from needle.modules.diagnostics import (
    MSDiagnostics,
    MSDiagnosticsOutput,
    CalDiagnostics,
    CalDiagnosticsContext,
    MSDiagnosticsContext,
)
from needle.config.calibrate import CalibrationSolution


@pytest.fixture
def mock_ms(tmp_path):
    ms = tmp_path / "test.ms"
    ms.mkdir()
    return ms


def test_diagnostics_output_all_files():
    out = MSDiagnosticsOutput(antenna_amp_stats_plot=Path("test.png"))
    assert Path("test.png") in out.all_files


@patch("needle.modules.diagnostics.validate_path_ms")
def test_ms_diagnostics_init(mock_validate, mock_ms):
    diag = MSDiagnostics(ms=mock_ms, output_dir=None)
    assert diag.ms == mock_ms
    assert diag.output_dir == mock_ms.parent


@patch("needle.modules.diagnostics.validate_path_ms")
def test_ms_diagnostics_output_paths(mock_validate, mock_ms):
    diag = MSDiagnostics(ms=mock_ms, output_dir=mock_ms.parent)
    assert diag.amp_phase_vs_time_plot == mock_ms.parent / "test_amp_phase_vs_time.png"
    assert diag.flag_summary_data == mock_ms.parent / "test_flag_summary.json"


@patch("needle.modules.diagnostics.open_table")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_active_antenna_indices(mock_validate, mock_open_table, mock_ms):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.side_effect = [np.array([0, 1, 0]), np.array([1, 0, 1])]  # ANTENNA1  # ANTENNA2
    diag = MSDiagnostics(ms=mock_ms, output_dir=None)
    indices = diag._active_antenna_indices
    assert indices == [0, 1]


@patch("needle.modules.diagnostics.validate_path_ms")
def test_valid_ms_raises_if_missing(mock_validate, tmp_path):
    missing = tmp_path / "missing.ms"
    with pytest.raises(ValueError):
        MSDiagnostics(ms=missing, output_dir=None)


@patch("needle.modules.diagnostics.validate_path_ms")
def test_output_dir_created(mock_validate, tmp_path):
    ms = tmp_path / "test.ms"
    ms.mkdir()
    output_dir = tmp_path / "results"
    assert not output_dir.exists()
    MSDiagnostics(ms=ms, output_dir=output_dir)
    assert output_dir.exists()


@patch("needle.modules.diagnostics.validate_path_ms")
def test_tb_query(mock_validate, mock_ms):
    diag = MSDiagnostics(ms=mock_ms, output_dir=None, spw=2)
    assert diag.tb_query == "DATA_DESC_ID==2"


@patch("needle.modules.diagnostics.open_msmetadata")
@patch("needle.modules.diagnostics.open_table")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_inactive_antenna_names(mock_validate, mock_open_table, mock_open_md, mock_ms):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.side_effect = [np.array([0, 2]), np.array([2, 0])]  # ANTENNA1, ANTENNA2

    mock_md = mock_open_md.return_value.__enter__.return_value
    mock_md.antennanames.return_value = ["ant0", "ant1", "ant2"]

    diag = MSDiagnostics(ms=mock_ms, output_dir=None)
    assert diag._inactive_antenna_names == ["ant1"]


@patch.object(MSDiagnostics, "run_all_diagnostics")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_ms_diagnostics_context_execute(mock_validate, mock_run_all, mock_ms):
    """Test that MSDiagnosticsContext.execute() builds and runs MSDiagnostics correctly.

    Catches a bug where execute() references self.gcal/self.bpcal, neither of
    which is a field on MSDiagnosticsContext, and passes gcal=/bpcal= into
    MSDiagnostics(...), which no longer accepts those kwargs. This currently
    raises AttributeError.
    """
    ctx = MSDiagnosticsContext(ms=mock_ms, output_dir=None)

    result = ctx.execute()

    assert result.ms == mock_ms
    assert result.output_dir == mock_ms.parent
    mock_run_all.assert_called_once()


@patch("needle.modules.diagnostics._save_fig")
@patch("needle.modules.diagnostics.open_table")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_flag_summary_reports_inactive_antennas(mock_validate, mock_open_table, mock_save_fig, mock_ms):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    flags = np.array([[[False, False]]])
    ant1 = np.array([0, 0])
    ant2 = np.array([0, 0])
    mock_tb.getcol.side_effect = [flags, ant1, ant2]

    diag = MSDiagnostics(ms=mock_ms, output_dir=mock_ms.parent)
    diag._active_antenna_indices = [0]
    diag._antenna_names = ["ant0"]
    diag._inactive_antenna_names = ["ant1"]

    summary = diag.flag_summary()
    assert summary["inactive_antennas"] == ["ant1"]


@patch("needle.modules.diagnostics._save_fig")
@patch("needle.modules.diagnostics.open_table")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_antenna_amp_stats_computes_mean_and_std(mock_validate, mock_open_table, mock_save_fig, mock_ms):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_subtb = mock_tb.query.return_value

    data = np.array([[[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]])  # shape (1, 2, 4)
    flags = np.zeros_like(data, dtype=bool)
    ant1 = np.array([0, 0, 1, 1])
    mock_subtb.getcol.side_effect = [data, flags, ant1]

    diag = MSDiagnostics(ms=mock_ms, output_dir=mock_ms.parent)
    diag._active_antenna_indices = [0, 1]
    diag._antenna_names = ["ant0", "ant1"]

    diag.antenna_amp_stats()

    with open(diag.antenna_amp_stats_data) as f:
        stats = json.load(f)

    ant0_vals = [1.0, 2.0, 5.0, 6.0]
    ant1_vals = [3.0, 4.0, 7.0, 8.0]
    assert stats["ant0"]["mean"] == pytest.approx(np.mean(ant0_vals), abs=1e-6)
    assert stats["ant0"]["std"] == pytest.approx(np.std(ant0_vals), abs=1e-6)
    assert stats["ant1"]["mean"] == pytest.approx(np.mean(ant1_vals), abs=1e-6)
    assert stats["ant1"]["std"] == pytest.approx(np.std(ant1_vals), abs=1e-6)


@patch("needle.modules.diagnostics._save_fig")
@patch("needle.modules.diagnostics.open_table")
@patch("needle.modules.diagnostics.validate_path_ms")
def test_antenna_amp_stats_skips_antenna_with_no_data(mock_validate, mock_open_table, mock_save_fig, mock_ms):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_subtb = mock_tb.query.return_value

    data = np.array([[[1.0, 2.0]]])
    flags = np.zeros_like(data, dtype=bool)
    ant1 = np.array([0, 0])  # antenna 1 has no rows at all
    mock_subtb.getcol.side_effect = [data, flags, ant1]

    diag = MSDiagnostics(ms=mock_ms, output_dir=mock_ms.parent)
    diag._active_antenna_indices = [0, 1]
    diag._antenna_names = ["ant0", "ant1"]

    diag.antenna_amp_stats()

    with open(diag.antenna_amp_stats_data) as f:
        stats = json.load(f)

    assert "ant0" in stats
    assert "ant1" not in stats


@pytest.fixture
def mock_solution(tmp_path):
    gcal = tmp_path / "test.gcal"
    bpcal = tmp_path / "test.bpcal"
    gcal.mkdir()
    bpcal.mkdir()
    return CalibrationSolution(gcal=gcal, bpcal=bpcal)


@patch.object(CalDiagnostics, "run_all_diagnostics")
def test_cal_diagnostics_context_execute(mock_run_all, mock_solution):
    ctx = CalDiagnosticsContext(solution=mock_solution, output_dir=None)

    result = ctx.execute()

    assert result.gcal == mock_solution.gcal
    assert result.bpcal == mock_solution.bpcal
    mock_run_all.assert_called_once()


def test_cal_diagnostics_init(mock_solution):
    cd = CalDiagnostics(solution=mock_solution, output_dir=None)
    assert cd.gcal == mock_solution.gcal
    assert cd.bpcal == mock_solution.bpcal
    assert cd.output_dir == mock_solution.gcal.parent


def test_cal_diagnostics_output_paths(mock_solution):
    cd = CalDiagnostics(solution=mock_solution, output_dir=mock_solution.gcal.parent)
    assert cd.gain_caltable_plot == mock_solution.gcal.parent / "test_gain_caltable.png"
    assert cd.bandpass_caltable_plot == mock_solution.gcal.parent / "test_bandpass_caltable.png"


def test_cal_diagnostics_output_dir_created(tmp_path):
    gcal = tmp_path / "test.gcal"
    bpcal = tmp_path / "test.bpcal"
    gcal.mkdir()
    bpcal.mkdir()
    solution = CalibrationSolution(gcal=gcal, bpcal=bpcal)

    output_dir = tmp_path / "cal_results"
    assert not output_dir.exists()
    CalDiagnostics(solution=solution, output_dir=output_dir)
    assert output_dir.exists()


@patch("needle.modules.diagnostics.open_table")
def test_cal_active_antenna_indices(mock_open_table, mock_solution):
    mock_tb = mock_open_table.return_value.__enter__.return_value
    mock_tb.getcol.return_value = np.array([1, 0, 1, 2])

    cd = CalDiagnostics(solution=mock_solution, output_dir=None)
    assert cd._active_antenna_indices == [0, 1, 2]
