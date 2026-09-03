import pytest
from unittest.mock import MagicMock, patch
from needle.modules.calibrate import (
    SolveCalibrationContext,
    ApplyCalibrationContext,
    solve_calibration,
    apply_calibration,
)
from needle.config.calibrate import SolveCalibrationConfig, ApplyCalibrationConfig, CalibrationSolution


@pytest.fixture
def mock_ms(tmp_path):
    cal = tmp_path / "cal.ms"
    cal.mkdir()
    tgt = tmp_path / "tgt.ms"
    tgt.mkdir()
    return cal, tgt


@pytest.fixture
def mock_solution(tmp_path):
    gcal = tmp_path / "cal.gcal"
    bpcal = tmp_path / "cal.bpcal"
    gcal.mkdir()
    bpcal.mkdir()
    return CalibrationSolution(gcal=gcal, bpcal=bpcal)


def test_solve_calibration_context_paths(mock_ms):
    """Test that SolveCalibrationContext correctly resolves output paths."""
    cal, _ = mock_ms
    cfg = SolveCalibrationConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}})
    ctx = SolveCalibrationContext(cfg=cfg, cal=cal)
    assert ctx.bpcal_path == cal.with_suffix(".bpcal")
    assert ctx.gcal_path == cal.with_suffix(".gcal")


def test_solve_calibration_context_cmd(mock_ms):
    """Test that SolveCalibrationContext generates the expected CASA commands."""
    cal, _ = mock_ms
    cfg = SolveCalibrationConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}})
    ctx = SolveCalibrationContext(cfg=cfg, cal=cal)
    cmds = ctx.cmd
    assert len(cmds) == 3
    assert "setjy" in cmds[0][2]
    assert "bandpass" in cmds[1][2]
    assert "gaincal" in cmds[2][2]


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
def test_solve_calibration(mock_execute, mock_ms):
    """Test the execution of the calibration solving pipeline."""
    cal, _ = mock_ms
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    cfg = SolveCalibrationConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}})
    ctx = SolveCalibrationContext(cfg=cfg, cal=cal)

    result = solve_calibration(ctx)

    assert result.gcal == ctx.gcal_path
    assert result.bpcal == ctx.bpcal_path
    assert mock_execute.called


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
@patch("needle.modules.calibrate.shutil.rmtree")
def test_solve_calibration_removes_existing_solutions(mock_rmtree, mock_execute, mock_ms):
    """Test that pre-existing gcal/bpcal tables are removed before solving."""
    cal, _ = mock_ms
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    cfg = SolveCalibrationConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}})
    ctx = SolveCalibrationContext(cfg=cfg, cal=cal)

    ctx.gcal_path.mkdir()
    ctx.bpcal_path.mkdir()

    solve_calibration(ctx)

    assert mock_rmtree.call_count == 2
    mock_rmtree.assert_any_call(ctx.gcal_path)
    mock_rmtree.assert_any_call(ctx.bpcal_path)


def test_apply_calibration_context_paths(mock_ms, mock_solution):
    """Test that ApplyCalibrationContext correctly resolves output paths."""
    _, tgt = mock_ms
    cfg = ApplyCalibrationConfig.model_validate({"applycal": {}, "split": {}})
    ctx = ApplyCalibrationContext(cfg=cfg, cal=mock_solution, tgt=tgt)
    assert ctx.calibrated_tgt_path == tgt.parent / "tgt_calibrated.ms"


def test_apply_calibration_context_cmd(mock_ms, mock_solution):
    """Test that ApplyCalibrationContext generates the expected CASA commands.

    Catches a bug where _applycal_cmd/_split_cmd reference self._bpcal_path,
    self.gcal_path, and self._calibrated_tgt_path, none of which exist on
    ApplyCalibrationContext (should be self.cal.bpcal, self.cal.gcal, and
    self.calibrated_tgt_path respectively). This currently raises AttributeError.
    """
    _, tgt = mock_ms
    cfg = ApplyCalibrationConfig.model_validate({"applycal": {}, "split": {}})
    ctx = ApplyCalibrationContext(cfg=cfg, cal=mock_solution, tgt=tgt)

    cmds = ctx.cmd

    assert len(cmds) == 2
    assert "applycal" in cmds[0][2]
    assert str(mock_solution.bpcal) in cmds[0][2]
    assert str(mock_solution.gcal) in cmds[0][2]
    assert "split" in cmds[1][2]
    assert str(ctx.calibrated_tgt_path) in cmds[1][2]


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
def test_apply_calibration(mock_execute, mock_ms, mock_solution):
    """Test the execution of the calibration application pipeline."""
    _, tgt = mock_ms
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    cfg = ApplyCalibrationConfig.model_validate({"applycal": {}, "split": {}})
    ctx = ApplyCalibrationContext(cfg=cfg, cal=mock_solution, tgt=tgt)

    result = apply_calibration(ctx)

    assert result == tgt.parent / "tgt_calibrated.ms"
    assert mock_execute.called


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
@patch("needle.modules.calibrate.shutil.rmtree")
def test_apply_calibration_removes_existing_output(mock_rmtree, mock_execute, mock_ms, mock_solution):
    """Test that a pre-existing calibrated target is removed before applying."""
    _, tgt = mock_ms
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    cfg = ApplyCalibrationConfig.model_validate({"applycal": {}, "split": {}})
    ctx = ApplyCalibrationContext(cfg=cfg, cal=mock_solution, tgt=tgt)

    ctx.calibrated_tgt_path.mkdir()

    apply_calibration(ctx)

    mock_rmtree.assert_called_once_with(ctx.calibrated_tgt_path)
