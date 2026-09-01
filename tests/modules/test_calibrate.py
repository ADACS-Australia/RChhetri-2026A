import pytest
from unittest.mock import MagicMock, patch
from needle.modules.calibrate import CalibrateContext, calibrate_observation
from needle.config.calibrate import CalibrateConfig


@pytest.fixture
def mock_ms(tmp_path):
    cal = tmp_path / "cal.ms"
    cal.mkdir()
    tgt = tmp_path / "tgt.ms"
    tgt.mkdir()
    return cal, tgt


def test_calibrate_context_paths(mock_ms):
    """Test that CalibrateContext correctly resolves output paths."""
    cal, tgt = mock_ms
    cfg = CalibrateConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}, "applycal": {}, "split": {}})
    ctx = CalibrateContext(cfg=cfg, cal=cal, tgt=tgt)

    assert ctx._bpcal_path == cal.with_suffix(".bpcal")
    assert ctx._gcal_path == cal.with_suffix(".gcal")
    assert ctx._calibrated_tgt_path == tgt.parent / "tgt_calibrated.ms"


def test_calibrate_context_cmd(mock_ms):
    """Test that CalibrateContext generates the expected CASA commands."""
    cal, tgt = mock_ms
    cfg = CalibrateConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}, "applycal": {}, "split": {}})
    ctx = CalibrateContext(cfg=cfg, cal=cal, tgt=tgt)

    cmds = ctx.cmd
    assert len(cmds) == 5
    assert "setjy" in cmds[0][2]
    assert "bandpass" in cmds[1][2]
    assert "gaincal" in cmds[2][2]
    assert "applycal" in cmds[3][2]
    assert "split" in cmds[4][2]


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
def test_calibrate_observation(mock_execute, mock_ms):
    """Test the execution of the calibration pipeline for an observation."""
    cal, tgt = mock_ms
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]

    cfg = CalibrateConfig.model_validate({"setjy": {}, "bandpass": {}, "gaincal": {}, "applycal": {}, "split": {}})
    ctx = CalibrateContext(cfg=cfg, cal=cal, tgt=tgt)

    result = calibrate_observation(ctx)
    assert result.tgt == tgt.parent / "tgt_calibrated.ms"
    assert result.gcal == ctx._gcal_path
    assert result.bpcal == ctx._bpcal_path
    assert mock_execute.called
