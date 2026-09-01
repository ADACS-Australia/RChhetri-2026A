import pytest
from unittest.mock import MagicMock, patch
from needle.modules.flag import FlagContext, flag_observation
from needle.config.flag import FlagConfig, QuackConfig, ClipConfig
from pydantic import ValidationError


@pytest.fixture
def mock_ms(tmp_path):
    ms = tmp_path / "test.ms"
    ms.mkdir()
    return ms


def test_flag_context_cmd(mock_ms):
    """Test that FlagContext generates the correct command-line arguments."""
    cfg = FlagConfig(quack=QuackConfig(), clip=ClipConfig())
    ctx = FlagContext(cfg=cfg, ms=mock_ms)
    cmds = ctx.cmd
    assert len(cmds) == 2
    assert "quack" in cmds[0][2]
    assert "flagdata" in cmds[0][2]


def test_flag_context_no_active_steps(mock_ms):
    """Test that FlagContext raises an error when no flagging steps are enabled."""
    cfg = FlagConfig(quack=None, clip=None)
    with pytest.raises(ValidationError, match="No flagging steps configured"):
        FlagContext(cfg=cfg, ms=mock_ms)


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
def test_flag_observation(mock_execute, mock_ms):
    """Test the execution of the flagging process on an observation."""
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    cfg = FlagConfig(quack=QuackConfig())
    ctx = FlagContext(cfg=cfg, ms=mock_ms)

    flag_observation(ctx)
    assert mock_execute.called
