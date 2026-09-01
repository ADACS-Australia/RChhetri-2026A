import pytest
from unittest.mock import MagicMock, patch
from needle.modules.clean import WSCleanContext, run_clean, WSCleanOutput
from needle.config.clean import WSCleanConfig


@pytest.fixture
def mock_ms(tmp_path):
    ms = tmp_path / "test.ms"
    ms.mkdir()
    return ms


def test_wsclean_context_name(mock_ms):
    """Test that WSCleanContext generates the correct name/prefix."""
    cfg = WSCleanConfig(tag="testtag")
    ctx = WSCleanContext(cfg=cfg, ms=mock_ms)
    assert "testtag" in ctx.name
    assert str(mock_ms.with_suffix("")) in ctx.name


def test_wsclean_context_cmd(mock_ms):
    """Test that WSCleanContext generates the expected wsclean command."""
    cfg = WSCleanConfig(size=1024, niter=100)
    ctx = WSCleanContext(cfg=cfg, ms=mock_ms)
    cmd = ctx.cmd[0]
    assert "wsclean" in cmd
    assert "-size" in cmd
    assert "1024" in cmd
    assert "-niter" in cmd
    assert "100" in cmd
    assert str(mock_ms) in cmd


def test_wsclean_output_globs(tmp_path):
    """Test that WSCleanOutput correctly identifies output files using globs."""
    prefix = tmp_path / "test"
    (tmp_path / "test-image.fits").touch()
    (tmp_path / "test-psf.fits").touch()

    output = WSCleanOutput(prefix=prefix)
    assert len(output.image) == 1
    assert len(output.psf) == 1
    assert len(output.dirty) == 0


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
def test_run_clean(mock_execute, mock_ms):
    """Test the execution of the wsclean tool."""
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    cfg = WSCleanConfig()
    ctx = WSCleanContext(cfg=cfg, ms=mock_ms)

    output = run_clean(ctx)
    assert isinstance(output, WSCleanOutput)
    assert output.prefix == ctx.name
