from pathlib import Path
from unittest.mock import MagicMock, patch
from needle.modules.convert import ConvertContext, convert_to_ms


def test_convert_context_output():
    """Test output path resolution in ConvertContext."""
    ctx = ConvertContext(input=Path("test.uvfits"))
    assert ctx.output == Path("test.ms")

    ctx2 = ConvertContext(input=Path("test.uvfits"), output_dir=Path("/tmp"))
    assert ctx2.output == Path("/tmp/test.ms")


def test_convert_context_cmd_uvfits():
    """Test command generation for UVFITS to MS conversion."""
    ctx = ConvertContext(input=Path("test.uvfits"))
    cmd = ctx.cmd[0]
    assert "importuvfits" in cmd[2]
    assert "test.uvfits" in cmd[2]
    assert "test.ms" in cmd[2]


def test_convert_context_cmd_mir():
    """Test command generation for Miriad to MS conversion."""
    ctx = ConvertContext(input=Path("test.mir"))
    cmd = ctx.cmd[0]
    assert "importmiriad" in cmd[2]
    assert "test.mir" in cmd[2]
    assert "test.ms" in cmd[2]


def test_convert_context_cmd_ms_no_output_dir(tmp_path):
    """Test command generation when input is already an MS and no output dir is specified."""
    ms_path = tmp_path / "test.ms"
    ms_path.mkdir()
    ctx = ConvertContext(input=ms_path)
    assert ctx.cmd == [[]]


@patch("needle.modules.needle_context.SubprocessExecContext.execute")
def test_convert_to_ms(mock_execute, tmp_path):
    """Test the execution of the format conversion to MS."""
    mock_execute.return_value = [MagicMock(stdout="done", stderr="")]
    input_path = tmp_path / "test.uvfits"
    input_path.touch()

    ctx = ConvertContext(input=input_path, output_dir=tmp_path / "out")
    assert not ctx.output.exists()  # ensure output doesn't exist so it runs

    result = convert_to_ms(ctx)
    assert result == tmp_path / "out" / "test.ms"
    assert mock_execute.called
