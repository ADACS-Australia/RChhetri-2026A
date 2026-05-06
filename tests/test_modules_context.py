import pytest
import subprocess
from unittest.mock import MagicMock, patch
from needle.modules.needle_context import NeedleContext, SubprocessExecContext
from needle.config.base import ContainerConfig


def test_needle_context_execute_not_implemented():
    """Test that base NeedleContext raises NotImplementedError for execute."""
    ctx = NeedleContext()
    with pytest.raises(NotImplementedError):
        ctx.execute()


class MockSubprocessContext(SubprocessExecContext):
    @property
    def cmd(self) -> list[list[str]]:
        return [["ls", "-l"], ["pwd"]]


def test_subprocess_exec_context_resolved_cmds():
    """Test command resolution in SubprocessExecContext."""
    ctx = MockSubprocessContext()
    assert ctx._resolved_cmds == [["ls", "-l"], ["pwd"]]


def test_subprocess_exec_context_with_runtime(tmp_path):
    """Test command resolution with a container runtime."""
    image = tmp_path / "test.sif"
    image.touch()
    runtime = ContainerConfig(image=image)
    ctx = MockSubprocessContext(runtime=runtime)

    resolved = ctx._resolved_cmds
    assert len(resolved) == 2
    assert resolved[0][:2] == ["apptainer", "exec"]
    assert resolved[0][-2:] == ["ls", "-l"]


def test_subprocess_exec_context_validate_cmd():
    """Test validation of command format in SubprocessExecContext."""

    class BadCmdContext(SubprocessExecContext):
        @property
        def cmd(self) -> list[str]:  # type: ignore
            return ["ls", "-l"]

    with pytest.raises(ValueError, match="cmd must be a list of lists"):
        BadCmdContext()


@patch("subprocess.run")
def test_subprocess_exec_context_execute(mock_run):
    """Test successful execution of subprocess commands."""
    mock_run.return_value = MagicMock(spec=subprocess.CompletedProcess, stdout="ok", stderr="")
    ctx = MockSubprocessContext()
    results = ctx.execute()

    assert len(results) == 2
    assert mock_run.call_count == 2
    mock_run.assert_any_call(["ls", "-l"], capture_output=True, text=True, check=True)
    mock_run.assert_any_call(["pwd"], capture_output=True, text=True, check=True)


@patch("subprocess.run")
def test_subprocess_exec_context_execute_fail(mock_run):
    """Test error handling when a subprocess command fails."""
    mock_run.side_effect = subprocess.CalledProcessError(1, ["ls"], stderr="error")
    ctx = MockSubprocessContext()
    with pytest.raises(RuntimeError):
        ctx.execute()
