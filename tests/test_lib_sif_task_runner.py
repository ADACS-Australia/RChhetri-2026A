import pytest
import subprocess
import base64
import cloudpickle
from unittest.mock import MagicMock, patch
from needle.lib.sif_task_runner import SifTaskRunner
from needle.config.container import ContainerConfig

def mock_task(x, y):
    return x + y

@pytest.fixture
def mock_image(tmp_path):
    img = tmp_path / "test.sif"
    img.touch()
    return img

@patch("subprocess.run")
def test_sif_task_runner_run_in_container(mock_run, mock_image):
    cfg = ContainerConfig(image=mock_image)
    runner = SifTaskRunner(cfg)
    
    # Mock subprocess result
    expected_result = 5
    mock_proc = MagicMock(spec=subprocess.CompletedProcess)
    mock_proc.returncode = 0
    mock_proc.stdout = base64.b64encode(cloudpickle.dumps(expected_result))
    mock_run.return_value = mock_proc
    
    result = runner._run_in_container(mock_task, {"x": 2, "y": 3})
    
    assert result == expected_result
    mock_run.assert_called_once()
    args, kwargs = mock_run.call_args
    assert "apptainer" in args[0]
    assert str(mock_image) in args[0]
    assert "python" in args[0]

@patch("subprocess.run")
def test_sif_task_runner_submit(mock_run, mock_image):
    cfg = ContainerConfig(image=mock_image)
    runner = SifTaskRunner(cfg)
    
    mock_proc = MagicMock(spec=subprocess.CompletedProcess)
    mock_proc.returncode = 0
    mock_proc.stdout = base64.b64encode(cloudpickle.dumps(10))
    mock_run.return_value = mock_proc
    
    with runner:
        future = runner.submit(MagicMock(fn=mock_task), {"x": 5, "y": 5})
        result = future.result()
        assert result == 10

def test_sif_task_runner_not_started(mock_image):
    cfg = ContainerConfig(image=mock_image)
    runner = SifTaskRunner(cfg)
    with pytest.raises(RuntimeError, match="The task runner must be started"):
        runner.submit(MagicMock(fn=mock_task), {})
