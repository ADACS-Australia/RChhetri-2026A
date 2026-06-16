import pytest
import subprocess
from pathlib import Path
from unittest.mock import MagicMock
from needle.modules.casa_data import CasaDataUpdateContext, download_casa_rundata

def test_casa_data_update_context_cmd():
    path = Path("/tmp/casa_data")
    ctx = CasaDataUpdateContext(casa_data_path=path)
    assert "casaconfig.data_update(path='/tmp/casa_data')" in ctx.cmd[0][2]

def test_download_casa_rundata_success(tmp_path):
    data_path = tmp_path / "casa_data"
    data_path.mkdir()
    readme = data_path / "readme.txt"
    readme.touch()
    
    ctx = MagicMock(spec=CasaDataUpdateContext)
    ctx.casa_data_path = data_path
    mock_proc = MagicMock(spec=subprocess.CompletedProcess)
    mock_proc.stdout = "Success"
    mock_proc.stderr = ""
    ctx.execute.return_value = [mock_proc]
    
    download_casa_rundata(ctx)
    
    mock_proc.check_returncode.assert_called_once()

def test_download_casa_rundata_no_readme(tmp_path):
    data_path = tmp_path / "casa_data"
    data_path.mkdir()
    # readme.txt is NOT created
    
    ctx = MagicMock(spec=CasaDataUpdateContext)
    ctx.casa_data_path = data_path
    mock_proc = MagicMock(spec=subprocess.CompletedProcess)
    mock_proc.stdout = "Success"
    mock_proc.stderr = ""
    ctx.execute.return_value = [mock_proc]
    
    # Should complete without error but log a warning (not checked here)
    download_casa_rundata(ctx)
    
    mock_proc.check_returncode.assert_called_once()

def test_download_casa_rundata_process_error():
    ctx = MagicMock(spec=CasaDataUpdateContext)
    mock_proc = MagicMock(spec=subprocess.CompletedProcess)
    mock_proc.stdout = ""
    mock_proc.stderr = "Error"
    # Make check_returncode raise an error
    mock_proc.check_returncode.side_effect = subprocess.CalledProcessError(1, "cmd")
    ctx.execute.return_value = [mock_proc]
    
    with pytest.raises(subprocess.CalledProcessError):
        download_casa_rundata(ctx)
