import pytest
import yaml
from pathlib import Path
from unittest.mock import patch
from pydantic import ValidationError
from needle.config.cluster import ScalingConfig, LocalConfig, ClusterConfig

def test_scaling_config_valid_interval():
    cfg = ScalingConfig(interval="10s")
    assert cfg.interval == "10s"
    assert cfg.adapt_kwargs["interval"] == "10s"

def test_scaling_config_invalid_interval():
    with pytest.raises(ValidationError, match="Invalid interval"):
        ScalingConfig(interval="10sec")

def test_scaling_config_scheduler_options():
    cfg = ScalingConfig(dashboard_port=9999)
    assert cfg.scheduler_options == {"dashboard_address": ":9999"}

def test_cluster_config_validate_local():
    data = {
        "type": "local",
        "scaling": {"max_workers": 2},
        "local": {"cores": 4, "memory": "8GB"}
    }
    assert ClusterConfig.validate(data, quiet=True) is True

def test_cluster_config_validate_slurm():
    data = {
        "type": "slurm",
        "slurm": {"account": "my_account", "queue": "debug"}
    }
    assert ClusterConfig.validate(data, quiet=True) is True

def test_cluster_config_load_from_file(tmp_path):
    cfg_data = {
        "type": "local",
        "scaling": {"max_workers": 5}
    }
    cfg_path = tmp_path / "cluster.yaml"
    with open(cfg_path, "w") as f:
        yaml.dump(cfg_data, f)
    
    cfg = ClusterConfig.load(cfg_path)
    assert cfg.type == "local"
    assert cfg.scaling.max_workers == 5

def test_cluster_config_get_config_no_file():
    with patch("pathlib.Path.home", return_value=Path("/tmp/fake_home")):
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Expected file /tmp/fake_home/.needle_cluster.yaml does not exist"):
                ClusterConfig.get_config()

@patch("needle.config.cluster.SifLocalCluster")
@patch("needle.config.cluster.DaskTaskRunner")
def test_cluster_config_to_task_runner_local(mock_runner, mock_cluster):
    cfg = ClusterConfig(type="local", local=LocalConfig(cores=2))
    cfg.to_task_runner()
    mock_runner.assert_called_once()
    kwargs = mock_runner.call_args.kwargs
    assert kwargs["cluster_class"] == mock_cluster
    assert kwargs["cluster_kwargs"]["cores"] == 2
