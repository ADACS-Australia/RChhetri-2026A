import pytest
import yaml
from pathlib import Path
from unittest.mock import patch
from pydantic import ValidationError
from needle.config.cluster import ScalingConfig, ClusterConfig


def test_scaling_config_valid_interval():
    cfg = ScalingConfig(interval="10s")
    assert cfg.interval == "10s"
    assert cfg.adapt_kwargs["interval"] == "10s"


def test_scaling_config_invalid_interval():
    with pytest.raises(ValidationError, match="Invalid interval"):
        ScalingConfig(interval="10sec")


def test_scaling_config_scheduler_options():
    cfg = ScalingConfig(dashboard_port=9999, interface=None)
    assert cfg.scheduler_options == {"dashboard_address": ":9999"}
    cfg = ScalingConfig(dashboard_port=9999, interface="anything")
    assert cfg.scheduler_options == {"dashboard_address": ":9999", "interface": "anything"}


def test_cluster_config_validate_local():
    data = {"type": "local", "scaling": {"max_workers": 2}, "local": {"cores": 4, "memory": "8GB"}}
    assert ClusterConfig.validate(data, quiet=True) is True


def test_cluster_config_validate_slurm():
    data = {"type": "slurm", "slurm": {"account": "my_account", "queue": "debug"}}
    assert ClusterConfig.validate(data, quiet=True) is True


def test_cluster_config_load_from_file(tmp_path):
    cfg_data = {"type": "local", "scaling": {"max_workers": 5}}
    cfg_path = tmp_path / "cluster.yaml"
    with open(cfg_path, "w") as f:
        yaml.dump(cfg_data, f)

    cfg = ClusterConfig.load(cfg_path)
    assert cfg.type == "local"
    assert cfg.scaling.max_workers == 5


def test_cluster_config_load_missing_section():
    data = {"local": {"cores": 1}}  # no 'type' at all
    with pytest.raises(ValueError, match="missing required section\\(s\\): 'type'"):
        ClusterConfig.load(data)


def test_cluster_config_load_missing_nested_field():
    # container provided but missing one of its own required fields
    data = {"type": "local", "container": {}}
    with pytest.raises(ValueError, match="missing required field\\(s\\): 'container\\."):
        ClusterConfig.load(data)


def test_cluster_config_get_config_no_file():
    with patch("pathlib.Path.home", return_value=Path("/tmp/fake_home")):
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(
                FileNotFoundError, match="Expected file /tmp/fake_home/.needle_cluster.yaml does not exist"
            ):
                ClusterConfig.get_config()


def test_to_cluster_local_calls_correct_class_and_adapt():
    data = {"type": "local", "scaling": {"min_workers": 1, "max_workers": 3}, "local": {"cores": 2, "memory": "4GB"}}
    cfg = ClusterConfig.load(data)

    with patch("needle.config.cluster.SifLocalCluster") as mock_local_cls:
        mock_instance = mock_local_cls.return_value
        cluster = cfg.to_cluster()

        mock_local_cls.assert_called_once()
        kwargs = mock_local_cls.call_args.kwargs
        assert kwargs["cores"] == 2
        assert kwargs["memory"] == "4GB"
        assert kwargs["scheduler_options"] == cfg.scaling.scheduler_options

        mock_instance.adapt.assert_called_once_with(**cfg.scaling.adapt_kwargs)
        assert cluster is mock_instance


def test_to_cluster_slurm_calls_correct_class():
    data = {"type": "slurm", "slurm": {"account": "acct", "queue": "debug"}}
    cfg = ClusterConfig.load(data)

    with (
        patch("needle.config.cluster.SifSLURMCluster") as mock_slurm_cls,
        patch("needle.config.cluster.SifLocalCluster") as mock_local_cls,
    ):
        cfg.to_cluster()

        mock_slurm_cls.assert_called_once()
        mock_local_cls.assert_not_called()
        kwargs = mock_slurm_cls.call_args.kwargs
        assert kwargs["account"] == "acct"
        assert kwargs["queue"] == "debug"


def test_to_cluster_with_container_sets_casa_and_needle_env(tmp_path, monkeypatch):
    monkeypatch.setenv("CASASITECONFIG", "/opt/casa/config")
    monkeypatch.setenv("NEEDLE_CONFIG", "/custom/needle.yaml")

    fake_image = tmp_path / "some_image.sif"
    fake_image.touch()

    data = {
        "type": "local",
        "local": {"cores": 2, "memory": "4GB"},
        "container": {"image": str(fake_image), "binds": [], "env": {}},
    }
    cfg = ClusterConfig.load(data)

    with (
        patch("needle.config.cluster.set_casa_config") as mock_set_casa,
        patch("needle.config.cluster.SifLocalCluster") as mock_local_cls,
    ):
        cfg.to_cluster()

        mock_set_casa.assert_called_once()

        # env vars propagated onto the container config
        assert cfg.container.env["CASASITECONFIG"] == "/opt/casa/config"
        assert cfg.container.env["NEEDLE_CONFIG"] == "/custom/needle.yaml"

        # binds added for both config files
        assert "/opt/casa/config:/opt/casa/config" in cfg.container.binds
        assert "/custom/needle.yaml:/custom/needle.yaml" in cfg.container.binds

        # container_cfg passed through to the cluster class
        kwargs = mock_local_cls.call_args.kwargs
        assert kwargs["container_cfg"] is cfg.container


def test_to_cluster_with_container_appends_extra_binds(tmp_path, monkeypatch):
    monkeypatch.setenv("CASASITECONFIG", "/opt/casa/config")
    monkeypatch.setenv("NEEDLE_CONFIG", "/custom/needle.yaml")

    fake_image = tmp_path / "some_image.sif"
    fake_image.touch()

    data = {
        "type": "local",
        "local": {"cores": 2, "memory": "4GB"},
        "container": {"image": str(fake_image), "binds": [], "env": {}},
    }
    cfg = ClusterConfig.load(data)

    with (
        patch("needle.config.cluster.set_casa_config"),
        patch("needle.config.cluster.SifLocalCluster"),
    ):
        cfg.to_cluster(extra_binds=["/extra:/extra"])

        assert "/extra:/extra" in cfg.container.binds
        # existing binds (casa/needle configs) should still be there too
        assert "/opt/casa/config:/opt/casa/config" in cfg.container.binds
        assert "/custom/needle.yaml:/custom/needle.yaml" in cfg.container.binds
