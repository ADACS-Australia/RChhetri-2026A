import pytest
from unittest.mock import patch, MagicMock

from distributed import Client, LocalCluster

from needle.lib.dask_runner import build_dask_client, _close_stray_clients
from needle.config.cluster import ClusterConfig


def test_close_stray_clients_only_closes_clients_on_same_scheduler():
    cluster_kwargs = dict(n_workers=1, threads_per_worker=1, processes=False, dashboard_address=None)

    with LocalCluster(**cluster_kwargs) as cluster_a, LocalCluster(**cluster_kwargs) as cluster_b:
        with (
            Client(cluster_a) as main_client,
            Client(cluster_a.scheduler_address) as stray_client,
            Client(cluster_b) as other_cluster_client,
        ):
            assert stray_client.scheduler.address == main_client.scheduler.address
            assert other_cluster_client.scheduler.address != main_client.scheduler.address

            _close_stray_clients(main_client)

            # The extra client on the same scheduler is closed
            assert stray_client.status == "closed"
            # The client we passed in is left alone (the caller closes it)
            assert main_client.status == "running"
            # Clients on a different scheduler are untouched
            assert other_cluster_client.status == "running"


def test_build_dask_client_uses_provided_cfg():
    mock_cfg = MagicMock(spec=ClusterConfig)
    mock_cfg.type = "local"
    mock_cluster = MagicMock()
    mock_cfg.to_cluster.return_value = mock_cluster

    with (
        patch("needle.lib.dask_runner.ClusterConfig") as mock_cfg_cls,
        patch("needle.lib.dask_runner.Client") as mock_client_cls,
    ):
        mock_client_instance = mock_client_cls.return_value

        with build_dask_client(mock_cfg) as (client, cluster):
            assert client is mock_client_instance
            assert cluster is mock_cluster

        mock_cfg_cls.get_config.assert_not_called()  # shouldn't load from disk
        mock_cfg.to_cluster.assert_called_once()
        mock_client_cls.assert_called_once_with(mock_cluster)


def test_build_dask_client_falls_back_to_get_config():
    mock_cfg = MagicMock(spec=ClusterConfig)
    mock_cfg.type = "slurm"
    mock_cluster = MagicMock()
    mock_cfg.to_cluster.return_value = mock_cluster

    with patch("needle.lib.dask_runner.ClusterConfig") as mock_cfg_cls, patch("needle.lib.dask_runner.Client"):
        mock_cfg_cls.get_config.return_value = mock_cfg

        with build_dask_client() as (client, cluster):
            pass

        mock_cfg_cls.get_config.assert_called_once()
        mock_cfg.to_cluster.assert_called_once()


def test_build_dask_client_closes_client_and_cluster_on_success():
    mock_cfg = MagicMock(spec=ClusterConfig)
    mock_cfg.type = "local"
    mock_cluster = MagicMock()
    mock_cfg.to_cluster.return_value = mock_cluster

    with patch("needle.lib.dask_runner.Client") as mock_client_cls:
        mock_client_instance = mock_client_cls.return_value

        with build_dask_client(mock_cfg):
            pass

        mock_client_instance.close.assert_called_once()
        mock_cluster.close.assert_called_once()


def test_build_dask_client_closes_client_and_cluster_on_exception():
    mock_cfg = MagicMock(spec=ClusterConfig)
    mock_cfg.type = "local"
    mock_cluster = MagicMock()
    mock_cfg.to_cluster.return_value = mock_cluster

    with patch("needle.lib.dask_runner.Client") as mock_client_cls:
        mock_client_instance = mock_client_cls.return_value

        with pytest.raises(RuntimeError, match="boom"):
            with build_dask_client(mock_cfg):
                raise RuntimeError("boom")

        # cleanup must still happen even though an error was raised inside the block
        mock_client_instance.close.assert_called_once()
        mock_cluster.close.assert_called_once()
