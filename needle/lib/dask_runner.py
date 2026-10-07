from contextlib import contextmanager
import logging
from typing import Generator, Optional, Tuple

from dask_jobqueue.local import LocalCluster
from distributed import Client

from needle.config.cluster import ClusterConfig


def _close_stray_clients(client: Client):
    """Close stray clients (e.g. Client(client_address) made inside tasks) that are still attached to this
    scheduler, so they don't try to reconnect once the cluster goes away."""
    address = client.scheduler.address
    for other in list(Client._instances):
        if other is client:
            continue
        try:
            if getattr(other.scheduler, "address", None) == address:
                other.close()
        except Exception:
            pass


@contextmanager
def build_dask_client(
    cluster_cfg: Optional[ClusterConfig] = None,
) -> Generator[Tuple[Client, LocalCluster], None, None]:
    """Builds a Dask client using the cluster configuration."""
    logger = logging.getLogger("needle-cli")

    if not cluster_cfg:
        cluster_cfg = ClusterConfig.get_config()
    logger.info(f"Using {cluster_cfg.type} cluster")
    client = None
    cluster = None
    try:
        cluster = cluster_cfg.to_cluster()
        logger.info(f"Cluster info: {cluster}")
        client = Client(cluster)
        yield client, cluster
    finally:
        # Silence reconnect noise from any client that outlives the scheduler.
        dist_logger = logging.getLogger("distributed.client")
        old_level = dist_logger.level
        dist_logger.setLevel(logging.CRITICAL)
        try:
            if client:
                _close_stray_clients(client)
                client.close()
            if cluster:
                cluster.close()
        finally:
            dist_logger.setLevel(old_level)
