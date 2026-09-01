from contextlib import contextmanager
import logging
from typing import Generator, Optional, Tuple
from dask_jobqueue.local import LocalCluster


from distributed import Client

from needle.config.cluster import ClusterConfig


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
        if client:
            client.close()
        if cluster:
            cluster.close()
