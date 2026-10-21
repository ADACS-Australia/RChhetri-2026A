from contextlib import contextmanager
import functools
import logging
from pathlib import Path
from typing import Generator, Optional, Tuple

from dask_jobqueue.local import LocalCluster
from distributed import Client

from needle.config.cluster import ClusterConfig
from needle.lib.logging import beam_logger, current_beam


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


def map_in_beam(client, fn, *iterables, **kwargs):
    """client.map(fn, ...), with each call's needle.* logs routed to the enclosing beam's beam.log.

    Call this within a beam_logger() ctx manager in the same way you'd call client.map() but with the client as the
    first argument.

    e.g. client.gather(map_in_beam(client, fn, args))
    """
    beam = current_beam()
    if beam is None:
        raise RuntimeError("map_in_beam must be called inside a beam_logger block")
    beam_dir, level = beam
    wrapped = functools.partial(_run_with_beam_logging, beam_dir, level, fn)
    return client.map(wrapped, *iterables, **kwargs)


def submit_in_beam(client, fn, *args, **kwargs):
    """client.submit(fn, ...), with fn's needle.* logs routed to the enclosing beam's beam.log.

    Call this within a beam_logger() ctx manager in the same way you'd call client.submit() but with the client as the
    first argument.

    e.g. submit_in_beam(client, fn, args).result()
    """
    beam = current_beam()
    if beam is None:
        raise RuntimeError("submit_in_beam must be called inside a beam_logger block")
    return client.submit(_run_with_beam_logging, *beam, fn, *args, **kwargs)


def _run_with_beam_logging(beam_dir: Path, level: str, fn, *args, **kwargs):
    """Runs on the worker: enters beam_logger, then calls the real function."""
    with beam_logger(beam_dir, level):
        return fn(*args, **kwargs)


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
