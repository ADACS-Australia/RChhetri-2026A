"""
Diagnostic flow that builds the configured cluster, waits for a worker (failing fast on SLURM jobs that die before
connecting), and submits a trivial job to prove actual execution works"""

from dask_jobqueue.slurm import SLURMCluster
from distributed import Client
from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

from needle.config.cluster import ClusterConfig
from needle.lib.logging import setup_logging
from needle.tasks.test_cluster import wait_for_worker_task, run_test_job_task


@flow(task_runner=ThreadPoolTaskRunner(max_workers=1))
def test_cluster_flow(client: Client, cluster: SLURMCluster, cluster_cfg: ClusterConfig) -> None:
    """Raises on any failure, so the flow run's state in Prefect accurately
    reflects whether the cluster is actually usable right now.
    """
    logger = setup_logging("INFO")
    logger.info(f"Dask dashboard: {client.dashboard_link}")

    cluster.get_logs()
    cluster
    wait_for_worker_task(
        cluster,
        log_directory=cluster_cfg.slurm.log_directory,
        min_workers=1,
    )
    result = run_test_job_task(client)
    if result != 42:
        raise RuntimeError(f"Test job returned unexpected result: {result!r} (expected 42).")
    logger.info("PASS: cluster is up and executed a test job correctly.")
