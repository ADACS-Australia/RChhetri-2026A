import logging
from pathlib import Path
import time

from distributed import Client
from prefect import task
from prefect.cache_policies import NO_CACHE
from dask_jobqueue.slurm import SLURMCluster

from needle.lib.logging import setup_logging
from needle.lib.slurm import (
    get_stuck_job_ids,
    get_slurm_job_states_by_id,
    read_job_log,
    summarize_active,
    summarize_failure,
)

logger = logging.getLogger("needle-cli")


def _plain_test_fn(x: int) -> int:
    """Just a simple function to test that a worker can execute"""
    return x * 2


@task(cache_policy=NO_CACHE)
def wait_for_worker_task(
    cluster: SLURMCluster,
    log_directory: str,
    job_name="dask-worker",
    min_workers: int = 1,
    timeout: float | None = None,
    poll_interval: float = 5.0,
    log_every: float = 30.0,
) -> None:
    start = time.time()
    last_log = 0.0

    while True:
        n_observed = len(cluster.observed)
        if n_observed >= min_workers:
            logger.info(f"{n_observed} worker(s) connected.")
            return

        elapsed = time.time() - start
        stuck = get_stuck_job_ids(cluster)

        if stuck:
            try:
                states = get_slurm_job_states_by_id(list(stuck.values()))
            except Exception as exc:
                logger.warning(f"Couldn't query SLURM states for jobs {list(stuck.values())}: {exc}")
                states = None

            if states is not None and summarize_active(states) == 0 and summarize_failure(states) > 0:
                logs = []
                for name, job_id in stuck.items():
                    full_path = Path(log_directory) / Path(f"{job_name}-{job_id}.err")
                    logs.append(
                        f"--- Job {job_id} (worker {name}) ---\n{read_job_log(full_path, 20)} \n --- Full log available at {full_path} --- "
                    )
                raise RuntimeError(f"Job(s) {stuck} confirmed failed by SLURM (states: {states}).\n{'\n\n'.join(logs)}")
            # else: sacct says still PENDING/RUNNING - genuinely still
            # waiting on a busy queue, not a failure. Keep polling.

        if timeout is not None and elapsed > timeout:
            raise RuntimeError(f"No workers connected after {int(elapsed)}s.")

        if elapsed - last_log >= log_every:
            logger.info(
                f"Waiting for workers ({n_observed}/{min_workers}, "
                f"stuck job IDs: {list(stuck.values()) or 'none'}, {int(elapsed)}s elapsed)."
            )
            last_log = elapsed

        time.sleep(poll_interval)


@task(cache_policy=NO_CACHE)
def run_test_job_task(client: Client) -> int:
    """Submits a trivial job through the real cluster and returns the result."""
    logger = setup_logging("INFO")
    future = client.submit(_plain_test_fn, 21)
    result = future.result(timeout=120)
    logger.info(f"Test job returned {result}")
    return result
