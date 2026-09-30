from pathlib import Path
from typing import Literal

from distributed import Client
from prefect import Flow, flow, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

from needle.tasks.diagnostics import cal_diagnostics_task, ms_diagnostics_task
from needle.lib.logging import setup_logging


@flow(
    name="diagnostics",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(),
    persist_result=True,
)
def diagnostics_flow(
    client_address: str,
    ms: list[Path],
    cal_soln: list[Path],
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
) -> Flow:
    """The needle pipeline. Runs all operations end-to-end.
    Note - the client must be created in-flow as the Client object cannot be json-serialized.

    :param cfg: The needle config object. Contains all static configuration options for the run.
    :param client_address: The addresss of the running Dask scheduler. Used to construct the Dask client.
    :param work_dir: The directory containing all of the data to work with.
    """
    logger = setup_logging(log_level)
    client = Client(client_address)
    logger.debug("Diagnostics flow initiated")
    if ms:
        logger.info(f"Running diagnostics on MS table(s): {ms}")
    if cal_soln:
        logger.info(f"Running diagnostics on calibration solution(s): {cal_soln}")

    ms_diags = []
    for m in ms:
        ms_diags.append(ms_diagnostics_task.map(unmapped(client), m, unmapped(log_level)))
    cal_soln_diags = []
    for c in cal_soln:
        cal_soln_diags.append(cal_diagnostics_task.map(unmapped(client), c, unmapped(log_level)))
    return ms_diags, cal_soln_diags
