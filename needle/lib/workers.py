"""Definitions for the long-running production worker processes.

Each role maps a short CLI name to the Prefect work pool, the worker name that
``needle start-workers`` registers, and the screen session used to keep the
process alive on its node.
"""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class WorkerRole:
    """Metadata for one production worker role."""

    name: str
    "Short role name used on the command line, e.g. 'pipeline'."

    pool: str
    "Prefect work pool that the worker polls."

    worker: str
    "Worker name registered with Prefect (set by ``needle start-workers``)."

    screen: str
    "Name of the screen session that owns the worker process."

    node: Literal["login", "data_mover"]
    "Node where the worker runs."


WORKER_ROLES: dict[str, WorkerRole] = {
    "pipeline": WorkerRole(
        name="pipeline",
        pool="process-observation-ozstar",
        worker="needle-pipeline",
        screen="needle-prod-pipeline",
        node="login",
    ),
    "download": WorkerRole(
        name="download",
        pool="download-observation-ozstar",
        worker="needle-download",
        screen="needle-prod-download",
        node="data_mover",
    ),
}

ROLE_ORDER: tuple[str, ...] = tuple(WORKER_ROLES)
"Stable order used for output and deployment."

WORKER_POOLS: dict[str, str] = {name: role.pool for name, role in WORKER_ROLES.items()}
"Mapping used by the ``start-workers`` command."
