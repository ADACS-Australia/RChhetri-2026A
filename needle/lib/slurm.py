from collections import deque
from pathlib import Path
import subprocess

# Terminal states meaning the job is definitely never coming back on its own.
FAILURE_STATES = {"FAILED", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "BOOT_FAIL"}
# States meaning the job might still become a healthy worker - don't panic yet.
ACTIVE_STATES = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING"}


def get_stuck_job_ids(cluster) -> dict[str, str]:
    """Returns {worker_spec_name: job_id} for workers that have been
    requested but haven't connected yet. job_id is the real SLURM job ID
    pulled from dask-jobqueue's own Job object - not part of documented
    public API, so this is guarded defensively.
    """
    stuck_names = cluster.requested - cluster.observed
    result: dict[str, str] = {}
    for name in stuck_names:
        job = cluster.workers.get(name)
        job_id = getattr(job, "job_id", None) if job is not None else None
        if job_id:
            result[name] = str(job_id)
    return result


def get_slurm_job_states_by_id(job_ids: list[str]) -> dict[str, int]:
    """Counts of SLURM job states for the given exact job IDs, via sacct.
    Filters out sub-step rows (e.g. '<id>.batch', '<id>.extern') that sacct
    reports alongside the main job row, keeping only the authoritative
    state for each requested job ID.
    """
    if not job_ids:
        return {}

    cmd = [
        "sacct",
        "-j",
        ",".join(job_ids),
        "--noheader",
        "--parsable2",
        "--format=JobID,State",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

    wanted = set(job_ids)
    counts: dict[str, int] = {}
    for line in result.stdout.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 2:
            continue
        job_id_field, state_field = parts[0], parts[1]
        if job_id_field not in wanted:
            continue  # skip .batch / .extern sub-steps
        state = state_field.split()[0]  # drop e.g. "CANCELLED by 1234"
        counts[state] = counts.get(state, 0) + 1
    return counts


def read_job_log(log_path: str | Path, n_lines: int = 20) -> str:
    """Reads the last n_lines of a SLURM job's own log file directly"""
    try:
        with open(log_path) as f:
            lines = deque(f, maxlen=n_lines)
        return "".join(lines)
    except OSError as exc:
        return f"(couldn't read {log_path}: {exc})"


def summarize_failure(states: dict[str, int]) -> int:
    return sum(v for k, v in states.items() if k in FAILURE_STATES)


def summarize_active(states: dict[str, int]) -> int:
    return sum(v for k, v in states.items() if k in ACTIVE_STATES)
