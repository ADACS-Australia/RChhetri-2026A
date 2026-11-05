from pathlib import Path

from prefect import task

from needle.config.beam import BeamInput, BeamPair
from needle.config.calibrate import CalInput


@task(task_run_name="extract_tgt_from_beam{beam.beam}")
def extract_tgt_task(beam: BeamInput, dependency=None) -> Path:
    """Extract target from a BeamPair Object"""
    _ = dependency
    return beam.path


@task(task_run_name="extract_cal_from_beam{beam.beam}")
def extract_cal_task(beam: BeamInput, dependency=None) -> CalInput | None:
    """Extract calibrator from a BeamPair Object"""
    _ = dependency
    return beam.cal if isinstance(beam, BeamPair) else None


@task(task_run_name="extract_name_from_beam{beam.beam}")
def extract_name_task(beam: BeamInput, dependency=None) -> str:
    """Extract the observation name from a BeamPair object."""
    _ = dependency
    return beam.name
