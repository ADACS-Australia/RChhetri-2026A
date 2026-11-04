from pathlib import Path

from prefect import task

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution


@task(task_run_name="extract_tgt_from_beam{pair.beam}")
def extract_tgt_task(pair: BeamPair, dependency=None) -> Path:
    """Extract target from a BeamPair Object"""
    _ = dependency
    return pair.tgt


@task(task_run_name="extract_cal_from_beam{pair.beam}")
def extract_cal_task(pair: BeamPair, dependency=None) -> Path | CalibrationSolution:
    """Extract calibrator from a BeamPair Object"""
    _ = dependency
    return pair.cal


@task(task_run_name="extract_name_from_beam{pair.beam}")
def extract_name_task(pair: BeamPair, dependency=None) -> str:
    """Extract the observation name from a BeamPair object."""
    _ = dependency
    return pair.name
