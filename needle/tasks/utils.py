from pathlib import Path

from prefect import task

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution


@task()
def extract_tgt_task(pair: BeamPair) -> Path:
    """Extract target from a BeamPair Object"""
    return pair.tgt


@task()
def extract_cal_task(pair: BeamPair) -> Path | CalibrationSolution:
    """Extract calibrator from a BeamPair Object"""
    return pair.cal
