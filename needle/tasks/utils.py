from pathlib import Path

from prefect import task

from needle.config.beam import BeamPair
from needle.modules.calibrate import CalibrateOutput


@task()
def beam_pair_extract_tgt_task(pair: BeamPair) -> Path:
    """Extract target from a BeamPair Object"""
    return pair.tgt


@task()
def beam_pair_extract_cal_task(pair: BeamPair) -> Path:
    """Extract calibrator from a BeamPair Object"""
    return pair.cal


@task()
def cal_output_extract_tgt_task(cal_output: CalibrateOutput) -> Path:
    """Pulls the calibrated target ms from the CalibrateOuptut object"""
    return cal_output.tgt
