import logging
from pathlib import Path
import re

from prefect import task

from needle.config.beam import BeamInput, BeamPair
from needle.lib.logging import NEEDLE_LOGGER
from needle.modules.beam import find_beams, BEAM_DIR_PATTERN


@task(task_run_name="find_beam_pairs")
def find_beams_task(search_dir: Path, **kwargs) -> list[BeamPair]:
    """Searches a directory for beam inputs

    :param search_dir: The directory to scan
    """
    fn_inputs = locals().items()
    logger = logging.getLogger(NEEDLE_LOGGER)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    beams = find_beams(search_dir=search_dir)
    logger.info("Found the following beam inputs:")
    for b in beams:
        if isinstance(b, BeamPair):
            logger.debug(f"{b.beam} | tgt - {b.tgt} | cal - {b.cal}")
        else:
            logger.debug(f"{b.beam} | calibrated - {b.ms}")
    return beams


@task(task_run_name="setup_beam_dir_beam{beam.beam}")
def setup_beam_dir_task(beam: BeamInput, **kwargs) -> BeamPair:
    """Sets up a beamXX directory for the provided beam in its parent directory

    :param beam: The beam input object to make the directory and move the files for
    """
    fn_inputs = locals().items()
    logger = logging.getLogger(NEEDLE_LOGGER)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    # Check if the data is already in the beam directory:
    parent = beam.path.parent
    base_dir = parent.parent if re.fullmatch(BEAM_DIR_PATTERN, parent.name) else parent
    new_dir = base_dir / f"beam{beam.beam}"

    log_file = new_dir / "beam.log"
    if log_file.exists():
        log_file.unlink()

    # Move the data to the new directory
    logger.info(f"Moving all beam files to {new_dir}")
    beam.move_files(new_dir)
    return beam
