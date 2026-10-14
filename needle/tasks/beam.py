from pathlib import Path
import re

from prefect import task

from needle.config.beam import BeamPair
from needle.lib.logging import setup_logging
from needle.modules.beam import find_beam_pairs, BEAM_DIR_PATTERN


@task
def find_beam_pairs_task(search_dir: Path, log_level: str = "INFO", **kwargs) -> list[BeamPair]:
    """Searches a directory for beam pairs

    :param search_dir: The directory to scan
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    beam_pairs = find_beam_pairs(search_dir=search_dir)
    logger.debug("Found the following beam pairs:")
    for bp in beam_pairs:
        logger.debug(f"{bp.beam} | tgt - {bp.tgt} | cal - {bp.cal}")
    return beam_pairs


@task
def setup_beam_dir_task(beam_pair: BeamPair, log_level: str = "INFO", **kwargs) -> BeamPair:
    """Sets up a beamXX directory for the provided beam in its parent directory

    :param beam_pair: The beam pair object to make the directory and move the files for
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    # Check if the data is already in the beam directory:
    parent = beam_pair.tgt.parent
    base_dir = parent.parent if re.fullmatch(BEAM_DIR_PATTERN, parent.name) else parent
    new_dir = base_dir / f"beam{beam_pair.beam}"

    # Move the data to the new directory
    logger.info(f"Moving all beam files to {new_dir}")
    beam_pair.move_files(new_dir)
    return beam_pair
