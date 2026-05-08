import logging
from pathlib import Path

from prefect import task

from needle.config.beam import BeamPair
from needle.lib.logging import setup_logging


@task
def setup_beam_dir_task(beam_pair: BeamPair, log_level: str = "INFO") -> BeamPair:
    """Sets up a beamXX directory for the provided beam in its parent directory

    :param beam_pair: The beam pair object to make the directory and move the files for
    """
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    new_dir = beam_pair.tgt.parent / Path(f"beam{beam_pair.beam}")
    logging.info(f"Moving files to {new_dir}")
    beam_pair.move_files(new_dir)
    return beam_pair
