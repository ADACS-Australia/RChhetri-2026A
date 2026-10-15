from contextlib import contextmanager
import logging
from pathlib import Path
import sys
from typing import Optional, Literal

from needle.lib.constants import (
    RED,
    GREEN,
    YELLOW,
    BOLD,
    CYAN,
    FMT_RST,
    NEEDLE_LOGGER,
    NEEDLE_WATCHER_LOGGER,
    NEEDLE_BEAM_LOGGER_FMT,
    PREFECT_RUN_LOGGERS,
)

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
COLOUR_FORMAT = "%(asctime)s | %(levelname)s | %(name)s - %(message)s"

LogLevels = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class ColourLevelFormatter(logging.Formatter):
    """Formatter that colours only the level name."""

    COLOURS = {
        logging.DEBUG: CYAN,
        logging.INFO: GREEN,
        logging.WARNING: YELLOW,
        logging.ERROR: f"{BOLD}{RED}",
        logging.CRITICAL: f"{BOLD}{RED}",
    }

    def format(self, record: logging.LogRecord) -> str:
        original = record.levelname
        colour = self.COLOURS.get(record.levelno, "")
        record.levelname = f"{colour}{original:<8}{FMT_RST}" if colour else original
        try:
            return super().format(record)
        finally:
            record.levelname = original  # restore so other handlers see the plain name


def _make_formatter(stream_is_tty: bool) -> logging.Formatter:
    if stream_is_tty:
        return ColourLevelFormatter(fmt=COLOUR_FORMAT, datefmt=DATE_FORMAT)
    return logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)


def _file_handler(path: Path, level: str) -> logging.FileHandler:
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(_make_formatter(False))  # never colour files
    return handler


def setup_logging(level: LogLevels = "INFO", log_file: Path = Path("./needle.log")) -> logging.Logger:
    """Flow-level logger. Prefect supplies console + UI handlers.
    This adds the file to both the needle logger and Prefect's run loggers.

    :param level: The level to log at.
    :param log_file: The file to write logs to.
    """
    log_file = Path(log_file)
    needle_logger = logging.getLogger(NEEDLE_LOGGER)
    needle_logger.setLevel(level)
    targets = [needle_logger, *(logging.getLogger(n) for n in PREFECT_RUN_LOGGERS)]

    # Remove file handlers from a previous run (one instance may sit on several loggers)
    old = set()
    for lg in targets:
        for h in list(lg.handlers):
            if isinstance(h, logging.FileHandler):
                lg.removeHandler(h)
                old.add(h)
    for h in old:
        h.close()

    log_file.unlink(missing_ok=True)
    handler = _file_handler(log_file, level)  # one handler, one file descriptor
    for lg in targets:
        lg.addHandler(handler)
    return needle_logger


@contextmanager
def beam_logger(beam_dir: Path, level: str = "INFO"):
    """Task-level logger: writes only to <beam_dir>/beam.log, never to stdout."""
    logger = logging.getLogger(NEEDLE_BEAM_LOGGER_FMT.format(beam_dir=beam_dir.stem))
    logger.setLevel(level)
    logger.propagate = False

    handler = _file_handler(Path(beam_dir) / "beam.log", level)
    logger.addHandler(handler)
    try:
        yield logger
    finally:
        logger.removeHandler(handler)  # no duplicate handlers on re-run
        handler.close()  # no leaked file descriptors


def setup_watcher_logger(log_file: Optional[Path] = None, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(NEEDLE_WATCHER_LOGGER)
    logger.setLevel(level)

    if log_file:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(_make_formatter(False))  # never colour files
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_make_formatter(sys.stdout.isatty()))
    logger.addHandler(handler)
    logger.propagate = False
    return logger
