import logging
from pathlib import Path
import sys
from typing import Optional

from needle.lib.constants import RED, GREEN, YELLOW, BOLD, CYAN, FMT_RST

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
COLOUR_FORMAT = "%(asctime)s | %(levelname)s | %(name)s - %(message)s"


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


def setup_logging(level: str = "INFO") -> logging.Logger:
    needle_logger = logging.getLogger("needle")
    needle_logger.setLevel(level)

    if not needle_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(_make_formatter(sys.stdout.isatty()))
        needle_logger.addHandler(handler)
    return needle_logger


def setup_watcher_logger(log_file: Optional[Path] = None, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("needle.watcher")
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
