import logging
from pathlib import Path
import sys
from typing import Optional


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure logging for the needle package.

    Sets up the needle logger hierarchy with a consistent format and
    the specified log level. Should be called at the start of every
    task and flow since ProcessTaskRunner gives each task its own process.

    :param level: Log level string e.g. "INFO", "DEBUG", "WARNING"
    :return: Configured logger
    """
    needle_logger = logging.getLogger("needle")
    needle_logger.setLevel(level)

    # Only add handler if one doesn't already exist
    if not needle_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        needle_logger.addHandler(handler)
    return needle_logger


def setup_watcher_logger(log_file: Optional[Path] = None, level: str = "INFO") -> logging.Logger:
    """Set up a logger for the watcher that writes to a file.

    :param log_file: Path to the file to output logs to. If none, uses stdout
    :param level: The logging level
    :return: Configured logger
    """
    logger = logging.getLogger("needle.watcher")
    logger.setLevel(level)

    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent watcher logs from propagating to the root logger
    logger.propagate = False

    return logger
