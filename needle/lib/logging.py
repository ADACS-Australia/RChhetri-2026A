import logging
import sys


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure logging for the needle package.

    Sets up the needle logger hierarchy with a consistent format and
    the specified log level. Should be called at the start of every
    task and flow since ProcessTaskRunner gives each task its own process.

    :param level: Log level string e.g. "INFO", "DEBUG", "WARNING"
    """
    needle_logger = logging.getLogger("needle")
    needle_logger.setLevel(level)

    # Only add handler if one doesn't already exist
    if not needle_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        needle_logger.addHandler(handler)
    return needle_logger
