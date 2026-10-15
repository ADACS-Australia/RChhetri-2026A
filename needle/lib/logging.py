import contextvars
import copy
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
NEEDLE_LOGGER = "needle"
NEEDLE_BEAM_LOGGER_FMT = "needle.beam.{beam_dir}"
PREFECT_RUN_LOGGERS = ("prefect.flow_runs", "prefect.task_runs")


class NeedleFileFormatter(logging.Formatter):
    """Adds Prefect's 'Task run ...' prefix to file lines."""

    def format(self, record: logging.LogRecord) -> str:
        prefix = ""
        if task := getattr(record, "task_run_name", None):
            prefix = f"Task run {task!r} - "
        elif flow := getattr(record, "flow_run_name", None):
            prefix = f"Flow run {flow!r} - "
        if not prefix:
            return super().format(record)
        rec = copy.copy(record)
        rec.msg = f"{prefix}{record.getMessage()}"
        rec.args = ()
        return super().format(rec)


def _file_handler(path: Path, level: str, mode: str = "a") -> logging.FileHandler:
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, mode=mode, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(NeedleFileFormatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    return handler


class BeamRoutingHandler(logging.Handler):
    """Forwards needle.* records to the active beam's file handler, if any."""

    def emit(self, record: logging.LogRecord) -> None:
        handler = _current_beam_handler.get()
        if handler is not None and record.levelno >= handler.level:
            handler.handle(record)


class DropBeamScoped(logging.Filter):
    """Drops needle.* records emitted inside a beam_logger block (the router handles those)."""

    def filter(self, record: logging.LogRecord) -> bool:
        in_beam = _current_beam_handler.get() is not None
        return not (in_beam and record.name.startswith(NEEDLE_LOGGER + "."))


_drop_beam_scoped = DropBeamScoped()


def _ensure_beam_router(level: str) -> None:
    needle = logging.getLogger(NEEDLE_LOGGER)
    if not any(isinstance(h, BeamRoutingHandler) for h in needle.handlers):
        needle.addHandler(BeamRoutingHandler())
    if needle.level == logging.NOTSET:  # e.g. Dask worker where setup_logging never ran
        needle.setLevel(level)


def setup_logging(level: str = "INFO", log_file: Path = Path("./needle.log")) -> logging.Logger:
    """Flow-level logger. Call once, in the flow. Prefect supplies console + UI handlers."""
    log_file = Path(log_file)
    needle_logger = logging.getLogger(NEEDLE_LOGGER)
    needle_logger.setLevel(level)
    targets = [needle_logger, *(logging.getLogger(n) for n in PREFECT_RUN_LOGGERS)]

    old = set()
    for lg in targets:
        for h in list(lg.handlers):
            if isinstance(h, logging.FileHandler):
                lg.removeHandler(h)
                old.add(h)
    for h in old:
        h.close()

    log_file.unlink(missing_ok=True)
    handler = _file_handler(log_file, level)
    for lg in targets:
        lg.addHandler(handler)

    _ensure_beam_router(level)
    # Keep beam-scoped module records off every flow-level handler (not the router)
    for h in needle_logger.handlers:
        if not isinstance(h, BeamRoutingHandler) and _drop_beam_scoped not in h.filters:
            h.addFilter(_drop_beam_scoped)
    return needle_logger


# Holds the active beam's file handler for the current thread/task
_current_beam_handler: contextvars.ContextVar[Optional[logging.Handler]] = contextvars.ContextVar(
    "needle_current_beam_handler", default=None
)
# Holds the current beam information
_current_beam: contextvars.ContextVar[Optional[tuple[Path, str]]] = contextvars.ContextVar(
    "needle_current_beam", default=None
)


def current_beam() -> Optional[tuple[Path, str]]:
    return _current_beam.get()


@contextmanager
def beam_logger(beam_dir: Path, level: str = "INFO"):
    """Writes to <beam_dir>/beam.log. Also captures needle.* module logs emitted
    in this thread while the block is active."""
    beam_dir = Path(beam_dir)
    _ensure_beam_router(level)

    logger = logging.getLogger(NEEDLE_BEAM_LOGGER_FMT.format(beam_dir=beam_dir.name.replace(".", "_")))
    logger.setLevel(level)
    logger.propagate = False

    handler = _file_handler(beam_dir / "beam.log", level)
    logger.addHandler(handler)
    token = _current_beam_handler.set(handler)
    beam_token = _current_beam.set((beam_dir, level))
    try:
        yield logger
    finally:
        _current_beam.reset(beam_token)
        _current_beam_handler.reset(token)
        logger.removeHandler(handler)
        handler.close()
