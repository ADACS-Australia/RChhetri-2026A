import logging

import pytest

from needle.lib.logging import (
    NEEDLE_LOGGER,
    BeamRoutingHandler,
    DropBeamScoped,
    setup_logging,
)

# Loggers mutated by the setup functions; saved/restored around every test
TOUCHED_LOGGERS = (NEEDLE_LOGGER, "prefect.flow_runs", "prefect.task_runs")


def _snapshot(logger: logging.Logger) -> tuple[list[logging.Handler], int, bool]:
    return list(logger.handlers), logger.level, logger.propagate


@pytest.fixture(autouse=True)
def isolate_loggers():
    """Keep handler/level changes made by one test from leaking into the next."""
    saved = {name: _snapshot(logging.getLogger(name)) for name in TOUCHED_LOGGERS}
    yield
    for name, (handlers, level, propagate) in saved.items():
        logger = logging.getLogger(name)
        # setup_logging swaps file handlers in and closes the old ones — don't resurrect a dead handler
        logger.handlers = [h for h in handlers if getattr(h, "stream", "no-stream") is not None]
        logger.setLevel(level)
        logger.propagate = propagate


def _file_handlers(logger: logging.Logger) -> list[logging.FileHandler]:
    return [h for h in logger.handlers if isinstance(h, logging.FileHandler)]


def _stream_handlers(logger: logging.Logger) -> list[logging.StreamHandler]:
    """Plain stdout handlers — note FileHandler is a StreamHandler subclass."""
    return [
        h for h in logger.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    ]


def test_setup_logging(tmp_path):
    log_file = tmp_path / "needle.log"

    returned_logger = setup_logging(level="DEBUG", log_file=log_file)

    assert returned_logger.name == NEEDLE_LOGGER
    assert returned_logger.level == logging.DEBUG
    # Console output comes from Prefect's root handler, so we must keep propagating
    assert returned_logger.propagate is True
    # Beam-scoped module records are routed to the beam file, not the flow handlers
    assert any(isinstance(h, BeamRoutingHandler) for h in returned_logger.handlers)

    file_handlers = _file_handlers(returned_logger)
    assert len(file_handlers) == 1
    assert file_handlers[0].baseFilename == str(log_file.absolute())
    assert _drop_beam_scoped_in(file_handlers[0])

    # Prefect's run loggers mirror into the same file
    for name in ("prefect.flow_runs", "prefect.task_runs"):
        mirrored = _file_handlers(logging.getLogger(name))
        assert [h.baseFilename for h in mirrored] == [str(log_file.absolute())]

    returned_logger.info("flow level message")
    for handler in returned_logger.handlers:
        handler.flush()
    contents = log_file.read_text()
    assert "flow level message" in contents
    assert "needle - flow level message" in contents
    assert "| INFO" in contents


def _drop_beam_scoped_in(handler: logging.Handler) -> bool:
    return any(isinstance(f, DropBeamScoped) for f in handler.filters)


def test_setup_logging_is_idempotent(tmp_path):
    setup_logging(level="INFO", log_file=tmp_path / "first.log")
    returned_logger = setup_logging(level="INFO", log_file=tmp_path / "second.log")

    # Re-running must swap the file handler rather than stack a second one
    file_handlers = _file_handlers(returned_logger)
    assert len(file_handlers) == 1
    assert file_handlers[0].baseFilename == str(tmp_path / "second.log")

    returned_logger.info("only written once")
    for handler in returned_logger.handlers:
        handler.flush()
    assert (tmp_path / "second.log").read_text().count("only written once") == 1
