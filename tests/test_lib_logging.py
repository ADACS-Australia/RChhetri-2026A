import logging
import sys
from pathlib import Path
from needle.lib.logging import setup_logging, setup_watcher_logger

def test_setup_logging():
    # Clear existing handlers for test isolation
    logger = logging.getLogger("needle")
    logger.handlers = []
    
    returned_logger = setup_logging(level="DEBUG")
    assert returned_logger.level == logging.DEBUG
    assert len(returned_logger.handlers) == 1
    assert isinstance(returned_logger.handlers[0], logging.StreamHandler)
    assert returned_logger.handlers[0].stream == sys.stdout

def test_setup_watcher_logger_stream():
    # Clear existing handlers
    logger = logging.getLogger("needle.watcher")
    logger.handlers = []
    
    returned_logger = setup_watcher_logger(level="WARNING")
    assert returned_logger.level == logging.WARNING
    assert len(returned_logger.handlers) == 1
    assert isinstance(returned_logger.handlers[0], logging.StreamHandler)
    assert returned_logger.propagate is False

def test_setup_watcher_logger_file(tmp_path):
    log_file = tmp_path / "watcher.log"
    logger = logging.getLogger("needle.watcher")
    logger.handlers = []
    
    returned_logger = setup_watcher_logger(log_file=log_file)
    assert len(returned_logger.handlers) == 1
    assert isinstance(returned_logger.handlers[0], logging.FileHandler)
    assert returned_logger.handlers[0].baseFilename == str(log_file.absolute())
