from unittest.mock import MagicMock, patch
import pytest
from needle.modules.watcher import watch
from needle.config.watcher import WatcherConfig
from needle.config.data import DataConfig


class StopIterationException(Exception):
    pass


@patch("needle.modules.watcher.DataSource")
@patch("needle.modules.watcher.emit_observation_ready")
@patch("needle.modules.watcher.time.sleep")
def test_watch_one_iteration(mock_sleep, mock_emit, mock_data_source_cls):
    """Test one iteration of the watcher's loop."""
    watcher_cfg = WatcherConfig(poll_interval=1)
    data_cfg = DataConfig(source="local:///tmp", staging_dir="/tmp/staged")

    mock_data_source = MagicMock()
    mock_data_source_cls.from_str.return_value = mock_data_source
    mock_data_source.get_ready_entries.return_value = ["obs1"]

    # Make sleep raise an exception to break the infinite loop
    mock_sleep.side_effect = StopIterationException

    with pytest.raises(StopIterationException):
        watch(watcher_cfg, data_cfg)

    mock_data_source.get_ready_entries.assert_called_once_with(data_cfg.stability_check)
    mock_emit.assert_called_once_with(entry_name="obs1", resource_id="needle.watcher")
    mock_data_source.mark_received.assert_called_once_with("obs1")
    mock_sleep.assert_called_once_with(1)
