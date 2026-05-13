from pathlib import Path
from unittest.mock import patch
from needle.lib.events import (
    emit_observation_ready,
    emit_observation_staged,
    OBSERVATION_READY_EVENT,
    OBSERVATION_STAGED_EVENT,
)


@patch("needle.lib.events.emit_event")
def test_emit_observation_ready(mock_emit):
    """Test emitting the observation ready event."""
    emit_observation_ready(entry_name="obs1", resource_id="test.resource")

    mock_emit.assert_called_once()
    _, kwargs = mock_emit.call_args
    assert kwargs["event"] == OBSERVATION_READY_EVENT
    assert kwargs["resource"] == {"prefect.resource.id": "test.resource"}
    assert kwargs["payload"] == {"entry_name": "obs1"}


@patch("needle.lib.events.emit_event")
def test_emit_observation_staged(mock_emit):
    """Test emitting the observation staged event."""
    staged_dir = Path("/tmp/staged/obs1")
    emit_observation_staged(entry_name="obs1", staged_dir=staged_dir, resource_id="test.resource")

    mock_emit.assert_called_once()
    _, kwargs = mock_emit.call_args
    assert kwargs["event"] == OBSERVATION_STAGED_EVENT
    assert kwargs["resource"] == {"prefect.resource.id": "test.resource"}
    assert kwargs["payload"] == {"entry_name": "obs1", "staged_dir": staged_dir}
