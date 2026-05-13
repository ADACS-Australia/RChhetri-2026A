from pathlib import Path

from pydantic import BaseModel
from prefect.events import emit_event, Event

OBSERVATION_READY_EVENT = "needle.observation.ready"
OBSERVATION_STAGED_EVENT = "needle.observation.staged"


class ObservationReadyPayload(BaseModel):
    "Payload to emit for the 'observation ready' event"

    entry_name: str
    "Name of the entry that is ready"


class ObservationStagedPayload(BaseModel):
    "Payload to emit for the 'observation staged' event"

    entry_name: str
    "Name of the entry that is staged"

    staged_dir: Path
    "Location on-disk to the directory containing the data files"


def emit_observation_ready(entry_name: str, resource_id: str) -> Event:
    payload = ObservationReadyPayload(entry_name=entry_name)
    return emit_event(
        event=OBSERVATION_READY_EVENT,
        resource={"prefect.resource.id": resource_id},
        payload=payload.model_dump(),
    )


def emit_observation_staged(entry_name: str, staged_dir: Path, resource_id: str) -> Event:
    payload = ObservationStagedPayload(
        entry_name=entry_name,
        staged_dir=staged_dir,
    )
    return emit_event(
        event=OBSERVATION_STAGED_EVENT,
        resource={"prefect.resource.id": resource_id},
        payload=payload.model_dump(),
    )
