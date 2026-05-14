from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from needle.config.data import DataConfig
from needle.lib.datasource import DataSource
from needle.lib.events import emit_observation_staged

COURIER_RESOURCE_ID = "needle.courier"


def _courier_flow_name():
    """Should be called only at runtime. Generates the name of the flow"""
    params = flow_run.get_parameters()
    return f"courier-{params['entry_name']}"


@flow(name="needle-courier", log_prints=True, flow_run_name=_courier_flow_name)
def courier_flow(data_cfg: DataConfig, entry_name: str):
    """Receive a staged observation (entry_name) and moves it to the appropriate location.

    Pulls the observation from the data source into the staging directory, then emits a
    needle.observation.staged event for the pipeline to consume.

    The final staged_dir contains the data files and is of the form: 'data_cfg.staging_dir / entry_name'

    Emits an event to indicate the data has been staged and is ready for work.

    :param data_cfg: The configuration information for the data
    :param entry_name: The name of this entry - identifies the observation
    :raises RuntimeError: Raised if an error occurs during event emission
    """
    logger = get_run_logger()
    data_source = DataSource.from_str(data_cfg.source)
    staged_dir = data_source.receive(entry_name, data_cfg.staging_dir)
    logger.info(f"Staged {data_cfg.source}/{entry_name} to {staged_dir}")

    event = emit_observation_staged(entry_name=entry_name, staged_dir=staged_dir, resource_id=COURIER_RESOURCE_ID)
    if not event:
        raise RuntimeError(f"Event failed to emit event for source: '{data_cfg.source}' and entry '{entry_name}'")
    logger.info(f"Emitted event for {entry_name}: {event.model_dump()}")
