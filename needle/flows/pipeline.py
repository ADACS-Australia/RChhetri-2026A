from pathlib import Path

from distributed import Client
from prefect import Flow, flow, unmapped
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterId,
    TaskRunFilter,
    TaskRunFilterState,
    TaskRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
from prefect.futures import PrefectFuture, as_completed
from prefect.runtime import flow_run
from prefect.task_runners import ThreadPoolTaskRunner

from needle.config.cluster import ClusterConfig
from needle.config.pipeline import NeedleConfig
from needle.lib.logging import setup_logging
from needle.modules.inspect import MSInfo
from needle.tasks.beam import setup_beam_dir_task, find_beam_pairs_task
from needle.tasks.calibrate import solve_calibration_task, apply_calibration_task
from needle.tasks.casa_data import update_casa_data
from needle.tasks.clean import clean_task, interval_clean_task, predict_task
from needle.tasks.convert import convert_beam_pair_task
from needle.tasks.diagnostics import cal_diagnostics_task, ms_diagnostics_task, pre_cal_diagnostics
from needle.tasks.flag import flag_ms_pair_task
from needle.tasks.inspect import inspect_ms_task
from needle.tasks.mask import create_mask_task
from needle.tasks.source_find import source_find_task
from needle.tasks.utils import extract_cal_task, extract_tgt_task

FutureList = list[PrefectFuture]
OptionalFutureList = list[PrefectFuture | None]


def _pipeline_flow_name() -> str:
    """Should be called only at runtime. Generates the name of the flow"""
    params = flow_run.get_parameters()
    target_obs = Path(params["work_dir"]).stem
    return f"pipeline-{target_obs}"


def _unmapped_defaults(cfg: NeedleConfig) -> dict:
    return {"log_level": unmapped(cfg.flow.log_level), "overwrite": unmapped(cfg.flow.overwrite)}


@flow(
    name="needle-pipeline",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(),
    persist_result=True,
    flow_run_name=_pipeline_flow_name,
)
def needle_pipeline(cfg: NeedleConfig, client_address: str, client_address_hm: str, work_dir: Path | str) -> Flow:
    """The needle pipeline. Runs all operations end-to-end.
    Note - the client must be created in-flow as the Client object cannot be json-serialized.

    :param cfg: The needle config object. Contains all static configuration options for the run.
    :param client_address: The addresss of the running Dask scheduler. Used to construct the Dask client.
    :param client_address_hm: The addresss of the running Dask scheduler with additional memory. Used for memory intensive jobs
    :param work_dir: The directory containing all of the data to work with.
    """
    logger = setup_logging(cfg.flow.log_level)
    logger.debug(f"Config: {cfg}")
    client = Client(client_address)
    client_hm = Client(client_address_hm)
    defaults = _unmapped_defaults(cfg)

    ##########################
    ## Pre-processing tasks ##
    ##########################

    # Update the casa measures dataset before doing anything
    update_casa_data(data_path=cfg.data.casa_dir / "casadata", runtime=ClusterConfig.get_config().container)
    # Get the beam pairs to work with
    beam_pairs = find_beam_pairs_task(search_dir=Path(work_dir), **defaults)
    f_beam_pairs = setup_beam_dir_task.map(beam_pairs, **defaults)
    # Convert pairs to measurement sets
    f_beam_pairs = convert_beam_pair_task.map(client=unmapped(client), pair=f_beam_pairs, **defaults)

    #########################
    ## Flag and  calibrate ##
    #########################

    f_beam_pairs = flag_ms_pair_task.map(
        client=unmapped(client), ms_pair=f_beam_pairs, cfg=unmapped(cfg.flag), **defaults
    )
    # Check that the beam is good enough to use
    f_beam_pairs = pre_cal_diagnostics.map(client=unmapped(client_hm), beam=f_beam_pairs, **defaults)
    # Solve and apply calibration
    f_cal = extract_cal_task.map(pair=f_beam_pairs)
    f_tgt = extract_tgt_task.map(pair=f_beam_pairs)
    f_cal_soln = solve_calibration_task.map(
        client=unmapped(client), cal=f_cal, cfg=unmapped(cfg.calibrate_solve), **defaults
    )
    f_tgt = apply_calibration_task.map(
        client=unmapped(client), cfg=unmapped(cfg.calibrate_apply), cal=f_cal_soln, tgt=f_tgt, **defaults
    )

    ##########################
    ## Post Cal Diagnostics ##
    ##########################

    # Inspect result
    f_inspect_tgt = inspect_ms_task.map(unmapped(client), f_tgt, unmapped(cfg.flow.log_level))
    f_inspect_cal = inspect_ms_task.map(unmapped(client), f_cal, unmapped(cfg.flow.log_level))
    # Perform diagnostics
    f_cal_diagnostics = ms_diagnostics_task.map(unmapped(client_hm), f_cal, **defaults)
    f_tgt_diagnostics = ms_diagnostics_task.map(unmapped(client_hm), f_tgt, **defaults)
    f_cal_soln_diagnostics = cal_diagnostics_task.map(unmapped(client_hm), f_cal_soln, **defaults)

    ####################################
    ## Clean, mask and model subtract ##
    ####################################

    if cfg.flow.skip_to_deep_clean:  # Auto-masking route
        f_deep_image = clean_task.with_options(task_run_name="auto_mask_deep_clean_{ms.stem}").map(
            unmapped(client), f_tgt, cfg=unmapped(cfg.deep_clean), **defaults
        )
        f_mask = [None] * len(f_tgt)
    else:  # Source-finding route
        f_shallow_image = clean_task.with_options(task_run_name="shallow_clean_{ms.stem}").map(
            unmapped(client), f_tgt, cfg=unmapped(cfg.shallow_clean), **defaults
        )
        f_json_sources = source_find_task.map(  # Source finding
            unmapped(client), f_shallow_image, cfg=unmapped(cfg.source_find), **defaults
        )
        f_mask = create_mask_task.map(  # Create masks over the sources in prep for deep cleaning
            unmapped(client), f_json_sources, f_shallow_image, cfg=unmapped(cfg.create_mask), **defaults
        )
        f_deep_image = clean_task.with_options(task_run_name="deep_clean_{ms.stem}").map(
            unmapped(client), f_tgt, cfg=unmapped(cfg.deep_clean), mask=f_mask, **defaults
        )
    # Model creation - generates the MODEL_DATA column
    f_model_create = predict_task.with_options(task_run_name="predict_{ms.stem}").map(
        unmapped(client), f_tgt, cfg=unmapped(cfg.deep_clean), dependencies=f_deep_image, **defaults
    )
    # Model subtract - removes the MODEL_DATA from the DATA visibilities
    f_model_subtract = clean_task.with_options(task_run_name="model_subtract_{ms.stem}").map(
        unmapped(client), f_model_create, cfg=unmapped(cfg.model_subtract), mask=f_mask, **defaults
    )

    #######################
    ## Interval Cleaning ##
    #######################

    f_interval_clean = _submit_interval_cleans(client, cfg, f_tgt, f_inspect_tgt, f_model_subtract, f_mask, logger)
    # one task per (MS, interval) combination
    # all_tgt, all_model_subtracts, all_masks, all_intervals = _expand_intervals(
    #     f_tgt=f_tgt,
    #     f_inspect_tgt=f_inspect_tgt,
    #     f_model_subtract=f_model_subtract,
    #     f_mask=f_mask,
    #     n_intervals=cfg.flow.interval_tasks,
    # )

    # f_interval_clean = interval_clean_task.with_options(name="interval_clean").map(
    #     unmapped(client),
    #     all_tgt,
    #     cfg=unmapped(cfg.interval_clean),
    #     mask=all_masks,
    #     interval=all_intervals,  # each task gets its own slice
    #     dependencies=all_model_subtracts,
    #     **defaults,
    # )

    ###################################
    ## Finish and check for failures ##
    ###################################
    for f in (f_cal_diagnostics, f_tgt_diagnostics, f_cal_soln_diagnostics, f_inspect_cal):
        f.wait()  # Don't use .result() as it causes crashing on failure
    for f in f_interval_clean:
        f.wait()
    _report_failures(logger=logger)


def _submit_interval_cleans(
    client: Client,
    cfg: NeedleConfig,
    f_tgt: FutureList,
    f_inspect_tgt: FutureList,
    f_model_subtract: FutureList,
    f_mask: OptionalFutureList,
    logger,
) -> FutureList:
    """Submits interval cleans for each beam as soon as its inspect task finishes.
    Beams whose inspect failed (or was skipped) are ignored."""
    # as_completed yields the futures in finish order, so map each back to its beam index
    idx_by_run = {f.task_run_id: i for i, f in enumerate(f_inspect_tgt)}
    f_interval_clean = []

    for inspect in as_completed(f_inspect_tgt):
        if not inspect.state.is_completed():
            continue  # dead beam, the failure report will cover it
        i = idx_by_run[inspect.task_run_id]
        try:
            ms_info = MSInfo.from_json(inspect.result())
            intervals = _generate_ms_intervals(ms_info.time.n_integrations, cfg.flow.interval_tasks)
        except Exception as e:
            logger.error(f"Could not plan intervals for beam index {i}: {type(e).__name__}: {e}")
            continue

        for interval in intervals:
            f_interval_clean.append(
                interval_clean_task.submit(
                    client,
                    f_tgt[i],
                    cfg=cfg.interval_clean,
                    mask=f_mask[i],
                    interval=interval,
                    dependencies=f_model_subtract[i],
                    log_level=cfg.flow.log_level,
                    overwrite=cfg.flow.overwrite,
                )
            )
    return f_interval_clean


def _report_failures(logger) -> int:
    """Logs every task run in this flow run that itself failed (NotReady downstream
    tasks are Pending, so they are excluded). Returns the number of failures."""
    with get_client(sync_client=True) as client:
        failed = client.read_task_runs(
            flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[flow_run.id])),
            task_run_filter=TaskRunFilter(
                state=TaskRunFilterState(type=TaskRunFilterStateType(any_=[StateType.FAILED, StateType.CRASHED]))
            ),
        )
    for tr in failed:
        msg = tr.state.message or tr.state.name
        if "BeamUnusable" in msg:
            logger.warning(f"{tr.name} dropped: {msg}")
        else:
            logger.error(f"{tr.name} FAILED: {msg}")
    return len(failed)


def _generate_ms_intervals(n_integrations: int, n_intervals: int = 1) -> list[tuple[int, int]]:
    """Generates `n_intervals` equal (or close to equal) intervals for cleaning for a measurement set with
    `n_integrations` time integrations"""
    if not n_integrations or n_integrations < 1:
        raise ValueError(f"Erroneous value for n_integrations: {n_integrations}")
    if not 1 <= n_intervals <= n_integrations:
        raise ValueError(f"n_intervals must be between 1 and {n_integrations}, got {n_intervals}")

    base, extra = divmod(n_integrations, n_intervals)
    intervals = []
    start = 0
    for i in range(n_intervals):
        size = base + (1 if i < extra else 0)  # first `extra` chunks get one more
        end = start + size - 1
        intervals.append((start, end))
        start = end + 1

    return intervals


# def _ok(fut: PrefectFuture) -> bool:
#     # Checks if the state of the future is complete
#     fut.wait()
#     return fut.state.is_completed()


# def _expand_intervals(
#     f_tgt: FutureList,
#     f_inspect_tgt: FutureList,
#     f_model_subtract: FutureList,
#     f_mask: OptionalFutureList,
#     n_intervals: int,
# ) -> tuple[FutureList, FutureList, FutureList, list[tuple[int, int]]]:
#     """Compute intervals per MS and flatten everything for mapping.
#     Each MS fans out into n_intervals tasks, so we replicate tgt/subtract futures accordingly"""
#     all_tgt, all_model_subtracts, all_masks, all_intervals = [], [], [], []
#     for tgt, inspect, subtract, mask in zip(f_tgt, f_inspect_tgt, f_model_subtract, f_mask):
#         if not (_ok(tgt) and _ok(inspect)):
#             continue
#         ms_info = MSInfo.from_json(inspect.result())
#         for interval in _generate_ms_intervals(ms_info.time.n_integrations, n_intervals):
#             all_tgt.append(tgt)
#             all_model_subtracts.append(subtract)
#             all_masks.append(mask)
#             all_intervals.append(interval)
#     return all_tgt, all_model_subtracts, all_masks, all_intervals
