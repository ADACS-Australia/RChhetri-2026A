from pathlib import Path
from typing import Tuple

from prefect import Flow, flow, unmapped
from prefect.futures import PrefectFuture
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.runtime import flow_run
from distributed import Client

from needle.tasks.utils import extract_cal_task, extract_tgt_task
from needle.tasks.calibrate import solve_calibration_task, apply_calibration_task
from needle.tasks.convert import convert_task
from needle.tasks.flag import flag_ms_task

from needle.config.pipeline import NeedleConfig
from needle.config.cluster import ClusterConfig
from needle.lib.logging import setup_logging
from needle.modules.inspect import MSInfo
from needle.tasks.beam import setup_beam_dir_task, find_beam_pairs_task
from needle.tasks.casa_data import update_casa_data
from needle.tasks.clean import clean_task, interval_clean_task, predict_task
from needle.tasks.diagnostics import cal_diagnostics_task, ms_diagnostics_task
from needle.tasks.inspect import inspect_ms_task
from needle.tasks.mask import create_mask_task
from needle.tasks.source_find import source_find_task

FutureList = list[PrefectFuture]


def _split_ms_into_intervals(inspect_path: Path, n_intervals: int = 1) -> list[tuple[int, int]]:
    ms_info = MSInfo.from_json(inspect_path)
    corrected_column = ms_info.data_columns.get("DATA")
    if not corrected_column:
        raise RuntimeError(f"Expected column 'DATA' is absent in measurement set: {inspect_path}")
    assert len(corrected_column) == 2, "DATA column should have length 2"

    total = corrected_column[1]
    chunk_size = total // n_intervals

    intervals = []
    for i in range(n_intervals):
        start = i * chunk_size
        end = total if i == n_intervals - 1 else start + chunk_size
        intervals.append((start, end))

    return intervals


def _unmapped_defaults(cfg: NeedleConfig) -> dict:
    return {"log_level": unmapped(cfg.flow.log_level)}


def _inspect_and_diagnose(
    client: Client, cfg: NeedleConfig, f_tgt: FutureList, f_cal: FutureList, f_cal_output: FutureList
) -> Tuple[FutureList, FutureList, FutureList, FutureList, FutureList]:
    """Inspects the data and runs diagnostics on it"""
    defaults = _unmapped_defaults(cfg)

    # Inspect the tgt and cal source
    f_inspect_tgt = inspect_ms_task.map(unmapped(client), f_tgt, unmapped(cfg.flow.log_level))
    f_inspect_cal = inspect_ms_task.map(unmapped(client), f_cal, unmapped(cfg.flow.log_level))

    # Run diagnostics on the calibrator and tgt MS
    f_cal_diagnostics = ms_diagnostics_task.map(unmapped(client), f_cal, **defaults)
    f_tgt_diagnostics = ms_diagnostics_task.map(unmapped(client), f_tgt, **defaults)

    # Run diagnostics on calibrated target and calibrator solution tables
    f_cal_soln_diagnostics = cal_diagnostics_task.map(unmapped(client), f_cal_output, **defaults)
    return (f_inspect_cal, f_inspect_tgt, f_cal_diagnostics, f_tgt_diagnostics, f_cal_soln_diagnostics)


def _source_find_and_mask(client: Client, cfg: NeedleConfig, f_shallow_image: FutureList) -> FutureList:
    """Source find on an image and create a mask"""
    defaults = _unmapped_defaults(cfg)
    f_json_sources = source_find_task.map(unmapped(client), f_shallow_image, cfg=unmapped(cfg.source_find), **defaults)
    # Create masks over the sources in preparation for deep cleaning
    return create_mask_task.map(
        unmapped(client), f_json_sources, f_shallow_image, cfg=unmapped(cfg.create_mask), **defaults
    )


def _expand_intervals(
    f_tgt: FutureList,
    f_inspect_tgt: FutureList,
    f_model_subtract: FutureList,
    f_mask: FutureList,
    n_intervals: int,
) -> tuple[FutureList, FutureList, FutureList, list[tuple[int, int]]]:
    """Compute intervals per MS and flatten everything for mapping.
    Each MS fans out into n_intervals tasks, so we replicate tgt/subtract futures accordingly"""
    all_tgt = []
    all_model_subtracts = []
    all_masks = []
    all_intervals = []
    for tgt, inspect, subtract, mask in zip(f_tgt, f_inspect_tgt, f_model_subtract, f_mask):
        inspect_path = inspect.result()  # resolve the path from the future
        intervals = _split_ms_into_intervals(inspect_path, n_intervals=n_intervals)
        for interval in intervals:
            all_tgt.append(tgt)
            all_model_subtracts.append(subtract)
            all_masks.append(mask)
            all_intervals.append(interval)
    return all_tgt, all_model_subtracts, all_masks, all_intervals


def _pipeline_flow_name() -> str:
    """Should be called only at runtime. Generates the name of the flow"""
    params = flow_run.get_parameters()
    target_obs = Path(params["work_dir"]).stem
    return f"pipeline-{target_obs}"


@flow(
    name="needle-pipeline",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(),
    persist_result=True,
    flow_run_name=_pipeline_flow_name,
)
def needle_pipeline(client: Client, cfg: NeedleConfig, work_dir: Path | str) -> Flow:
    logger = setup_logging(cfg.flow.log_level)
    logger.debug(f"Config: {cfg}")
    defaults = _unmapped_defaults(cfg)

    # Update the casa measures dataset before doing anything
    update_casa_data(data_path=cfg.data.staging_dir / "casadata", runtime=ClusterConfig.get_config().container)

    # Get the beam pairs to work with
    beam_pairs = find_beam_pairs_task(search_dir=Path(work_dir), log_level=cfg.flow.log_level)
    logger.info(f"Found beam pairs: {beam_pairs}")
    f_beam_pairs = setup_beam_dir_task.map(beam_pairs, log_level=unmapped(cfg.flow.log_level))

    # Exctract the individual calibrators and targets
    f_cal = extract_cal_task.map(pair=f_beam_pairs)
    f_tgt = extract_tgt_task.map(pair=f_beam_pairs)

    # Convert pairs to measurement sets
    f_cal = convert_task.map(unmapped(client), f_cal, **defaults)
    f_tgt = convert_task.map(unmapped(client), f_tgt, **defaults)

    # Flag, calibrate and inspect
    f_tgt = flag_ms_task.map(client=client, ms=f_tgt, cfg=unmapped(cfg.flag), **defaults)
    f_cal = flag_ms_task.map(client=client, ms=f_cal, cfg=unmapped(cfg.flag), **defaults)
    f_cal_output = solve_calibration_task.map(client=client, cal=f_cal, cfg=unmapped(cfg.calibrate_solve), **defaults)
    f_tgt = apply_calibration_task.map(
        client=client, cfg=unmapped(cfg.calibrate_apply), cal=f_cal_output, tgt=f_tgt, **defaults
    )
    f_inspect_tgt = inspect_ms_task.map(unmapped(client), f_tgt, unmapped(cfg.flow.log_level))
    f_inspect_cal = inspect_ms_task.map(unmapped(client), f_cal, unmapped(cfg.flow.log_level))
    f_cal_diagnostics = ms_diagnostics_task.map(unmapped(client), f_cal, **defaults)
    f_tgt_diagnostics = ms_diagnostics_task.map(unmapped(client), f_tgt, **defaults)
    f_cal_soln_diagnostics = cal_diagnostics_task.map(unmapped(client), f_cal_output, **defaults)

    # Clean, mask and model subtract
    f_shallow_image = clean_task.with_options(name="shallow_clean").map(
        unmapped(client), f_tgt, cfg=unmapped(cfg.shallow_clean), **defaults
    )
    f_mask = _source_find_and_mask(client=client, cfg=cfg, f_shallow_image=f_shallow_image)
    f_deep_image = clean_task.with_options(name="deep_clean").map(
        unmapped(client), f_tgt, cfg=unmapped(cfg.deep_clean), mask=f_mask, **defaults
    )
    f_model_create = predict_task.map(
        unmapped(client), f_tgt, cfg=unmapped(cfg.deep_clean), dependencies=f_deep_image, **defaults
    )
    # Model subtract - removes the MODEL_DATA from the DATA visibilities
    f_model_subtract = clean_task.with_options(name="model_subtract").map(
        unmapped(client), f_model_create, cfg=unmapped(cfg.model_subtract), mask=f_mask, **defaults
    )

    # Clean on each interval - one task per (MS, interval) combination
    all_tgt, all_model_subtracts, all_masks, all_intervals = _expand_intervals(
        f_tgt=f_tgt,
        f_inspect_tgt=f_inspect_tgt,
        f_model_subtract=f_model_subtract,
        f_mask=f_mask,
        n_intervals=cfg.flow.interval_tasks,
    )
    f_interval_clean = interval_clean_task.map(
        unmapped(client),
        all_tgt,
        cfg=unmapped(cfg.interval_clean),
        mask=all_masks,
        interval=all_intervals,  # each task gets its own slice
        dependencies=all_model_subtracts,
        **defaults,
    )

    for f in (f_cal_diagnostics, f_tgt_diagnostics, f_interval_clean, f_inspect_cal, f_cal_soln_diagnostics):
        f.result()  # Wait on the last output so that the flow doesn't end
