import os
from pathlib import Path
from typing import Optional, Tuple, List

from prefect import Flow, flow, unmapped
from prefect.future import PrefectFuture
from prefect_dask import DaskTaskRunner

from needle.lib.logging import setup_logging
from needle.config.pipeline import PipelineConfig
from needle.modules.inspect_ms import MSInfo
from needle.tasks.calibrate import calibrate_pair_task, extract_tgt_task
from needle.tasks.clean import clean_task, interval_clean_task, predict_task
from needle.tasks.convert import convert_beam_pair_task, extract_cal_task
from needle.tasks.diagnostics import diagnostics_task, diagnostics_cal_output_task
from needle.tasks.flag import flag_ms_pair_task
from needle.tasks.inspect import inspect_pair_task
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


def _unmapped_defaults(cfg: PipelineConfig) -> dict:
    return {"runtime": unmapped(cfg.flow.runtime), "log_level": unmapped(cfg.flow.log_level)}


def _flag_and_calibrate(cfg: PipelineConfig, f_ms_pairs: FutureList) -> Tuple[FutureList, FutureList, FutureList]:
    defaults = _unmapped_defaults(cfg)
    flag_pair_futures = flag_ms_pair_task.map(f_ms_pairs, cfg=unmapped(cfg.flag), **defaults)
    f_cal_output = calibrate_pair_task.map(flag_pair_futures, cfg=unmapped(cfg.calibrate), **defaults)
    f_tgt = extract_tgt_task.map(f_cal_output)
    f_cal = extract_cal_task.map(f_cal_output)
    return (f_cal_output, f_tgt, f_cal)


def _inspect_and_diagnose(
    cfg: PipelineConfig, f_ms_pairs: FutureList, f_cal_output: FutureList
) -> Tuple[FutureList, FutureList, FutureList]:
    defaults = _unmapped_defaults(cfg)
    f_inspect = inspect_pair_task.map(f_ms_pairs, **defaults)
    # Run diagnostics on the calibrator MS
    f_cal_diagnostics = diagnostics_task.map(extract_cal_task.map(f_ms_pairs), **defaults)
    # Run diagnostics on calibrated target and calibrator solution tables
    f_tgt_diagnostics = diagnostics_cal_output_task.map(f_cal_output, **defaults)
    return (f_inspect, f_cal_diagnostics, f_tgt_diagnostics)


def _source_find_and_mask(cfg: PipelineConfig, f_shallow_image: FutureList) -> FutureList:
    """Source find on an image and create a mask"""
    defaults = _unmapped_defaults(cfg)
    f_json_sources = source_find_task.map(f_shallow_image, cfg=unmapped(cfg.source_find), **defaults)
    # Create masks over the sources in preparation for deep cleaning
    return create_mask_task.map(
        f_json_sources,
        f_shallow_image,
        cfg=unmapped(cfg.create_mask),
        log_level=unmapped(cfg.flow.log_level),
    )


def _create_and_subtract_model(
    cfg: PipelineConfig, f_tgt: FutureList, f_deep_image: FutureList, f_mask: FutureList
) -> FutureList:
    """Creates a sky model and subtracts it from the data"""
    defaults = _unmapped_defaults(cfg)
    # Create Model - updates the ms in place with the MODEL_DATA columnn
    f_model_create = predict_task.map(f_tgt, cfg=unmapped(cfg.deep_clean), wait_for_=f_deep_image, **defaults)
    # Model subtract - removes the MODEL_DATA from the DATA visibilities
    return clean_task.with_options(name="model_subtract").map(
        f_model_create, cfg=unmapped(cfg.model_subtract), mask=f_mask, **defaults
    )


def _expand_intervals(
    f_tgt: FutureList,
    f_inspect: FutureList,
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
    for tgt, inspect, subtract, mask in zip(f_tgt, f_inspect, f_model_subtract, f_mask):
        inspect_path = inspect.result()  # resolve the path from the future
        intervals = _split_ms_into_intervals(inspect_path, n_intervals=n_intervals)
        for interval in intervals:
            all_tgt.append(tgt)
            all_model_subtracts.append(subtract)
            all_masks.append(mask)
            all_intervals.append(interval)
    return all_tgt, all_model_subtracts, all_masks, all_intervals


# Note that CASA and BANE are not thread-safe. Multiple instances can't run concurrently in the same process.
# so ThreadPooolRunner will not work
@flow(log_prints=True, task_runner=DaskTaskRunner(), persist_result=True)
def needle_pipeline(cfg: PipelineConfig) -> Flow:
    logger = setup_logging(cfg.flow.log_level)
    logger.debug(f"Config: {cfg}")

    os.makedirs(cfg.flow.beams_dir, exist_ok=True)  # Must be done in serial
    defaults = _unmapped_defaults(cfg)

    # Convert pairs to measurement sets and set up working directories
    f_ms_pairs = convert_beam_pair_task.map(cfg.flow.beam_pairs, **defaults)
    f_cal_output, f_tgt, _ = _flag_and_calibrate(cfg=cfg)
    f_inspect, f_cal_diagnostics, f_tgt_diagnostics = _inspect_and_diagnose(
        cfg=cfg, f_ms_pairs=f_ms_pairs, f_cal_output=f_cal_output
    )

    f_shallow_image = clean_task.with_options(name="shallow_clean").map(
        f_tgt, cfg=unmapped(cfg.shallow_clean), **defaults
    )
    f_mask = _source_find_and_mask(cfg=cfg, f_shallow_image=f_shallow_image)
    f_deep_image = clean_task.with_options(name="deep_clean").map(
        f_tgt, cfg=unmapped(cfg.deep_clean), mask=f_mask, **defaults
    )
    f_model_subtract = _create_and_subtract_model(cfg=cfg, f_deep_image=f_deep_image, f_mask=f_mask)

    # Clean on each interval - one task per (MS, interval) combination
    all_tgt, all_model_subtracts, all_masks, all_intervals = _expand_intervals(
        f_tgt=f_tgt,
        f_inspect=f_inspect,
        f_model_subtract=f_model_subtract,
        f_mask=f_mask,
        n_intervals=cfg.flow.interval_tasks,
    )
    f_interval_clean = interval_clean_task.map(
        all_tgt,
        cfg=unmapped(cfg.interval_clean),
        mask=all_masks,
        interval=all_intervals,  # each task gets its own slice
        wait_for_=all_model_subtracts,
        **defaults,
    )

    for f in (f_cal_diagnostics, f_tgt_diagnostics, f_interval_clean):
        f.result()  # Wait on the last output so that the flow doesn't end
