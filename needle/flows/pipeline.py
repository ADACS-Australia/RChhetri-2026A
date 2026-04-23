import os
from pathlib import Path
from typing import Tuple

from prefect import Flow, flow, unmapped
from prefect_dask import DaskTaskRunner

from needle.lib.logging import setup_logging
from needle.config.pipeline import PipelineConfig
from needle.modules.inspect_ms import MSInspectResult
from needle.tasks.calibrate import calibrate_pair_task
from needle.tasks.clean import clean_task, interval_clean_task, predict_task
from needle.tasks.convert import convert_beam_pair_task
from needle.tasks.flag import flag_ms_pair_task
from needle.tasks.inspect import inspect_pair_task, inspect_ms_task
from needle.tasks.mask import create_mask_task
from needle.tasks.source_find import source_find_task


def _split_ms_into_intervals(inspect_path: Path, n_intervals: int = 1) -> list[Tuple[int, int]]:
    ms_info = MSInspectResult.from_json(inspect_path)
    corrected_column = ms_info.data_colums.get("CORRECTED_DATA")
    if not corrected_column:
        raise RuntimeError(f"Expected column 'CORRECTED_DATA' is absent in measurement set: {inspect_path}")
    assert len(corrected_column) == 2, "Corrected column should have length 2"

    start, end = corrected_column[0], corrected_column[1]
    total = end - start + 1
    interval_size = total // n_intervals

    intervals = []
    for i in range(n_intervals):
        interval_start = start + i * interval_size
        # Last interval absorbs any remainder from uneven division
        interval_end = end if i == n_intervals - 1 else interval_start + interval_size - 1
        intervals.append((interval_start, interval_end))

    return intervals


# Note that CASA and BANE are not thread-safe. Multiple instances can't run concurrently in the same process.
# so ThreadPooolRunner will not work
@flow(log_prints=True, task_runner=DaskTaskRunner(), persist_result=True)
def needle_pipeline(cfg: PipelineConfig) -> Flow:
    logger = setup_logging(cfg.flow.log_level)
    logger.debug(f"Config: {cfg}")

    os.makedirs(cfg.flow.beams_dir, exist_ok=True)  # Must be done in serial

    # Convert pairs to measurement sets and set up working directories
    ms_pairs_futures = convert_beam_pair_task.map(
        cfg.flow.beam_pairs, runtime=unmapped(cfg.flow.runtime), log_level=unmapped(cfg.flow.log_level)
    )

    # Inspect the data - not used but nice to have
    inspect_futures = inspect_pair_task.map(ms_pairs_futures, log_level=unmapped(cfg.flow.log_level))

    # Flag the data
    flag_pair_futures = flag_ms_pair_task.map(
        ms_pairs_futures,
        cfg=unmapped(cfg.flag),
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Calibrate - returns calibrated target ms
    tgt_futures = calibrate_pair_task.map(
        flag_pair_futures,
        cfg=unmapped(cfg.calibrate),
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Inspect the calibrated_data - used later for interval cleaning
    calibrated_inspect_futures = inspect_ms_task.map(tgt_futures, log_level=unmapped(cfg.flow.log_level))

    # Shallow Clean
    shallow_image_futures = clean_task.with_options(name="shallow_clean").map(
        tgt_futures,
        cfg=unmapped(cfg.shallow_clean),
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Source find on the shallow-cleaned image
    json_sources = source_find_task.map(
        shallow_image_futures,
        cfg=unmapped(cfg.source_find),
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Create masks over the sources in preparation for deep cleaning
    mask_output_futures = create_mask_task.map(
        json_sources,
        shallow_image_futures,
        cfg=unmapped(cfg.create_mask),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Deep Clean
    deep_image_futures = clean_task.with_options(name="deep_clean").map(
        tgt_futures,
        cfg=unmapped(cfg.deep_clean),
        mask=mask_output_futures,
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Create Model - updates the ms in place with the MODEL_DATA columnn
    model_creation_futures = predict_task.map(
        tgt_futures,
        cfg=unmapped(cfg.deep_clean),
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
        wait_for_=deep_image_futures,
    )

    # Model subtract - removes the MODEL_DATA from the DATA visibilities
    model_subtract_futures = clean_task.with_options(name="model_subtract").map(
        model_creation_futures,
        cfg=unmapped(cfg.model_subtract),
        mask=mask_output_futures,
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
    )

    # Compute intervals per MS and flatten everything for mapping
    # Each MS fans out into n_intervals tasks, so we replicate tgt/subtract futures accordingly
    all_tgt = []
    all_subtracts = []
    all_masks = []
    all_intervals = []

    for tgt, inspect, subtract, mask in zip(
        tgt_futures, calibrated_inspect_futures, model_subtract_futures, mask_output_futures
    ):
        inspect_path = inspect.result()  # resolve the path from the future
        intervals = _split_ms_into_intervals(inspect_path, n_intervals=cfg.interval_clean.n_intervals)
        for interval in intervals:
            all_tgt.append(tgt)
            all_subtracts.append(subtract)
            all_masks.append(mask)
            all_intervals.append(interval)

    # Clean on each interval - one task per (MS, interval) combination
    interval_clean_futures = interval_clean_task.map(
        all_tgt,
        cfg=unmapped(cfg.interval_clean),
        mask=all_masks,
        interval=all_intervals,  # each task gets its own slice
        runtime=unmapped(cfg.flow.runtime),
        log_level=unmapped(cfg.flow.log_level),
        wait_for_=all_subtracts,
    )

    # # Clean on each interval - produces a list of images for each beam using the model-subtracted visibilities
    # interval_clean_futures = interval_clean_task.map(
    #     tgt_futures,
    #     cfg=unmapped(cfg.interval_clean),
    #     mask=mask_output_futures,
    #     runtime=unmapped(cfg.flow.runtime),
    #     log_level=unmapped(cfg.flow.log_level),
    #     wait_for_=model_subtract_futures,
    # )

    [f.result() for f in inspect_futures]
    [f.result() for f in interval_clean_futures]  # Wait on the last output so that the flow doesn't end
