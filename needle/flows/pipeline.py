import os

from prefect import Flow, flow, unmapped
from prefect.task_runners import ProcessPoolTaskRunner

from needle.lib.flow import BEAMS_DIR
from needle.lib.logging import setup_logging
from needle.models.pipeline import PipelineConfig
from needle.tasks.calibrate import calibrate_pair_task
from needle.tasks.clean import clean_task, interval_clean_task
from needle.tasks.convert import convert_beam_pair_task
from needle.tasks.flag import flag_ms_pair_task
from needle.tasks.inspect import inspect_pair_task
from needle.tasks.mask import create_mask_task
from needle.tasks.source_find import source_find_task


# CASA and BANE are not thread-safe. Multiple instances can't run concurrently in the same process.
# ProcessTaskRunner gives each task its own process and isolated runtime.
@flow(log_prints=True, task_runner=ProcessPoolTaskRunner(), persist_result=True)
def needle_pipeline(cfg: PipelineConfig) -> Flow:
    logger = setup_logging(cfg.flow.log_level)
    logger.debug(f"Config: {cfg}")

    os.makedirs(BEAMS_DIR, exist_ok=True)  # Must be done in serial

    # Convert pairs to measurement sets and set up working directories
    ms_pairs_futures = convert_beam_pair_task.map(cfg.flow.beam_pairs, log_level=unmapped(cfg.flow.log_level))

    # Inspect the data - not used but nice to have
    inspect_futures = inspect_pair_task.map(ms_pairs_futures, log_level=unmapped(cfg.flow.log_level))

    # Flag the data
    flag_pair_futures = flag_ms_pair_task.map(
        ms_pairs_futures, cfg=unmapped(cfg.flag), log_level=unmapped(cfg.flow.log_level)
    )

    # Calibrate - returns calibrated target ms
    tgt_futures = calibrate_pair_task.map(
        flag_pair_futures, cfg=unmapped(cfg.calibrate), log_level=unmapped(cfg.flow.log_level)
    )

    # Shallow Clean
    shallow_image_futures = clean_task.with_options(name="shallow_clean").map(
        tgt_futures, cfg=unmapped(cfg.shallow_clean), log_level=unmapped(cfg.flow.log_level)
    )

    # Source find on the shallow-cleaned image
    json_sources = source_find_task.map(
        shallow_image_futures, cfg=unmapped(cfg.source_find), log_level=unmapped(cfg.flow.log_level)
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
        tgt_futures, cfg=unmapped(cfg.deep_clean), mask=mask_output_futures, log_level=unmapped(cfg.flow.log_level)
    )

    # Clean on each interval - produces a list of images for each beam
    interval_clean_futures = interval_clean_task.map(
        tgt_futures, cfg=unmapped(cfg.interval_clean), mask=mask_output_futures, log_level=unmapped(cfg.flow.log_level)
    )

    [f.result() for f in inspect_futures]
    [f.result() for f in deep_image_futures]
    [f.result() for f in interval_clean_futures]  # Wait on the last output so that the flow doesn't end
