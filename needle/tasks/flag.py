from pathlib import Path
from typing import Optional

from prefect import task

from needle.config.pipeline import ContainerConfig
from needle.lib.flow import CACHE_STRATEGY, CACHE_EXPIRATION
from needle.lib.logging import setup_logging
from needle.config.flag import FlagConfig
from needle.config.pipeline import MSBeamPair
from needle.modules.flag import flag_observation, FlagContext


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def flag_ms_task(ms: Path, cfg: FlagConfig, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO") -> Path:
    """Flags a measurement set. Returns the same measurement set"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    try:
        ctx = FlagContext(runtime=runtime, cfg=cfg, ms=ms)
        logger.info(f"Flagging measurement set: {ms.name}")
        flag_observation(ctx)
    except ValueError as e:
        logger.warning(str(e))
        logger.warning("Attempting to continue anyway...")
    return ms


@task(cache_policy=CACHE_STRATEGY, persist_result=True, cache_expiration=CACHE_EXPIRATION)
def flag_ms_pair_task(
    ms_pair: MSBeamPair, cfg: FlagConfig, runtime: Optional[ContainerConfig] = None, log_level: str = "INFO"
) -> MSBeamPair:
    """Flags a pair of measurement sets. Returns the same measurement set pair"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    try:
        tgt_ctx = FlagContext(runtime=runtime, cfg=cfg, ms=ms_pair.tgt)
        cal_ctx = FlagContext(runtime=runtime, cfg=cfg, ms=ms_pair.cal)
        flag_observation(tgt_ctx)
        flag_observation(cal_ctx)
    except ValueError as e:
        logger.warning(str(e))
        logger.warning("Attempting to continue anyway...")
    return ms_pair
