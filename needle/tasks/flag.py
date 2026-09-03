from pathlib import Path

from prefect import task
from prefect.cache_policies import NO_CACHE
from distributed import Client

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution
from needle.lib.logging import setup_logging
from needle.config.flag import FlagConfig
from needle.modules.flag import flag_observation, FlagContext


@task(cache_policy=NO_CACHE)
def flag_ms_task(client: Client, ms: Path | CalibrationSolution, cfg: FlagConfig, log_level: str = "INFO") -> Path:
    """Flags a measurement set. Returns the same measurement set"""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    if isinstance(ms, CalibrationSolution):  # No-op
        return ms

    logger.info(f"Flagging measurement set: {ms.name}")
    ctx = FlagContext(cfg=cfg, ms=ms)
    client.submit(flag_observation, ctx).result()
    return ms


@task(cache_policy=NO_CACHE)
def flag_ms_pair_task(client: Client, ms_pair: BeamPair, cfg: FlagConfig, log_level: str = "INFO") -> BeamPair:
    """Flags a pair of measurement sets. Returns the same measurement set pair"""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    tgt_ctx = FlagContext(cfg=cfg, ms=ms_pair.tgt)
    cal_ctx = FlagContext(cfg=cfg, ms=ms_pair.cal)
    client.gather(client.map(flag_observation, (tgt_ctx, cal_ctx)))
    return ms_pair
