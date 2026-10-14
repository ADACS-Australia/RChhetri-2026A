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
def flag_ms_task(
    client: Client,
    ms: Path | CalibrationSolution,
    cfg: FlagConfig,
    log_level: str = "INFO",
    overwrite: bool = False,
    **kwargs,
) -> Path:
    """Flags a measurement set. Returns the same measurement set."""
    fn_inputs = locals().items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    if isinstance(ms, CalibrationSolution):  # No-op
        return ms

    logger.info(f"Flagging measurement set: {ms.name}")
    ctx = FlagContext(cfg=cfg, ms=ms)
    if ctx.output.exists() and overwrite is False:
        logger.info(f"Found existing flagversion for tgt: {ctx.output}\nWill not reflag.")
        return ms
    client.submit(flag_observation, ctx).result()
    return ms


@task(cache_policy=NO_CACHE)
def flag_ms_pair_task(
    client: Client, ms_pair: BeamPair, cfg: FlagConfig, log_level: str = "INFO", overwrite: bool = False, **kwargs
) -> BeamPair:
    """Flags a pair of measurement sets. Returns the same measurement set pair."""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    logger = setup_logging(log_level)
    logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

    task_ctxs = []
    tgt_ctx = FlagContext(cfg=cfg, ms=ms_pair.tgt)
    if tgt_ctx.output.exists() and overwrite is False:
        logger.info(f"Found existing flagversion for tgt: {tgt_ctx.output}\nWill not reflag.")
    else:
        task_ctxs.append(tgt_ctx)
    cal_ctx = FlagContext(cfg=cfg, ms=ms_pair.cal)
    if cal_ctx.output.exists() and overwrite is False:
        logger.info(f"Found existing flagversion for tgt: {tgt_ctx.output}\nWill not reflag.")
    else:
        task_ctxs.append(cal_ctx)

    client.gather(client.map(flag_observation, task_ctxs))
    return ms_pair
