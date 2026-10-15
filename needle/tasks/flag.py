from pathlib import Path

from prefect import task
from prefect.cache_policies import NO_CACHE
from distributed import Client

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution
from needle.config.flag import FlagConfig
from needle.lib.dask import submit_in_beam, map_in_beam
from needle.lib.logging import beam_logger
from needle.modules.flag import flag_observation, FlagContext


@task(cache_policy=NO_CACHE, task_run_name="flag_{ms.stem}")
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
    with beam_logger(ms.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        if isinstance(ms, CalibrationSolution):  # No-op
            return ms

        logger.info(f"Flagging measurement set: {ms.name}")
        ctx = FlagContext(cfg=cfg, ms=ms)
        if ctx.output.exists() and overwrite is False:
            logger.info(f"Found existing flagversion for tgt: {ctx.output}\nWill not reflag.")
            return ms
        submit_in_beam(client, flag_observation, ctx).result()
        logger.info(f"Completed flagging task for {ms.name}")
    return ms


@task(cache_policy=NO_CACHE, task_run_name="flag_beam{beam.beam}")
def flag_ms_pair_task(
    client: Client, beam: BeamPair, cfg: FlagConfig, log_level: str = "INFO", overwrite: bool = False, **kwargs
) -> BeamPair:
    """Flags a pair of measurement sets. Returns the same measurement set pair."""
    fn_inputs = {k: v for k, v in locals().items() if k != "client"}.items()
    with beam_logger(beam.tgt.parent, log_level) as logger:
        logger.debug("Inputs:\n" + "\n\t".join([f"{name}: {value}" for name, value in fn_inputs]))

        task_ctxs = []
        tgt_ctx = FlagContext(cfg=cfg, ms=beam.tgt)
        if tgt_ctx.output.exists() and overwrite is False:
            logger.info(f"Found existing flagversion for tgt: {tgt_ctx.output}\nWill not reflag.")
        else:
            task_ctxs.append(tgt_ctx)

        if not isinstance(beam.cal, CalibrationSolution):
            cal_ctx = FlagContext(cfg=cfg, ms=beam.cal)
            if cal_ctx.output.exists() and overwrite is False:
                logger.info(f"Found existing flagversion for tgt: {tgt_ctx.output}\nWill not reflag.")
            else:
                task_ctxs.append(cal_ctx)

        logger.info(f"Beginning flagging task for beam {beam.beam}")
        client.gather(map_in_beam(client, flag_observation, task_ctxs))
        logger.info(f"Completed flagging task for beam {beam.beam}")
    return beam
