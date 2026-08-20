"""
The CLI for needle.
Many of the imports are lazy-loaded so that the CLI is more responsive.
"""

import logging
import importlib
import sys
import threading
import time
from pathlib import Path

import click
from prefect_dask import DaskTaskRunner

from needle.config.cluster import ClusterConfig
from needle.config.pipeline import NeedleConfig
from needle.lib.logging import setup_logging

logger = logging.getLogger("needle-cli")


class LazyGroup(click.Group):
    """A click.Group whose subcommands are imported only when actually invoked list their names or one-line
    descriptions in --help, which uses a manually supplied short_help string instead of importing the command
    """

    def __init__(self, *args, lazy_subcommands: dict[str, tuple[str, str]] | None = None, **kwargs):
        # lazy_subcommands: {name: (import_target "module.path:attr", short_help)}
        super().__init__(*args, **kwargs)
        self.lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, _):
        return sorted(set(self.lazy_subcommands) | set(self.commands))

    def get_command(self, ctx, name):
        if name in self.lazy_subcommands:
            target, _ = self.lazy_subcommands[name]
            module_path, attr = target.split(":")
            module = importlib.import_module(module_path)
            cmd = getattr(module, attr)
            if not isinstance(cmd, click.Command):
                raise ValueError(f"Lazy loading of {target!r} failed by returning a non-command object: {cmd!r}")
            return cmd
        return super().get_command(ctx, name)

    def format_commands(self, ctx, formatter):
        # Overridden because Click's default implementation calls get_command() on every listed name to read its help
        # text — which would import every module
        rows = []
        for name in self.list_commands(ctx):
            if name in self.lazy_subcommands:
                _, short_help = self.lazy_subcommands[name]
            else:
                cmd = self.get_command(ctx, name)
                short_help = cmd.get_short_help_str() if cmd else ""
            rows.append((name, short_help))
        if rows:
            with formatter.section("Commands"):
                formatter.write_dl(rows)


def _setup_cli_logging(level: str = "INFO"):
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)


def _load_task_runner(mode: str | None, cfg: NeedleConfig) -> DaskTaskRunner:
    if mode is None:
        mode = "cluster" if (Path.home() / ".needle_cluster.yaml").exists() else "local"

    if mode == "cluster":
        cluster_cfg = ClusterConfig.get_config()
        logger.info(f"Using {cluster_cfg.type} cluster")
        return cluster_cfg.to_task_runner()

    logger.info("Using local environment for task runs")
    return DaskTaskRunner(cluster_kwargs={"n_workers": cfg.flow.max_workers, "threads_per_worker": 1})


def _mode_and_log_level_options(f):
    f = click.option(
        "--cluster",
        "mode",
        flag_value="cluster",
        default=None,
        help="Run using a cluster configured with ~/.needle_cluster.yaml",
    )(f)
    f = click.option(
        "--local",
        "mode",
        flag_value="local",
        default=None,
        help="Run locally without any cluster or container",
    )(f)
    f = click.option(
        "--log-level",
        "--log_level",
        "log_level",
        type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
        default="INFO",
        help="Logging level",
    )(f)
    return f


@click.group(help="Needle pipeline CLI.", context_settings={"max_content_width": 120})
def cli():
    pass


@cli.command()
@click.option(
    "--work-dir",
    "work_dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Location of the calibration and target observation pairs.",
)
@_mode_and_log_level_options
def run(work_dir, mode, log_level):
    """Runs the Needle Pipeline now.

    Expects a .needle.yaml to be in the user home. See setup_env.sh for setup help.
    """
    from needle.flows.pipeline import needle_pipeline

    _setup_cli_logging(log_level)
    setup_logging(log_level)
    cfg = NeedleConfig.get_config()

    needle_pipeline.with_options(task_runner=_load_task_runner(mode, cfg))(cfg=cfg, work_dir=str(work_dir))


def _watch_and_restart(watcher_cfg, data_cfg):
    """Run watch(), restarting on failure after restart_delay seconds."""
    from needle.modules.watcher import watch

    while True:
        try:
            watch(watcher_cfg, data_cfg)
        except Exception as e:
            logger.error(f"Watcher crashed: {e} — restarting in 30s", exc_info=True)
            time.sleep(30)


@cli.command()
@_mode_and_log_level_options
def serve(mode, log_level):
    """Starts the Watcher, Courier, and Needle Pipeline, served to the Prefect Server.

    Expects a .needle.yaml to be in the user home. See setup_env.sh for setup help.
    """
    from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
    from needle.flows.pipeline import needle_pipeline
    from needle.flows.courier import courier_flow, COURIER_RESOURCE_ID
    from needle.lib.events import OBSERVATION_READY_EVENT, OBSERVATION_STAGED_EVENT
    from needle.modules.watcher import WATCHER_RESOURCE_ID

    _setup_cli_logging(log_level)
    setup_logging(log_level)
    cfg = NeedleConfig.get_config()

    watcher_thread = threading.Thread(target=_watch_and_restart, args=(cfg.watcher, cfg.data), daemon=True)
    watcher_thread.start()
    logger.info(f"Watcher started — source: {cfg.data.source}, polling every {cfg.watcher.poll_interval}s")

    # We cannot use prefect's serve() function to serve multiple flows as it ignores the configured taskrunner
    courier_thread = threading.Thread(
        target=courier_flow.serve,
        kwargs={
            "name": "needle-courier",
            "parameters": {"data_cfg": cfg.data.to_kwargs()},
            "triggers": [
                DeploymentEventTrigger(
                    name="observation-ready-trigger",
                    enabled=True,
                    expect={OBSERVATION_READY_EVENT},
                    match={"prefect.resource.id": WATCHER_RESOURCE_ID},
                    parameters={"entry_name": "{{ event.payload.entry_name }}"},
                    flow_run_name="courier-{{ event.payload.entry_name }}",
                )
            ],
        },
        daemon=True,
    )
    courier_thread.start()
    logger.info("Courier deployment started")

    needle_pipeline.with_options(
        task_runner=_load_task_runner(mode, cfg),
        result_storage=cfg.data.staging_dir / Path("prefect_cache"),
        persist_result=False,
    ).serve(
        name="needle-pipeline",
        parameters={"cfg": cfg.to_kwargs()},
        triggers=[
            DeploymentEventTrigger(
                name="observation-staged-trigger",
                enabled=True,
                expect={OBSERVATION_STAGED_EVENT},
                match={"prefect.resource.id": COURIER_RESOURCE_ID},
                parameters={"work_dir": "{{ event.payload.staged_dir }}"},
            )
        ],
    )


@cli.command()
@click.option(
    "-c",
    "--cfg",
    "cfg_path",
    default=Path.home() / ".needle.yaml",
    type=click.Path(exists=True, path_type=Path),
    help="Path to the config YAML file.",
)
@click.option("-p", "--pretty-print", "pretty_print", is_flag=True, help="Whether to pretty print the config")
def validate(cfg_path, pretty_print):
    """Validates a needle pipeline YAML config file. Optionally pretty-prints to stdout."""
    valid = NeedleConfig.validate(source=cfg_path)
    if not valid:
        return
    if pretty_print:
        click.echo()
        NeedleConfig.load(path=cfg_path).pretty_print()


module = LazyGroup(
    name="module",
    help="Run an individual pipeline module directly, bypassing the full pipeline.",
    lazy_subcommands={
        "calibrate": ("needle.modules.calibrate:entrypoint", "Determine and apply calibration solutions"),
        "casadata": ("needle.modules.casa_data:entrypoint", "Update the casa run data"),
        "clean": ("needle.modules.clean:entrypoint", "Run WSClean on a measurement set"),
        "convert": ("needle.modules.convert:entrypoint", "Convert a .uvfits/.mir file to a measurement set"),
        "diagnostics": ("needle.modules.diagnostics:entrypoint", "Run diagnostics on a measurement set"),
        "flag": ("needle.modules.flag:entrypoint", "Flag a measurement set"),
        "inspect": ("needle.modules.inspect:entrypoint", "Inspect a measurement set"),
        "mask": ("needle.modules.mask:entrypoint", "Find sources in a .fits image"),
        "source-find": ("needle.modules.source_find:entrypoint", "Find sources in a .fits image"),
    },
)
cli.add_command(module)


if __name__ == "__main__":
    cli()
