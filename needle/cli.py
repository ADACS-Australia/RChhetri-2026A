"""
The CLI for needle.
Many of the imports are lazy-loaded so that the CLI is more responsive.
"""

import logging
import importlib
import os
import sys
from pathlib import Path

import click

from needle.config.cluster import ClusterConfig
from needle.config.pipeline import NeedleConfig
from needle.deployment import deploy
from needle.lib.constants import WORKFLOW_UMASK
from needle.lib.dask import build_dask_client
from needle.lib.logging import setup_logging
from needle.lib.workers import WORKER_POOLS

logger = logging.getLogger("needle-cli")

REQUIRED_ENV_VARS = (
    "NEEDLE_CONFIG",
    "NEEDLE_CLUSTER_CONFIG",
    "PREFECT_API_URL",
    "PREFECT_API_AUTH_STRING",
)


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


def _log_level_options(f):
    f = click.option(
        "--log-level",
        "--log_level",
        "log_level",
        type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
        default="INFO",
        help="Logging level",
    )(f)
    return f


def _assert_required_env():
    all_available = True
    for v in REQUIRED_ENV_VARS:
        if not os.environ.get(v):
            all_available = False
            logger.error(f"Missing environment variable: '{v}'")
    return all_available


@click.group(help="Needle pipeline CLI.", context_settings={"max_content_width": 120})
def cli():
    # Set the umask here to allow group access to all files and folders written by the pipeline
    os.umask(WORKFLOW_UMASK)


@cli.command()
@click.argument("work_dir", required=True, type=click.Path(exists=True, file_okay=False, path_type=Path))
def run(work_dir):
    """Runs the pipeline on a directory (work_dir) now."""
    from needle.flows.pipeline import needle_pipeline
    from prefect.task_runners import ThreadPoolTaskRunner

    _assert_required_env()
    cfg = NeedleConfig.get_config()
    cluster_cfg = ClusterConfig.get_cluster_config()
    needle_pipeline.with_options(task_runner=ThreadPoolTaskRunner(max_workers=cfg.flow.max_threads))(
        cfg=cfg, cluster_cfg=cluster_cfg, work_dir=str(work_dir)
    )


@cli.command()
@_log_level_options
@click.argument("roles", nargs=-1, required=True, type=click.Choice(sorted(WORKER_POOLS)))
def start_workers(log_level, roles):
    """Start workers for the given roles.

    Roles map to work pools, so each can be run on a separate node:
    `needle start-workers download` for download-observation-ozstar, and
    `needle start-workers pipeline` for process-observation-ozstar.
    """
    import asyncio

    from prefect.utilities.processutils import setup_signal_handlers_worker
    from prefect.workers.process import ProcessWorker

    _assert_required_env()
    _setup_cli_logging(log_level)
    setup_logging(log_level)

    setup_signal_handlers_worker(os.getpid(), "the process workers", logger.info)

    # Deduplicate while preserving the order given on the command line.
    selected = list(dict.fromkeys(roles))

    async def _run_workers() -> None:
        workers = []
        for name in selected:
            pool = WORKER_POOLS[name]
            logger.info(f"Starting {name} worker on work pool '{pool}'")
            workers.append(ProcessWorker(name=f"needle-{name}", work_pool_name=pool, create_pool_if_not_found=True))
        await asyncio.gather(*(worker.start(printer=logger.info) for worker in workers))

    try:
        asyncio.run(_run_workers())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down workers")


@cli.command(name="check-cluster")
@_log_level_options
@click.option(
    "--mem",
    "memory",
    default="2GB",
    show_default=True,
    type=str,
    help="The amount of memory to allocate to the test worker",
)
@click.option(
    "--cores",
    "cores",
    default=1,
    show_default=True,
    type=int,
    help="The number of cores to allocate to the test worker",
)
@click.option(
    "--procs",
    "processes",
    default=1,
    show_default=True,
    type=int,
    help="The number of processes to allocate to the test worker",
)
@click.option(
    "--time",
    "walltime",
    default="00:05:00",
    show_default=True,
    type=str,
    help="The amount of walltime to allow the worker to run for",
)
def check_cluster(log_level, memory, cores, processes, walltime):
    """Runs a diagnostic flow that spins up the configured cluster and confirms a job can execute on it.
    Exits non-zero on failure and provides logging information.
    """
    from needle.flows.check_cluster import check_cluster_flow

    _setup_cli_logging(log_level)
    setup_logging(log_level)

    cluster_cfg = ClusterConfig.get_config()
    if cluster_cfg.type != "slurm":
        logger.error(f"Cluster config must be of type 'slurm' to run test. Found type '{cluster_cfg.type}'")
        raise SystemExit(1)
    # Lower the settings for the test
    cluster_cfg.slurm.memory = memory
    cluster_cfg.slurm.cores = cores
    cluster_cfg.slurm.processes = processes
    cluster_cfg.slurm.walltime = walltime
    with build_dask_client(cluster_cfg) as (client, cluster):
        try:
            check_cluster_flow(client, cluster, cluster_cfg)
        except Exception as exc:
            logger.error(f"Cluster test failed: {exc}")
            raise SystemExit(1)
        raise SystemExit(0)


@cli.command()
@click.argument(
    "cfg_path", default=Path.home() / ".needle.yaml", required=False, type=click.Path(exists=True, path_type=Path)
)
@click.option("-p", "--pretty-print", "pretty_print", is_flag=True, help="Whether to pretty print the config")
def validate(cfg_path, pretty_print):
    """Validates a needle pipeline YAML config file. Optionally pretty-prints to stdout."""
    valid = NeedleConfig.validate(source=cfg_path)
    if not valid:
        return
    if pretty_print:
        NeedleConfig.load(path=cfg_path).pretty_print().show()


@cli.command()
@click.option(
    "-c",
    "--cfg",
    "cfg_path",
    default=Path.home() / ".needle.yaml",
    type=click.Path(exists=True, path_type=Path),
    help="Path to the config YAML file.",
)
@click.argument("section", default=None, required=False)
def display(section, cfg_path):
    """Loads and renders either the entire config state, or one section (e.g. clean, flag) of it.
    Use dot annotation for sub-sub sections (e.g. flag.quack)."""
    cfg = NeedleConfig.load(cfg_path)
    cfg.show(section)


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
        "imstack": ("needle.modules.imstack:entrypoint", "Build an HDF5 image stack from WSClean FITS images"),
        "inspect": ("needle.modules.inspect:entrypoint", "Inspect a measurement set"),
        "mask": ("needle.modules.mask:entrypoint", "Find sources in a .fits image"),
        "source-find": ("needle.modules.source_find:entrypoint", "Find sources in a .fits image"),
    },
)
cli.add_command(module)
cli.add_command(deploy)


if __name__ == "__main__":
    cli()
