import logging
from pathlib import Path
import re
from typing import Literal, Optional
import yaml

from prefect_dask import DaskTaskRunner
from pydantic import ValidationError, field_validator

from needle.config.base import NeedleModel
from needle.config.container import ContainerConfig
from needle.lib.cluster_slurm import SifSLURMCluster
from needle.lib.cluster_local import SifLocalCluster

logger = logging.getLogger(__name__)


class ScalingConfig(NeedleModel):
    """Worker scaling configuration"""

    min_workers: int = 0
    "Minimum number of workers"
    max_workers: int = 1
    "Maximum number of workers"
    interval: str = "5s"
    "The interval between checking for scaling updates e.g. '10s', '1m'"
    wait_count: int = 120
    "The number of scaling intervals to wait for a worker before cancelling"
    dashboard_port: int = 8787
    "Port for the Dask dashboard"

    @field_validator("interval")
    @classmethod
    def _valid_interval(cls, v: str) -> str:
        if not re.match(r"^\d+(\.\d+)?(ms|s|m|h)$", v):
            raise ValueError(
                f"Invalid interval '{v}'. Must be a number followed by a unit: ms, s, m, or h. E.g. '10s', '1m'."
            )
        return v

    @property
    def scheduler_options(self) -> dict:
        """Returns the scheduler_options dictionary for DaskTaskRunner"""
        return {"dashboard_address": f":{self.dashboard_port}"}

    @property
    def adapt_kwargs(self) -> dict:
        """Returns the adapt_kwargs dictionary for DaskTaskRunner"""
        return {
            "minimum": self.min_workers,
            "maximum": self.max_workers,
            "wait_count": self.wait_count,
            "interval": self.interval,
        }


class SlurmConfig(NeedleModel):
    """SLURM-specific configuration passed directly to dask-jobqueue"""

    account: Optional[str] = None
    "SLURM account to charge"
    queue: Optional[str] = None
    "SLURM queue/partition to submit to"
    cores: Optional[int] = None
    "Number of cores per job. Must be 1 as CASA is not threadsafe."
    memory: Optional[str] = None
    "Memory per job e.g. '64GB'"
    processes: Optional[int] = None
    "Number of Python processes per job"
    walltime: Optional[str] = None
    "Maximum walltime per job e.g. '02:00:00'"
    local_directory: Optional[str] = None
    "Local directory for workers to use for scratch space"
    log_directory: Optional[str] = None
    "Directory to write worker logs to"
    job_script_prologue: Optional[list[str]] = None
    "Lines to prepend to the job script e.g. module loads"
    job_extra_directives: Optional[list[str]] = None
    "Extra SLURM directives"


class LocalConfig(NeedleModel):
    """Local cluster configuration"""

    cores: Optional[int] = None
    "Number of cores per worker"
    memory: Optional[str] = None
    "Memory per worker e.g. '4GB'"


class ClusterConfig(NeedleModel):
    """Top-level cluster configuration"""

    type: Literal["local", "slurm"]
    "Cluster type - 'local' for local container workers, 'slurm' for SLURM cluster"
    scaling: ScalingConfig = ScalingConfig()
    "Worker scaling configuration"
    container: Optional[ContainerConfig] = None
    "Container configuration for worker execution"
    slurm: Optional[SlurmConfig] = None
    "SLURM-specific configuration (required when type is 'slurm')"
    local: Optional[LocalConfig] = None
    "Local cluster configuration (used when type is 'local')"

    @classmethod
    def get_config(cls) -> "ClusterConfig":
        cfg_path = Path.home() / ".needle_cluster.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(f"Expected file {cfg_path} does not exist")
        return cls.load(cfg_path)

    def to_task_runner(self, extra_binds: Optional[list[str]] = None) -> DaskTaskRunner:
        """Creates the task runner object

        :param extra_binds: Any additional path bindings to add to the container execution command if using a container.
            Will be ignored if not using a container.
        :return: The DaskTaskRunner object
        """

        if extra_binds and self.container:
            logger.info(f"Adding additional binds to task runner container: {extra_binds}")
            self.container.binds = (self.container.binds or []) + extra_binds

        cluster_kwargs = {"container_cfg": self.container, "scheduler_options": self.scaling.scheduler_options}
        if self.type == "slurm" and self.slurm:
            cluster_kwargs.update(self.slurm.model_dump(exclude_none=True))
        elif self.type == "local" and self.local:
            cluster_kwargs.update(self.local.model_dump(exclude_none=True))

        cluster_class = SifLocalCluster if self.type == "local" else SifSLURMCluster
        return DaskTaskRunner(
            cluster_class=cluster_class, cluster_kwargs=cluster_kwargs, adapt_kwargs=self.scaling.adapt_kwargs
        )

    @classmethod
    def load(cls, source: Path | str | dict) -> "ClusterConfig":
        if isinstance(source, dict):
            data = source
        else:
            with open(Path(source)) as f:
                data = yaml.safe_load(f)
        try:
            return cls.model_validate(data)
        except ValidationError as e:
            missing = [err["loc"][0] for err in e.errors() if err["type"] == "missing"]
            if missing:
                fields = ", ".join(f"'{f}'" for f in missing)
                raise ValueError(f"Cluster config is missing required section(s): {fields}") from e
            raise

    @classmethod
    def validate(cls, source: Path | str | dict, quiet: bool = False) -> bool:
        def emit(msg: str):
            if not quiet:
                print(msg)

        raw = source if isinstance(source, dict) else yaml.safe_load(Path(source).read_text())
        emit("\n--- Cluster Config Validation ---")
        errors = {}
        validated = {}

        for f, field_info in cls.model_fields.items():
            section_type = field_info.annotation
            section_data = raw.get(f)
            if not (isinstance(section_type, type) and issubclass(section_type, NeedleModel)):
                continue
            try:
                validated[f] = section_type.model_validate(section_data or {})
            except ValidationError as e:
                errors[f] = e

        for f in cls.model_fields:
            if f in validated:
                emit(f"  ✓ {f}: {type(validated[f]).__name__}")
            elif f in errors:
                emit(f"  ✗ {f}: FAILED")

        if errors:
            emit(f"\n{len(errors)} section(s) failed validation:\n")
            for f, exc in errors.items():
                emit(f"[{f}]")
                for err in exc.errors():
                    loc = " -> ".join(str(i) for i in err["loc"])
                    prefix = f"{loc}: " if loc else ""
                    emit(f"  {prefix}{err['msg']}")
                emit("")
        else:
            emit("\nAll sections validated OK.")

        return not errors
