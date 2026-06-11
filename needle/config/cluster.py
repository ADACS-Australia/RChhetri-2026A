import logging
from pathlib import Path
from typing import Literal, Optional
import yaml

from prefect_dask import DaskTaskRunner
from pydantic import ValidationError

from needle.config.base import NeedleModel
from needle.config.container import ContainerConfig
from needle.lib.cluster_slurm import SifSLURMCluster
from needle.lib.cluster_local import SifLocalCluster

logger = logging.getLogger(__name__)


class ClusterScalingConfig(NeedleModel):
    """Worker scaling configuration"""

    min_workers: int = 1
    "Minimum number of workers"
    max_workers: int = 1
    "Maximum number of workers"
    dashboard_port: int = 8787
    "Port for the Dask dashboard"


class SlurmConfig(NeedleModel):
    """SLURM-specific configuration passed directly to dask-jobqueue"""

    account: Optional[str] = None
    "SLURM account to charge"
    queue: Optional[str] = None
    "SLURM queue/partition to submit to"
    cores: Optional[int] = None
    "Number of cores per job"
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
    scaling: ClusterScalingConfig = ClusterScalingConfig()
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

    def to_task_runner(self, extra_binds: Optional[list[str]]) -> DaskTaskRunner:
        """Creates the task runner object

        :param extra_binds: Any additional path bindings to add to the container execution command if using a container.
            Will be ignored if not using a container.
        :return: The DaskTaskRunner object
        """

        if extra_binds and self.container:
            logging.info(f"Adding additional binds to task runner container: {extra_binds}")
            self.container.binds = (self.container.binds or []) + extra_binds

        cluster_kwargs = {
            "container_cfg": self.container,
            "scheduler_options": {"dashboard_address": f":{self.scaling.dashboard_port}"},
        }
        if self.type == "slurm" and self.slurm:
            cluster_kwargs.update(self.slurm.model_dump(exclude_none=True))
        elif self.type == "local" and self.local:
            cluster_kwargs.update(self.local.model_dump(exclude_none=True))

        cluster_class = SifLocalCluster if self.type == "local" else SifSLURMCluster
        return DaskTaskRunner(
            cluster_class=cluster_class,
            cluster_kwargs=cluster_kwargs,
            adapt_kwargs={"minimum": self.scaling.min_workers, "maximum": self.scaling.max_workers},
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
