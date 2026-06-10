from pathlib import Path
from typing import Literal, Optional
import yaml

from prefect_dask import DaskTaskRunner
from pydantic import ValidationError

from needle.config.base import NeedleModel
from needle.config.container import ContainerConfig
from needle.lib.cluster_slurm import SifSLURMCluster
from needle.lib.cluster_local import SifLocalCluster


class ClusterResourceConfig(NeedleModel):
    """Resource allocation per worker"""

    cores: int = 1
    "Number of cores per worker"
    memory: str = "4GB"
    "Memory per worker e.g. '4GB'"


class ClusterScalingConfig(NeedleModel):
    """Worker scaling configuration"""

    min_workers: int = 1
    "Minimum number of workers"
    max_workers: int = 1
    "Maximum number of workers"
    dashboard_port: int = 8787
    "Port for the Dask dashboard"


class SlurmConfig(NeedleModel):
    """SLURM-specific configuration"""

    queue: Optional[str] = None
    "SLURM queue/partition to submit to"
    walltime: Optional[str] = None
    "Maximum walltime per job e.g. '01:00:00'"
    job_extra_directives: Optional[list[str]] = None
    "Extra SLURM directives e.g. ['--mem-per-cpu=2GB']"


class ClusterConfig(NeedleModel):
    """Top-level cluster configuration"""

    type: Literal["local", "slurm"]
    "Cluster type - 'local' for local container workers, 'slurm' for SLURM cluster"
    resources: ClusterResourceConfig = ClusterResourceConfig()
    "Resource allocation per worker"
    scaling: ClusterScalingConfig = ClusterScalingConfig()
    "Worker scaling configuration"
    container: Optional[ContainerConfig] = None
    "Container configuration for worker execution"
    slurm: Optional[SlurmConfig] = None
    "SLURM-specific configuration (required when type is 'slurm')"

    @classmethod
    def get_config(cls) -> "ClusterConfig":
        cfg_path = Path.home() / ".needle_cluster.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(f"Expected file {cfg_path} does not exist")
        return cls.load(cfg_path)

    def to_task_runner(self) -> DaskTaskRunner:
        cluster_kwargs = {
            **self.resources.model_dump(),
            "container_cfg": self.container,
            "scheduler_options": {"dashboard_address": f":{self.scaling.dashboard_port}"},
        }
        if self.type == "slurm" and self.slurm:
            cluster_kwargs.update(self.slurm.model_dump(exclude_none=True))

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
        """Validates each section independently then the whole config"""

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
