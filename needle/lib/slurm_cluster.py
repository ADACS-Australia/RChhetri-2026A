from pathlib import Path
from typing import Optional

from dask_jobqueue.slurm import SLURMJob, SLURMCluster

from needle.config.container import ContainerConfig


class SifSLURMJob(SLURMJob):
    def __init__(
        self,
        *args,
        container_cfg: Optional[ContainerConfig] = None,
        **kwargs,
    ):
        self.container_cfg = container_cfg
        if container_cfg:
            # prepend the container executable command
            kwargs["python"] = f"{' '.join(container_cfg.to_args())} python"
        super().__init__(*args, **kwargs)


class SifSLURMCluster(SLURMCluster):
    job_cls = SifSLURMJob
