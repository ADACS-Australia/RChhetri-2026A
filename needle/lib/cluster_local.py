from typing import Optional

from dask_jobqueue.local import LocalJob, LocalCluster

from needle.config.container import ContainerConfig


class SifLocalJob(LocalJob):
    def __init__(self, *args, container_cfg: Optional[ContainerConfig] = None, **kwargs):
        if container_cfg:
            kwargs["python"] = f"{' '.join(container_cfg.to_args())} python"
        super().__init__(*args, **kwargs)


class SifLocalCluster(LocalCluster):
    job_cls = SifLocalJob
