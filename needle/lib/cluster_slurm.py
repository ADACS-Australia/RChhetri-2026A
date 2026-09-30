from typing import Optional

import dask
from dask_jobqueue.slurm import SLURMJob, SLURMCluster

from needle.config.container import ContainerConfig
from needle.lib.constants import WORKFLOW_UMASK


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
        #
        # Set a group-friendly umask in the job shell before the worker starts.
        # Keep any prologue passed in directly or set in the dask-jobqueue config.
        prologue = kwargs.pop("job_script_prologue", None)
        if prologue is None:
            prologue = dask.config.get(f"jobqueue.{self.config_name}.job-script-prologue", default=None) or []
        kwargs["job_script_prologue"] = [f"umask {WORKFLOW_UMASK:03o}", *prologue]

        super().__init__(*args, **kwargs)


class SifSLURMCluster(SLURMCluster):
    job_cls = SifSLURMJob
