import concurrent.futures
import subprocess
import uuid
import cloudpickle
import base64
from typing import Any, Iterable, Optional

from prefect.task_runners import TaskRunner
from prefect.futures import PrefectConcurrentFuture
from needle.config.container import ContainerConfig

# Script that runs inside the container
_INNER_SCRIPT = """
import sys, base64, cloudpickle
payload = base64.b64decode(sys.stdin.buffer.read())
fn, args, kwargs = cloudpickle.loads(payload)
result = fn(*args, **kwargs)
sys.stdout.buffer.write(base64.b64encode(cloudpickle.dumps(result)))
"""


class SifTaskRunner(TaskRunner):
    def __init__(self, cfg: ContainerConfig):
        self.cfg = cfg
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        super().__init__()

    def duplicate(self):
        return SifTaskRunner(self.cfg)

    def _run_in_container(self, task_fn, parameters: dict) -> Any:
        # Separate args Prefect injects from actual task parameters
        args = []
        kwargs = {k: v for k, v in parameters.items()}

        payload = base64.b64encode(cloudpickle.dumps((task_fn, args, kwargs)))

        cmd = self.cfg.to_args() + ["python", "-c", _INNER_SCRIPT]
        result = subprocess.run(
            cmd,
            input=payload,
            capture_output=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Container task failed:\n{result.stderr.decode().strip()}")

        return cloudpickle.loads(base64.b64decode(result.stdout))

    def submit(
        self,
        task: Any,
        parameters: dict,
        wait_for: Optional[Iterable[PrefectConcurrentFuture]] = None,
        dependencies: Optional[dict] = None,
    ) -> PrefectConcurrentFuture:
        if not self._started:
            raise RuntimeError("The task runner must be started before submitting work.")

        task_run_id = uuid.uuid4()
        future = self._executor.submit(self._run_in_container, task.fn, parameters)
        return PrefectConcurrentFuture(task_run_id=task_run_id, wrapped_future=future)

    def __enter__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._started = True
        return self

    def __exit__(self, *args):
        self._executor.shutdown(wait=True)
        self._started = False
