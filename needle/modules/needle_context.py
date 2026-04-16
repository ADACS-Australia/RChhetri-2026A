import logging
import subprocess

from pydantic import model_validator

from needle.config.base import NeedleModel, ContainerConfig

logger = logging.getLogger(__name__)


class NeedleContext(NeedleModel):
    """The context to pass to a module. Contains everything needed to execute a piece of work"""

    def execute(self) -> None:
        """The function that does the work"""
        raise NotImplementedError(f"{type(self).__name__} must implement execute()")


class SubprocessExecContext(NeedleContext):
    """The context to pass to a module. Contains everything needed to execute a piece of work

    This class caters to the use-case of running one or more subprocesses in serial.
    The runtime may optionally be set to an ContainerConfig class"""

    runtime: ContainerConfig | None = None
    "Runtime information. An optional ContainerConfig. None is interpreted as the local runtime."

    @property
    def cmd(self) -> list[list[str]]:
        """The command or commands to execute. Each inner list is a separate command.
        Should be overridden by child classes so that execute() does something."""
        raise NotImplementedError(f"{type(self).__name__} must implement cmd")

    @property
    def _resolved_cmds(self) -> list[list[str]]:
        """Prepends runtime args to each command if runtime is set"""
        if self.runtime:
            # It is important not to prepend to an empty command
            return [self.runtime.to_args() + c for c in self.cmd if c]
        return self.cmd

    @model_validator(mode="after")
    def _validate_cmd(self) -> "NeedleContext":
        """Ensures that cmd is a list of lists.
        The lists contained are allowed to be empty as they will be no-ops in execute."""
        cmd = self.cmd
        if not isinstance(cmd, list) or not all(isinstance(c, list) for c in cmd):
            raise ValueError("cmd must be a list of lists")
        return self

    def log_cmd(self) -> None:
        """Logs each command exactly as it will be run, including container
        wrapping if a runtime is set"""
        for c in self._resolved_cmds:
            logger.debug(" ".join(str(x) for x in c))

    def execute(self) -> list[subprocess.CompletedProcess]:
        """Executes the commands using the runtime provided.
        Returns a list of CompletedProcess objects for each command run.
        """
        results = []
        for c in self._resolved_cmds:
            if not len(c):
                logger.info("Found empty command, skipping execution")
                continue
            try:
                result = subprocess.run(c, capture_output=True, text=True, check=True)
                results.append(result)
            except subprocess.CalledProcessError as e:
                logger.error(f"Command failed with exit code {e.returncode}")
                logger.error(f"stdout: {e.stdout}")
                logger.error(f"stderr: {e.stderr}")
                raise
        return results
