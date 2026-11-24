"""Shared remote and Prefect helpers for needle deployments.

This module contains the reusable pieces used by the ``needle deploy`` CLI:
Prefect worker queries, single-connection SSH wrappers, remote marker parsing,
probe/teardown/bootstrap scripts, and status rendering.  It does not define a
CLI entrypoint itself.
"""

from __future__ import annotations

import asyncio
import base64
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping

import click

from needle.lib.workers import ROLE_ORDER, WORKER_ROLES, WorkerRole

MARKER = "@@NEEDLE@@"
PREFECT_ENV_VARS = ("PREFECT_API_URL", "PREFECT_API_AUTH_STRING")

NODE_LABELS = {"login": "login node", "data_mover": "data mover"}
SCREEN_NODE_NAMES = ("login", "data_mover")


@dataclass
class PrefectState:
    """The Prefect-visible state of one production worker."""

    role: str
    pool: str
    worker: str
    exists: bool = False
    status: str | None = None
    last_heartbeat: datetime | None = None
    worker_id: str | None = None
    error: str | None = None

    @property
    def online(self) -> bool:
        return self.status == "ONLINE" and self.error is None


@dataclass
class ScreenSession:
    """A session as reported by ``screen -list``."""

    name: str
    pid: int
    state: str

    @property
    def running(self) -> bool:
        return self.state.startswith(("Attached", "Detached"))


@dataclass
class ScreenState:
    """The screen-visible state of one production worker."""

    role: str
    name: str
    node: str
    host: str | None = None
    exists: bool = False
    state: str | None = None
    pid: int | None = None
    error: str | None = None

    @property
    def running(self) -> bool:
        return self.exists and self.state is not None and self.state.startswith(("Attached", "Detached"))


@dataclass
class RemoteNode:
    name: str
    host: str | None = None
    exit_code: int | None = None
    sessions: dict[str, ScreenSession] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class DeployAction:
    node: str
    role: str
    action: str


@dataclass
class CheckResult:
    node: str
    name: str
    status: str


@dataclass
class RemoteOutput:
    nodes: dict[str, RemoteNode]
    actions: list[DeployAction]
    errors: list[str] = field(default_factory=list)
    complete: bool = False
    checks: list[CheckResult] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_age(value: datetime | None) -> str:
    if value is None:
        return "never"
    value = _as_utc(value)
    seconds = max(0, int((datetime.now(timezone.utc) - value).total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s ago"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m ago"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours:02d}h ago"


def _indent(text: str, spaces: int = 2) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line if line else line for line in text.splitlines())


def _worker_sort_key(worker) -> tuple[int, float]:
    heartbeat = getattr(worker, "last_heartbeat_time", None)
    timestamp = heartbeat.timestamp() if heartbeat is not None else 0.0
    online = getattr(getattr(worker, "status", None), "value", "") == "ONLINE"
    return (1 if online else 0, timestamp)


## Prefect API ##


def _query_prefect() -> dict[str, PrefectState]:
    """Read worker records for both production work pools.

    A missing work pool or a failed API request is recorded on that role rather than aborting the whole query, so
    ``status`` can still report screen state.
    """
    from prefect.client.orchestration import get_client
    from prefect.exceptions import ObjectNotFound

    async def _read() -> dict[str, PrefectState]:
        states = {
            name: PrefectState(
                role=name,
                pool=WORKER_ROLES[name].pool,
                worker=WORKER_ROLES[name].worker,
            )
            for name in ROLE_ORDER
        }
        async with get_client() as client:
            for name in ROLE_ORDER:
                spec = WORKER_ROLES[name]
                state = states[name]
                try:
                    workers = await client.read_workers_for_work_pool(spec.pool)
                except ObjectNotFound:
                    state.error = f"work pool {spec.pool!r} not found"
                    continue
                except Exception as exc:
                    state.error = f"{type(exc).__name__}: {exc}"
                    continue

                matches = [worker for worker in workers if worker.name == spec.worker]
                if not matches:
                    continue

                chosen = max(matches, key=_worker_sort_key)
                state.exists = True
                state.status = getattr(chosen.status, "value", str(chosen.status))
                state.last_heartbeat = _as_utc(getattr(chosen, "last_heartbeat_time", None))
                state.worker_id = str(chosen.id)
        return states

    return asyncio.run(_read())


def _ensure_work_pools() -> list[str]:
    """Create any missing production work pools, returning the names created."""
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate
    from prefect.exceptions import ObjectNotFound

    async def _run() -> list[str]:
        created: list[str] = []
        async with get_client() as client:
            for name in ROLE_ORDER:
                pool = WORKER_ROLES[name].pool
                try:
                    await client.read_work_pool(pool)
                except ObjectNotFound:
                    await client.create_work_pool(WorkPoolCreate(name=pool, type="process"))
                    created.append(pool)
        return created

    return asyncio.run(_run())


def _states_with_error(message: str) -> dict[str, PrefectState]:
    return {
        name: PrefectState(
            role=name,
            pool=WORKER_ROLES[name].pool,
            worker=WORKER_ROLES[name].worker,
            error=message,
        )
        for name in ROLE_ORDER
    }


def _ready(state: PrefectState, not_before: datetime | None) -> bool:
    if not state.online:
        return False
    if not_before is None:
        return True
    heartbeat = state.last_heartbeat
    return heartbeat is not None and heartbeat >= not_before


def _wait_for_online(
    *,
    timeout: float,
    interval: float,
    not_before: Mapping[str, datetime | None] | None = None,
) -> dict[str, PrefectState]:
    """Poll the API until both workers are online, or the timeout expires."""
    deadline = time.monotonic() + max(0.0, timeout)
    not_before = not_before or {}
    while True:
        try:
            states = _query_prefect()
        except Exception as exc:
            states = _states_with_error(f"{type(exc).__name__}: {exc}")

        if all(_ready(states[name], not_before.get(name)) for name in ROLE_ORDER):
            return states

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return states
        time.sleep(min(max(interval, 0.1), remaining))


_ENV_LINE = re.compile(r"^(?:export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)$")


def _unquote_env_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        quote = value[0]
        value = value[1:-1]
        if quote == '"':
            value = value.replace('\\"', '"').replace("\\n", "\n")
        return value
    # Only strip an inline comment when it is separated by whitespace.
    return re.sub(r"\s+#.*$", "", value).rstrip()


def _load_env_file(path: Path, environ: MutableMapping[str, str] | None = None) -> list[str]:
    """Load simple ``KEY=value``/``export KEY=value`` lines without overriding.

    Returns the names of variables that were actually set. This intentionally avoids a new dependency just to read the
    project's existing ``.env`` file.
    """
    target = os.environ if environ is None else environ
    loaded: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        if match is None:
            continue
        key = match.group("key")
        if target.get(key):
            continue
        target[key] = _unquote_env_value(match.group("value"))
        loaded.append(key)
    return loaded


def _ensure_prefect_env(env_file: Path | None) -> None:
    if all(os.environ.get(name) for name in PREFECT_ENV_VARS):
        return

    candidates: list[Path] = []
    if env_file is not None:
        candidates.append(env_file)
    else:
        candidates.extend(
            [
                Path.cwd() / ".env",
                Path(__file__).resolve().parent.parent / ".env",
            ]
        )

    for candidate in candidates:
        if candidate.is_file():
            _load_env_file(candidate)
            if all(os.environ.get(name) for name in PREFECT_ENV_VARS):
                return

    missing = ", ".join(name for name in PREFECT_ENV_VARS if not os.environ.get(name))
    checked = ", ".join(str(path) for path in candidates)
    raise click.ClickException(
        f"Missing Prefect environment variable(s): {missing}. "
        f"Set them or pass --env-file (checked: {checked or 'no paths'})."
    )


## SSH / remote scripts ##


def _run_ssh(host: str, script: str, timeout: float) -> subprocess.CompletedProcess[str]:
    command = [
        "ssh",
        "-o",
        "ConnectTimeout=20",
        "-o",
        "ServerAliveInterval=30",
        host,
        "bash -s",
    ]
    try:
        return subprocess.run(
            command,
            input=script,
            text=True,
            capture_output=True,
            timeout=None if timeout <= 0 else timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise click.ClickException("The 'ssh' executable was not found.") from exc
    except subprocess.TimeoutExpired as exc:
        raise click.ClickException(f"SSH command timed out after {timeout:g}s.") from exc


def _as_function(name: str, body: str) -> str:
    return f"{name}() {{\n{_indent(body)}\n}}\n{name}\n"


def _wrap_nodes(login_body: str, data_mover_body: str, data_mover: str) -> str:
    """Compose one remote script that visits both worker nodes.

    The data-mover script is base64 encoded so nesting it inside the login-node script does not require fragile
    multi-level shell quoting.
    The login node is expected to reach the data mover without a second interactive 2FA prompt.
    """
    data_mover_script = _as_function("data_mover_node", data_mover_body)
    encoded = base64.b64encode(data_mover_script.encode("utf-8")).decode("ascii")
    return "\n".join(
        [
            "set -o pipefail",
            "",
            "login_node() {",
            _indent(login_body),
            "}",
            "",
            f'echo "{MARKER}NODE_BEGIN:login@@"',
            "login_node",
            "rc=$?",
            f'echo "{MARKER}NODE_END:login:${{rc}}@@"',
            "",
            f'echo "{MARKER}NODE_BEGIN:data_mover@@"',
            (
                f"printf '%s' {shlex.quote(encoded)} | base64 --decode | "
                f"ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 "
                f"{shlex.quote(data_mover)} bash -s"
            ),
            "rc=$?",
            f'echo "{MARKER}NODE_END:data_mover:${{rc}}@@"',
            f'echo "{MARKER}END@@"',
            "exit 0",
            "",
        ]
    )


def _probe_body(node: str) -> str:
    return f"""\
echo "{MARKER}HOST:{node}:$(hostname 2>/dev/null || uname -n)@@"
if command -v screen >/dev/null 2>&1; then
  echo "{MARKER}SCREEN_BEGIN:{node}@@"
  screen -list 2>&1 || true
  echo "{MARKER}SCREEN_END:{node}@@"
else
  echo "{MARKER}SCREEN_ERROR:{node}:screen not found@@"
fi
"""


def _build_probe_script(data_mover: str) -> str:
    return _wrap_nodes(_probe_body("login"), _probe_body("data_mover"), data_mover)


def _preflight_lines(
    node: str,
    *,
    install: bool,
    strict: bool,
    workdir: str,
    env_file: str,
    require_screen: bool = True,
) -> list[str]:
    """Check node prerequisites, optionally installing user-space dependencies.

    ``uv`` and the project dependencies are installable without root.  System
    packages such as ``screen`` and ``git`` are checked and, where possible, a
    module of the same name is loaded; they are never installed with sudo.
    """
    path = lambda value: f'"{value}"'  # noqa: E731 - allow remote $HOME/~ expansion
    fail = ["  return 2"] if strict else []
    lines: list[str] = [
        f'echo "{MARKER}HOST:{node}:$(hostname 2>/dev/null || uname -n)@@"',
        'export PATH="$HOME/.local/bin:$PATH"',
        "",
    ]
    if require_screen:
        lines.extend(
            [
                "# screen",
                "if ! command -v screen >/dev/null 2>&1; then",
                "  if command -v module >/dev/null 2>&1; then",
                "    module load screen >/dev/null 2>&1 || true",
                "  fi",
                "fi",
                "if command -v screen >/dev/null 2>&1; then",
                f'  echo "{MARKER}CHECK:{node}:screen:OK@@"',
                "else",
                f'  echo "{MARKER}CHECK:{node}:screen:MISSING@@"',
                f'  echo "{MARKER}ERROR:{node}:screen is required but was not found; load a module or ask system support@@"',
                *fail,
                "fi",
                "",
            ]
        )
    lines.extend(
        [
            "# git is needed by the Prefect deployment pull step, not by the worker itself",
            "if ! command -v git >/dev/null 2>&1; then",
            "  if command -v module >/dev/null 2>&1; then",
            "    module load git >/dev/null 2>&1 || true",
            "  fi",
            "fi",
            "if command -v git >/dev/null 2>&1; then",
            f'  echo "{MARKER}CHECK:{node}:git:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:git:MISSING@@"',
            f'  echo "{MARKER}NOTE:{node}:git not found; Prefect deployment pull steps may fail@@"',
            "fi",
            "",
            "# uv",
        ]
    )
    if install:
        lines.extend(
            [
                "uv_state=OK",
                "if ! command -v uv >/dev/null 2>&1; then",
                "  if command -v curl >/dev/null 2>&1; then",
                "    if ! (curl -LsSf https://astral.sh/uv/install.sh | sh) 1>&2; then",
                f'      echo "{MARKER}ERROR:{node}:uv installation failed@@"',
                *fail,
                "    fi",
                "  elif command -v wget >/dev/null 2>&1; then",
                "    if ! (wget -qO- https://astral.sh/uv/install.sh | sh) 1>&2; then",
                f'      echo "{MARKER}ERROR:{node}:uv installation failed@@"',
                *fail,
                "    fi",
                "  else",
                f'    echo "{MARKER}ERROR:{node}:neither curl nor wget is available to install uv@@"',
                *fail,
                "  fi",
                '  export PATH="$HOME/.local/bin:$PATH"',
                "  uv_state=INSTALLED",
                "fi",
                "if command -v uv >/dev/null 2>&1; then",
                f'  echo "{MARKER}CHECK:{node}:uv:${{uv_state}}@@"',
                "else",
                f'  echo "{MARKER}CHECK:{node}:uv:MISSING@@"',
                f'  echo "{MARKER}ERROR:{node}:uv is not installed@@"',
                *fail,
                "fi",
            ]
        )
    else:
        lines.extend(
            [
                "if command -v uv >/dev/null 2>&1; then",
                f'  echo "{MARKER}CHECK:{node}:uv:OK@@"',
                "else",
                f'  echo "{MARKER}CHECK:{node}:uv:MISSING@@"',
                f'  echo "{MARKER}ERROR:{node}:uv is not installed@@"',
                *fail,
                "fi",
            ]
        )
    lines.extend(
        [
            "",
            "# work directory and Python project",
            f"if [ -d {path(workdir)} ]; then",
            f'  echo "{MARKER}CHECK:{node}:workdir:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:workdir:MISSING@@"',
            f'  echo "{MARKER}ERROR:{node}:work directory not found: {workdir}@@"',
            *fail,
            "fi",
            f"if [ -f {path(workdir + '/pyproject.toml')} ]; then",
            f'  echo "{MARKER}CHECK:{node}:pyproject:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:pyproject:MISSING@@"',
            f'  echo "{MARKER}ERROR:{node}:pyproject.toml not found under {workdir}@@"',
            *fail,
            "fi",
        ]
    )
    if install:
        lines.extend(
            [
                f"if [ -f {path(workdir + '/pyproject.toml')} ]; then",
                '  sync_args=""',
                f"  if [ -f {path(workdir + '/uv.lock')} ]; then sync_args=--frozen; fi",
                f"  if (cd {path(workdir)} && uv sync $sync_args) 1>&2; then",
                f'    echo "{MARKER}CHECK:{node}:project-sync:OK@@"',
                "  else",
                f'    echo "{MARKER}CHECK:{node}:project-sync:FAILED@@"',
                f'    echo "{MARKER}ERROR:{node}:uv sync failed in {workdir}@@"',
                *fail,
                "  fi",
                "fi",
            ]
        )
    else:
        lines.append(f'echo "{MARKER}CHECK:{node}:project-sync:SKIPPED@@"')
    lines.extend(
        [
            "",
            "# environment file and Prefect credentials",
            f"if [ -f {path(env_file)} ]; then",
            f'  echo "{MARKER}CHECK:{node}:env-file:OK@@"',
            "  set -a",
            f"  if . {path(env_file)}; then",
            "    env_loaded=1",
            "  else",
            "    env_loaded=0",
            "  fi",
            "  set +a",
            "else",
            f'  echo "{MARKER}CHECK:{node}:env-file:MISSING@@"',
            f'  echo "{MARKER}ERROR:{node}:environment file not found: {env_file}@@"',
            *fail,
            "  env_loaded=0",
            "fi",
            'if [ "$env_loaded" -eq 1 ]; then',
            f'  echo "{MARKER}CHECK:{node}:env-load:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:env-load:FAILED@@"',
            f'  echo "{MARKER}ERROR:{node}:could not load {env_file}@@"',
            *fail,
            "fi",
            'if [ "$env_loaded" -eq 1 ] && [ -n "${PREFECT_API_URL:-}" ] && [ -n "${PREFECT_API_AUTH_STRING:-}" ]; then',
            f'  echo "{MARKER}CHECK:{node}:prefect-env:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:prefect-env:MISSING@@"',
            f'  echo "{MARKER}ERROR:{node}:{env_file} does not export PREFECT_API_URL and PREFECT_API_AUTH_STRING@@"',
            *fail,
            "fi",
            "",
            "# config files are needed by flows, but not to start the worker",
            'if [ -n "${NEEDLE_CONFIG:-}" ] && [ -f "$NEEDLE_CONFIG" ]; then',
            f'  echo "{MARKER}CHECK:{node}:needle-config:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:needle-config:MISSING@@"',
            f'  echo "{MARKER}NOTE:{node}:NEEDLE_CONFIG is unset or missing: ${{NEEDLE_CONFIG:-<unset>}}@@"',
            "fi",
            'if [ -n "${NEEDLE_CLUSTER_CONFIG:-}" ] && [ -f "$NEEDLE_CLUSTER_CONFIG" ]; then',
            f'  echo "{MARKER}CHECK:{node}:cluster-config:OK@@"',
            "else",
            f'  echo "{MARKER}CHECK:{node}:cluster-config:MISSING@@"',
            f'  echo "{MARKER}NOTE:{node}:NEEDLE_CLUSTER_CONFIG is unset or missing: ${{NEEDLE_CLUSTER_CONFIG:-<unset>}}@@"',
            "fi",
            "",
        ]
    )
    return lines


def _build_bootstrap_parts(
    data_mover: str,
    *,
    install: bool,
    workdir: str,
    env_file: str,
    require_screen: bool = True,
) -> tuple[str, str, str]:
    login_body = "\n".join(
        _preflight_lines(
            "login",
            install=install,
            strict=False,
            workdir=workdir,
            env_file=env_file,
            require_screen=require_screen,
        )
    )
    data_mover_body = "\n".join(
        _preflight_lines(
            "data_mover",
            install=install,
            strict=False,
            workdir=workdir,
            env_file=env_file,
            require_screen=require_screen,
        )
    )
    return (
        login_body,
        data_mover_body,
        _wrap_nodes(login_body, data_mover_body, data_mover),
    )


def _teardown_body(node: str, roles: Iterable[WorkerRole]) -> str:
    quote = shlex.quote
    lines: list[str] = [
        f'echo "{MARKER}HOST:{node}:$(hostname 2>/dev/null || uname -n)@@"',
        "",
        "if ! command -v screen >/dev/null 2>&1; then",
        f'  echo "{MARKER}SCREEN_ERROR:{node}:screen not found@@"',
        "  return 2",
        "fi",
        "",
        "screen_line() {",
        '  screen -list 2>/dev/null | grep -F -- ".$1 " | head -n 1',
        "}",
        "",
        "screen_exists() {",
        '  [ -n "$(screen_line "$1")" ]',
        "}",
        "",
        "stop_screen() {",
        '  name="$1"',
        '  screen -S "$name" -X quit >/dev/null 2>&1 || true',
        "  i=0",
        '  while screen_exists "$name" && [ "$i" -lt 20 ]; do',
        "    sleep 0.25",
        "    i=$((i + 1))",
        "  done",
        '  if screen_exists "$name"; then',
        "    screen -wipe >/dev/null 2>&1 || true",
        "  fi",
        "}",
        "",
    ]

    for spec in roles:
        screen = quote(spec.screen)
        lines.extend(
            [
                f"# {spec.name}",
                f"if screen_exists {screen}; then",
                f"  stop_screen {screen}",
                f"  if screen_exists {screen}; then",
                f'    echo "{MARKER}ERROR:{node}:screen {spec.screen} could not be stopped@@"',
                "  else",
                f'    echo "{MARKER}ACTION:{node}:{spec.name}:STOPPED@@"',
                "  fi",
                "else",
                f'  echo "{MARKER}ACTION:{node}:{spec.name}:NOT_RUNNING@@"',
                "fi",
                "",
            ]
        )
    return "\n".join(lines)


def _build_teardown_parts(data_mover: str) -> tuple[str, str, str]:
    login_roles = [WORKER_ROLES[name] for name in ROLE_ORDER if WORKER_ROLES[name].node == "login"]
    data_mover_roles = [WORKER_ROLES[name] for name in ROLE_ORDER if WORKER_ROLES[name].node == "data_mover"]
    login_body = _teardown_body("login", login_roles)
    data_mover_body = _teardown_body("data_mover", data_mover_roles)
    return login_body, data_mover_body, _wrap_nodes(login_body, data_mover_body, data_mover)


## Remote output parsing ##


_SCREEN_LINE = re.compile(r"^\s*(?P<pid>\d+)\.(?P<name>\S+)\s+\((?P<state>[^)]+)\)")


def _parse_screen_listing(text: str) -> dict[str, ScreenSession]:
    sessions: dict[str, ScreenSession] = {}
    for line in text.splitlines():
        match = _SCREEN_LINE.match(line)
        if match is None:
            continue
        sessions[match.group("name")] = ScreenSession(
            name=match.group("name"),
            pid=int(match.group("pid")),
            state=match.group("state"),
        )
    return sessions


def _parse_remote_output(stdout: str) -> RemoteOutput:
    nodes = {name: RemoteNode(name) for name in SCREEN_NODE_NAMES}
    actions: list[DeployAction] = []
    errors: list[str] = []
    checks: list[CheckResult] = []
    notes: list[str] = []
    seen_nodes: set[str] = set()
    screen_lines: list[str] | None = None
    complete = False

    for raw_line in stdout.splitlines():
        line = raw_line.rstrip("\r")
        if line.startswith(MARKER):
            payload = line[len(MARKER) :]
            if payload.endswith("@@"):
                payload = payload[:-2]
            if payload.startswith("NODE_BEGIN:"):
                node = payload.split(":", 1)[1]
                seen_nodes.add(node)
                nodes.setdefault(node, RemoteNode(node))
            elif payload.startswith("NODE_END:"):
                _, node, raw_exit = payload.split(":", 2)
                node_state = nodes.setdefault(node, RemoteNode(node))
                try:
                    node_state.exit_code = int(raw_exit)
                except ValueError:
                    node_state.errors.append(f"invalid remote exit status {raw_exit!r}")
                else:
                    if node_state.exit_code != 0:
                        node_state.errors.append(f"remote node exited with status {node_state.exit_code}")
            elif payload.startswith("HOST:"):
                _, node, host = payload.split(":", 2)
                nodes.setdefault(node, RemoteNode(node)).host = host
            elif payload.startswith("SCREEN_BEGIN:"):
                screen_lines = []
            elif payload.startswith("SCREEN_END:"):
                node = payload.split(":", 1)[1]
                if screen_lines is not None:
                    nodes.setdefault(node, RemoteNode(node)).sessions.update(
                        _parse_screen_listing("\n".join(screen_lines))
                    )
                screen_lines = None
            elif payload.startswith("SCREEN_ERROR:"):
                _, node, message = payload.split(":", 2)
                nodes.setdefault(node, RemoteNode(node)).errors.append(message)
            elif payload.startswith("ACTION:"):
                _, node, role, action = payload.split(":", 3)
                actions.append(DeployAction(node=node, role=role, action=action))
            elif payload.startswith("ERROR:"):
                _, node, message = payload.split(":", 2)
                nodes.setdefault(node, RemoteNode(node)).errors.append(message)
            elif payload.startswith("CHECK:"):
                _, node, name, status = payload.split(":", 3)
                checks.append(CheckResult(node=node, name=name, status=status))
            elif payload.startswith("NOTE:"):
                _, node, message = payload.split(":", 2)
                notes.append(f"{node}: {message}")
            elif payload == "END":
                complete = True
            continue

        if screen_lines is not None:
            screen_lines.append(line)

    if screen_lines is not None:
        errors.append("remote output ended while listing screen sessions")
    for node_name in seen_nodes:
        node_state = nodes.setdefault(node_name, RemoteNode(node_name))
        if node_state.exit_code is None:
            node_state.errors.append("remote node did not report completion")
    if not complete:
        errors.append("remote command did not report completion")

    return RemoteOutput(
        nodes=nodes,
        actions=actions,
        errors=errors,
        complete=complete,
        checks=checks,
        notes=notes,
    )


def _screen_state_for_role(role: WorkerRole, remote: RemoteOutput) -> ScreenState:
    node_state = remote.nodes.get(role.node)
    if node_state is None:
        return ScreenState(
            role=role.name,
            name=role.screen,
            node=role.node,
            error="node was not contacted",
        )
    if node_state.errors:
        return ScreenState(
            role=role.name,
            name=role.screen,
            node=role.node,
            host=node_state.host,
            error="; ".join(node_state.errors),
        )
    session = node_state.sessions.get(role.screen)
    if session is None:
        return ScreenState(role=role.name, name=role.screen, node=role.node, host=node_state.host)
    return ScreenState(
        role=role.name,
        name=role.screen,
        node=role.node,
        host=node_state.host,
        exists=True,
        state=session.state,
        pid=session.pid,
    )


def _screen_screens(remote: RemoteOutput) -> dict[str, ScreenState]:
    return {name: _screen_state_for_role(WORKER_ROLES[name], remote) for name in ROLE_ORDER}


## Status rendering ##


def _verdict(prefect: PrefectState, screen: ScreenState) -> str:
    if prefect.error or screen.error:
        return "UNKNOWN"
    if prefect.online and screen.running:
        return "HEALTHY"
    if prefect.online and not screen.running:
        return "DEGRADED (worker online, screen missing)"
    if not prefect.online and screen.running:
        return "DEGRADED (screen running, worker offline)"
    if screen.exists:
        return "STALE (dead screen session)"
    return "STOPPED"


def _healthy(prefect: PrefectState, screen: ScreenState) -> bool:
    return _verdict(prefect, screen) == "HEALTHY"


def _format_prefect(state: PrefectState) -> str:
    if state.error:
        return f"UNKNOWN ({state.error})"
    if not state.exists:
        return "NO WORKER RECORD"
    heartbeat = _format_age(state.last_heartbeat)
    return f"{state.status} (last heartbeat {heartbeat})"


def _format_screen(state: ScreenState) -> str:
    if state.error:
        return f"UNKNOWN ({state.error})"
    if not state.exists:
        return "MISSING"
    if state.running:
        return f"RUNNING (pid {state.pid}, {state.state})"
    return f"STALE ({state.state})"


def _status_payload(
    prefect_states: Mapping[str, PrefectState],
    screens: Mapping[str, ScreenState],
) -> dict:
    return {
        "api_url": os.environ.get("PREFECT_API_URL"),
        "workers": {
            name: {
                "role": name,
                "node": WORKER_ROLES[name].node,
                "pool": WORKER_ROLES[name].pool,
                "worker": WORKER_ROLES[name].worker,
                "screen": WORKER_ROLES[name].screen,
                "prefect": {
                    "exists": prefect_states[name].exists,
                    "status": prefect_states[name].status,
                    "last_heartbeat": _iso(prefect_states[name].last_heartbeat),
                    "worker_id": prefect_states[name].worker_id,
                    "error": prefect_states[name].error,
                },
                "screen_state": {
                    "exists": screens[name].exists,
                    "state": screens[name].state,
                    "pid": screens[name].pid,
                    "host": screens[name].host,
                    "error": screens[name].error,
                },
                "verdict": _verdict(prefect_states[name], screens[name]),
                "healthy": _healthy(prefect_states[name], screens[name]),
            }
            for name in ROLE_ORDER
        },
    }


def _print_status(prefect_states: Mapping[str, PrefectState], screens: Mapping[str, ScreenState]) -> None:
    click.echo("Production worker status")
    click.echo(f"Prefect API: {os.environ.get('PREFECT_API_URL', '<unset>')}")
    click.echo()
    for name in ROLE_ORDER:
        spec = WORKER_ROLES[name]
        prefect = prefect_states[name]
        screen = screens[name]
        node_label = NODE_LABELS.get(spec.node, spec.node)
        host = screen.host or "unknown host"
        click.echo(f"{name}:")
        click.echo(f"  pool:    {spec.pool}")
        click.echo(f"  worker:  {spec.worker}")
        click.echo(f"  prefect: {_format_prefect(prefect)}")
        click.echo(f"  screen:  {spec.screen} on {node_label} ({host}) - {_format_screen(screen)}")
        click.echo(f"  verdict: {_verdict(prefect, screen)}")
        click.echo()
