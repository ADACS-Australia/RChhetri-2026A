"""Shared remote and Prefect helpers for needle deployments.

This module contains the reusable pieces used by the ``needle deploy`` CLI:
Prefect worker queries, single-connection SSH wrappers, remote marker parsing,
probe/teardown scripts, and status rendering.  It does not define a CLI
entrypoint itself.
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
PRODUCTION_DEPLOYMENTS = (
    "download-observation/download-observation-ozstar",
    "needle-pipeline/needle-pipeline-ozstar",
)
TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "deploy_templates"


def render_template(name: str, **values: str) -> str:
    """Render a shell template by replacing ``@@NAME@@`` placeholders."""
    text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
    values["marker"] = MARKER
    for key, value in values.items():
        text = text.replace(f"@@{key.upper()}@@", str(value))
    return text


__all__ = [
    "MARKER",
    "NODE_LABELS",
    "PREFECT_ENV_VARS",
    "PRODUCTION_DEPLOYMENTS",
    "SCREEN_NODE_NAMES",
    "CheckResult",
    "DeployAction",
    "PrefectResourceState",
    "PrefectState",
    "RemoteNode",
    "RemoteOutput",
    "ScreenSession",
    "ScreenState",
    "build_teardown_parts",
    "ensure_prefect_env",
    "ensure_work_pools",
    "format_prefect",
    "format_screen",
    "healthy",
    "load_env_file",
    "parse_remote_output",
    "parse_screen_listing",
    "probe_body",
    "preflight_lines",
    "print_status",
    "query_prefect",
    "query_prefect_resources",
    "render_template",
    "run_ssh",
    "screen_screens",
    "screen_state_for_role",
    "states_with_error",
    "status_payload",
    "verdict",
    "wait_for_online",
    "wrap_nodes",
]


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
        """Whether Prefect currently reports this worker as online."""
        return self.status == "ONLINE" and self.error is None


@dataclass
class PrefectResourceState:
    """Existence state of a Prefect work pool or deployment."""

    kind: str
    name: str
    exists: bool = False
    error: str | None = None


@dataclass
class ScreenSession:
    """A session as reported by ``screen -list``."""

    name: str
    pid: int
    state: str

    @property
    def running(self) -> bool:
        """Whether the session is attached or detached rather than dead."""
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
        """Whether the expected screen session exists and is not dead."""
        return self.exists and self.state is not None and self.state.startswith(("Attached", "Detached"))


@dataclass
class RemoteNode:
    """Result reported by one node of a remote script."""

    name: str
    host: str | None = None
    exit_code: int | None = None
    sessions: dict[str, ScreenSession] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class DeployAction:
    """A deployment action emitted by the remote script."""

    node: str
    role: str
    action: str


@dataclass
class CheckResult:
    """One prerequisite check emitted by a status script."""

    node: str
    name: str
    status: str


@dataclass
class RemoteOutput:
    """Parsed marker output from one or more remote deployment scripts."""

    nodes: dict[str, RemoteNode]
    actions: list[DeployAction]
    errors: list[str] = field(default_factory=list)
    complete: bool = False
    checks: list[CheckResult] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _iso(value: datetime | None) -> str | None:
    """Return an ISO timestamp or ``None``."""
    return value.isoformat() if value is not None else None


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalise a datetime to timezone-aware UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_age(value: datetime | None) -> str:
    """Format a heartbeat timestamp as a short relative age."""
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
    """Indent every non-empty line of ``text``."""
    prefix = " " * spaces
    return "\n".join(prefix + line if line else line for line in text.splitlines())


def _worker_sort_key(worker) -> tuple[int, float]:
    """Sort worker records by online status, then most recent heartbeat."""
    heartbeat = getattr(worker, "last_heartbeat_time", None)
    timestamp = heartbeat.timestamp() if heartbeat is not None else 0.0
    online = getattr(getattr(worker, "status", None), "value", "") == "ONLINE"
    return (1 if online else 0, timestamp)


## Prefect API ##


def query_prefect() -> dict[str, PrefectState]:
    """Read worker records for both production work pools.

    A missing work pool or a failed API request is recorded on that role rather than aborting the whole query, so
    ``status`` can still report screen state.
    """
    from prefect.client.orchestration import get_client
    from prefect.exceptions import ObjectNotFound

    async def _read() -> dict[str, PrefectState]:
        """Read worker records for every configured role."""
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


def query_prefect_resources() -> list[PrefectResourceState]:
    """Check that production work pools and deployments are registered."""
    from prefect.client.orchestration import get_client
    from prefect.exceptions import ObjectNotFound

    async def _read() -> list[PrefectResourceState]:
        resources: list[PrefectResourceState] = []
        async with get_client() as client:
            for pool in sorted({role.pool for role in WORKER_ROLES.values()}):
                state = PrefectResourceState(kind="work pool", name=pool)
                try:
                    await client.read_work_pool(pool)
                except ObjectNotFound:
                    state.error = "not found"
                except Exception as exc:
                    state.error = f"{type(exc).__name__}: {exc}"
                else:
                    state.exists = True
                resources.append(state)

            for deployment in PRODUCTION_DEPLOYMENTS:
                state = PrefectResourceState(kind="deployment", name=deployment)
                try:
                    await client.read_deployment_by_name(deployment)
                except ObjectNotFound:
                    state.error = "not found"
                except Exception as exc:
                    state.error = f"{type(exc).__name__}: {exc}"
                else:
                    state.exists = True
                resources.append(state)
        return resources

    return asyncio.run(_read())


def ensure_work_pools() -> list[str]:
    """Create any missing production work pools, returning the names created."""
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate
    from prefect.exceptions import ObjectNotFound

    async def _run() -> list[str]:
        """Create missing work pools and return their names."""
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


def states_with_error(message: str) -> dict[str, PrefectState]:
    """Build worker states carrying a shared Prefect-query error."""
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
    """Whether a worker is online and has heartbeated after ``not_before``."""
    if not state.online:
        return False
    if not_before is None:
        return True
    heartbeat = state.last_heartbeat
    return heartbeat is not None and heartbeat >= not_before


def wait_for_online(
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
            states = query_prefect()
        except Exception as exc:
            states = states_with_error(f"{type(exc).__name__}: {exc}")

        if all(_ready(states[name], not_before.get(name)) for name in ROLE_ORDER):
            return states

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return states
        time.sleep(min(max(interval, 0.1), remaining))


_ENV_LINE = re.compile(r"^(?:export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)$")


def _unquote_env_value(value: str) -> str:
    """Remove simple shell quoting and inline comments from an env value."""
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        quote = value[0]
        value = value[1:-1]
        if quote == '"':
            value = value.replace('\\"', '"').replace("\\n", "\n")
        return value
    # Only strip an inline comment when it is separated by whitespace.
    return re.sub(r"\s+#.*$", "", value).rstrip()


def load_env_file(path: Path, environ: MutableMapping[str, str] | None = None) -> list[str]:
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


def ensure_prefect_env(env_file: Path | None) -> None:
    """Ensure local Prefect API credentials are available.

    Existing environment variables take precedence.  If they are missing, a
    local ``.env``/``--env-file`` is loaded without overriding them.
    """
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
            load_env_file(candidate)
            if all(os.environ.get(name) for name in PREFECT_ENV_VARS):
                return

    missing = ", ".join(name for name in PREFECT_ENV_VARS if not os.environ.get(name))
    checked = ", ".join(str(path) for path in candidates)
    raise click.ClickException(
        f"Missing Prefect environment variable(s): {missing}. "
        f"Set them or pass --env-file (checked: {checked or 'no paths'})."
    )


## SSH / remote scripts ##


def run_ssh(host: str, script: str, timeout: float) -> subprocess.CompletedProcess[str]:
    """Run a shell script over one SSH connection and capture its output."""
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
    """Wrap a shell body in a function and call it immediately."""
    return f"{name}() {{\n{_indent(body)}\n}}\n{name}\n"


def wrap_nodes(login_body: str, data_mover_body: str, data_mover: str) -> str:
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


def probe_body(node: str) -> str:
    """Return the shell body that reports a node's hostname and screen sessions."""
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


def preflight_lines(
    node: str,
    *,
    install: bool,
    strict: bool,
    workdir: str,
    env_file: str,
    require_screen: bool = True,
    require_cli: bool = False,
    fallback_image: str | None = None,
) -> list[str]:
    """Return shell lines that check node prerequisites.

    ``install`` allows user-space installation of ``uv`` and project
    dependencies.  ``strict`` makes failed required checks return immediately.
    """
    return render_template(
        "preflight.sh",
        node=node,
        workdir=workdir,
        env_file=env_file,
        install="1" if install else "0",
        strict="1" if strict else "0",
        require_screen="1" if require_screen else "0",
        require_cli="1" if require_cli else "0",
        fallback_image=fallback_image or "",
    ).splitlines()


def _teardown_body(node: str, roles: Iterable[WorkerRole]) -> str:
    """Return the shell body that stops the requested worker screens."""
    quote = shlex.quote
    lines: list[str] = [
        f'echo "{MARKER}HOST:{node}:$(hostname 2>/dev/null || uname -n)@@"',
        "",
        "if ! command -v screen >/dev/null 2>&1; then",
        f'  echo "{MARKER}SCREEN_ERROR:{node}:screen not found@@"',
        "  return 2",
        "fi",
        "",
        *render_template("screen.sh", node=node).splitlines(),
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


def build_teardown_parts(data_mover: str) -> tuple[str, str, str]:
    """Build one-SSH teardown scripts for both production worker nodes."""
    login_roles = [WORKER_ROLES[name] for name in ROLE_ORDER if WORKER_ROLES[name].node == "login"]
    data_mover_roles = [WORKER_ROLES[name] for name in ROLE_ORDER if WORKER_ROLES[name].node == "data_mover"]
    login_body = _teardown_body("login", login_roles)
    data_mover_body = _teardown_body("data_mover", data_mover_roles)
    return login_body, data_mover_body, wrap_nodes(login_body, data_mover_body, data_mover)


## Remote output parsing ##


_SCREEN_LINE = re.compile(r"^\s*(?P<pid>\d+)\.(?P<name>\S+)\s+\((?P<state>[^)]+)\)")


def parse_screen_listing(text: str) -> dict[str, ScreenSession]:
    """Parse ``screen -list`` output into a mapping of session name to state."""
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


def parse_remote_output(stdout: str) -> RemoteOutput:
    """Parse marker-delimited output from a remote deployment script."""
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
                        parse_screen_listing("\n".join(screen_lines))
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


def screen_state_for_role(role: WorkerRole, remote: RemoteOutput) -> ScreenState:
    """Project a role's expected screen session out of parsed remote output."""
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


def screen_screens(remote: RemoteOutput) -> dict[str, ScreenState]:
    """Return screen state for every configured production worker role."""
    return {name: screen_state_for_role(WORKER_ROLES[name], remote) for name in ROLE_ORDER}


## Status rendering ##


def verdict(prefect: PrefectState, screen: ScreenState) -> str:
    """Combine Prefect and screen state into a human-readable verdict."""
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


def healthy(prefect: PrefectState, screen: ScreenState) -> bool:
    """Whether both Prefect and the expected screen report a healthy worker."""
    return verdict(prefect, screen) == "HEALTHY"


def format_prefect(state: PrefectState) -> str:
    """Format a worker's Prefect state for CLI output."""
    if state.error:
        return f"UNKNOWN ({state.error})"
    if not state.exists:
        return "NO WORKER RECORD"
    heartbeat = _format_age(state.last_heartbeat)
    return f"{state.status} (last heartbeat {heartbeat})"


def format_screen(state: ScreenState) -> str:
    """Format a worker's screen state for CLI output."""
    if state.error:
        return f"UNKNOWN ({state.error})"
    if not state.exists:
        return "MISSING"
    if state.running:
        return f"RUNNING (pid {state.pid}, {state.state})"
    return f"STALE ({state.state})"


def status_payload(
    prefect_states: Mapping[str, PrefectState],
    screens: Mapping[str, ScreenState],
) -> dict:
    """Build the JSON-serialisable status payload for production workers."""
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
                "verdict": verdict(prefect_states[name], screens[name]),
                "healthy": healthy(prefect_states[name], screens[name]),
            }
            for name in ROLE_ORDER
        },
    }


def print_status(prefect_states: Mapping[str, PrefectState], screens: Mapping[str, ScreenState]) -> None:
    """Print a human-readable production worker status report."""
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
        click.echo(f"  prefect: {format_prefect(prefect)}")
        click.echo(f"  screen:  {spec.screen} on {node_label} ({host}) - {format_screen(screen)}")
        click.echo(f"  verdict: {verdict(prefect, screen)}")
        click.echo()
