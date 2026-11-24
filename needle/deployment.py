"""Generic environment deployment for needle.

Two environments are supported:

``prod``
    Code at ``/fred/oz513/software/needle``, configs under
    ``/fred/oz513/software/config``, workers in screen sessions.

``dev``
    Code at ``$HOME/software/needle``, configs under
    ``$HOME/software/config``, no workers.

Both environments are driven by an environment file in ``$HOME``
(``.needle.prod.env`` / ``.needle.dev.env``) which points at the corresponding
config YAML files.  Deployments update a remote git checkout, install the
project with ``uv``, create configured directories, and optionally upload a
locally built SIF image.
"""

from __future__ import annotations

import base64
import io
import json
import os
import shlex
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from shutil import which
from typing import Iterable
from urllib.parse import urlparse

import click

from needle.lib.deployment import (
    MARKER,
    NODE_LABELS,
    SCREEN_NODE_NAMES,
    RemoteOutput,
    _build_bootstrap_parts,
    _build_probe_script,
    _build_teardown_parts,
    _ensure_prefect_env,
    _ensure_work_pools,
    _format_prefect,
    _healthy,
    _parse_remote_output,
    _print_status,
    _query_prefect,
    _run_ssh,
    _screen_screens,
    _states_with_error,
    _status_payload,
    _wait_for_online,
    _wrap_nodes,
)
from needle.lib.workers import WORKER_ROLES, WorkerRole

DEFAULT_DOCKER_IMAGE = os.environ.get("NEEDLE_IMAGE", "needle:latest")
DEFAULT_SSH_TIMEOUT = 3600.0
DEFAULT_WAIT = 60.0


@dataclass(frozen=True)
class Environment:
    name: str
    root: str
    env_file: str
    workers: bool


ENVIRONMENTS: dict[str, Environment] = {
    "prod": Environment(
        name="prod",
        root="/fred/oz513/software/needle",
        env_file="$HOME/.needle.prod.env",
        workers=True,
    ),
    "dev": Environment(
        name="dev",
        root="$HOME/software/needle",
        env_file="$HOME/.needle.dev.env",
        workers=False,
    ),
}


_DIRS_CODE = """\
import os, sys
import yaml

def expand(value):
    if value is None:
        return None
    value = str(value)
    if value.startswith("s3://"):
        return None
    value = value.removeprefix("local://")
    value = os.path.expandvars(os.path.expanduser(value))
    return value if os.path.isabs(value) else None

def add_dir(paths, value):
    path = expand(value)
    if path:
        paths.add(path)

def add_parent(paths, value):
    path = expand(value)
    if path:
        paths.add(os.path.dirname(path))

needle = yaml.safe_load(open(sys.argv[1])) or {}
cluster = yaml.safe_load(open(sys.argv[2])) or {}
paths = set()
data = needle.get("data") or {}
for key in ("work_dir", "download_dir", "staging_dir", "casa_dir"):
    add_dir(paths, data.get(key))
add_dir(paths, data.get("source"))
add_parent(paths, (needle.get("watcher") or {}).get("log_file"))
slurm = cluster.get("slurm") or {}
add_dir(paths, slurm.get("local_directory"))
add_dir(paths, slurm.get("log_directory"))
add_parent(paths, (cluster.get("container") or {}).get("image"))
for path in sorted(paths):
    print(path)
"""

_IMAGE_CODE = """\
import os, sys
import yaml

cfg = yaml.safe_load(open(sys.argv[1])) or {}
image = (cfg.get("container") or {}).get("image")
if image:
    print(os.path.expandvars(os.path.expanduser(str(image))))
"""


def _python_exec(code: str) -> str:
    """Return a single-line ``python -c`` program that executes ``code``.

    Keeping helper programs on one physical line means they survive the indentation added when a node body is wrapped
    in a shell function.
    """
    encoded = base64.b64encode(code.encode("utf-8")).decode("ascii")
    return f'import base64; exec(base64.b64decode("{encoded}"))'


## Local git / image build helpers ##


def _git_output(source: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()


def _http_url(url: str) -> str:
    """Convert common SSH git remotes to their HTTPS form."""
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("git+ssh://"):
        url = url[len("git+") :]
    if url.startswith("ssh://"):
        parsed = urlparse(url)
        if parsed.hostname and parsed.path:
            return f"https://{parsed.hostname}/{parsed.path.lstrip('/')}"
        return url
    if url.startswith("git://"):
        return "https://" + url[len("git://") :]
    if "@" in url and ":" in url and not url.startswith("file://"):
        user_host, path = url.split(":", 1)
        host = user_host.split("@", 1)[1]
        if host and path:
            return f"https://{host}/{path.lstrip('/')}"
    return url


def _git_info(source: Path, repo: str | None, branch: str | None) -> tuple[str, str]:
    if not source.is_dir():
        raise click.ClickException(f"Local source directory does not exist: {source}")
    try:
        git_root = Path(_git_output(source, "rev-parse", "--show-toplevel"))
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise click.ClickException(f"{source} is not a git checkout.") from exc

    if repo:
        resolved_repo = repo
    else:
        try:
            resolved_repo = _http_url(_git_output(git_root, "remote", "get-url", "origin"))
        except subprocess.CalledProcessError as exc:
            raise click.ClickException("No git remote 'origin' found; pass --repo.") from exc
    if not resolved_repo:
        raise click.ClickException("No git remote 'origin' found; pass --repo.")

    resolved_branch = branch or _git_output(git_root, "rev-parse", "--abbrev-ref", "HEAD")
    if not resolved_branch or resolved_branch == "HEAD":
        raise click.ClickException("Local checkout is in detached HEAD state; pass --branch.")

    return resolved_repo, resolved_branch


def _build_image(source: Path, docker_image: str, local_image: Path) -> None:
    for executable in ("make", "apptainer"):
        if which(executable) is None:
            raise click.ClickException(f"'{executable}' is required to build the image but was not found.")

    local_image.parent.mkdir(parents=True, exist_ok=True)

    click.echo(f"Building Docker image {docker_image}...")
    env = os.environ.copy()
    env["NEEDLE_IMAGE"] = docker_image
    subprocess.run(["make", "needle"], cwd=source, check=True, env=env)

    click.echo(f"Building Apptainer image {local_image}...")
    subprocess.run(
        ["apptainer", "build", "--force", str(local_image), f"docker-daemon://{docker_image}"],
        cwd=source,
        check=True,
    )


## Remote shell script ##


def _uv_install_lines(node: str) -> list[str]:
    return [
        'export PATH="$HOME/.local/bin:$PATH"',
        "if ! command -v uv >/dev/null 2>&1; then",
        "  if command -v curl >/dev/null 2>&1; then",
        "    if ! (curl -LsSf https://astral.sh/uv/install.sh | sh) 1>&2; then",
        f'      echo "{MARKER}ERROR:{node}:uv installation failed@@"',
        "      return 1",
        "    fi",
        "  elif command -v wget >/dev/null 2>&1; then",
        "    if ! (wget -qO- https://astral.sh/uv/install.sh | sh) 1>&2; then",
        f'      echo "{MARKER}ERROR:{node}:uv installation failed@@"',
        "      return 1",
        "    fi",
        "  else",
        f'    echo "{MARKER}ERROR:{node}:neither curl nor wget is available to install uv@@"',
        "    return 1",
        "  fi",
        '  export PATH="$HOME/.local/bin:$PATH"',
        "fi",
        "if ! command -v uv >/dev/null 2>&1; then",
        f'  echo "{MARKER}ERROR:{node}:uv is not installed@@"',
        "  return 1",
        "fi",
    ]


def _git_check_lines(node: str) -> list[str]:
    return [
        "if ! command -v git >/dev/null 2>&1; then",
        "  if command -v module >/dev/null 2>&1; then",
        "    module load git >/dev/null 2>&1 || true",
        "  fi",
        "fi",
        "if ! command -v git >/dev/null 2>&1; then",
        f'  echo "{MARKER}ERROR:{node}:git is required to update the checkout but was not found@@"',
        "  return 1",
        "fi",
    ]


def _screen_lines(node: str) -> list[str]:
    return [
        "if ! command -v screen >/dev/null 2>&1; then",
        "  if command -v module >/dev/null 2>&1; then",
        "    module load screen >/dev/null 2>&1 || true",
        "  fi",
        "fi",
        "if ! command -v screen >/dev/null 2>&1; then",
        f'  echo "{MARKER}ERROR:{node}:screen is required to run workers but was not found@@"',
        "  return 1",
        "fi",
        "screen_line() {",
        '  screen -list 2>/dev/null | grep -F -- ".$1 " | head -n 1',
        "}",
        "screen_exists() {",
        '  [ -n "$(screen_line "$1")" ]',
        "}",
        "screen_running() {",
        '  line="$(screen_line "$1")"',
        "  [ -n \"$line\" ] && printf '%s\\n' \"$line\" | grep -Eq '\\((Attached|Detached)\\)'",
        "}",
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
        "start_worker() {",
        '  role="$1"',
        '  name="$2"',
        '  cmd="$3"',
        '  if screen_running "$name"; then',
        f'    echo "{MARKER}ACTION:{node}:${{role}}:EXISTS@@"',
        "    return 0",
        "  fi",
        '  if screen_exists "$name"; then',
        '    stop_screen "$name"',
        "  fi",
        '  screen -dmS "$name" bash -lc "$cmd"',
        "  rc=$?",
        '  if [ "$rc" -ne 0 ]; then',
        f'    echo "{MARKER}ERROR:{node}:screen -dmS $name failed with status $rc@@"',
        "    return 1",
        "  fi",
        "  sleep 1",
        '  if screen_running "$name"; then',
        f'    echo "{MARKER}ACTION:{node}:${{role}}:STARTED@@"',
        "  else",
        f'    echo "{MARKER}ERROR:{node}:screen $name exited immediately@@"',
        "    return 1",
        "  fi",
        "}",
    ]


def _node_body(
    env: Environment,
    node: str,
    *,
    repo: str,
    branch: str,
    image_file: str | None,
    worker_roles: Iterable[WorkerRole],
    restart_workers: bool = False,
    install_tool: bool = False,
) -> str:
    q = shlex.quote
    roles = list(worker_roles)
    lines: list[str] = [
        f'echo "{MARKER}HOST:{node}:$(hostname 2>/dev/null || uname -n)@@"',
        *_uv_install_lines(node),
        *_git_check_lines(node),
    ]
    if roles:
        lines.extend(_screen_lines(node))
    lines.extend(
        [
            f'ROOT="{env.root}"',
            f'ENV_FILE="{env.env_file}"',
            f"REPO={q(repo)}",
            f"BRANCH={q(branch)}",
            f"RESTART_WORKERS={'1' if restart_workers else '0'}",
            "",
            'mkdir -p "$(dirname "$ROOT")" || return 1',
            'if [ ! -f "$ENV_FILE" ]; then',
            f'  echo "{MARKER}ERROR:{node}:environment file not found: $ENV_FILE@@"',
            "  return 1",
            "fi",
            "set -a",
            'if ! . "$ENV_FILE"; then',
            "  set +a",
            f'  echo "{MARKER}ERROR:{node}:could not load environment file: $ENV_FILE@@"',
            "  return 1",
            "fi",
            "set +a",
            'if [ -z "${NEEDLE_CONFIG:-}" ]; then',
            f'  echo "{MARKER}ERROR:{node}:NEEDLE_CONFIG is not set in $ENV_FILE@@"',
            "  return 1",
            "fi",
            'if [ -z "${NEEDLE_CLUSTER_CONFIG:-}" ]; then',
            f'  echo "{MARKER}ERROR:{node}:NEEDLE_CLUSTER_CONFIG is not set in $ENV_FILE@@"',
            "  return 1",
            "fi",
            'mkdir -p "$(dirname "$NEEDLE_CONFIG")" "$(dirname "$NEEDLE_CLUSTER_CONFIG")" || return 1',
            'if [ ! -f "$NEEDLE_CONFIG" ]; then',
            f'  echo "{MARKER}ERROR:{node}:needle config not found: $NEEDLE_CONFIG@@"',
            "  return 1",
            "fi",
            'if [ ! -f "$NEEDLE_CLUSTER_CONFIG" ]; then',
            f'  echo "{MARKER}ERROR:{node}:cluster config not found: $NEEDLE_CLUSTER_CONFIG@@"',
            "  return 1",
            "fi",
            "",
            'if [ -d "$ROOT/.git" ]; then',
            '  if git -C "$ROOT" remote get-url origin >/dev/null 2>&1; then',
            '    git -C "$ROOT" remote set-url origin "$REPO" || return 1',
            "  else",
            '    git -C "$ROOT" remote add origin "$REPO" || return 1',
            "  fi",
            '  if ! git -C "$ROOT" fetch --prune origin; then',
            f'    echo "{MARKER}ERROR:{node}:git fetch failed in $ROOT@@"',
            "    return 1",
            "  fi",
            '  if ! git -C "$ROOT" checkout "$BRANCH"; then',
            f'    echo "{MARKER}ERROR:{node}:git checkout $BRANCH failed in $ROOT@@"',
            "    return 1",
            "  fi",
            '  if ! git -C "$ROOT" pull --ff-only origin "$BRANCH"; then',
            f'    echo "{MARKER}ERROR:{node}:git pull failed in $ROOT; resolve local changes or branch state@@"',
            "    return 1",
            "  fi",
            'elif [ ! -e "$ROOT" ] || [ -z "$(ls -A "$ROOT" 2>/dev/null)" ]; then',
            '  if ! git clone --branch "$BRANCH" "$REPO" "$ROOT"; then',
            f'    echo "{MARKER}ERROR:{node}:git clone failed; check remote access to $REPO@@"',
            "    return 1",
            "  fi",
            "else",
            f'  echo "{MARKER}ERROR:{node}:$ROOT exists but is not a git checkout@@"',
            "  return 1",
            "fi",
            f'echo "{MARKER}ACTION:{node}:code:UPDATED@@"',
            f'echo "{MARKER}NOTE:{node}:code $(git -C $ROOT rev-parse --short HEAD) (${{BRANCH}})@@"',
            "",
            'cd "$ROOT" || return 1',
            'sync_args=""',
            "if [ -f uv.lock ]; then sync_args=--frozen; fi",
            "if ! uv sync $sync_args; then",
            f'  echo "{MARKER}ERROR:{node}:uv sync failed in $ROOT@@"',
            "  return 1",
            "fi",
            f'echo "{MARKER}ACTION:{node}:sync:OK@@"',
            "",
        ]
    )
    if install_tool:
        lines.extend(
            [
                'if ! uv tool install --editable "$ROOT" --force; then',
                f'  echo "{MARKER}ERROR:{node}:uv tool install --editable failed in $ROOT@@"',
                "  return 1",
                "fi",
                f'echo "{MARKER}ACTION:{node}:cli:INSTALLED@@"',
                'if ! bash -lc \'case ":$PATH:" in *":$HOME/.local/bin:"*) exit 0;; *) exit 1;; esac\' >/dev/null 2>&1; then',
                f'  echo "{MARKER}NOTE:{node}:add $HOME/.local/bin to PATH to use the needle command from any directory@@"',
                "fi",
                "",
            ]
        )
    lines.extend(
        [
            "# Create all configured local directories, ignoring paths we cannot write.",
            f'if ! DIRS=$(uv run python -c {q(_python_exec(_DIRS_CODE))} "$NEEDLE_CONFIG" "$NEEDLE_CLUSTER_CONFIG"); then',
            f'  echo "{MARKER}ERROR:{node}:could not read configured directories@@"',
            "  return 1",
            "fi",
            'if [ -n "$DIRS" ]; then',
            "  while IFS= read -r d; do",
            '    [ -z "$d" ] && continue',
            '    if ! mkdir -p "$d"; then',
            f'      echo "{MARKER}NOTE:{node}:could not create directory: $d@@"',
            "    fi",
            '  done <<< "$DIRS"',
            "fi",
            "",
        ]
    )

    if image_file is not None:
        lines.extend(
            [
                f'IMAGE_FILE="{image_file}"',
                'if [ -f "$IMAGE_FILE" ]; then',
                f'  if ! IMAGE_DEST=$(uv run python -c {q(_python_exec(_IMAGE_CODE))} "$NEEDLE_CLUSTER_CONFIG"); then',
                f'    echo "{MARKER}ERROR:{node}:could not read container.image from $NEEDLE_CLUSTER_CONFIG@@"',
                "    return 1",
                "  fi",
                '  if [ -z "$IMAGE_DEST" ]; then',
                f'    echo "{MARKER}ERROR:{node}:container.image is not set in $NEEDLE_CLUSTER_CONFIG@@"',
                "    return 1",
                "  fi",
                '  mkdir -p "$(dirname "$IMAGE_DEST")" || return 1',
                '  if ! install -m 0644 "$IMAGE_FILE" "$IMAGE_DEST"; then',
                f'    echo "{MARKER}ERROR:{node}:could not install image to $IMAGE_DEST@@"',
                "    return 1",
                "  fi",
                f'  echo "{MARKER}ACTION:{node}:image:INSTALLED@@"',
                "else",
                f'  echo "{MARKER}ACTION:{node}:image:SKIPPED@@"',
                "fi",
                "",
            ]
        )

    for spec in roles:
        command = (
            f'cd "$ROOT" && export PATH="$HOME/.local/bin:$PATH" && '
            f'set -a && . "$ENV_FILE" && set +a && '
            f"exec uv run needle start-workers {spec.name}"
        )
        lines.extend(
            [
                f"# {spec.name}",
                'if [ "$RESTART_WORKERS" -eq 1 ] && screen_exists '
                f"{shlex.quote(spec.screen)}; then stop_screen {shlex.quote(spec.screen)}; fi",
                f"start_worker {spec.name} {shlex.quote(spec.screen)} {shlex.quote(command)} || true",
                "",
            ]
        )

    return "\n".join(lines)


def _wrap_login(login_body: str) -> str:
    return "\n".join(
        [
            'DEPLOY_DIR="$1"',
            "set -o pipefail",
            "",
            "login_node() {",
            "\n".join("  " + line if line else line for line in login_body.splitlines()),
            "}",
            "",
            f'echo "{MARKER}NODE_BEGIN:login@@"',
            "login_node",
            "rc=$?",
            f'echo "{MARKER}NODE_END:login:${{rc}}@@"',
            f'echo "{MARKER}END@@"',
            "exit 0",
            "",
        ]
    )


def build_remote_script(
    env: Environment,
    *,
    repo: str,
    branch: str,
    data_mover: str,
    start_workers: bool,
    image_file: str | None,
    restart_workers: bool = False,
    install_tool: bool = False,
) -> str:
    login_roles = [WORKER_ROLES["pipeline"]] if start_workers else []
    data_mover_roles = [WORKER_ROLES["download"]] if start_workers else []
    login_body = _node_body(
        env,
        "login",
        repo=repo,
        branch=branch,
        image_file=image_file,
        worker_roles=login_roles,
        restart_workers=restart_workers,
        install_tool=install_tool,
    )
    if not start_workers:
        return _wrap_login(login_body)

    data_mover_body = _node_body(
        env,
        "data_mover",
        repo=repo,
        branch=branch,
        image_file=None,
        worker_roles=data_mover_roles,
        restart_workers=restart_workers,
    )

    return 'DEPLOY_DIR="$1"\n' + _wrap_nodes(login_body, data_mover_body, data_mover)


def _make_tar(script: str, image: Path | None) -> Path:
    handle, name = tempfile.mkstemp(prefix="needle-deploy-", suffix=".tar")
    os.close(handle)
    path = Path(name)
    try:
        with tarfile.open(path, "w") as archive:
            payload = script.encode("utf-8")
            info = tarfile.TarInfo("deploy.sh")
            info.mode = 0o755
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
            if image is not None and image.is_file():
                archive.add(image, arcname="needle.sif")
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def _run_ssh_tar(host: str, tar_path: Path, timeout: float) -> subprocess.CompletedProcess[bytes]:
    remote_command = 'd=$(mktemp -d) && trap \'rm -rf "$d"\' EXIT && tar -xpf - -C "$d" && bash "$d/deploy.sh" "$d"'
    command = [
        "ssh",
        "-o",
        "ConnectTimeout=20",
        "-o",
        "ServerAliveInterval=30",
        host,
        remote_command,
    ]
    try:
        with tar_path.open("rb") as handle:
            return subprocess.run(
                command,
                stdin=handle,
                capture_output=True,
                timeout=None if timeout <= 0 else timeout,
                check=False,
            )
    except FileNotFoundError as exc:
        raise click.ClickException("The 'ssh' executable was not found.") from exc
    except subprocess.TimeoutExpired as exc:
        raise click.ClickException(f"SSH deployment timed out after {timeout:g}s.") from exc


def _common_options(function):
    function = click.option(
        "--source",
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        default=Path.cwd,
        show_default="current directory",
        help="Local git checkout to deploy from.",
    )(function)
    function = click.option(
        "--repo",
        default=None,
        help="Override the git remote URL (default: HTTPS form of origin).",
    )(function)
    function = click.option("--branch", default=None, help="Branch to deploy (default: current local branch).")(
        function
    )
    function = click.option(
        "--ssh-host",
        default="ozstar",
        show_default=True,
        help="SSH host alias for the OzSTAR login node.",
    )(function)
    function = click.option(
        "--data-mover",
        default="data-mover01",
        show_default=True,
        help="Data-mover host used by the download worker.",
    )(function)
    function = click.option(
        "--build-image",
        is_flag=True,
        help="Build the Docker/SIF image locally and upload it to the configured image path.",
    )(function)
    function = click.option(
        "--docker-image",
        default=DEFAULT_DOCKER_IMAGE,
        show_default=True,
        help="Docker image tag to build.",
    )(function)
    function = click.option(
        "--local-image",
        type=click.Path(dir_okay=False, path_type=Path),
        default=None,
        help="Local SIF output path (default: <source>/needle.sif).",
    )(function)
    function = click.option(
        "--env-file",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
        help="Local .env with Prefect credentials, used when waiting for workers.",
    )(function)
    function = click.option(
        "--ssh-timeout",
        type=float,
        default=DEFAULT_SSH_TIMEOUT,
        show_default=True,
        help="Seconds to wait for the SSH deployment (including image upload).",
    )(function)
    function = click.option(
        "--wait",
        "wait_seconds",
        type=float,
        default=DEFAULT_WAIT,
        show_default=True,
        help="Seconds to wait for production workers to report ONLINE (0 disables waiting).",
    )(function)
    function = click.option("--dry-run", is_flag=True, help="Print local and remote commands without executing.")(
        function
    )
    return function


def _deploy(env_name: str, *, start_workers: bool, restart_workers: bool = False, **kwargs) -> None:
    env = ENVIRONMENTS[env_name]
    source: Path = kwargs["source"]
    repo, branch = _git_info(source, kwargs["repo"], kwargs["branch"])
    build_image: bool = kwargs["build_image"]
    local_image: Path = kwargs["local_image"] or (source / "needle.sif")
    dry_run: bool = kwargs["dry_run"]

    click.echo(f"Environment: {env.name}")
    click.echo(f"  remote root: {env.root}")
    click.echo(f"  env file:    {env.env_file}")
    click.echo("  configs:     $NEEDLE_CONFIG / $NEEDLE_CLUSTER_CONFIG from the env file")
    click.echo(f"  repository:  {repo}")
    click.echo(f"  branch:      {branch}")
    click.echo(f"  workers:     {'yes' if start_workers else 'no'}")
    click.echo(f"  image:       {'build and upload' if build_image else 'unchanged'}")
    click.echo()

    if build_image:
        if dry_run:
            click.echo(f"Would run: make needle (in {source})")
            click.echo(f"Would run: apptainer build --force {local_image} docker-daemon://{kwargs['docker_image']}")
        else:
            try:
                _build_image(source, kwargs["docker_image"], local_image)
            except subprocess.CalledProcessError as exc:
                raise click.ClickException(f"Image build failed with exit status {exc.returncode}.") from exc
            if not local_image.is_file():
                raise click.ClickException(f"Image build completed but {local_image} was not created.")

    # The uploaded image is visible at $DEPLOY_DIR/needle.sif inside the remote script.
    remote_image_placeholder = "$DEPLOY_DIR/needle.sif" if build_image else None
    script = build_remote_script(
        env,
        repo=repo,
        branch=branch,
        data_mover=kwargs["data_mover"],
        start_workers=start_workers,
        image_file=remote_image_placeholder,
        restart_workers=restart_workers,
        install_tool=env.name == "dev",
    )

    if dry_run:
        click.echo("Remote deployment script (not executed):")
        click.echo(script)
        return

    tar_path = _make_tar(script, local_image if build_image else None)
    try:
        click.echo(f"Opening one SSH connection to '{kwargs['ssh_host']}' (2FA may be requested once)...")
        result = _run_ssh_tar(kwargs["ssh_host"], tar_path, kwargs["ssh_timeout"])
    finally:
        tar_path.unlink(missing_ok=True)

    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    remote = _parse_remote_output(stdout)

    if stderr.strip():
        click.echo(stderr.strip(), err=True)

    for action in remote.actions:
        click.echo(f"  {action.node}: {action.role} {action.action}")
    for note in remote.notes:
        click.echo(f"  note: {note}")
    for node in remote.nodes.values():
        for error in node.errors:
            click.echo(f"  error ({NODE_LABELS.get(node.name, node.name)}): {error}", err=True)
    for error in remote.errors:
        click.echo(f"  error: {error}", err=True)

    node_errors = [error for node in remote.nodes.values() for error in node.errors]
    if not remote.complete or node_errors or any(node.exit_code not in (0, None) for node in remote.nodes.values()):
        raise click.ClickException("Remote deployment did not complete successfully.")

    if start_workers and kwargs["wait_seconds"] > 0:
        _ensure_prefect_env(kwargs["env_file"])
        click.echo(f"Waiting up to {kwargs['wait_seconds']:g}s for workers to report ONLINE...")
        states = _wait_for_online(timeout=kwargs["wait_seconds"], interval=2.0)
        click.echo()
        click.echo("Prefect worker state:")
        for name in ("pipeline", "download"):
            click.echo(f"  {name}: {_format_prefect(states[name])}")
        if not all(states[name].online for name in ("pipeline", "download")):
            raise SystemExit(1)


@click.group(help="Deploy needle to a production or development environment.")
def deploy():
    """Generic environment deployment."""


@deploy.command()
@_common_options
@click.option("--no-workers", is_flag=True, help="Install code/image only; do not start production workers.")
@click.option("--restart-workers", is_flag=True, help="Stop and restart already-running production screens.")
def prod(**kwargs):
    """Deploy production: update code, optionally build image, and start workers."""
    _deploy(
        "prod",
        start_workers=not kwargs.pop("no_workers"),
        restart_workers=kwargs.pop("restart_workers"),
        **kwargs,
    )


@deploy.command()
@_common_options
def dev(**kwargs):
    """Deploy development: update code under $HOME/software/needle."""
    _deploy("dev", start_workers=False, **kwargs)


BOOTSTRAP_REQUIRED_CHECKS = {
    "screen",
    "uv",
    "workdir",
    "pyproject",
    "env-file",
    "env-load",
    "prefect-env",
    "needle-config",
    "cluster-config",
    "git",
}


def _print_checks(remote) -> None:
    for node_name in SCREEN_NODE_NAMES:
        node = remote.nodes.get(node_name)
        node_checks = [item for item in remote.checks if item.node == node_name]
        if not node_checks and not (node is not None and (node.host or node.errors)):
            continue
        host = node.host if node is not None and node.host else "unknown host"
        click.echo(f"{NODE_LABELS.get(node_name, node_name)} ({host}):")
        if not node_checks:
            click.echo("  no checks were reported")
        for item in node_checks:
            click.echo(f"  {item.name:<16} {item.status}")
        click.echo()

    for note in remote.notes:
        click.echo(f"  note: {note}", err=True)
    for node in remote.nodes.values():
        for error in node.errors:
            click.echo(f"  error ({NODE_LABELS.get(node.name, node.name)}): {error}", err=True)
    for error in remote.errors:
        click.echo(f"  error: {error}", err=True)


def _check_failures(remote, required: set[str]) -> bool:
    failed = [item for item in remote.checks if item.name in required and item.status in {"MISSING", "FAILED"}]
    project_sync_failed = any(item.name == "project-sync" and item.status == "FAILED" for item in remote.checks)
    return bool(failed or project_sync_failed)


@deploy.command()
@click.argument("environment", type=click.Choice(sorted(ENVIRONMENTS)))
@click.option("--ssh-host", default="ozstar", show_default=True, help="SSH host alias for the OzSTAR login node.")
@click.option(
    "--data-mover", default="data-mover01", show_default=True, help="Data-mover host for the download worker."
)
@click.option(
    "--env-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Local .env with Prefect credentials.",
)
@click.option("--ssh-timeout", type=float, default=DEFAULT_SSH_TIMEOUT, show_default=True)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON (production only).")
def status(environment, ssh_host, data_mover, env_file, ssh_timeout, as_json):
    """Show deployment and worker status for prod or dev."""
    env = ENVIRONMENTS[environment]

    if environment != "prod":
        login_body = _build_bootstrap_parts(
            data_mover,
            install=False,
            workdir=env.root,
            env_file=env.env_file,
            require_screen=False,
        )[0]
        result = _run_ssh(ssh_host, _wrap_login(login_body), ssh_timeout)
        remote = _parse_remote_output(result.stdout)
        _print_checks(remote)
        required = BOOTSTRAP_REQUIRED_CHECKS - {"screen"}
        if not remote.complete or _check_failures(remote, required):
            raise SystemExit(1)
        return

    try:
        _ensure_prefect_env(env_file)
    except click.ClickException as exc:
        prefect_states = _states_with_error(str(exc))
    else:
        try:
            prefect_states = _query_prefect()
        except Exception as exc:
            prefect_states = _states_with_error(f"{type(exc).__name__}: {exc}")

    try:
        result = _run_ssh(ssh_host, _build_probe_script(data_mover), ssh_timeout)
        remote = _parse_remote_output(result.stdout)
        if result.returncode != 0:
            reason = f"SSH command exited with status {result.returncode}"
            stderr = result.stderr.strip()
            if stderr:
                reason += f": {stderr.splitlines()[-1]}"
            remote.errors.append(reason)
            for node in remote.nodes.values():
                if not node.errors:
                    node.errors.append(reason)
    except click.ClickException as exc:
        error = str(exc)
        from needle.lib.deployment import RemoteNode

        remote = RemoteOutput(
            nodes={name: RemoteNode(name, errors=[error]) for name in SCREEN_NODE_NAMES},
            actions=[],
            errors=[error],
            complete=False,
        )

    screens = _screen_screens(remote)
    if as_json:
        payload = _status_payload(prefect_states, screens)
        if remote.errors:
            payload["remote_errors"] = remote.errors
        if not remote.complete:
            payload["remote_complete"] = False
        click.echo(json.dumps(payload, indent=2))
    else:
        _print_status(prefect_states, screens)
        for error in remote.errors:
            click.echo(f"remote error: {error}", err=True)

    if not all(_healthy(prefect_states[name], screens[name]) for name in ("pipeline", "download")):
        raise SystemExit(1)


@deploy.command()
@click.argument("environment", type=click.Choice(sorted(ENVIRONMENTS)))
@click.option("--ssh-host", default="ozstar", show_default=True, help="SSH host alias for the OzSTAR login node.")
@click.option(
    "--data-mover", default="data-mover01", show_default=True, help="Data-mover host for the download worker."
)
@click.option(
    "--env-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Local .env with Prefect credentials.",
)
@click.option("--ssh-timeout", type=float, default=DEFAULT_SSH_TIMEOUT, show_default=True)
@click.option("--check", is_flag=True, help="Only check; do not install uv or create work pools.")
@click.option("--dry-run", is_flag=True, help="Show the planned remote scripts without running them.")
def bootstrap(environment, ssh_host, data_mover, env_file, ssh_timeout, check, dry_run):
    """Check prerequisites and optionally install user-space dependencies."""
    env = ENVIRONMENTS[environment]

    if not check and not dry_run and env.workers:
        try:
            _ensure_prefect_env(env_file)
        except click.ClickException as exc:
            click.echo(f"Prefect bootstrap skipped: {exc}", err=True)
        else:
            try:
                created_pools = _ensure_work_pools()
            except Exception as exc:
                click.echo(f"Prefect bootstrap failed: {type(exc).__name__}: {exc}", err=True)
            else:
                for pool in created_pools:
                    click.echo(f"Created missing work pool: {pool}")
                click.echo("Prefect work pools ready.")

    install = not check
    login_body, data_mover_body, script = _build_bootstrap_parts(
        data_mover,
        install=install,
        workdir=env.root,
        env_file=env.env_file,
        require_screen=env.workers,
    )
    if not env.workers:
        script = _wrap_login(login_body)

    if dry_run:
        click.echo("Planned bootstrap actions (not executed):")
        click.echo(f"  1. ssh {ssh_host} bash -s")
        if env.workers:
            click.echo(f"  2. within that session: ssh {data_mover} bash -s")
        click.echo()
        click.echo(f"--- login node script ({ssh_host}) ---")
        click.echo(login_body)
        if env.workers:
            click.echo()
            click.echo(f"--- data-mover script ({data_mover}) ---")
            click.echo(data_mover_body)
        return

    click.echo(f"Opening one SSH connection to '{ssh_host}' (2FA may be requested)...")
    result = _run_ssh(ssh_host, script, ssh_timeout)
    remote = _parse_remote_output(result.stdout)
    _print_checks(remote)

    required = BOOTSTRAP_REQUIRED_CHECKS if env.workers else BOOTSTRAP_REQUIRED_CHECKS - {"screen"}
    if not remote.complete or _check_failures(remote, required):
        raise SystemExit(1)
    click.echo("Bootstrap checks passed.")


@deploy.command()
@click.argument("environment", type=click.Choice(sorted(ENVIRONMENTS)))
@click.option("--ssh-host", default="ozstar", show_default=True, help="SSH host alias for the OzSTAR login node.")
@click.option(
    "--data-mover", default="data-mover01", show_default=True, help="Data-mover host for the download worker."
)
@click.option("--ssh-timeout", type=float, default=DEFAULT_SSH_TIMEOUT, show_default=True)
@click.option("--dry-run", is_flag=True, help="Show the planned remote scripts without running them.")
def teardown(environment, ssh_host, data_mover, ssh_timeout, dry_run):
    """Stop production workers; development has no workers to stop."""
    if environment != "prod":
        click.echo("Development deployments do not have workers.")
        return

    login_body, data_mover_body, script = _build_teardown_parts(data_mover)

    if dry_run:
        click.echo("Planned teardown actions (not executed):")
        click.echo(f"  1. ssh {ssh_host} bash -s")
        click.echo(f"  2. within that session: ssh {data_mover} bash -s")
        click.echo()
        click.echo(f"--- login node script ({ssh_host}) ---")
        click.echo(login_body)
        click.echo()
        click.echo(f"--- data-mover script ({data_mover}) ---")
        click.echo(data_mover_body)
        return

    click.echo(f"Opening one SSH connection to '{ssh_host}' (2FA may be requested once)...")
    result = _run_ssh(ssh_host, script, ssh_timeout)
    remote = _parse_remote_output(result.stdout)

    if result.stderr.strip():
        click.echo(result.stderr.strip(), err=True)

    actions = {action.role: action for action in remote.actions}
    for name in ("pipeline", "download"):
        action = actions.get(name)
        if action is None:
            click.echo(f"  {name}: no teardown action was reported", err=True)
        else:
            click.echo(f"  {name}: {action.action} on {NODE_LABELS.get(action.node, action.node)}")

    node_errors = [error for node in remote.nodes.values() for error in node.errors]
    for node in remote.nodes.values():
        for error in node.errors:
            click.echo(f"  error ({NODE_LABELS.get(node.name, node.name)}): {error}", err=True)
    for error in remote.errors:
        click.echo(f"  error: {error}", err=True)

    if (
        not remote.complete
        or node_errors
        or any(node.exit_code not in (0, None) for node in remote.nodes.values())
        or any(name not in actions for name in ("pipeline", "download"))
    ):
        raise click.ClickException("Remote teardown did not complete successfully.")
