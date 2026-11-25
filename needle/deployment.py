"""Generic environment deployment for needle.

Two deployment environments are supported:

``prod``
    Code at ``/fred/oz513/software/needle``, env file at ``$HOME/.needle.prod.env``, and production workers in
    screen sessions on the login node and data mover.

``dev``
    Code at ``$HOME/software/needle``, env file at ``$HOME/.needle.dev.env``, and no workers.  A development
    deployment also installs an editable ``needle`` command into ``~/.local/bin``.

The env file is the source of truth for the config paths through ``NEEDLE_CONFIG`` and
``NEEDLE_CLUSTER_CONFIG``.  Deployments update a remote git checkout, run ``uv sync``, create configured
directories, optionally install a SIF image, and—for production—start or update worker screens.

The data mover shares the login node's filesystem, so only the login node performs the git update, dependency
sync, and image installation.  The data mover only starts its worker from the shared checkout.
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
from typing import Iterable, Mapping
from urllib.parse import urlparse

import click

from needle.lib.deployment import (
    MARKER,
    NODE_LABELS,
    SCREEN_NODE_NAMES,
    RemoteOutput,
    build_teardown_parts,
    ensure_prefect_env,
    ensure_work_pools,
    format_prefect,
    healthy,
    parse_remote_output,
    preflight_lines,
    print_status,
    probe_body,
    query_prefect,
    query_prefect_resources,
    render_template,
    run_ssh,
    screen_screens,
    states_with_error,
    status_payload,
    wait_for_online,
    wrap_nodes,
)
from needle.lib.workers import WORKER_ROLES, WorkerRole

DEFAULT_SSH_TIMEOUT = 3600.0
DEFAULT_WAIT = 60.0


@dataclass(frozen=True)
class Environment:
    """Static deployment settings for one environment."""

    name: str
    "Environment name used on the command line (``prod`` or ``dev``)."

    root: str
    "Remote checkout path. May contain shell-expanded variables such as ``$HOME``."

    env_file: str
    "Remote env file sourced by deployment and worker commands."

    workers: bool
    "Whether this environment runs screen-managed Prefect workers."

    image_fallback: str | None = None
    "SIF destination used when the cluster config has no ``container.image``."


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
        image_fallback="$HOME/software/images/needle.sif",
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


## Local git helpers ##


def _git_output(source: Path, *args: str) -> str:
    """Run a git command in ``source`` and return its stripped stdout."""
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
    """Resolve the remote URL and branch to deploy from a local git checkout.

    When ``repo`` is omitted, the ``origin`` URL is converted to an HTTPS URL
    where possible so remote nodes do not need SSH credentials for the repository.
    """
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


def _local_env_file(env_name: str, source: Path) -> Path | None:
    """Return the canonical local env file for an environment, if present."""
    explicit = os.environ.get("NEEDLE_ENV_FILE")
    if explicit:
        path = Path(explicit)
        return path if path.is_file() else None
    candidate = source / f".needle.{env_name}.env"
    return candidate if candidate.is_file() else None


def _local_config_files(env_name: str, source: Path) -> dict[str, Path]:
    """Return the environment-specific local YAML configs that exist."""
    names = (f".needle.{env_name}.yaml", f".needle_cluster.{env_name}.yaml")
    return {name: source / name for name in names if (source / name).is_file()}


## Remote shell script ##


def _uv_install_lines(node: str) -> list[str]:
    """Return shell lines that ensure ``uv`` is available on a node."""
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
    """Return shell lines that ensure ``git`` is available on a node."""
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
    """Return shell helpers for screen inspection, cleanup, and worker startup."""
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
        *render_template("screen.sh", node=node).splitlines(),
    ]


def _env_setup_lines(
    env: Environment,
    node: str,
    config_install: str = "",
    env_install: str = "",
) -> list[str]:
    """Return shell lines that load the env file and validate deployment paths.

    The env file is allowed to return a non-zero status (for example because a
    module load failed); deployment continues as long as the required variables
    and config files are present.
    """
    return render_template(
        "env_setup.sh",
        node=node,
        root=env.root,
        env_file=env.env_file,
        config_install=config_install,
        env_install=env_install,
    ).splitlines()


def _worker_command_line(spec: WorkerRole) -> str:
    """Return the shell command run inside a worker's screen session."""
    return (
        f'cd "$ROOT" || exit 1; '
        'export PATH="$HOME/.local/bin:$PATH"; '
        'set -a; . "$ENV_FILE"; env_rc=$?; set +a; '
        'if [ "$env_rc" -ne 0 ]; then echo "warning: $ENV_FILE returned $env_rc" >&2; fi; '
        f"exec uv run needle start-workers {spec.name}"
    )


def _worker_start_lines(spec: WorkerRole, restart_workers: bool) -> list[str]:
    """Return shell lines that optionally restart and then start a worker screen."""
    restart = "1" if restart_workers else "0"
    return [
        f"# {spec.name}",
        f'if [ "{restart}" -eq 1 ] && screen_exists {shlex.quote(spec.screen)}; then '
        f"stop_screen {shlex.quote(spec.screen)}; fi",
        f"start_worker {spec.name} {shlex.quote(spec.screen)} {shlex.quote(_worker_command_line(spec))} || true",
        "",
    ]


def _worker_only_body(
    env: Environment,
    node: str,
    *,
    worker_roles: Iterable[WorkerRole],
    restart_workers: bool = False,
) -> str:
    """Body for a node that shares the login node's checkout and environment."""
    roles = list(worker_roles)
    lines: list[str] = [
        f'echo "{MARKER}HOST:{node}:$(hostname 2>/dev/null || uname -n)@@"',
        *_uv_install_lines(node),
    ]
    if roles:
        lines.extend(_screen_lines(node))
    lines.extend(_env_setup_lines(env, node))
    lines.extend(
        [
            'if [ ! -d "$ROOT" ]; then',
            f'  echo "{MARKER}ERROR:{node}:work directory not found: $ROOT (expected shared with the login node)@@"',
            "  return 1",
            "fi",
            "",
        ]
    )
    for spec in roles:
        lines.extend(_worker_start_lines(spec, restart_workers))
    return "\n".join(lines)


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
    upload_env: bool = False,
) -> str:
    """Generate the login-node shell body.

    The login node owns the shared checkout: it updates git, runs ``uv sync``,
    creates configured directories, optionally installs an image and dev CLI,
    and starts any workers assigned to this node.
    """
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
            f"REPO={q(repo)}",
            f"BRANCH={q(branch)}",
            "",
        ]
    )
    config_install = render_template("config_install.sh", env_name=env.name, node=node)
    env_install = render_template("env_install.sh", node=node) if upload_env else ""
    lines.extend(_env_setup_lines(env, node, config_install, env_install))
    lines.extend(
        [
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
                '  IMAGE_DEST=""',
                f'  if ! IMAGE_DEST=$(uv run python -c {q(_python_exec(_IMAGE_CODE))} "$NEEDLE_CLUSTER_CONFIG" 2>/dev/null); then',
                f'    echo "{MARKER}NOTE:{node}:could not read container.image from $NEEDLE_CLUSTER_CONFIG@@"',
                '    IMAGE_DEST=""',
                "  fi",
                '  if [ -z "$IMAGE_DEST" ]; then',
                f'    IMAGE_DEST="{env.image_fallback or ""}"',
                "  fi",
                '  if [ -z "$IMAGE_DEST" ]; then',
                f'    echo "{MARKER}ERROR:{node}:container.image is not set and no image fallback is configured@@"',
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
        lines.extend(_worker_start_lines(spec, restart_workers))

    return "\n".join(lines)


def _wrap_login(login_body: str) -> str:
    """Wrap a login-only shell body in the remote marker protocol."""
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
    upload_env: bool = False,
) -> str:
    """Build the remote shell script for a complete environment deployment.

    The login node performs code/image installation.  When workers are enabled,
    a second body starts the download worker on the data mover using the shared
    checkout rather than pulling or syncing separately.
    """
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
        upload_env=upload_env,
    )
    if not start_workers:
        return _wrap_login(login_body)

    data_mover_body = _worker_only_body(
        env,
        "data_mover",
        worker_roles=data_mover_roles,
        restart_workers=restart_workers,
    )

    return 'DEPLOY_DIR="$1"\n' + wrap_nodes(login_body, data_mover_body, data_mover)


def _make_tar(
    script: str,
    image: Path | None,
    extra_files: Mapping[str, Path] | None = None,
) -> Path:
    """Create a temporary tar containing ``deploy.sh``, optional files, and a SIF."""
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
            for arcname, file_path in (extra_files or {}).items():
                if file_path.is_file():
                    archive.add(file_path, arcname=arcname)
            if image is not None and image.is_file():
                archive.add(image, arcname="needle.sif")
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def _run_ssh_tar(host: str, tar_path: Path, timeout: float) -> subprocess.CompletedProcess[bytes]:
    """Stream a deployment tar to ``host`` and execute its embedded script."""
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
    """Attach options shared by the ``deploy prod`` and ``deploy dev`` commands."""
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
        "--upload-image",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
        help="Upload an existing SIF.",
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
    return function


def _deploy(env_name: str, *, start_workers: bool, restart_workers: bool = False, **kwargs) -> None:
    """Run one environment deployment end to end.

    This resolves local git information, uploads an optional existing image and
    local config files, streams the remote script over SSH, and waits for
    production workers when requested.
    """
    env = ENVIRONMENTS[env_name]
    source: Path = kwargs["source"]
    repo, branch = _git_info(source, kwargs["repo"], kwargs["branch"])
    upload_image: Path | None = kwargs["upload_image"]
    image_to_upload = upload_image
    image_mode = f"upload {upload_image}" if upload_image is not None else "unchanged"

    local_env = _local_env_file(env.name, source)
    config_files = _local_config_files(env.name, source)
    extra_files = dict(config_files)
    if local_env is not None:
        extra_files["deploy.env"] = local_env

    click.echo(f"Environment: {env.name}")
    click.echo(f"  remote root: {env.root}")
    click.echo(f"  env file:    {env.env_file}")
    click.echo("  configs:     $NEEDLE_CONFIG / $NEEDLE_CLUSTER_CONFIG from the env file")
    click.echo(f"  upload env:  {local_env or 'none (using existing remote env)'}")
    click.echo(f"  upload files: {', '.join(config_files) if config_files else 'none'}")
    click.echo(f"  repository:  {repo}")
    click.echo(f"  branch:      {branch}")
    click.echo(f"  workers:     {'yes' if start_workers else 'no'}")
    click.echo(f"  image:       {image_mode}")
    click.echo()

    if env.workers and start_workers:
        try:
            ensure_prefect_env(local_env)
            created_pools = ensure_work_pools()
        except Exception as exc:
            raise click.ClickException(f"Could not prepare Prefect work pools: {type(exc).__name__}: {exc}") from exc
        for pool in created_pools:
            click.echo(f"Created missing work pool: {pool}")

    # The uploaded image is visible at $DEPLOY_DIR/needle.sif inside the remote script.
    remote_image_placeholder = "$DEPLOY_DIR/needle.sif" if image_to_upload is not None else None
    script = build_remote_script(
        env,
        repo=repo,
        branch=branch,
        data_mover=kwargs["data_mover"],
        start_workers=start_workers,
        image_file=remote_image_placeholder,
        restart_workers=restart_workers,
        install_tool=env.name == "dev",
        upload_env=local_env is not None,
    )

    tar_path = _make_tar(script, image_to_upload, extra_files=extra_files)
    try:
        click.echo(f"Opening one SSH connection to '{kwargs['ssh_host']}' (2FA may be requested once)...")
        result = _run_ssh_tar(kwargs["ssh_host"], tar_path, kwargs["ssh_timeout"])
    finally:
        tar_path.unlink(missing_ok=True)

    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    remote = parse_remote_output(stdout)

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
        ensure_prefect_env(None)
        click.echo(f"Waiting up to {kwargs['wait_seconds']:g}s for workers to report ONLINE...")
        states = wait_for_online(timeout=kwargs["wait_seconds"], interval=2.0)
        click.echo()
        click.echo("Prefect worker state:")
        for name in ("pipeline", "download"):
            click.echo(f"  {name}: {format_prefect(states[name])}")
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
    """Deploy production: update code, upload an optional image, and start workers."""
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


def _print_checks(remote) -> None:
    """Print per-node prerequisite checks, notes, and remote errors."""
    for node_name in SCREEN_NODE_NAMES:
        node = remote.nodes.get(node_name)
        node_checks = [item for item in remote.checks if item.node == node_name]
        if not node_checks and not (node is not None and node.errors):
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
    """Return whether any required check is absent, failed, or sync failed."""
    present = {item.name for item in remote.checks}
    missing = required - present
    failed = [item for item in remote.checks if item.name in required and item.status in {"MISSING", "FAILED"}]
    project_sync_failed = any(item.name == "project-sync" and item.status == "FAILED" for item in remote.checks)
    return bool(missing or failed or project_sync_failed)


STATUS_REQUIRED_CHECKS = {
    "screen",
    "uv",
    "git",
    "workdir",
    "pyproject",
    "env-file",
    "env-load",
    "prefect-env",
    "needle-config",
    "cluster-config",
    "image",
}


def _status_script(env: Environment, data_mover: str) -> str:
    """Build the remote prerequisite/status script for an environment."""
    login_checks = "\n".join(
        preflight_lines(
            "login",
            install=False,
            strict=False,
            workdir=env.root,
            env_file=env.env_file,
            require_screen=env.workers,
            require_cli=not env.workers,
            fallback_image=env.image_fallback,
        )
    )
    if not env.workers:
        return _wrap_login(login_checks)
    login_body = login_checks + "\n" + probe_body("login")
    return wrap_nodes(login_body, probe_body("data_mover"), data_mover)


def _print_resources(resources) -> None:
    """Print Prefect work-pool/deployment existence checks."""
    click.echo("Prefect resources:")
    for resource in resources:
        if resource.exists:
            click.echo(f"  {resource.kind:<12} {resource.name}: OK")
        else:
            click.echo(f"  {resource.kind:<12} {resource.name}: MISSING ({resource.error})")


@deploy.command()
@click.argument("environment", type=click.Choice(sorted(ENVIRONMENTS)))
@click.option("--ssh-host", default="ozstar", show_default=True, help="SSH host alias for the OzSTAR login node.")
@click.option(
    "--data-mover", default="data-mover01", show_default=True, help="Data-mover host for the download worker."
)
@click.option("--ssh-timeout", type=float, default=DEFAULT_SSH_TIMEOUT, show_default=True)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def status(environment, ssh_host, data_mover, ssh_timeout, as_json):
    """Check whether an environment is ready to run."""
    env = ENVIRONMENTS[environment]

    try:
        result = run_ssh(ssh_host, _status_script(env, data_mover), ssh_timeout)
        remote = parse_remote_output(result.stdout)
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
        from needle.lib.deployment import RemoteNode

        error = str(exc)
        remote = RemoteOutput(
            nodes={name: RemoteNode(name, errors=[error]) for name in SCREEN_NODE_NAMES},
            actions=[],
            errors=[error],
            complete=False,
        )

    _print_checks(remote)
    required = set(STATUS_REQUIRED_CHECKS)
    if not env.workers:
        required.discard("screen")
        required.add("dev-cli")
    missing_checks = sorted(required - {item.name for item in remote.checks})
    if missing_checks:
        click.echo(f"  missing checks: {', '.join(missing_checks)}", err=True)
    checks_failed = not remote.complete or _check_failures(remote, required)

    if not env.workers:
        if as_json:
            click.echo(
                json.dumps(
                    {
                        "environment": env.name,
                        "checks": [{"node": c.node, "name": c.name, "status": c.status} for c in remote.checks],
                        "complete": remote.complete,
                        "ok": not checks_failed,
                    },
                    indent=2,
                )
            )
        if checks_failed:
            raise SystemExit(1)
        return

    resources = []
    resource_error = None
    local_env = _local_env_file(env.name, Path.cwd())
    try:
        ensure_prefect_env(local_env)
        prefect_states = query_prefect()
        resources = query_prefect_resources()
    except Exception as exc:
        resource_error = f"{type(exc).__name__}: {exc}"
        prefect_states = states_with_error(resource_error)

    screens = screen_screens(remote)
    if as_json:
        payload = status_payload(prefect_states, screens)
        payload["environment"] = env.name
        payload["checks"] = [{"node": c.node, "name": c.name, "status": c.status} for c in remote.checks]
        payload["resources"] = [
            {"kind": r.kind, "name": r.name, "exists": r.exists, "error": r.error} for r in resources
        ]
        if resource_error:
            payload["resource_error"] = resource_error
        if remote.errors:
            payload["remote_errors"] = remote.errors
        if not remote.complete:
            payload["remote_complete"] = False
        click.echo(json.dumps(payload, indent=2))
    else:
        if resource_error:
            click.echo(f"Prefect resources: UNKNOWN ({resource_error})", err=True)
        else:
            _print_resources(resources)
            click.echo()
        print_status(prefect_states, screens)
        for error in remote.errors:
            click.echo(f"remote error: {error}", err=True)

    resources_ok = bool(resources) and all(r.exists for r in resources)
    workers_ok = all(healthy(prefect_states[name], screens[name]) for name in ("pipeline", "download"))
    if checks_failed or not resources_ok or not workers_ok:
        raise SystemExit(1)


@deploy.command()
@click.argument("environment", type=click.Choice(sorted(ENVIRONMENTS)))
@click.option("--ssh-host", default="ozstar", show_default=True, help="SSH host alias for the OzSTAR login node.")
@click.option(
    "--data-mover", default="data-mover01", show_default=True, help="Data-mover host for the download worker."
)
@click.option("--ssh-timeout", type=float, default=DEFAULT_SSH_TIMEOUT, show_default=True)
def teardown(environment, ssh_host, data_mover, ssh_timeout):
    """Stop production workers; development has no workers to stop."""
    if environment != "prod":
        click.echo("Development deployments do not have workers.")
        return

    _, _, script = build_teardown_parts(data_mover)

    click.echo(f"Opening one SSH connection to '{ssh_host}' (2FA may be requested once)...")
    result = run_ssh(ssh_host, script, ssh_timeout)
    remote = parse_remote_output(result.stdout)

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
