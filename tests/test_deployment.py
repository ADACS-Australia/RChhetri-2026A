import subprocess
import tarfile

import pytest
from click.testing import CliRunner

from needle import deployment
from needle.deployment import ENVIRONMENTS, _http_url, _make_tar, build_remote_script
from needle.lib.deployment import PrefectState


def _fake_git_info(source, repo, branch):
    return "git@example.test:needle.git", "main"


def _remote_output(actions=(), complete=True, exit_code=0):
    lines = [
        "@@NEEDLE@@NODE_BEGIN:login@@",
        "@@NEEDLE@@HOST:login:farnarkle1@@",
    ]
    for role, action in actions:
        if role not in {"download"}:
            lines.append(f"@@NEEDLE@@ACTION:login:{role}:{action}@@")
    lines.append(f"@@NEEDLE@@NODE_END:login:{exit_code}@@")
    if any(role == "download" for role, _ in actions):
        lines.extend(
            [
                "@@NEEDLE@@NODE_BEGIN:data_mover@@",
                "@@NEEDLE@@HOST:data_mover:data-mover01@@",
            ]
        )
        for role, action in actions:
            if role == "download":
                lines.append(f"@@NEEDLE@@ACTION:data_mover:{role}:{action}@@")
        lines.append(f"@@NEEDLE@@NODE_END:data_mover:{exit_code}@@")
    if complete:
        lines.append("@@NEEDLE@@END@@")
    return "\n".join(lines)


def _online(role):
    spec = deployment.WORKER_ROLES[role]
    return PrefectState(role=role, pool=spec.pool, worker=spec.worker, exists=True, status="ONLINE")


def test_environment_layouts():
    prod = ENVIRONMENTS["prod"]
    dev = ENVIRONMENTS["dev"]

    assert prod.root == "/fred/oz513/software/needle"
    assert prod.env_file == "$HOME/.needle.prod.env"
    assert prod.workers is True

    assert dev.root == "$HOME/software/needle"
    assert dev.env_file == "$HOME/.needle.dev.env"
    assert dev.workers is False


def test_http_url_converts_ssh_remotes():
    assert _http_url("git@github.com:ADACS-Australia/RChhetri-2026A.git") == (
        "https://github.com/ADACS-Australia/RChhetri-2026A.git"
    )
    assert _http_url("ssh://git@github.com/ADACS-Australia/RChhetri-2026A.git") == (
        "https://github.com/ADACS-Australia/RChhetri-2026A.git"
    )
    assert _http_url("git://github.com/owner/repo.git") == "https://github.com/owner/repo.git"
    assert _http_url("https://github.com/owner/repo.git") == "https://github.com/owner/repo.git"


def test_build_remote_script_prod_workers():
    script = build_remote_script(
        ENVIRONMENTS["prod"],
        repo="git@example.test:needle.git",
        branch="main",
        data_mover="data-mover01",
        start_workers=True,
        image_file="$DEPLOY_DIR/needle.sif",
    )

    assert 'ROOT="/fred/oz513/software/needle"' in script
    assert 'ENV_FILE="$HOME/.needle.prod.env"' in script
    assert "DEFAULT_NEEDLE_CONFIG" not in script
    assert "DEFAULT_CLUSTER_CONFIG" not in script
    assert "NEEDLE_CONFIG is not set in $ENV_FILE" in script
    assert "NEEDLE_CLUSTER_CONFIG is not set in $ENV_FILE" in script
    assert "git clone --branch" in script
    assert 'remote set-url origin "$REPO"' in script
    assert "pull --ff-only" in script
    assert "uv sync" in script
    assert 'IMAGE_FILE="$DEPLOY_DIR/needle.sif"' in script
    assert "start-workers pipeline" in script
    assert "data-mover01" in script
    assert script.count("ssh -o BatchMode=yes") == 1

    import base64
    import re

    encoded = re.search(r"printf '%s' ([^ ]+) \| base64 --decode", script)
    assert encoded is not None
    data_mover_script = base64.b64decode(encoded.group(1)).decode()
    assert "start-workers download" in data_mover_script


def test_build_remote_script_can_restart_workers():
    script = build_remote_script(
        ENVIRONMENTS["prod"],
        repo="git@example.test:needle.git",
        branch="main",
        data_mover="data-mover01",
        start_workers=True,
        image_file=None,
        restart_workers=True,
    )

    assert "RESTART_WORKERS=1" in script
    assert 'if [ "$RESTART_WORKERS" -eq 1 ] && screen_exists' in script


def test_build_remote_script_dev_is_login_only():
    script = build_remote_script(
        ENVIRONMENTS["dev"],
        repo="git@example.test:needle.git",
        branch="main",
        data_mover="data-mover01",
        start_workers=False,
        image_file=None,
    )

    assert 'ROOT="$HOME/software/needle"' in script
    assert 'ENV_FILE="$HOME/.needle.dev.env"' in script
    assert "start_workers" not in script
    assert "data-mover01" not in script
    assert "ssh -o BatchMode=yes" not in script
    assert "NODE_BEGIN:login" in script


def test_make_tar_contains_script_and_image(tmp_path):
    image = tmp_path / "needle.sif"
    image.write_bytes(b"fake-sif")

    archive_path = _make_tar("#!/bin/bash\necho hi\n", image)
    try:
        with tarfile.open(archive_path) as archive:
            names = archive.getnames()
            assert names == ["deploy.sh", "needle.sif"]
            assert archive.extractfile("deploy.sh").read().startswith(b"#!/bin/bash")
            assert archive.extractfile("needle.sif").read() == b"fake-sif"
    finally:
        archive_path.unlink(missing_ok=True)


def test_deploy_prod_dry_run(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    monkeypatch.setattr(
        deployment,
        "_run_ssh_tar",
        lambda *args, **kwargs: pytest.fail("dry-run must not open SSH"),
    )

    result = CliRunner().invoke(deployment.deploy, ["prod", "--dry-run", "--no-workers"])

    assert result.exit_code == 0, result.output
    assert "remote root: /fred/oz513/software/needle" in result.output
    assert "workers:     no" in result.output
    assert "git clone --branch" in result.output
    assert "uv tool install --editable" not in result.output


def test_deploy_prod_build_image_dry_run(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    monkeypatch.setattr(
        deployment,
        "_build_image",
        lambda *args, **kwargs: pytest.fail("dry-run must not build"),
    )

    result = CliRunner().invoke(
        deployment.deploy,
        ["prod", "--dry-run", "--no-workers", "--build-image"],
    )

    assert result.exit_code == 0, result.output
    assert "Would run: make needle" in result.output
    assert "apptainer build --force" in result.output
    assert 'IMAGE_FILE="$DEPLOY_DIR/needle.sif"' in result.output


def test_deploy_dev_dry_run(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)

    result = CliRunner().invoke(deployment.deploy, ["dev", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "remote root: $HOME/software/needle" in result.output
    assert ".needle.dev.env" in result.output
    assert "data-mover01" not in result.output
    assert 'uv tool install --editable "$ROOT" --force' in result.output


def test_deploy_prod_no_workers(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    called = {}

    def run_ssh(host, tar_path, timeout):
        called["host"] = host
        return subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=_remote_output(actions=[("code", "UPDATED"), ("sync", "OK")]).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(deployment, "_run_ssh_tar", run_ssh)
    monkeypatch.setattr(
        deployment,
        "_wait_for_online",
        lambda **kwargs: pytest.fail("no workers means no Prefect wait"),
    )

    result = CliRunner().invoke(deployment.deploy, ["prod", "--no-workers", "--wait", "0"])

    assert result.exit_code == 0, result.output
    assert called["host"] == "ozstar"
    assert "login: code UPDATED" in result.output
    assert "login: sync OK" in result.output


def test_deploy_prod_workers_waits_for_prefect(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    monkeypatch.setattr(deployment, "_ensure_prefect_env", lambda env_file: None)
    monkeypatch.setattr(
        deployment,
        "_run_ssh_tar",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=_remote_output(
                actions=[
                    ("code", "UPDATED"),
                    ("sync", "OK"),
                    ("pipeline", "STARTED"),
                    ("download", "STARTED"),
                ]
            ).encode(),
            stderr=b"",
        ),
    )
    monkeypatch.setattr(
        deployment,
        "_wait_for_online",
        lambda **kwargs: {"pipeline": _online("pipeline"), "download": _online("download")},
    )

    result = CliRunner().invoke(deployment.deploy, ["prod"])

    assert result.exit_code == 0, result.output
    assert "login: pipeline STARTED" in result.output
    assert "data_mover: download STARTED" in result.output
    assert "Prefect worker state" in result.output


def test_deploy_fails_on_remote_error(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    stdout = "\n".join(
        [
            "@@NEEDLE@@NODE_BEGIN:login@@",
            "@@NEEDLE@@ERROR:login:git pull failed@@",
            "@@NEEDLE@@NODE_END:login:1@@",
            "@@NEEDLE@@END@@",
        ]
    )
    monkeypatch.setattr(
        deployment,
        "_run_ssh_tar",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout.encode(), stderr=b""),
    )

    result = CliRunner().invoke(deployment.deploy, ["prod", "--no-workers"])

    assert result.exit_code == 1
    assert "git pull failed" in result.output


def _probe_output() -> str:
    return "\n".join(
        [
            "@@NEEDLE@@NODE_BEGIN:login@@",
            "@@NEEDLE@@HOST:login:farnarkle1@@",
            "@@NEEDLE@@SCREEN_BEGIN:login@@",
            "    12345.needle-prod-pipeline (Detached)",
            "@@NEEDLE@@SCREEN_END:login@@",
            "@@NEEDLE@@NODE_END:login:0@@",
            "@@NEEDLE@@NODE_BEGIN:data_mover@@",
            "@@NEEDLE@@HOST:data_mover:data-mover01@@",
            "@@NEEDLE@@SCREEN_BEGIN:data_mover@@",
            "    67890.needle-prod-download (Detached)",
            "@@NEEDLE@@SCREEN_END:data_mover@@",
            "@@NEEDLE@@NODE_END:data_mover:0@@",
            "@@NEEDLE@@END@@",
        ]
    )


def _check_output() -> str:
    checks = [
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
    ]
    lines = ["@@NEEDLE@@NODE_BEGIN:login@@", "@@NEEDLE@@HOST:login:farnarkle1@@"]
    lines.extend(f"@@NEEDLE@@CHECK:login:{name}:OK@@" for name in checks)
    lines.extend(
        [
            "@@NEEDLE@@CHECK:login:project-sync:SKIPPED@@",
            "@@NEEDLE@@NODE_END:login:0@@",
            "@@NEEDLE@@END@@",
        ]
    )
    return "\n".join(lines)


def test_deploy_status_prod(monkeypatch):
    monkeypatch.setattr(deployment, "_ensure_prefect_env", lambda env_file: None)
    monkeypatch.setattr(
        deployment,
        "_query_prefect",
        lambda: {"pipeline": _online("pipeline"), "download": _online("download")},
    )
    monkeypatch.setattr(
        deployment,
        "_run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=_probe_output(), stderr=""
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["status", "prod"])

    assert result.exit_code == 0, result.output
    assert "HEALTHY" in result.output
    assert "needle-prod-pipeline" in result.output


def test_deploy_status_dev(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "_run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=_check_output(), stderr=""
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["status", "dev"])

    assert result.exit_code == 0, result.output
    assert "needle-config" in result.output
    assert "OK" in result.output


def test_deploy_teardown_dev_does_not_connect(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "_run_ssh",
        lambda *args, **kwargs: pytest.fail("dev teardown must not open SSH"),
    )

    result = CliRunner().invoke(deployment.deploy, ["teardown", "dev"])

    assert result.exit_code == 0
    assert "do not have workers" in result.output


def test_deploy_teardown_prod(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "_run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=_remote_output(actions=[("pipeline", "STOPPED"), ("download", "STOPPED")]),
            stderr="",
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["teardown", "prod"])

    assert result.exit_code == 0, result.output
    assert "pipeline: STOPPED" in result.output
    assert "download: STOPPED" in result.output


def test_deploy_bootstrap_check_prod(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "_ensure_work_pools",
        lambda: pytest.fail("--check must not create work pools"),
    )
    monkeypatch.setattr(
        deployment,
        "_run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=_check_output(), stderr=""
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["bootstrap", "prod", "--check"])

    assert result.exit_code == 0, result.output
    assert "Bootstrap checks passed" in result.output


def test_build_image_runs_make_and_apptainer(tmp_path, monkeypatch):
    calls = []

    run_kwargs = []

    def fake_run(command, **kwargs):
        calls.append(command)
        run_kwargs.append(kwargs)
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr(deployment, "which", lambda command: f"/usr/bin/{command}")
    monkeypatch.setattr(deployment.subprocess, "run", fake_run)
    image = tmp_path / "needle.sif"

    deployment._build_image(tmp_path, "example/needle:latest", image)

    assert calls[0] == ["make", "needle"]
    assert run_kwargs[0]["env"]["NEEDLE_IMAGE"] == "example/needle:latest"
    assert calls[1][:4] == ["apptainer", "build", "--force", str(image)]
    assert calls[1][4] == "docker-daemon://example/needle:latest"
