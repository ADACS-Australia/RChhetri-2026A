import subprocess
import tarfile

import pytest
from click.testing import CliRunner

from needle import deployment
from needle.deployment import ENVIRONMENTS, _http_url, _make_tar, build_remote_script
from needle.lib.deployment import PrefectResourceState, PrefectState


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
    assert dev.image_fallback == "$HOME/software/images/needle.sif"
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
    assert ".needle.prod.yaml" in script
    assert ".needle_cluster.prod.yaml" in script
    assert "uv sync" in script
    assert 'IMAGE_FILE="$DEPLOY_DIR/needle.sif"' in script
    assert "start-workers pipeline" in script
    assert "bash --noprofile --norc -c" in script
    assert "tee -a" in script
    assert "bash -lc" not in script
    assert "data-mover01" in script
    assert script.count("ssh -o BatchMode=yes") == 1

    import base64
    import re

    encoded = re.search(r"printf '%s' ([^ ]+) \| base64 --decode", script)
    assert encoded is not None
    data_mover_script = base64.b64decode(encoded.group(1)).decode()
    assert "start-workers download" in data_mover_script
    assert "git pull" not in data_mover_script
    assert "uv sync" not in data_mover_script
    assert "IMAGE_FILE" not in data_mover_script


def test_build_remote_script_uploads_env():
    script = build_remote_script(
        ENVIRONMENTS["prod"],
        repo="https://github.com/o/r.git",
        branch="main",
        data_mover="data-mover01",
        start_workers=False,
        image_file=None,
        upload_env=True,
    )

    assert "deploy.env" in script
    assert "install -m 0600" in script
    assert script.index("deploy.env") < script.index('. "$ENV_FILE"')


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

    assert 'if [ "1" -eq 1 ] && screen_exists' in script


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


def test_build_remote_script_dev_has_image_fallback():
    script = build_remote_script(
        ENVIRONMENTS["dev"],
        repo="https://github.com/o/r.git",
        branch="main",
        data_mover="data-mover01",
        start_workers=False,
        image_file="$DEPLOY_DIR/needle.sif",
    )

    assert "container.image" in script
    assert 'IMAGE_DEST="$HOME/software/images/needle.sif"' in script


def test_make_tar_contains_extra_config_files(tmp_path):
    config = tmp_path / ".needle.dev.yaml"
    cluster = tmp_path / ".needle_cluster.dev.yaml"
    config.write_text("data: {}\n")
    cluster.write_text("type: local\n")

    archive_path = _make_tar(
        "#!/bin/bash\n",
        None,
        extra_files={config.name: config, cluster.name: cluster},
    )
    try:
        with tarfile.open(archive_path) as archive:
            assert set(archive.getnames()) == {"deploy.sh", config.name, cluster.name}
            assert archive.extractfile(config.name).read() == config.read_bytes()
    finally:
        archive_path.unlink(missing_ok=True)


def test_deploy_uploads_config_files(tmp_path, monkeypatch):
    monkeypatch.delenv("NEEDLE_ENV_FILE", raising=False)
    config = tmp_path / ".needle.prod.yaml"
    cluster = tmp_path / ".needle_cluster.prod.yaml"
    env = tmp_path / ".needle.prod.env"
    config.write_text("data: {}\n")
    cluster.write_text("type: local\n")
    env.write_text("export PREFECT_API_URL=http://example/api\n")
    captured = {}

    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    monkeypatch.setattr(
        deployment,
        "_make_tar",
        lambda script, image, extra_files=None: (captured.update(extra_files=extra_files) or (tmp_path / "deploy.tar")),
    )
    (tmp_path / "deploy.tar").write_bytes(b"")
    monkeypatch.setattr(
        deployment,
        "_run_ssh_tar",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=_remote_output(actions=[("code", "UPDATED"), ("sync", "OK")]).encode(),
            stderr=b"",
        ),
    )

    result = CliRunner().invoke(
        deployment.deploy,
        ["prod", "--no-workers", "--wait", "0", "--source", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert captured["extra_files"] == {
        ".needle.prod.yaml": config,
        ".needle_cluster.prod.yaml": cluster,
        "deploy.env": env,
    }
    assert ".needle.yaml" not in captured["extra_files"]
    assert ".needle_cluster.yaml" not in captured["extra_files"]
    assert ".needle.prod.yaml" in result.output
    assert str(env) in result.output


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


def test_deploy_upload_existing_image(tmp_path, monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    image = tmp_path / "existing.sif"
    image.write_bytes(b"fake-sif")
    monkeypatch.setattr(
        deployment,
        "_run_ssh_tar",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=_remote_output(actions=[("code", "UPDATED"), ("sync", "OK"), ("image", "INSTALLED")]).encode(),
            stderr=b"",
        ),
    )

    result = CliRunner().invoke(
        deployment.deploy,
        ["prod", "--no-workers", "--wait", "0", "--upload-image", str(image)],
    )

    assert result.exit_code == 0, result.output
    assert "image:       upload" in result.output
    assert "login: image INSTALLED" in result.output


def test_deploy_dev(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    monkeypatch.setattr(
        deployment,
        "_run_ssh_tar",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=_remote_output(actions=[("code", "UPDATED"), ("sync", "OK"), ("cli", "INSTALLED")]).encode(),
            stderr=b"",
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["dev"])

    assert result.exit_code == 0, result.output
    assert "remote root: $HOME/software/needle" in result.output
    assert "login: cli INSTALLED" in result.output


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
        "wait_for_online",
        lambda **kwargs: pytest.fail("no workers means no Prefect wait"),
    )

    result = CliRunner().invoke(deployment.deploy, ["prod", "--no-workers", "--wait", "0"])

    assert result.exit_code == 0, result.output
    assert called["host"] == "ozstar"
    assert "login: code UPDATED" in result.output
    assert "login: sync OK" in result.output


def test_deploy_prod_workers_waits_for_prefect(monkeypatch):
    monkeypatch.setattr(deployment, "_git_info", _fake_git_info)
    monkeypatch.setattr(deployment, "ensure_prefect_env", lambda env_file: None)
    monkeypatch.setattr(deployment, "ensure_work_pools", lambda: [])
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
        "wait_for_online",
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
        "image",
        "dev-cli",
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


def _status_prod_output() -> str:
    checks = "\n".join(
        f"@@NEEDLE@@CHECK:login:{name}:OK@@"
        for name in (
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
        )
    )
    return "\n".join(
        [
            "@@NEEDLE@@NODE_BEGIN:login@@",
            "@@NEEDLE@@HOST:login:farnarkle1@@",
            checks,
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


def test_deploy_status_prod(monkeypatch):
    monkeypatch.setattr(deployment, "ensure_prefect_env", lambda env_file: None)
    monkeypatch.setattr(
        deployment,
        "query_prefect",
        lambda: {"pipeline": _online("pipeline"), "download": _online("download")},
    )
    monkeypatch.setattr(
        deployment,
        "query_prefect_resources",
        lambda: [
            PrefectResourceState(kind="work pool", name="download-observation-ozstar", exists=True),
            PrefectResourceState(kind="deployment", name="needle-pipeline-ozstar", exists=True),
        ],
    )
    monkeypatch.setattr(
        deployment,
        "run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=_status_prod_output(), stderr=""
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["status", "prod"])

    assert result.exit_code == 0, result.output
    assert "HEALTHY" in result.output
    assert "needle-prod-pipeline" in result.output


def test_deploy_status_dev(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=_check_output(), stderr=""
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["status", "dev"])

    assert result.exit_code == 0, result.output
    assert "needle-config" in result.output
    assert "OK" in result.output


def test_deploy_status_dev_fails_without_image(monkeypatch):
    stdout = _check_output().replace("CHECK:login:image:OK", "CHECK:login:image:MISSING")
    monkeypatch.setattr(
        deployment,
        "run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr=""),
    )

    result = CliRunner().invoke(deployment.deploy, ["status", "dev"])

    assert result.exit_code == 1
    assert "image" in result.output


def test_deploy_status_prod_fails_without_deployment(monkeypatch):
    monkeypatch.setattr(deployment, "ensure_prefect_env", lambda env_file: None)
    monkeypatch.setattr(
        deployment,
        "query_prefect",
        lambda: {"pipeline": _online("pipeline"), "download": _online("download")},
    )
    monkeypatch.setattr(
        deployment,
        "query_prefect_resources",
        lambda: [
            PrefectResourceState(kind="work pool", name="download-observation-ozstar", exists=True),
            PrefectResourceState(kind="deployment", name="needle-pipeline-ozstar", exists=False, error="not found"),
        ],
    )
    monkeypatch.setattr(
        deployment,
        "run_ssh",
        lambda host, script, timeout: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=_status_prod_output(), stderr=""
        ),
    )

    result = CliRunner().invoke(deployment.deploy, ["status", "prod"])

    assert result.exit_code == 1
    assert "needle-pipeline-ozstar: MISSING" in result.output


def test_deploy_teardown_dev_does_not_connect(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "run_ssh",
        lambda *args, **kwargs: pytest.fail("dev teardown must not open SSH"),
    )

    result = CliRunner().invoke(deployment.deploy, ["teardown", "dev"])

    assert result.exit_code == 0
    assert "do not have workers" in result.output


def test_deploy_teardown_prod(monkeypatch):
    monkeypatch.setattr(
        deployment,
        "run_ssh",
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
