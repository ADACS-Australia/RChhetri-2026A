from datetime import datetime, timezone

from needle.lib import deployment
from needle.lib.deployment import (
    PrefectState,
    RemoteNode,
    RemoteOutput,
    ScreenState,
    build_teardown_parts,
    format_screen,
    load_env_file,
    parse_remote_output,
    parse_screen_listing,
    preflight_lines,
    screen_state_for_role,
    status_payload,
    verdict,
)
from needle.lib.workers import WORKER_ROLES


def _prefect_state(role: str, *, online: bool, heartbeat: datetime | None = None) -> PrefectState:
    spec = WORKER_ROLES[role]
    return PrefectState(
        role=role,
        pool=spec.pool,
        worker=spec.worker,
        exists=True,
        status="ONLINE" if online else "OFFLINE",
        last_heartbeat=heartbeat or datetime.now(timezone.utc),
    )


def test_parse_screen_listing():
    sessions = parse_screen_listing(
        "\n".join(
            [
                "There are screens on:",
                "    12345.needle-prod-pipeline (Detached)",
                "    67890.needle-prod-download (Attached)",
                "    11111.old-worker (Dead ???)",
            ]
        )
    )

    assert sessions["needle-prod-pipeline"].pid == 12345
    assert sessions["needle-prod-pipeline"].running
    assert sessions["needle-prod-download"].state == "Attached"
    assert not sessions["old-worker"].running


def test_parse_remote_output_probe():
    stdout = "\n".join(
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
    remote = parse_remote_output(stdout)

    assert remote.complete
    assert remote.nodes["login"].host == "farnarkle1"
    assert remote.nodes["login"].sessions["needle-prod-pipeline"].pid == 12345
    assert remote.nodes["data_mover"].sessions["needle-prod-download"].state == "Detached"


def test_parse_remote_output_incomplete_and_node_error():
    remote = parse_remote_output(
        "\n".join(
            [
                "@@NEEDLE@@NODE_BEGIN:login@@",
                "@@NEEDLE@@NODE_END:login:255@@",
                "@@NEEDLE@@NODE_BEGIN:data_mover@@",
            ]
        )
    )

    assert not remote.complete
    assert remote.errors
    assert "remote node exited with status 255" in remote.nodes["login"].errors
    assert "remote node did not report completion" in remote.nodes["data_mover"].errors


def test_screen_state_for_role_reports_unknown_on_node_error():
    remote = RemoteOutput(
        nodes={"login": RemoteNode("login", errors=["ssh failed"]), "data_mover": RemoteNode("data_mover")},
        actions=[],
        complete=True,
    )
    state = screen_state_for_role(WORKER_ROLES["pipeline"], remote)

    assert state.error == "ssh failed"
    assert not state.running


def test_verdict_states():
    online = _prefect_state("pipeline", online=True)
    offline = _prefect_state("pipeline", online=False)
    running = ScreenState(role="pipeline", name="needle-prod-pipeline", node="login", exists=True, state="Detached")
    missing = ScreenState(role="pipeline", name="needle-prod-pipeline", node="login")
    dead = ScreenState(role="pipeline", name="needle-prod-pipeline", node="login", exists=True, state="Dead ???")
    unknown = ScreenState(role="pipeline", name="needle-prod-pipeline", node="login", error="boom")

    assert verdict(online, running) == "HEALTHY"
    assert verdict(online, missing).startswith("DEGRADED")
    assert verdict(offline, running).startswith("DEGRADED")
    assert verdict(offline, dead).startswith("STALE")
    assert verdict(offline, missing) == "STOPPED"
    assert verdict(online, unknown) == "UNKNOWN"


def test_format_screen():
    assert format_screen(ScreenState(role="pipeline", name="s", node="login")) == "MISSING"
    assert (
        format_screen(ScreenState(role="pipeline", name="s", node="login", exists=True, state="Detached", pid=42))
        == "RUNNING (pid 42, Detached)"
    )


def test_query_prefect_reads_worker_heartbeat(monkeypatch):
    heartbeat = datetime(2026, 11, 24, 0, 19, 59, tzinfo=timezone.utc)

    class FakeStatus:
        value = "ONLINE"

    class FakeWorker:
        name = "needle-pipeline"
        status = FakeStatus()
        last_heartbeat_time = heartbeat
        id = "worker-id"

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

        async def read_workers_for_work_pool(self, pool):
            return [FakeWorker()] if pool == WORKER_ROLES["pipeline"].pool else []

    monkeypatch.setattr("prefect.client.orchestration.get_client", lambda: FakeClient())
    states = deployment.query_prefect()

    assert states["pipeline"].status == "ONLINE"
    assert states["pipeline"].last_heartbeat == heartbeat
    assert states["pipeline"].worker_id == "worker-id"
    assert states["download"].exists is False


def test_load_env_file_does_not_override(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "export PREFECT_API_URL=https://example.test/api",
                'PREFECT_API_AUTH_STRING="user:pass"',
                "EXISTING=from-file",
                "EMPTY=",
            ]
        )
    )
    environ = {"EXISTING": "from-process"}
    loaded = load_env_file(env_file, environ)

    assert environ["PREFECT_API_URL"] == "https://example.test/api"
    assert environ["PREFECT_API_AUTH_STRING"] == "user:pass"
    assert environ["EXISTING"] == "from-process"
    assert environ["EMPTY"] == ""
    assert "PREFECT_API_URL" in loaded
    assert "EXISTING" not in loaded


def test_preflight_requires_env_config_paths():
    lines = "\n".join(
        preflight_lines(
            "login",
            install=False,
            strict=True,
            workdir="/fred/oz513/software/needle",
            env_file="/home/ksmith/.needle.prod.env",
        )
    )

    assert "NEEDLE_CONFIG is unset or missing" in lines
    assert "NEEDLE_CLUSTER_CONFIG is unset or missing" in lines
    assert ".needle.yaml" not in lines


def test_preflight_can_skip_screen_for_dev():
    lines = "\n".join(
        preflight_lines(
            "login",
            install=False,
            strict=True,
            workdir="$HOME/software/needle",
            env_file="$HOME/.needle.dev.env",
            require_screen=False,
        )
    )

    assert 'if [ "0" -eq 1 ]; then' in lines
    assert "git not found" in lines
    assert 'if [ -d "$HOME/software/needle" ]' in lines
    assert 'if [ -f "$HOME/.needle.dev.env" ]' in lines


def test_build_teardown_script_targets_both_screens():
    login_body, data_mover_body, script = build_teardown_parts("data-mover01")

    assert "stop_screen needle-prod-pipeline" in login_body
    assert "stop_screen needle-prod-download" in data_mover_body
    assert script.count("ssh -o BatchMode=yes") == 1


def test_ensure_work_pools_creates_only_missing(monkeypatch):
    from prefect.exceptions import ObjectNotFound

    created = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

        async def read_work_pool(self, name):
            if name == WORKER_ROLES["download"].pool:
                raise ObjectNotFound(None)
            return object()

        async def create_work_pool(self, work_pool):
            created.append(work_pool.name)

    monkeypatch.setattr("prefect.client.orchestration.get_client", lambda: FakeClient())
    created_by_needle = deployment.ensure_work_pools()

    assert created_by_needle == [WORKER_ROLES["download"].pool]
    assert created == [WORKER_ROLES["download"].pool]


def test_status_payload_contains_no_secrets(monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "https://example.test/api")
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "user:secret")
    states = {
        "pipeline": _prefect_state("pipeline", online=True),
        "download": _prefect_state("download", online=False),
    }
    screens = {
        "pipeline": ScreenState(
            role="pipeline", name="needle-prod-pipeline", node="login", exists=True, state="Detached"
        ),
        "download": ScreenState(role="download", name="needle-prod-download", node="data_mover"),
    }
    payload = status_payload(states, screens)

    assert payload["api_url"] == "https://example.test/api"
    assert "secret" not in str(payload)
    assert payload["workers"]["pipeline"]["healthy"] is True
    assert payload["workers"]["download"]["healthy"] is False
