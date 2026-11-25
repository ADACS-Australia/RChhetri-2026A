import click
import pytest
from click.testing import CliRunner

from needle.cli import _resolve_needle_config, cli


def _all_commands(cmd: click.Command, path: str = ""):
    """Recursively yield every command/subcommand path, e.g. 'module flag'."""
    full_path = f"{path} {cmd.name}".strip()
    yield full_path
    if isinstance(cmd, click.Group):
        for _, sub in cmd.commands.items():
            yield from _all_commands(sub, full_path)


@pytest.mark.parametrize("command_path", list(_all_commands(cli)))
def test_help_does_not_crash(command_path):
    runner = CliRunner()
    result = runner.invoke(cli, [*command_path.split()[1:], "--help"])
    assert result.exit_code == 0, result.output


def test_resolve_needle_config_from_env(tmp_path, monkeypatch):
    cfg = tmp_path / ".needle.dev.yaml"
    cfg.write_text("{}")
    monkeypatch.setenv("NEEDLE_CONFIG", str(cfg))

    assert _resolve_needle_config(None) == cfg


def test_resolve_needle_config_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("NEEDLE_CONFIG", raising=False)
    monkeypatch.setattr("needle.cli.Path.home", lambda: tmp_path)

    with pytest.raises(click.ClickException, match="Needle config not found"):
        _resolve_needle_config(None)


def test_validate_uses_needle_config_env(tmp_path, monkeypatch):
    cfg = tmp_path / ".needle.dev.yaml"
    cfg.write_text("{}")
    monkeypatch.setenv("NEEDLE_CONFIG", str(cfg))
    seen = {}

    def fake_validate(source):
        seen["source"] = source
        return True

    monkeypatch.setattr("needle.cli.NeedleConfig.validate", fake_validate)

    result = CliRunner().invoke(cli, ["validate"])

    assert result.exit_code == 0, result.output
    assert seen["source"] == cfg
