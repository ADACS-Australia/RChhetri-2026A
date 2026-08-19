import click
import pytest
from click.testing import CliRunner

from needle.cli import cli


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
