import importlib
import tomllib

import click
import mkdocs_gen_files

DOC_TEXT = """
# CLI Entrypoints
Needle exposes many command line entrypoints - mostly the Python modules that the main pipeline relies on and the pipeline itself.
This doc serves as a list of the available entrypoints with a brief description of their function.
## Available Entrypoints
"""

with open("pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)

entrypoints = pyproject.get("project", {}).get("scripts", {})


def _iter_commands(cmd: click.Command, path: str):
    """Yield (full command path, click.Command) for cmd and, recursively, every subcommand.

    Works uniformly for a plain click.Command (leaf) and a click.Group (e.g. `module`, or
    `clean` with its run/shallow/deep/subtract presets) since Click Groups nest arbitrarily.
    """
    yield path, cmd
    if isinstance(cmd, click.Group):
        for name, sub in cmd.commands.items():
            yield from _iter_commands(sub, f"{path} {name}")


def _write_command(f, path: str, cmd: click.Command, depth: int):
    heading = "#" * min(depth + 3, 6)
    f.write(f"{heading} `{path}`\n\n")

    help_text = (cmd.help or cmd.get_short_help_str() or "").strip()
    if help_text:
        f.write(f"{help_text}\n\n")

    # Skip Click's auto-added --help option; only show real, user-facing params.
    params = [p for p in cmd.params if p.name != "help"]
    if params:
        f.write("| Parameter | Description |\n|---|---|\n")
        for p in params:
            label = p.human_readable_name if isinstance(p, click.Argument) else ", ".join(p.opts)
            desc = (p.help or "").strip().replace("|", "\\|")
            f.write(f"| `{label}` | {desc} |\n")
        f.write("\n")


with mkdocs_gen_files.open("cli_entrypoints.md", "w") as f:
    f.write(DOC_TEXT)

    for name, target in entrypoints.items():
        module_path, func_name = target.split(":")
        f.write(f"## {name}\n\n")

        try:
            module = importlib.import_module(module_path)
            root_cmd = getattr(module, func_name)
        except Exception as e:
            f.write(f"*Could not import entrypoint `{target}`: {e}*\n\n")
            continue

        if not isinstance(root_cmd, click.Command):
            f.write(f"*`{target}` is not a click command/group — skipping.*\n\n")
            continue

        for path, cmd in _iter_commands(root_cmd, name):
            depth = path.count(" ")  # nesting level, used to pick a heading size
            _write_command(f, path, cmd, depth)
