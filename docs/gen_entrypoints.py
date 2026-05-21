import tomllib
import importlib
import argparse
from unittest.mock import patch

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

with mkdocs_gen_files.open("cli_entrypoints.md", "w") as f:
    f.write(DOC_TEXT)
    for name, target in entrypoints.items():
        module_path, func_name = target.split(":")

        description = ""
        try:
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)

            captured = {}

            original_init = argparse.ArgumentParser.__init__

            def patched_init(self, *args, **kwargs):
                original_init(self, *args, **kwargs)
                if self.description and not captured.get("description"):
                    captured["description"] = self.description

            with patch.object(argparse.ArgumentParser, "__init__", patched_init):
                with patch.object(argparse.ArgumentParser, "parse_args", side_effect=SystemExit(0)):
                    try:
                        func()
                    except SystemExit:
                        pass

            description = captured.get("description", "")
        except Exception as e:
            description = ""

        f.write(f"### `{name}`\n\n")
        if description:
            f.write(f"{description}\n\n")
        f.write(f"**Entrypoint:** `{target}`\n\n")
