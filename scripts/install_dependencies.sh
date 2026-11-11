#!/usr/bin/env bash
# run from root as ./scripts/install_dependencies.sh
set -euo pipefail

PY="$(command -v python3 || command -v python)"

# Install uv onto the worker if it isn't already there
if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

# Install the project's dependencies into that same interpreter
uv pip install -r pyproject.toml
