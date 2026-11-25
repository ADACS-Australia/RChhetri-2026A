echo "@@MARKER@@HOST:@@NODE@@:$(hostname 2>/dev/null || uname -n)@@"
export PATH="$HOME/.local/bin:$PATH"

if [ "@@REQUIRE_SCREEN@@" -eq 1 ]; then
  if ! command -v screen >/dev/null 2>&1; then
    if command -v module >/dev/null 2>&1; then
      module load screen >/dev/null 2>&1 || true
    fi
  fi
  if command -v screen >/dev/null 2>&1; then
    echo "@@MARKER@@CHECK:@@NODE@@:screen:OK@@"
  else
    echo "@@MARKER@@CHECK:@@NODE@@:screen:MISSING@@"
    echo "@@MARKER@@ERROR:@@NODE@@:screen is required but was not found; load a module or ask system support@@"
    if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
  fi
fi

if ! command -v git >/dev/null 2>&1; then
  if command -v module >/dev/null 2>&1; then
    module load git >/dev/null 2>&1 || true
  fi
fi
if command -v git >/dev/null 2>&1; then
  echo "@@MARKER@@CHECK:@@NODE@@:git:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:git:MISSING@@"
  echo "@@MARKER@@NOTE:@@NODE@@:git not found; Prefect deployment pull steps may fail@@"
fi

if [ "@@INSTALL@@" -eq 1 ]; then
  uv_state=OK
  if ! command -v uv >/dev/null 2>&1; then
    if command -v curl >/dev/null 2>&1; then
      if ! (curl -LsSf https://astral.sh/uv/install.sh | sh) 1>&2; then
        echo "@@MARKER@@ERROR:@@NODE@@:uv installation failed@@"
        if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
      fi
    elif command -v wget >/dev/null 2>&1; then
      if ! (wget -qO- https://astral.sh/uv/install.sh | sh) 1>&2; then
        echo "@@MARKER@@ERROR:@@NODE@@:uv installation failed@@"
        if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
      fi
    else
      echo "@@MARKER@@ERROR:@@NODE@@:neither curl nor wget is available to install uv@@"
      if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
    fi
    export PATH="$HOME/.local/bin:$PATH"
    uv_state=INSTALLED
  fi
  if command -v uv >/dev/null 2>&1; then
    echo "@@MARKER@@CHECK:@@NODE@@:uv:${uv_state}@@"
  else
    echo "@@MARKER@@CHECK:@@NODE@@:uv:MISSING@@"
    echo "@@MARKER@@ERROR:@@NODE@@:uv is not installed@@"
    if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
  fi
else
  if command -v uv >/dev/null 2>&1; then
    echo "@@MARKER@@CHECK:@@NODE@@:uv:OK@@"
  else
    echo "@@MARKER@@CHECK:@@NODE@@:uv:MISSING@@"
    echo "@@MARKER@@ERROR:@@NODE@@:uv is not installed@@"
    if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
  fi
fi

if [ -d "@@WORKDIR@@" ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:workdir:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:workdir:MISSING@@"
  echo "@@MARKER@@ERROR:@@NODE@@:work directory not found: @@WORKDIR@@@@"
  if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
fi
if [ -f "@@WORKDIR@@/pyproject.toml" ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:pyproject:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:pyproject:MISSING@@"
  echo "@@MARKER@@ERROR:@@NODE@@:pyproject.toml not found under @@WORKDIR@@@@"
  if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
fi
if [ "@@INSTALL@@" -eq 1 ]; then
  if [ -f "@@WORKDIR@@/pyproject.toml" ]; then
    sync_args=""
    if [ -f "@@WORKDIR@@/uv.lock" ]; then sync_args=--frozen; fi
    if (cd "@@WORKDIR@@" && uv sync $sync_args) 1>&2; then
      echo "@@MARKER@@CHECK:@@NODE@@:project-sync:OK@@"
    else
      echo "@@MARKER@@CHECK:@@NODE@@:project-sync:FAILED@@"
      echo "@@MARKER@@ERROR:@@NODE@@:uv sync failed in @@WORKDIR@@@@"
      if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
    fi
  fi
else
  echo "@@MARKER@@CHECK:@@NODE@@:project-sync:SKIPPED@@"
fi

if [ -f "@@ENV_FILE@@" ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:env-file:OK@@"
  set -a
  . "@@ENV_FILE@@"
  env_rc=$?
  set +a
  if [ "$env_rc" -ne 0 ]; then
    echo "@@MARKER@@NOTE:@@NODE@@:@@ENV_FILE@@ returned status $env_rc; continuing if required variables are set@@"
  fi
  env_loaded=1
else
  echo "@@MARKER@@CHECK:@@NODE@@:env-file:MISSING@@"
  echo "@@MARKER@@ERROR:@@NODE@@:environment file not found: @@ENV_FILE@@@@"
  if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
  env_loaded=0
fi
if [ "$env_loaded" -eq 1 ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:env-load:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:env-load:FAILED@@"
  echo "@@MARKER@@ERROR:@@NODE@@:could not load @@ENV_FILE@@@@"
  if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
fi
if [ "$env_loaded" -eq 1 ] && [ -n "${PREFECT_API_URL:-}" ] && [ -n "${PREFECT_API_AUTH_STRING:-}" ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:prefect-env:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:prefect-env:MISSING@@"
  echo "@@MARKER@@ERROR:@@NODE@@:@@ENV_FILE@@ does not export PREFECT_API_URL and PREFECT_API_AUTH_STRING@@"
  if [ "@@STRICT@@" -eq 1 ]; then return 2; fi
fi
if [ -n "${NEEDLE_CONFIG:-}" ] && [ -f "$NEEDLE_CONFIG" ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:needle-config:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:needle-config:MISSING@@"
  echo "@@MARKER@@NOTE:@@NODE@@:NEEDLE_CONFIG is unset or missing: ${NEEDLE_CONFIG:-<unset>}@@"
fi
if [ -n "${NEEDLE_CLUSTER_CONFIG:-}" ] && [ -f "$NEEDLE_CLUSTER_CONFIG" ]; then
  echo "@@MARKER@@CHECK:@@NODE@@:cluster-config:OK@@"
else
  echo "@@MARKER@@CHECK:@@NODE@@:cluster-config:MISSING@@"
  echo "@@MARKER@@NOTE:@@NODE@@:NEEDLE_CLUSTER_CONFIG is unset or missing: ${NEEDLE_CLUSTER_CONFIG:-<unset>}@@"
fi

IMAGE_DEST=""
if command -v uv >/dev/null 2>&1 && [ -f "@@WORKDIR@@/pyproject.toml" ] && [ -n "${NEEDLE_CLUSTER_CONFIG:-}" ] && [ -f "$NEEDLE_CLUSTER_CONFIG" ]; then
  if ! IMAGE_DEST=$(cd "@@WORKDIR@@" && uv run python -c 'import os,sys,yaml; cfg=yaml.safe_load(open(sys.argv[1])) or {}; image=(cfg.get("container") or {}).get("image"); print(os.path.expandvars(os.path.expanduser(str(image)))) if image else None' "$NEEDLE_CLUSTER_CONFIG" 2>/dev/null); then
    echo "@@MARKER@@NOTE:@@NODE@@:could not read container.image from $NEEDLE_CLUSTER_CONFIG@@"
    IMAGE_DEST=""
  fi
fi
if [ -z "$IMAGE_DEST" ]; then
  IMAGE_DEST="@@FALLBACK_IMAGE@@"
fi
if [ -n "$IMAGE_DEST" ]; then
  if [ -f "$IMAGE_DEST" ]; then
    echo "@@MARKER@@CHECK:@@NODE@@:image:OK@@"
  else
    echo "@@MARKER@@CHECK:@@NODE@@:image:MISSING@@"
    echo "@@MARKER@@NOTE:@@NODE@@:container image not found: $IMAGE_DEST@@"
  fi
else
  echo "@@MARKER@@CHECK:@@NODE@@:image:MISSING@@"
  echo "@@MARKER@@NOTE:@@NODE@@:container.image is not set and no image fallback is configured@@"
fi

if [ "@@REQUIRE_CLI@@" -eq 1 ]; then
  if [ -x "$HOME/.local/bin/needle" ]; then
    echo "@@MARKER@@CHECK:@@NODE@@:dev-cli:OK@@"
  else
    echo "@@MARKER@@CHECK:@@NODE@@:dev-cli:MISSING@@"
    echo "@@MARKER@@NOTE:@@NODE@@:editable needle CLI not found at $HOME/.local/bin/needle; run needle deploy dev@@"
  fi
fi
