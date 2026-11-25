ROOT="@@ROOT@@"
ENV_FILE="@@ENV_FILE@@"

mkdir -p "$(dirname "$ROOT")" || return 1
if [ ! -f "$ENV_FILE" ]; then
  echo "@@NEEDLE@@ERROR:@@NODE@@:environment file not found: $ENV_FILE@@"
  return 1
fi
set -a
. "$ENV_FILE"
env_rc=$?
set +a
if [ "$env_rc" -ne 0 ]; then
  echo "@@NEEDLE@@NOTE:@@NODE@@:$ENV_FILE returned status $env_rc; continuing if required variables are set@@"
fi
if [ -z "${NEEDLE_CONFIG:-}" ]; then
  echo "@@NEEDLE@@ERROR:@@NODE@@:NEEDLE_CONFIG is not set in $ENV_FILE@@"
  return 1
fi
if [ -z "${NEEDLE_CLUSTER_CONFIG:-}" ]; then
  echo "@@NEEDLE@@ERROR:@@NODE@@:NEEDLE_CLUSTER_CONFIG is not set in $ENV_FILE@@"
  return 1
fi
@@CONFIG_INSTALL@@
mkdir -p "$(dirname "$NEEDLE_CONFIG")" "$(dirname "$NEEDLE_CLUSTER_CONFIG")" || return 1
if [ ! -f "$NEEDLE_CONFIG" ]; then
  echo "@@NEEDLE@@ERROR:@@NODE@@:needle config not found: $NEEDLE_CONFIG@@"
  return 1
fi
if [ ! -f "$NEEDLE_CLUSTER_CONFIG" ]; then
  echo "@@NEEDLE@@ERROR:@@NODE@@:cluster config not found: $NEEDLE_CLUSTER_CONFIG@@"
  return 1
fi
if [ -z "${PREFECT_API_URL:-}" ] || [ -z "${PREFECT_API_AUTH_STRING:-}" ]; then
  echo "@@NEEDLE@@ERROR:@@NODE@@:$ENV_FILE does not export PREFECT_API_URL and PREFECT_API_AUTH_STRING@@"
  return 1
fi
