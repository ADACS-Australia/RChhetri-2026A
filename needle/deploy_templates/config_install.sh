if [ -n "${NEEDLE_CONFIG:-}" ] && [ -f "$DEPLOY_DIR/.needle.@@ENV_NAME@@.yaml" ]; then
  mkdir -p "$(dirname "$NEEDLE_CONFIG")" || return 1
  if ! install -m 0644 "$DEPLOY_DIR/.needle.@@ENV_NAME@@.yaml" "$NEEDLE_CONFIG"; then
    echo "@@MARKER@@ERROR:@@NODE@@:could not install .needle.@@ENV_NAME@@.yaml to $NEEDLE_CONFIG@@"
    return 1
  fi
  echo "@@MARKER@@ACTION:@@NODE@@:config:INSTALLED@@"
else
  echo "@@MARKER@@ACTION:@@NODE@@:config:SKIPPED@@"
fi
if [ -n "${NEEDLE_CLUSTER_CONFIG:-}" ] && [ -f "$DEPLOY_DIR/.needle_cluster.@@ENV_NAME@@.yaml" ]; then
  mkdir -p "$(dirname "$NEEDLE_CLUSTER_CONFIG")" || return 1
  if ! install -m 0644 "$DEPLOY_DIR/.needle_cluster.@@ENV_NAME@@.yaml" "$NEEDLE_CLUSTER_CONFIG"; then
    echo "@@MARKER@@ERROR:@@NODE@@:could not install .needle_cluster.@@ENV_NAME@@.yaml to $NEEDLE_CLUSTER_CONFIG@@"
    return 1
  fi
  echo "@@MARKER@@ACTION:@@NODE@@:cluster-config:INSTALLED@@"
else
  echo "@@MARKER@@ACTION:@@NODE@@:cluster-config:SKIPPED@@"
fi
