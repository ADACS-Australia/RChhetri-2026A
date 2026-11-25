if [ -f "$DEPLOY_DIR/deploy.env" ]; then
  mkdir -p "$(dirname "$ENV_FILE")" || return 1
  if ! install -m 0600 "$DEPLOY_DIR/deploy.env" "$ENV_FILE"; then
    echo "@@MARKER@@ERROR:@@NODE@@:could not install deploy.env to $ENV_FILE@@"
    return 1
  fi
  echo "@@MARKER@@ACTION:@@NODE@@:env:INSTALLED@@"
else
  echo "@@MARKER@@ACTION:@@NODE@@:env:SKIPPED@@"
fi
