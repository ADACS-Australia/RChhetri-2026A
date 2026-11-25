screen_line() {
  screen -list 2>/dev/null | awk -v n="$1" '$1 ~ ("^[0-9]+[.]" n "$") {print; exit}'
}

screen_exists() {
  [ -n "$(screen_line "$1")" ] || screen -S "$1" -Q select . >/dev/null 2>&1
}

screen_running() {
  line="$(screen_line "$1")"
  if [ -n "$line" ] && printf '%s\n' "$line" | grep -Eq '\((Attached|Detached)\)'; then
    return 0
  fi
  screen -S "$1" -Q select . >/dev/null 2>&1
}

stop_screen() {
  name="$1"
  screen -S "$name" -X quit >/dev/null 2>&1 || true
  i=0
  while screen_exists "$name" && [ "$i" -lt 20 ]; do
    sleep 0.25
    i=$((i + 1))
  done
  if screen_exists "$name"; then
    screen -wipe >/dev/null 2>&1 || true
  fi
}

start_worker() {
  role="$1"
  name="$2"
  cmd="$3"
  if screen_running "$name"; then
    echo "@@NEEDLE@@ACTION:@@NODE@@:${role}:EXISTS@@"
    return 0
  fi
  if screen_exists "$name"; then
    stop_screen "$name"
  fi
  log="$HOME/needle_logs/${name}.log"
  mkdir -p "$(dirname "$log")"
  screen -dmS "$name" bash --noprofile --norc -c "$cmd 2>&1 | tee -a \"$log\""
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "@@NEEDLE@@ERROR:@@NODE@@:screen -dmS $name failed with status $rc@@"
    return 1
  fi
  i=0
  while [ "$i" -lt 20 ]; do
    if screen_running "$name"; then
      echo "@@NEEDLE@@ACTION:@@NODE@@:${role}:STARTED@@"
      return 0
    fi
    sleep 0.5
    i=$((i + 1))
  done
  echo "@@NEEDLE@@ERROR:@@NODE@@:screen $name did not stay alive; log: $log@@"
  echo "    current screen sessions:"
  screen -list 2>&1 | sed "s/^/    /"
  if [ -f "$log" ]; then
    echo "    last log lines:"
    tail -n 20 "$log" | sed "s/^/    /"
  fi
  return 1
}
