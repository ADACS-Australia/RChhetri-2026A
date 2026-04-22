#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="setonix"
REMOTE_PORT=4200
LOCAL_PORT=4200
MONITOR_PORT=4201
BIND_ADDR="127.0.0.1"
PID_FILE="/tmp/tunnel_${REMOTE_HOST}_${REMOTE_PORT}.pid"

die() {
    echo "ERROR: $*" >&2
    exit 1
}
info() { echo "  --> $*"; }

tunnel_pid() {
    # Find the autossh process for this specific tunnel
    pgrep -f "autossh.*-R ${REMOTE_PORT}:${BIND_ADDR}:${LOCAL_PORT}.*${REMOTE_HOST}" 2>/dev/null | head -1 || true
}

is_up() {
    local pid
    pid=$(tunnel_pid)
    [[ -n "$pid" ]]
}

cmd_up() {
    if is_up; then
        info "Tunnel is already up (PID $(tunnel_pid))."
        exit 0
    fi

    info "Starting tunnel ${BIND_ADDR}:${LOCAL_PORT} -> ${REMOTE_HOST}:${REMOTE_PORT} ..."

    AUTOSSH_PIDFILE="$PID_FILE" \
        autossh -M "$MONITOR_PORT" -f -N \
        -R "${REMOTE_PORT}:${BIND_ADDR}:${LOCAL_PORT}" \
        -o "ServerAliveInterval=30" \
        -o "ServerAliveCountMax=3" \
        -o "ExitOnForwardFailure=yes" \
        "$REMOTE_HOST"

    # Give autossh a moment to fork and register
    sleep 1

    if is_up; then
        info "Tunnel is up (PID $(tunnel_pid))."
    else
        die "Tunnel failed to start. Check SSH connectivity to '${REMOTE_HOST}'."
    fi
}

cmd_down() {
    local pid
    pid=$(tunnel_pid)

    if [[ -z "$pid" ]]; then
        info "Tunnel is not running."
        exit 0
    fi

    info "Stopping tunnel (PID ${pid}) ..."
    kill "$pid"

    # Wait up to 5 seconds for it to exit
    local i=0
    while kill -0 "$pid" 2>/dev/null && ((i < 5)); do
        sleep 1
        ((i++)) || true
    done

    if kill -0 "$pid" 2>/dev/null; then
        info "Process didn't exit cleanly; sending SIGKILL ..."
        kill -9 "$pid"
    fi

    rm -f "$PID_FILE"
    info "Tunnel stopped."
}

cmd_check() {
    local pid
    pid=$(tunnel_pid)

    if [[ -n "$pid" ]]; then
        echo "  Status : UP"
        echo "  PID    : ${pid}"
        echo "  Tunnel : ${BIND_ADDR}:${LOCAL_PORT} -> ${REMOTE_HOST}:${REMOTE_PORT}"
    else
        echo "  Status : DOWN"
    fi
}

usage() {
    echo "Usage: $(basename "$0") [--up | --down | --check]"
    echo ""
    echo "  --up      Start the tunnel (no-op if already running)"
    echo "  --down    Stop the tunnel"
    echo "  --check   Report the current tunnel status"
    exit 1
}

[[ $# -eq 1 ]] || usage

case "$1" in
--up) cmd_up ;;
--down) cmd_down ;;
--check) cmd_check ;;
*) usage ;;
esac
