#!/usr/bin/env bash
set -euo pipefail

error() {
    echo "[ERROR] $*" >&2
    exit 1
}
warn() { echo "[WARN]  $*" >&2; }
success() { echo "[OK]    $*"; }

usage() {
    cat <<EOF
USAGE
    $(basename "$0") -n <needle_cfg> -a <casa_cfg> [-c <cluster_cfg>]

OPTIONS
    -n, --needle   Path to the needle config file   (required)
    -c, --cluster  Path to the cluster config file  (optional)
    -a, --casa     Path to the CASA config file     (required)
    -h, --help     Show this help screen and exit

DESCRIPTION
    A helper script to set up Needle config files
    Validates that the supplied config paths exist on disk, then creates
    symlinks in the following locations:
        needle_cfg  -> ~/.needle.yaml
        cluster_cfg -> ~/.cluster.yaml
        casa_cfg    -> ~/.casa/config.py
    For needle and cluster: if a real file (non-symlink) already exists at
    the target, the script aborts with an error.
    For casa: if a real file already exists at the target, the user is prompted
    for confirmation before overwriting. An existing symlink is always replaced
    without prompting. The ~/.casa directory is created if it does not exist.

EXAMPLES
    $(basename "$0") -n /etc/needle/needle.conf
    $(basename "$0") -n /etc/needle/needle.conf -c /etc/cluster/cluster.conf
    $(basename "$0") -n /etc/needle/needle.conf -a /opt/casa/config.py
    $(basename "$0") -n /etc/needle/needle.conf -c /etc/cluster/cluster.conf -a /opt/casa/config.py
EOF
}

# Link a config file
#   $1 = source path (absolute)
#   $2 = friendly name (for messages)
#   $3 = destination path (absolute)
link_config() {
    local src="$1"
    local name="$2"
    local dest="$3"

    # Resolve to absolute path (handles relative paths, .., etc.)
    src="$(realpath -e "$src" 2>/dev/null)" ||
        error "$name path does not exist: $1"

    # Create destination directory if it doesn't exist
    local destdir
    destdir="$(dirname "$dest")"
    if [[ ! -d "$destdir" ]]; then
        mkdir -p "$destdir"
        success "$name: created directory '$destdir'"
    fi

    # Real file at destination: ask for confirmation
    if [[ -e "$dest" && ! -L "$dest" ]]; then
        warn "$name: a real file already exists at '$dest'."
        read -r -p "         Overwrite it with a symlink? [y/N] " reply
        case "$reply" in
        [yY][eE][sS] | [yY])
            rm "$dest"
            ;;
        *)
            echo "[SKIP]  $name: leaving '$dest' untouched."
            return 0
            ;;
        esac
    fi

    # Remove a stale/old symlink if present
    if [[ -L "$dest" ]]; then
        warn "$name: replacing existing symlink at '$dest'"
        rm "$dest"
    fi

    ln -s "$src" "$dest"
    success "$name -> symlinked '$src' -> '$dest'"
}

needle_cfg=""
cluster_cfg=""
casa_cfg=""

# Show help when no arguments are provided
if [[ $# -eq 0 ]]; then
    usage
    exit 0
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
    -n | --needle)
        [[ -n "${2:-}" ]] || error "Option $1 requires an argument."
        needle_cfg="$2"
        shift 2
        ;;
    -c | --cluster)
        [[ -n "${2:-}" ]] || error "Option $1 requires an argument."
        cluster_cfg="$2"
        shift 2
        ;;
    -a | --casa)
        [[ -n "${2:-}" ]] || error "Option $1 requires an argument."
        casa_cfg="$2"
        shift 2
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        error "Unknown option: $1  (run with -h for help)"
        ;;
    esac
done

[[ -n "$needle_cfg" ]] || error "needle_cfg (-n) is required.  Run with -h for help."
[[ -n "$casa_cfg" ]] || error "casa_cfg (-a) is required.  Run with -h for help."

link_config "$needle_cfg" "needle_cfg" "$HOME/.needle.yaml"

if [[ -n "$cluster_cfg" ]]; then
    link_config "$cluster_cfg" "cluster_cfg" "$HOME/.needle_cluster.yaml"
fi

link_config "$casa_cfg" "casa_cfg" "$HOME/.casa/config.py"

echo ""
echo "Done."
