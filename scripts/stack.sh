#!/usr/bin/env bash
# stack.sh - bring the cohort browser stack up under docker or podman.
#
# setup.sh resolves a compose command once and prints it; this script resolves it
# on every run and preflights the host so the podman-specific failures (missing
# /etc/mtab, unqualified image names, unwritable storage roots) surface as a
# named error instead of a compose stack trace.
#
# usage:
#   scripts/stack.sh doctor        # preflight only, no containers touched
#   scripts/stack.sh up [args]     # doctor, then compose up --build -d
#   scripts/stack.sh down [args]
#   scripts/stack.sh logs [args]
#   scripts/stack.sh ps
#   scripts/stack.sh compose ...   # raw passthrough to the resolved compose

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/gnomad-browser"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
PKG_ROOT="${PKG_ROOT:-/mnt/sdb/packages}"
PYTHON_PACKAGES_DIR="${PKG_ROOT}/python"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[stack]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

# ── compose resolution ────────────────────────────────────────────────────────

COMPOSE=()
RUNTIME=""

resolve_runtime() {
    if command -v docker &>/dev/null && docker version &>/dev/null; then
        RUNTIME="docker"
    elif command -v podman &>/dev/null; then
        RUNTIME="podman"
    elif command -v docker &>/dev/null; then
        RUNTIME="docker"   # present but daemon down; let compose report it
    else
        error "neither docker nor podman found. run ./setup.sh first."
    fi
}

resolve_compose() {
    if [ "${RUNTIME}" = "docker" ] && docker compose version &>/dev/null; then
        COMPOSE=(docker compose)
    elif [ "${RUNTIME}" = "podman" ] && podman compose version &>/dev/null; then
        COMPOSE=(podman compose)
    elif command -v docker-compose &>/dev/null; then
        COMPOSE=(docker-compose)
    elif command -v podman-compose &>/dev/null; then
        COMPOSE=(podman-compose)
    elif PYTHONPATH="${PYTHON_PACKAGES_DIR}" python3 -c "import podman_compose" &>/dev/null; then
        COMPOSE=(env "PYTHONPATH=${PYTHON_PACKAGES_DIR}" python3 -m podman_compose)
    else
        error "no compose implementation found. run ./setup.sh to install podman-compose."
    fi
}

# ── preflight ─────────────────────────────────────────────────────────────────

check_mtab() {
    # macOS and other non-Linux hosts have no /etc/mtab and do not need one.
    if [ "$(uname -s)" != "Linux" ]; then
        return 0
    fi

    # podman's OCI runtime (crun/runc) and the python compose implementations read
    # /etc/mtab. On this server it is a dangling symlink, which aborts every build.
    if [ -e /etc/mtab ] && head -1 /etc/mtab &>/dev/null; then
        info "/etc/mtab ok"
        return 0
    fi

    if [ -L /etc/mtab ]; then
        warn "/etc/mtab is a dangling symlink -> $(readlink /etc/mtab)"
    else
        warn "/etc/mtab is missing"
    fi
    cat >&2 <<'FIX'

  fix (needs root, one time, survives reboot on systemd hosts):

      sudo ln -sf /proc/self/mounts /etc/mtab

  if you cannot get root on this host, run the stack without compose - see DEMO.md
  for the native service startup path.

FIX
    return 1
}

check_registries() {
    [ "${RUNTIME}" = "podman" ] || return 0
    # podman refuses short image names when no unqualified-search registry is set.
    # The compose file pins fully-qualified names, so this is advisory only.
    if ! grep -rqs 'unqualified-search-registries' /etc/containers/registries.conf /etc/containers/registries.conf.d 2>/dev/null; then
        warn "podman has no unqualified-search-registries; compose images are fully qualified so this is fine"
    fi
}

check_storage() {
    [ "${RUNTIME}" = "podman" ] || return 0
    local conf="${HOME}/.config/containers/storage.conf"
    [ -f "${conf}" ] || { warn "no ${conf}; podman will use the boot disk"; return 0; }

    local root
    for key in graphRoot runRoot; do
        root="$(awk -F'"' -v k="${key}" '$0 ~ "^"k" *=" {print $2}' "${conf}" | head -1)"
        [ -n "${root}" ] || continue
        if [ ! -d "${root}" ]; then
            warn "${key} ${root} does not exist"
        elif [ ! -w "${root}" ]; then
            warn "${key} ${root} is not writable"
        else
            info "${key} ${root} ok"
        fi
    done

    if grep -q 'mount_program' "${conf}" && ! command -v fuse-overlayfs &>/dev/null; then
        warn "storage.conf sets mount_program but fuse-overlayfs is not on PATH"
    fi
}

check_tmpdir() {
    if [ -d /mnt/tmp ] && [ -w /mnt/tmp ]; then
        export TMPDIR="${TMPDIR:-/mnt/tmp}"
    fi
    info "TMPDIR=${TMPDIR:-/tmp}"
}

check_ports() {
    local busy=()
    for port in 9200 6379 8000 3000; do
        if command -v ss &>/dev/null && ss -ltn "sport = :${port}" 2>/dev/null | grep -q LISTEN; then
            busy+=("${port}")
        fi
    done
    if [ "${#busy[@]}" -gt 0 ]; then
        warn "ports already listening: ${busy[*]} (native services from DEMO.md?). Stop them or compose will fail to bind."
    fi
}

doctor() {
    resolve_runtime
    resolve_compose
    info "runtime: ${RUNTIME} $(${RUNTIME} --version 2>/dev/null | head -1)"
    info "compose: ${COMPOSE[*]}"
    [ -f "${COMPOSE_FILE}" ] || error "${COMPOSE_FILE} not found. run ./setup.sh first."
    check_tmpdir
    check_registries
    check_storage
    check_ports
    check_mtab || return 1
    info "preflight passed"
}

# ── commands ──────────────────────────────────────────────────────────────────

compose_run() {
    cd "${COMPOSE_DIR}"
    info "+ ${COMPOSE[*]} $*"
    "${COMPOSE[@]}" "$@"
}

cmd="${1:-doctor}"
shift || true

case "${cmd}" in
    doctor)
        doctor
        ;;
    up)
        doctor || error "preflight failed; not starting the stack"
        compose_run up --build -d "$@"
        info "browser http://localhost:3000  api http://localhost:8000  es http://localhost:9200"
        ;;
    down|logs|ps|build|restart|pull)
        resolve_runtime; resolve_compose
        compose_run "${cmd}" "$@"
        ;;
    compose)
        resolve_runtime; resolve_compose
        compose_run "$@"
        ;;
    *)
        error "unknown command '${cmd}'. usage: stack.sh {doctor|up|down|logs|ps|build|restart|pull|compose ...}"
        ;;
esac
