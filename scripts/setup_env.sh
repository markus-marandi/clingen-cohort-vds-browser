#!/usr/bin/env bash
# setup_env.sh - one-command environment bootstrap for VMs and laptops.
#
# Installs the pipeline dependencies (Hail, bcftools, tabix, JDK 17) through
# whichever runtime the target machine supports:
#
#   conda  - clingen-pipeline + clingen-tui environments from environment*.yml
#   docker - clingen-pipeline image built from ./Dockerfile (code is bind-mounted
#            at run time, not baked in)
#
# usage:
#   scripts/setup_env.sh conda           # create conda/micromamba environments
#   scripts/setup_env.sh docker          # build the pipeline container image
#   scripts/setup_env.sh install-docker  # install Docker Engine (Debian/Ubuntu, sudo)
#   scripts/setup_env.sh install-conda   # install micromamba to ~/.local/bin (no root)
#   scripts/setup_env.sh verify          # smoke-check whichever runtime is present

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIPELINE_ENV="clingen-pipeline"
TUI_ENV="clingen-tui"
IMAGE_NAME="clingen-pipeline"
HAIL_VERSION="0.2.135"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[env]${NC} $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC} $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

usage() {
    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# ── conda helpers ─────────────────────────────────────────────────────────────

CONDA_CMD=""

resolve_conda() {
    local candidate
    for candidate in micromamba mamba conda "$HOME/.local/bin/micromamba"; do
        if command -v "$candidate" &>/dev/null; then
            CONDA_CMD="$(command -v "$candidate")"
            return 0
        fi
    done
    return 1
}

env_exists() {
    "$CONDA_CMD" env list 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "$1"
}

# create_env <env-file> <env-name>
create_env() {
    local file="$1" name="$2"
    case "$(basename "$CONDA_CMD")" in
        micromamba)
            if env_exists "$name"; then
                info "environment '$name' exists - updating"
                "$CONDA_CMD" install -n "$name" -f "$file" -y
            else
                "$CONDA_CMD" create -n "$name" -f "$file" -y
            fi
            ;;
        *)  # conda / mamba
            if env_exists "$name"; then
                info "environment '$name' exists - updating"
                "$CONDA_CMD" env update -f "$file" --prune
            else
                "$CONDA_CMD" env create -f "$file"
            fi
            ;;
    esac
}

conda_run() {  # conda_run <env-name> <command...>
    "$CONDA_CMD" run -n "$1" "${@:2}"
}

# ── commands ──────────────────────────────────────────────────────────────────

cmd_install_conda() {
    if command -v micromamba &>/dev/null || [ -x "$HOME/.local/bin/micromamba" ]; then
        info "micromamba already installed"
    else
        local arch
        case "$(uname -s)/$(uname -m)" in
            Linux/x86_64)          arch="linux-64" ;;
            Linux/aarch64|Linux/arm64) arch="linux-aarch64" ;;
            Darwin/arm64)          arch="osx-arm64" ;;
            Darwin/x86_64)         arch="osx-64" ;;
            *) error "unsupported platform: $(uname -s)/$(uname -m)" ;;
        esac
        mkdir -p "$HOME/.local/bin"
        info "downloading micromamba (${arch})..."
        curl -Ls "https://micro.mamba.pm/api/micromamba/${arch}/latest" \
            | tar -xj -C "$HOME/.local" bin/micromamba
    fi

    if ! command -v micromamba &>/dev/null; then
        case ":${PATH}:" in
            *":$HOME/.local/bin:"*) ;;
            *) warn "$HOME/.local/bin is not on PATH - add it to your shell profile" ;;
        esac
    fi
    info "micromamba $("$HOME/.local/bin/micromamba" --version) ready"
}

cmd_conda() {
    resolve_conda \
        || error "no conda/mamba/micromamba found. run: scripts/setup_env.sh install-conda"
    info "using $(basename "$CONDA_CMD") $("$CONDA_CMD" --version 2>&1 | head -1)"

    create_env "${REPO_ROOT}/environment.yml" "$PIPELINE_ENV"
    create_env "${REPO_ROOT}/environment.tui.yml" "$TUI_ENV"

    local pipeline_python tui_python
    pipeline_python="$(conda_run "$PIPELINE_ENV" which python)"
    tui_python="$(conda_run "$TUI_ENV" which python)"

    info "smoke check:"
    conda_run "$PIPELINE_ENV" python -c "import hail as hl; print('  hail', hl.__version__)"
    conda_run "$PIPELINE_ENV" bcftools --version | head -1 | sed 's/^/  /'
    conda_run "$TUI_ENV" python -c "import textual; print('  textual', textual.__version__)"

    echo ""
    echo "next steps:"
    echo ""
    echo " 1. activate the pipeline environment:"
    echo "    conda activate $PIPELINE_ENV   # micromamba: micromamba activate $PIPELINE_ENV"
    echo ""
    echo " 2. run the pipeline:"
    echo "    python parallel_ingest_cohort.py --help"
    echo ""
    echo " 3. launch the TUI dashboard (keeps both environments separate):"
    echo "    CLINGEN_TUI_PYTHON=$tui_python \\"
    echo "    CLINGEN_PIPELINE_PYTHON=$pipeline_python \\"
    echo "    scripts/clingen-tui"
    echo ""
}

cmd_install_docker() {
    [ "$(uname -s)" = "Linux" ] \
        || error "install-docker targets Debian/Ubuntu Linux VMs; install Docker manually: https://docs.docker.com/engine/install/"
    command -v apt-get &>/dev/null \
        || error "only apt-based systems are supported here; install Docker manually: https://docs.docker.com/engine/install/"

    if command -v docker &>/dev/null && docker version &>/dev/null 2>&1; then
        info "docker already installed: $(docker --version)"
        return 0
    fi

    local sudo=""
    if [ "$(id -u)" -ne 0 ]; then
        command -v sudo &>/dev/null || error "run as root or install sudo first"
        sudo="sudo"
    fi

    export DEBIAN_FRONTEND=noninteractive
    $sudo apt-get update
    $sudo apt-get install -y ca-certificates curl gnupg

    . /etc/os-release
    if { [ "${ID:-}" = "ubuntu" ] || [ "${ID:-}" = "debian" ]; } \
        && [ -n "${VERSION_CODENAME:-}" ]; then
        info "installing Docker Engine from download.docker.com (${ID} ${VERSION_CODENAME})..."
        $sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL "https://download.docker.com/linux/${ID}/gpg" \
            | $sudo gpg --dearmor --batch --yes -o /etc/apt/keyrings/docker.gpg
        $sudo chmod a+r /etc/apt/keyrings/docker.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
            | $sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        $sudo apt-get update
        $sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
            docker-buildx-plugin docker-compose-plugin
    else
        warn "unsupported distro '${ID:-unknown}' for the official Docker repo - installing distro packages"
        $sudo apt-get install -y docker.io docker-compose-v2 \
            || $sudo apt-get install -y docker.io
    fi

    if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
        $sudo usermod -aG docker "$SUDO_USER" && \
            info "added $SUDO_USER to the docker group - log out and back in for it to apply"
    fi

    info "docker installed: $(docker --version)"
}

cmd_docker() {
    local runtime=""
    if command -v docker &>/dev/null && docker version &>/dev/null 2>&1; then
        runtime="docker"
    elif command -v podman &>/dev/null; then
        runtime="podman"
    else
        error "neither docker nor podman found. run: scripts/setup_env.sh install-docker"
    fi

    info "building ${IMAGE_NAME} image with ${runtime}..."
    "$runtime" build \
        -t "${IMAGE_NAME}:${HAIL_VERSION}" \
        -t "${IMAGE_NAME}:latest" \
        -f "${REPO_ROOT}/Dockerfile" "${REPO_ROOT}"

    info "smoke check:"
    "$runtime" run --rm "${IMAGE_NAME}:latest" bash -c \
        'python -c "import hail as hl; print(\"  hail\", hl.__version__)" && \
         bcftools --version | head -1 | sed "s/^/  /" && \
         tabix --version | head -1 | sed "s/^/  /" && \
         java -version 2>&1 | head -1 | sed "s/^/  /"'

    echo ""
    echo "next steps:"
    echo ""
    echo " 1. run any pipeline script with the repo bind-mounted into /work:"
    echo "    ${runtime} run --rm -it -v \"\$PWD\":/work -w /work ${IMAGE_NAME}:latest \\"
    echo "        python parallel_ingest_cohort.py --help"
    echo ""
    echo " 2. when exporting to Elasticsearch on the host, use host networking (Linux):"
    echo "    ${runtime} run --rm --network=host -v \"\$PWD\":/work -w /work ${IMAGE_NAME}:latest \\"
    echo "        python browser/data-pipeline/cohort_export.py --es-url http://localhost:9200 ..."
    echo ""
    echo " 3. the browser stack is separate: cd gnomad-browser && ${runtime} compose up --build"
    echo ""
}

cmd_verify() {
    local checked=0

    if resolve_conda; then
        info "conda runtime: $(basename "$CONDA_CMD")"
        conda_run "$PIPELINE_ENV" python -c "import hail as hl; print('  hail', hl.__version__)" \
            || error "$PIPELINE_ENV environment broken - re-run: scripts/setup_env.sh conda"
        conda_run "$PIPELINE_ENV" bcftools --version | head -1 | sed 's/^/  /'
        conda_run "$PIPELINE_ENV" tabix --version | head -1 | sed 's/^/  /'
        conda_run "$TUI_ENV" python -c "import textual; print('  textual', textual.__version__)" \
            || error "$TUI_ENV environment broken - re-run: scripts/setup_env.sh conda"
        checked=1
    fi

    local runtime=""
    if command -v docker &>/dev/null && docker version &>/dev/null 2>&1; then
        runtime="docker"
    elif command -v podman &>/dev/null; then
        runtime="podman"
    fi
    if [ -n "$runtime" ] && "$runtime" image inspect "${IMAGE_NAME}:latest" &>/dev/null; then
        info "container image: ${IMAGE_NAME}:latest"
        "$runtime" run --rm "${IMAGE_NAME}:latest" bash -c \
            'python -c "import hail as hl; print(\"  hail\", hl.__version__)" && \
             bcftools --version | head -1 | sed "s/^/  /"'
        checked=1
    fi

    [ "$checked" -eq 1 ] \
        || error "nothing found to verify. run 'scripts/setup_env.sh conda' or 'scripts/setup_env.sh docker' first."
    info "verify ok"
}

case "${1:-help}" in
    conda)          cmd_conda ;;
    docker)         cmd_docker ;;
    install-docker) cmd_install_docker ;;
    install-conda)  cmd_install_conda ;;
    verify)         cmd_verify ;;
    -h|--help|help) usage ;;
    *) error "unknown command '${1}'. run: scripts/setup_env.sh help" ;;
esac
