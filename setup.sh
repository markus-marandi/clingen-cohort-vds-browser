#!/usr/bin/env bash
# sets up the gnomad-browser with cohort patches and verifies python dependencies.
# run once from the repo root before starting the pipeline or the browser.

set -euo pipefail

# ---------------------------------------------------------------------------
# Direct all heavy data to the data disk
# ---------------------------------------------------------------------------
PKG_ROOT="/mnt/sdb/packages"
PYTHON_PACKAGES_DIR="${PKG_ROOT}/python"
PNPM_PACKAGES_DIR="${PKG_ROOT}"
TMP_DIR="/mnt/sdb/tmp"
# Ensure directories exist
mkdir -p "${PYTHON_PACKAGES_DIR}" "${PNPM_PACKAGES_DIR}" "${TMP_DIR}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNOMAD_BROWSER_DIR="${REPO_ROOT}/gnomad-browser"
PATCHES_DIR="${REPO_ROOT}/browser"
GNOMAD_BROWSER_REPO="https://github.com/broadinstitute/gnomad-browser.git"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

copy_first_existing_patch() {
    local destination="$1"
    shift

    local source_path
    for source_path in "$@"; do
        if [ -f "${source_path}" ]; then
            cp "${source_path}" "${destination}"
            return 0
        fi
    done

    warn "missing patch for ${destination}; keeping existing file"
}

# ── 1. check required tools ───────────────────────────────────────────────────

PYTHON_PACKAGES_DIR="/mnt/sdb/packages/python"
mkdir -p "${PYTHON_PACKAGES_DIR}"

# redirect podman image/container storage off the boot disk
if command -v podman &>/dev/null; then
    mkdir -p /mnt/sdb/containers/storage /mnt/sdb/containers/run
    mkdir -p ~/.config/containers
    mkdir -p /mnt/sdb/tmp/containers
    PODMAN_RUN_ROOT="/mnt/sdb/tmp/containers"
    mkdir -p /mnt/sdb/containers/storage "${PODMAN_RUN_ROOT}" "${TMP_DIR}"

    # storage.conf: image layers on /mnt/sdb, runtime state on local tmpfs
    if [ ! -f ~/.config/containers/storage.conf ]; then
        cat > ~/.config/containers/storage.conf <<EOF
[storage]
driver = "overlay"
graphRoot = "/mnt/sdb/containers/storage"
# runRoot must be on local tmpfs - crun bind-mounts /etc/hosts into container
# rootfs and this fails on /mnt/sdb even with fuse-overlayfs
runRoot = "${PODMAN_RUN_ROOT}"

[storage.options.overlay]
mount_program = "/usr/bin/fuse-overlayfs"
EOF
        info "podman storage.conf written"
    fi

    # containers.conf: force buildah tmpdir off /mnt/sdb
    # this VM sets TMPDIR=/mnt/sdb/tmp; buildah inherits it and puts container
    # rootfs temp mounts there, where crun cannot open /etc/hosts (EPERM)
    if [ ! -f ~/.config/containers/containers.conf ]; then
        mkdir -p /mnt/sdb/tmp/containers-tmp
        cat > ~/.config/containers/containers.conf <<EOF
[engine]
tmp_dir = "/mnt/sdb/tmp/containers-tmp"
EOF
        info "podman containers.conf written (tmp_dir -> /mnt/sdb/tmp/containers-tmp)"
    fi
fi

info "checking required tools..."

for tool in git python3; do
    command -v "$tool" &>/dev/null || error "'$tool' not found. install it and re-run."
done

# accept either docker or podman as the container runtime
if command -v docker &>/dev/null; then
    CONTAINER_CMD="docker"
elif command -v podman &>/dev/null; then
    CONTAINER_CMD="podman"
else
    error "neither docker nor podman found. install one and re-run."
fi
info "container runtime: ${CONTAINER_CMD} $(${CONTAINER_CMD} --version | head -1)"

# resolve compose command: prefer 'docker compose' plugin, then docker-compose,
# then podman-compose (installed to /mnt/sdb/packages/python if missing)
if ${CONTAINER_CMD} compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="${CONTAINER_CMD} compose"
elif command -v docker-compose &>/dev/null; then
    COMPOSE_CMD="docker-compose"
else
    if ! PYTHONPATH="${PYTHON_PACKAGES_DIR}" python3 -c "import podman_compose" 2>/dev/null; then
        info "installing podman-compose to /mnt/sdb/packages/python..."
        pip3 install --target "${PYTHON_PACKAGES_DIR}" podman-compose \
            || error "could not install podman-compose"
    fi
    COMPOSE_CMD="PYTHONPATH=${PYTHON_PACKAGES_DIR} python3 -m podman_compose"
    info "compose command: podman-compose (via python3 -m podman_compose)"
fi

if ! command -v pnpm &>/dev/null; then
    warn "pnpm not found. attempting install via npm..."
    npm install -g pnpm || error "could not install pnpm. install manually: https://pnpm.io/installation"
fi

# check bcftools and tabix for the pipeline (not required for browser-only setup)
for tool in bcftools tabix; do
    command -v "$tool" &>/dev/null \
        || warn "'$tool' not found - required to run the GVCF preprocessing pipeline"
done

# ── 2. clone gnomad-browser ───────────────────────────────────────────────────

if [ -d "${GNOMAD_BROWSER_DIR}/.git" ]; then
    info "gnomad-browser already cloned at ${GNOMAD_BROWSER_DIR}"
else
    info "cloning gnomad-browser..."
    git clone "${GNOMAD_BROWSER_REPO}" "${GNOMAD_BROWSER_DIR}"
fi

# ── 3. apply cohort patches ───────────────────────────────────────────────────

info "applying cohort patches to gnomad-browser..."

# new file: cohort export pipeline
cp "${PATCHES_DIR}/data-pipeline/cohort_export.py" \
   "${GNOMAD_BROWSER_DIR}/data-pipeline/cohort_export.py"

# new file: cohort variant queries for the graphql api
cp "${PATCHES_DIR}/graphql-api/src/queries/variant-datasets/cohort-variant-queries.ts" \
   "${GNOMAD_BROWSER_DIR}/graphql-api/src/queries/variant-datasets/cohort-variant-queries.ts"

# replacement: datasets.ts (adds cohort label + reference genome)
cp "${PATCHES_DIR}/graphql-api/src/datasets.ts" \
   "${GNOMAD_BROWSER_DIR}/graphql-api/src/datasets.ts"

# replacement: variant-queries.ts (routes dataset: 'cohort' to cohort queries)
cp "${PATCHES_DIR}/graphql-api/src/queries/variant-queries.ts" \
   "${GNOMAD_BROWSER_DIR}/graphql-api/src/queries/variant-queries.ts"

# docker-compose for local dev
cp "${PATCHES_DIR}/docker-compose.yml" \
   "${GNOMAD_BROWSER_DIR}/docker-compose.yml"

# keep dockerfile targets aligned with docker-compose build paths
copy_first_existing_patch "${GNOMAD_BROWSER_DIR}/graphql-api/Dockerfile" \
    "${PATCHES_DIR}/graphql-api/Dockerfile" \
    "${PATCHES_DIR}/graphql-api/src/Dockerfile"
copy_first_existing_patch "${GNOMAD_BROWSER_DIR}/browser/Dockerfile" \
    "${PATCHES_DIR}/browser/Dockerfile"

# exclude host node_modules and .git from the docker/podman build context
copy_first_existing_patch "${GNOMAD_BROWSER_DIR}/.dockerignore" \
    "${PATCHES_DIR}/dockerignore" \
    "${PATCHES_DIR}/.dockerignore"

info "patches applied"

# ── 4. install node dependencies ──────────────────────────────────────────────

PNPM_PACKAGES_DIR="/mnt/sdb/packages"
mkdir -p "${PNPM_PACKAGES_DIR}"

# redirect every pnpm directory off the boot disk:
#   store-dir   - content-addressable package cache
#   cache-dir   - http/metadata cache
#   state-dir   - pnpm state (last-update checks etc.)
# PNPM_HOME     - global bin / global packages
#
# link-workspace-packages resolves @gnomad/* from the local workspace
# (browser/package.json uses "*" not "workspace:*").
# shamefully-hoist avoids pnpm's virtual store chmod calls that fail on /mnt/sdb.
info "installing node dependencies (workspace root)..."
(
    cd "${GNOMAD_BROWSER_DIR}"
    cat > .npmrc <<EOF
link-workspace-packages=true
engine-strict=false
shamefully-hoist=true
store-dir=${PNPM_PACKAGES_DIR}/pnpm-store
cache-dir=${PNPM_PACKAGES_DIR}/pnpm-cache
state-dir=${PNPM_PACKAGES_DIR}/pnpm-state
EOF
    rm -f pnpm-lock.yaml
    PNPM_HOME="${PNPM_PACKAGES_DIR}/pnpm-home" pnpm install
)

# ── 5. check python deps ──────────────────────────────────────────────────────

info "checking python dependencies..."

python3 -c "import hail" 2>/dev/null \
    || warn "hail not installed. install: pip install hail"

python3 -c "import concurrent.futures, subprocess, shutil" 2>/dev/null \
    || error "missing stdlib modules (unexpected)"

# ── 6. done ───────────────────────────────────────────────────────────────────

echo ""
echo "================================================================"
echo " setup complete"
echo "================================================================"
echo ""
echo " next steps:"
echo ""
echo " 1. run the ingest pipeline:"
echo "    python parallel_ingest_cohort.py \\"
echo "        --raw-gvcf-dir /mnt/sdb/gvcf_ustina/andmebaas_test_valim/ \\"
echo "        --filtered-gvcf-dir /mnt/sdb/gvcf_ustina/temp/andmebaas_test_valim_filtered/ \\"
echo "        --output-vds-dir /mnt/sdb/gvcf_ustina/ \\"
echo "        --manifest-path /mnt/sdb/gvcf_ustina/ingest_manifest.json \\"
echo "        --n-cores 16 --memory-gb 64"
echo ""
echo " 2. start the browser stack:"
echo "    cd gnomad-browser && TMPDIR=/tmp ${COMPOSE_CMD} up --build"
echo ""
echo " 3. export cohort variants to elasticsearch:"
echo "    python gnomad-browser/data-pipeline/cohort_export.py \\"
echo "        --manifest-path /mnt/sdb/gvcf_ustina/ingest_manifest.json"
echo ""
echo " 4. open http://localhost:3000 and select dataset: 'Cohort'"
echo ""
