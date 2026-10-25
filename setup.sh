#!/usr/bin/env bash
# sets up the gnomad-browser with cohort patches and verifies python dependencies.
# run once from the repo root before starting the pipeline or the browser.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNOMAD_BROWSER_DIR="${REPO_ROOT}/gnomad-browser"
PATCHES_DIR="${REPO_ROOT}/browser"
GNOMAD_BROWSER_REPO="https://github.com/broadinstitute/gnomad-browser.git"

# Browser overlay base and validated fork source. Keep these pinned so a fresh
# checkout is reproducible; see docs/BROWSER_OVERLAY.md.
UPSTREAM_BASE_SHA="db245371f1e572106bda8481ef0f9a6baac6c00d"
FORK_SOURCE_SHA="aa552d5a547404bab767b448322ce08d38d6048b"
FORK_REMOTE_URL="https://github.com/markus-marandi/gnomad-browser.git"
PNPM_VERSION="8.14.3"

PKG_ROOT="${CLINGEN_PKG_ROOT:-/mnt/sdb/packages}"
PYTHON_PACKAGES_DIR="${PKG_ROOT}/python"
PNPM_PACKAGES_DIR="${PKG_ROOT}"
TMP_DIR="${CLINGEN_TMP_DIR:-/mnt/tmp}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

ensure_writable_dir() {
    local path="$1"
    mkdir -p "${path}" || error "could not create ${path}"
    [ -w "${path}" ] || error "${path} is not writable"
}

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

# Copy an overlay tree into its destination, preserving relative paths. This is
# what makes test files, fixtures, and snapshots part of the tracked overlay
# instead of an incomplete hand-maintained list.
apply_overlay_tree() {
    local source_dir="$1"
    local destination_dir="$2"

    [ -d "${source_dir}" ] || return 0

    local source_path relative_path
    while IFS= read -r -d '' source_path; do
        relative_path="${source_path#"${source_dir}/"}"
        mkdir -p "${destination_dir}/$(dirname "${relative_path}")"
        cp "${source_path}" "${destination_dir}/${relative_path}"
    done < <(find "${source_dir}" -type f -print0)
}

# ── 1. check required tools ───────────────────────────────────────────────────

ensure_writable_dir "${PYTHON_PACKAGES_DIR}"
ensure_writable_dir "${PNPM_PACKAGES_DIR}"
ensure_writable_dir "${TMP_DIR}"

# redirect podman image/container storage off the boot disk
if command -v podman &>/dev/null; then
    ensure_writable_dir "${PKG_ROOT}/containers/storage"
    ensure_writable_dir "${PKG_ROOT}/containers/run"
    mkdir -p ~/.config/containers
    ensure_writable_dir "${TMP_DIR}/containers"
    PODMAN_RUN_ROOT="${TMP_DIR}/containers"
    ensure_writable_dir "${PODMAN_RUN_ROOT}"

    # storage.conf: image layers under /mnt/sdb/packages, runtime state under /mnt/tmp
    if [ ! -f ~/.config/containers/storage.conf ]; then
        cat > ~/.config/containers/storage.conf <<EOF
[storage]
driver = "overlay"
graphRoot = "${PKG_ROOT}/containers/storage"
runRoot = "${PODMAN_RUN_ROOT}"

[storage.options.overlay]
mount_program = "/usr/bin/fuse-overlayfs"
EOF
        info "podman storage.conf written"
    fi

    # keep buildah temp data with the configured data root
    if [ ! -f ~/.config/containers/containers.conf ]; then
        ensure_writable_dir "${TMP_DIR}/containers-tmp"
        cat > ~/.config/containers/containers.conf <<EOF
[engine]
tmp_dir = "${TMP_DIR}/containers-tmp"
EOF
        info "podman containers.conf written (tmp_dir -> ${TMP_DIR}/containers-tmp)"
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
        info "installing podman-compose to ${PYTHON_PACKAGES_DIR}..."
        pip3 install --target "${PYTHON_PACKAGES_DIR}" podman-compose \
            || error "could not install podman-compose"
    fi
    COMPOSE_CMD="PYTHONPATH=${PYTHON_PACKAGES_DIR} python3 -m podman_compose"
    info "compose command: podman-compose (via python3 -m podman_compose)"
fi

command -v node &>/dev/null \
    || error "'node' not found. install Node 18.17.1 (see .tool-versions) and re-run."

if ! command -v pnpm &>/dev/null \
    || [ "$(pnpm --version)" != "${PNPM_VERSION}" ]; then
    warn "installing pnpm ${PNPM_VERSION} (required by pnpm-lock.yaml)..."
    npm install -g "pnpm@${PNPM_VERSION}" \
        || error "could not install pnpm ${PNPM_VERSION}"
fi

# check bcftools and tabix for the pipeline (not required for browser-only setup)
for tool in bcftools tabix; do
    command -v "$tool" &>/dev/null \
        || warn "'$tool' not found - required to run the GVCF preprocessing pipeline"
done

# ── 2. checkout pinned gnomad-browser base ────────────────────────────────────

if [ -d "${GNOMAD_BROWSER_DIR}/.git" ]; then
    current_sha="$(git -C "${GNOMAD_BROWSER_DIR}" rev-parse HEAD)"
    if [ "${current_sha}" = "${UPSTREAM_BASE_SHA}" ]; then
        info "gnomad-browser is at pinned base ${UPSTREAM_BASE_SHA}"
    else
        warn "gnomad-browser is at ${current_sha}, not pinned base ${UPSTREAM_BASE_SHA}"
        warn "leaving the existing checkout unchanged; use a fresh checkout for a reproducible build"
    fi
else
    info "cloning gnomad-browser..."
    git clone "${GNOMAD_BROWSER_REPO}" "${GNOMAD_BROWSER_DIR}"
    info "checking out pinned base ${UPSTREAM_BASE_SHA}..."
    git -C "${GNOMAD_BROWSER_DIR}" fetch origin "${UPSTREAM_BASE_SHA}"
    git -C "${GNOMAD_BROWSER_DIR}" checkout --detach "${UPSTREAM_BASE_SHA}"
fi

# ── 3. apply cohort overlay ───────────────────────────────────────────────────

info "applying cohort overlay to gnomad-browser..."

# The overlay mirrors the upstream tree. Copying whole trees keeps source files,
# tests, fixtures, and snapshots in sync with the tracked overlay.
apply_overlay_tree "${PATCHES_DIR}/browser-src" \
    "${GNOMAD_BROWSER_DIR}/browser/src"
apply_overlay_tree "${PATCHES_DIR}/graphql-api" \
    "${GNOMAD_BROWSER_DIR}/graphql-api"
apply_overlay_tree "${PATCHES_DIR}/dataset-metadata" \
    "${GNOMAD_BROWSER_DIR}/dataset-metadata"
apply_overlay_tree "${PATCHES_DIR}/data-pipeline" \
    "${GNOMAD_BROWSER_DIR}/data-pipeline"

# docker-compose for local dev
cp "${PATCHES_DIR}/docker-compose.yml" \
   "${GNOMAD_BROWSER_DIR}/docker-compose.yml"

# keep dockerfile targets aligned with docker-compose build paths
copy_first_existing_patch "${GNOMAD_BROWSER_DIR}/browser/Dockerfile" \
    "${PATCHES_DIR}/Dockerfile" \
    "${PATCHES_DIR}/browser/Dockerfile"

# exclude host node_modules and .git from the docker/podman build context
copy_first_existing_patch "${GNOMAD_BROWSER_DIR}/.dockerignore" \
    "${PATCHES_DIR}/dockerignore" \
    "${PATCHES_DIR}/.dockerignore"

info "patches applied"

# ── 4. install node dependencies ──────────────────────────────────────────────

ensure_writable_dir "${PNPM_PACKAGES_DIR}"

# redirect every pnpm directory off the boot disk:
#   store-dir   - content-addressable package cache
#   cache-dir   - http/metadata cache
#   state-dir   - pnpm state (last-update checks etc.)
# PNPM_HOME     - global bin / global packages
#
# link-workspace-packages resolves @gnomad/* from the local workspace
# (browser/package.json uses "*" not "workspace:*").
# shamefully-hoist avoids pnpm's virtual store chmod calls that fail on /mnt/sdb.
# The upstream lockfile is part of the pinned base; do not delete it.
info "installing node dependencies (workspace root, frozen lockfile)..."
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
    CI=true PNPM_HOME="${PNPM_PACKAGES_DIR}/pnpm-home" pnpm install --frozen-lockfile
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
echo "    ./scripts/stack.sh up"
echo "    (preflights the host, then runs: ${COMPOSE_CMD} up --build -d)"
echo "    ./scripts/stack.sh doctor   # check the host without starting anything"
echo ""
echo " 3. export cohort variants to elasticsearch:"
echo "    python gnomad-browser/data-pipeline/cohort_export.py \\"
echo "        --mt-path /path/to/cohort_annotated.mt \\"
echo "        --es-url http://localhost:9200 \\"
echo "        --index cohort_variants"
echo ""
echo " 4. open http://localhost:3000 and select dataset: 'Cohort'"
echo ""
