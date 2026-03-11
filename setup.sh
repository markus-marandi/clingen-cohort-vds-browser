#!/usr/bin/env bash
# sets up the gnomad-browser with cohort patches and verifies python dependencies.
# run once from the repo root before starting the pipeline or the browser.

set -euo pipefail

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

# ── 1. check required tools ───────────────────────────────────────────────────

info "checking required tools..."

for tool in git python3 docker; do
    command -v "$tool" &>/dev/null || error "'$tool' not found. install it and re-run."
done

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

info "patches applied"

# ── 4. install node dependencies ──────────────────────────────────────────────

info "installing graphql-api node dependencies..."
(cd "${GNOMAD_BROWSER_DIR}/graphql-api" && pnpm install)

info "installing browser node dependencies..."
(cd "${GNOMAD_BROWSER_DIR}/browser" && pnpm install)

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
echo "    cd gnomad-browser && docker compose up --build"
echo ""
echo " 3. export cohort variants to elasticsearch:"
echo "    python gnomad-browser/data-pipeline/cohort_export.py \\"
echo "        --manifest-path /mnt/sdb/gvcf_ustina/ingest_manifest.json"
echo ""
echo " 4. open http://localhost:3000 and select dataset: 'Cohort'"
echo ""
