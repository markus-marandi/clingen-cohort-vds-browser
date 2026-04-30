---
name: Server infrastructure — confirmed paths and tooling
description: Confirmed server paths, versions, and Podman/Docker situation as of 2026-04-30
type: project
---

All pipeline testing confirmed working on server as of 2026-04-30.

**Cohort stats:** 20 samples, 150,659 variants.

**Confirmed paths:**
- Python/Hail env: `/mnt/sdb/venvs/hail-39/bin/python`
- Hail version: 0.2.135-034ef3e08116
- VDS: `/mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds`
- Annotated MT: `/mnt/sdb/gvcf_ustina/cohort_annotated.mt`
- Manifest: `/mnt/sdb/gvcf_ustina/ingest_manifest.json`
- Elasticsearch binary: `/mnt/sdb/packages/elasticsearch/bin/elasticsearch` (v8.13.4, standalone)
- ES data dir: `/mnt/sdb/tmp/es-data`
- ES logs dir: `/mnt/sdb/tmp/es-logs`
- Project dir: `/mnt/sdb/gvcf_ustina/testing/clingen-cohort-vds-browser`

**Podman/Docker situation:**
- Server uses Podman 5.5.1, not Docker
- Podman has a config issue (`/etc/mtab symlink: invalid argument`) that prevents `docker compose up`
- Workaround: Elasticsearch runs as a standalone binary, not in a container
- The browser gnomAD stack (`docker compose up --build`) has NOT been tested yet — this is the next blocker

**Why:** Podman vs Docker matters for the browser stack deployment step and for packaging.

**How to apply:** When working on browser stack deployment or packaging, account for Podman — either fix the Podman config, use `podman compose`, or document a Docker-only requirement.
