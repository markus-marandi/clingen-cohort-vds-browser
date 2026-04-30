---
name: Packaging decision — Docker Compose bundle
description: Decided to package the full pipeline as a Docker Compose bundle for easy reuse
type: project
---

Packaging target is a Docker Compose bundle as of 2026-04-30.

**Why:** Target audience is research groups who need a simple one-command setup. Docker Compose wraps ingest + annotation + export + browser with volume mounts for GVCFs and metadata CSV inputs.

**How to apply:** When building packaging, favour Docker Compose over Snakemake/Nextflow. Keep HPC-specific workflow tooling out of scope unless the user explicitly requests it.
