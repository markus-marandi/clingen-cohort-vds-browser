# Project: Cohort Browser v1 (P-OLI-6)

- Team: Oligogenetic (OLI)
- Lead: Markus Mar
- Status: Backlog
- Start date: 2026-08-27
- Target date: 2026-09-26
- Priority: No priority
- Linear: https://linear.app/necto-tech/project/cohort-browser-v1-86942012c48c

## Summary

Ship the ClinGen cohort VDS browser and annotation pipeline: schematic-aligned schema, internal sample indices, two-VM public/internal split.

## Description

Delivery target ~2026-09-26 (four weeks from 2026-08-27).

**Sequencing**

* W1 — unblock + freeze schema. Podman blocker, dbNSFP VM download, then all rename/decision issues together. Renames force a reindex, so batch HGVS + clinvar_review_status + sex_chr into one migration. The predictor-set decision gates the ES mapping and GraphQL exposure — settle it first.
* W2 — internal indices. cohort_samples and cohort_sample_variants, both carrying the new schematic fields. Export profiles with the fail-closed allowlist land here, not later — every downstream task assumes the tier boundary exists.
* W3 — GraphQL + UI. Combined filter queries (UC-4/5/6), carrier resolvers behind DEPLOYMENT_MODE, filter controls, carrier views.
* W4 — deploy + validate. Two-VM split, one-way transfer, privacy threshold, full production run.

**Non-negotiables**: the export allowlist, the DEPLOYMENT_MODE gate, and the privacy threshold. Those three stand between a working browser and patient data on a public VM.

**Droppable if scope compresses**: gene-level frequency summaries, the assay-conditional clarification, the HPO denormalization cleanup.

118 points total. Tight for four weeks below 3 people.
