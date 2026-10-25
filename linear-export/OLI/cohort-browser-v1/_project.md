# Project: Cohort Browser v1 (P-OLI-6)

- Team: Oligogenetic (OLI)
- Lead: Markus Mar
- Status: See current implementation assessment
- Exported status: Backlog
- Reviewed: 2026-10-25
- Start date: 2026-08-27
- Historical target date: 2026-09-26
- Priority: No priority
- Linear: https://linear.app/necto-tech/project/cohort-browser-v1-86942012c48c

## Current scope and status

Ship the GRCh37-only internal cohort browser and annotation pipeline, including
validated metadata and internal sample/carrier/report workflows. All authenticated
internal users may see sample IDs. Public deployment is outside v1.

Use [current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
reviewed inventory. The 34 exported records are not 34 unfinished tasks. OLI-2,
OLI-21, OLI-22, and OLI-24 have implementations in main; OLI-9, OLI-25, OLI-27, and OLI-33
are partial. OLI-19, OLI-28, OLI-29, and OLI-30 are deferred public-release work.
Other rows distinguish design-only work, remaining work, decisions, and validation.

The pushed browser-reconciliation/runtime branch remains separate from main.
Source implementation, reported test results, and production acceptance must not
be treated as the same status. This update changes repo documentation only.

## Historical exported plan

The following was the original plan and is retained for context. Its public-tier
requirements, September target, four-week schedule, and 118-point total do not
represent the current release scope or a current effort estimate.

### Original summary

Ship the ClinGen cohort VDS browser and annotation pipeline: schematic-aligned schema, internal sample indices, two-VM public/internal split.

### Original description

Delivery target ~2026-09-26 (four weeks from 2026-08-27).

**Sequencing**

* W1 — unblock + freeze schema. Podman blocker, dbNSFP VM download, then all rename/decision issues together. Renames force a reindex, so batch HGVS + clinvar_review_status + sex_chr into one migration. The predictor-set decision gates the ES mapping and GraphQL exposure — settle it first.
* W2 — internal indices. cohort_samples and cohort_sample_variants, both carrying the new schematic fields. Export profiles with the fail-closed allowlist land here, not later — every downstream task assumes the tier boundary exists.
* W3 — GraphQL + UI. Combined filter queries (UC-4/5/6), carrier resolvers behind DEPLOYMENT_MODE, filter controls, carrier views.
* W4 — deploy + validate. Two-VM split, one-way transfer, privacy threshold, full production run.

**Non-negotiables**: the export allowlist, the DEPLOYMENT_MODE gate, and the privacy threshold. Those three stand between a working browser and patient data on a public VM.

**Droppable if scope compresses**: gene-level frequency summaries, the assay-conditional clarification, the HPO denormalization cleanup.

118 points total. Tight for four weeks below 3 people.
