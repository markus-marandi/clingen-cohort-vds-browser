# OLI-8: Drive reference_genome from Procedure instead of hardcoding GRCh37

- Status: Rescoped
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 2
- Labels: browser, decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-8/drive-reference-genome-from-procedure-instead-of-hardcoding-grch37

## Current assessment

The user confirmed a GRCh37-only first release on 2026-10-25. Runtime switching to GRCh38 is not required for v1. Validate input/procedure build compatibility and document accepted GRCh37/hg19 aliases instead.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

cohort-variant-queries.ts:29 returns a hardcoded `reference_genome: 'GRCh37'`. The new ERD makes ref_genome a per-procedure value. Decide whether the cohort is single-build (then assert it at ingest and keep the constant, documented) or mixed (then carry ref_genome through the export and the API).

## Comments

None.
