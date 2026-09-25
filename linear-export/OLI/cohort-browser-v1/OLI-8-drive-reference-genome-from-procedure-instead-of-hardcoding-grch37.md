# OLI-8: Drive reference_genome from Procedure instead of hardcoding GRCh37

- Status: Backlog
- Priority: High
- Estimate: 2
- Labels: browser, decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-8/drive-reference-genome-from-procedure-instead-of-hardcoding-grch37

## Description

cohort-variant-queries.ts:29 returns a hardcoded `reference_genome: 'GRCh37'`. The new ERD makes ref_genome a per-procedure value. Decide whether the cohort is single-build (then assert it at ingest and keep the constant, documented) or mixed (then carry ref_genome through the export and the API).

## Comments

None.
