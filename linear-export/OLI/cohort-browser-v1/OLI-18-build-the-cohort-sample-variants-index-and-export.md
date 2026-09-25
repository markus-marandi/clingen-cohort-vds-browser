# OLI-18: Build the cohort_sample_variants index and export

- Status: Backlog
- Priority: Urgent
- Estimate: 8
- Labels: export, internal
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-18/build-the-cohort-sample-variants-index-and-export

## Description

Per docs/SAMPLE_VARIANT_INDEX.md section 2b, plus two fields the new ERD requires that the design omits: var_pct (from the `AD` entry field) and in_report. Export non-ref entries from the annotated MT. Internal VM only. Serves UC-1 and UC-2. Sizing: millions of docs at 2000 samples.

## Comments

### Markus Mar — 2026-08-27T14:17:04Z

Design updated (2026-08-27): `docs/SAMPLE_VARIANT_INDEX.md` §2b now carries `var_pct` (float, `mt.AD[1] / hl.sum(mt.AD)`) and `in_report` (boolean), and the export sketch in §3 includes the var_pct expression. The `in_report` source still depends on OLI-10.
