# OLI-6: Rename clinvar_clnrevstat to clinvar_review_status

- Status: Remaining
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Medium
- Estimate: 1
- Labels: schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-6/rename-clinvar-clnrevstat-to-clinvar-review-status

## Current assessment

Review status is exposed as clinvar_clnrevstat. The requested clinvar_review_status naming remains. This concerns ClinVar metadata, not the status of a sample clinical report.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

New ERD names the field clinvar_review_status. Update annotate_cohort.py, the cohort_variants mapping in cohort_export.py:95, and any GraphQL/UI reference. Requires a reindex; do together with the HGVS rename to avoid two reindexes.

## Comments

None.
