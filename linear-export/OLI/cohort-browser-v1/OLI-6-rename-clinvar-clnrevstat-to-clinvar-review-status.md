# OLI-6: Rename clinvar_clnrevstat to clinvar_review_status

- Status: Backlog
- Priority: Medium
- Estimate: 1
- Labels: schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-6/rename-clinvar-clnrevstat-to-clinvar-review-status

## Description

New ERD names the field clinvar_review_status. Update annotate_cohort.py, the cohort_variants mapping in cohort_export.py:95, and any GraphQL/UI reference. Requires a reindex; do together with the HGVS rename to avoid two reindexes.

## Comments

None.
