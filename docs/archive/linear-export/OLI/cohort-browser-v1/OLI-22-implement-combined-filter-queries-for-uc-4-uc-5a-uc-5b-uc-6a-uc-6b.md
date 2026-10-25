# OLI-22: Implement combined filter queries for UC-4, UC-5A, UC-5B, UC-6A, UC-6B

- Status: Implemented in main
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 8
- Labels: graphql, browser
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-22/implement-combined-filter-queries-for-uc-4-uc-5a-uc-5b-uc-6a-uc-6b

## Current assessment

ce2823a implements cohort_filtered_variants with gene(s), ClinVar significance, consequence, impact, REVEL threshold, and chrom/position filters, plus mean/max AF summaries. Consequence matching, threshold boundaries, and pagination still need acceptance checks; the query itself is not missing.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Per docs/BROWSER_USE_CASES.md: gene_symbol + clinvar_sig (UC-4); multi-gene terms + clinvar_sig with per-gene aggregations (UC-5A); multi-gene terms + revel_score threshold with per-gene aggregations (UC-5B); consequence + gene_symbol (UC-6A); impact + chrom + pos range (UC-6B).

## Comments

None.
