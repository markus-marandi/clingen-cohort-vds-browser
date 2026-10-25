# OLI-5: Add clinvar_condition (CLNDN) to annotation and export

- Status: Remaining
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: schema, annotation, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-5/add-clinvar-condition-clndn-to-annotation-and-export

## Current assessment

ClinVar significance and review status exist. CLNDN/clinvar_condition is not requested or exported by the current main pipeline.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

New ERD requires clinvar_condition on ClinVar_annotation. annotate_cohort.py:343-348 extracts only CLNSIG and CLNREVSTAT, and the field is absent from the ES mapping. Add CLNDN to the ClinVar custom-annotation pull, the cohort_variants mapping, and the GraphQL variant payload.

## Comments

None.
