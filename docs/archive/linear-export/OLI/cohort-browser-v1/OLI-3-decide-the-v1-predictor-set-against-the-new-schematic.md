# OLI-3: Decide the v1 predictor set against the new schematic

- Status: Decision pending
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 1
- Labels: decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-3/decide-the-v1-predictor-set-against-the-new-schematic

## Current assessment

The predictor superset already exists in annotation, export, GraphQL, and the variant page. Agreeing which fields are required for v1 is still a scope decision; this no longer blocks recognizing OLI-24 as implemented.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

New ERD lists only cadd_score, revel_score, sift_score on VEP annotation. Code currently also produces polyphen_score, metarnn_score, clinpred_score, alphamissense_score, dbnsfp_popmax_af, gnomad_loeuf, gnomad_moeuf. Decide: narrow the export to the schematic, or keep the extras as a documented superset. UC-5B depends on revel_score. Blocks the ES mapping and GraphQL exposure tasks.

## Comments

None.
