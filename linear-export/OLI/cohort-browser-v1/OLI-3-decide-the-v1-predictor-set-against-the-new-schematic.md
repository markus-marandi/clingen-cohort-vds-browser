# OLI-3: Decide the v1 predictor set against the new schematic

- Status: Backlog
- Priority: Urgent
- Estimate: 1
- Labels: decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-3/decide-the-v1-predictor-set-against-the-new-schematic

## Description

New ERD lists only cadd_score, revel_score, sift_score on VEP annotation. Code currently also produces polyphen_score, metarnn_score, clinpred_score, alphamissense_score, dbnsfp_popmax_af, gnomad_loeuf, gnomad_moeuf. Decide: narrow the export to the schematic, or keep the extras as a documented superset. UC-5B depends on revel_score. Blocks the ES mapping and GraphQL exposure tasks.

## Comments

None.
