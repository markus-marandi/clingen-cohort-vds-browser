# OLI-15: Stop denormalizing hpo_terms onto samples; keep hpo_ids and join

- Status: Backlog
- Priority: Medium
- Estimate: 3
- Labels: metadata, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-15/stop-denormalizing-hpo-terms-onto-samples-keep-hpo-ids-and-join

## Description

annotate_cohort.py:391-393 writes both hpo_ids and hpo_terms arrays onto each sample column. With a versioned hpo_terms dimension table, the label belongs there and the sample should carry hpo_ids only, so a term rename does not require re-annotating samples. Update _load_hpo_ht and the cohort_samples mapping.

Droppable if scope compresses.

## Comments

None.
