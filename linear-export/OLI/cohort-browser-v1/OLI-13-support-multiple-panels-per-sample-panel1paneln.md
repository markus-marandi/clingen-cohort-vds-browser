# OLI-13: Support multiple panels per sample (Panel1..PanelN)

- Status: Backlog
- Priority: High
- Estimate: 3
- Labels: metadata, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-13/support-multiple-panels-per-sample-panel1paneln

## Description

New ERD models Panels as a sample_id-keyed table with Panel1/Panel2/Panel3. The metadata loader in annotate_cohort.py:366 expects a singular `panel` column. Load into a panels array on cohort_samples and handle samples with zero panels. Open item in TODO.md > Hail Pipeline.

## Comments

None.
