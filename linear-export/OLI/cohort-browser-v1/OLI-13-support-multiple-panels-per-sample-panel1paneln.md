# OLI-13: Support multiple panels per sample (Panel1..PanelN)

- Status: Design only
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: metadata, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-13/support-multiple-panels-per-sample-panel1paneln

## Current assessment

The design has a panels array, but the loader has no one-to-many panel grouping step. The supplied panel list is explicitly expandable, so it must not become a permanently closed enum. The user confirmed that samples without separately ordered panels use whole exome. See docs/INPUT_DATA.md for the TSV paths and completeness checks.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Prepared pilot branch

`feat/internal-sample-browser` (not merged/deployed): The pilot groups one-to-many panel rows into an array and preserves raw labels, including Unknown and whole exome; no closed vocabulary or panel expansion is introduced.
See [dragon handoff](../../../docs/DRAGON_SAMPLE_PILOT_HANDOFF.md).

## Original exported description

New ERD models Panels as a sample_id-keyed table with Panel1/Panel2/Panel3. The metadata loader in annotate_cohort.py:366 expects a singular `panel` column. Load into a panels array on cohort_samples and handle samples with zero panels. Open item in TODO.md > Hail Pipeline.

## Comments

None.
