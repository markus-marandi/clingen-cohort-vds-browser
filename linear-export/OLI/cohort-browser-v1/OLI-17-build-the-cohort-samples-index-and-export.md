# OLI-17: Build the cohort_samples index and export

- Status: Design only
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 5
- Labels: export, internal
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-17/build-the-cohort-samples-index-and-export

## Current assessment

cohort_samples has a documented mapping, but the exporter still reads variant rows only. No sample-column export or sample index creation path exists in main.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Prepared pilot branch

`feat/internal-sample-browser` (not merged/deployed): A standalone TSV metadata exporter and cohort_samples index creation path are now prepared. Real input export and integration are pending on dragon.
See [dragon handoff](../../../docs/DRAGON_SAMPLE_PILOT_HANDOFF.md).

## Original exported description

Per docs/SAMPLE_VARIANT_INDEX.md section 2a, updated for the new schematic (sex_chr, date_seq, run_id, panels, hpo_ids). Export from mt.cols(). Internal VM only. Serves UC-3.

## Comments

None.
