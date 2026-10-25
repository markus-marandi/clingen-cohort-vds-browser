# OLI-7: Model the Procedure entity (test, instrument, ref_genome)

- Status: Design only
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 5
- Labels: schema, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-7/model-the-procedure-entity-test-instrument-ref-genome

## Current assessment

Procedure fields are designed but have no dedicated validated internal export. V1 is GRCh37-only. Whether one sample can have multiple sequencing procedures remains unresolved; do not assume resequencing is excluded.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Prepared pilot branch

`feat/internal-sample-browser` (not merged/deployed): The pilot carries the supplied test/instrument/ref_genome fields with each sample and requires one procedure row per sample. Resequencing identity is deferred.
See [dragon handoff](../../../docs/DRAGON_SAMPLE_PILOT_HANDOFF.md).

## Original exported description

New ERD adds a Procedure table keyed on sample_id: test, instrument, ref_genome. `test` and `instrument` currently only ride along in the metadata CSV via annotate_cols; ref_genome does not exist anywhere. Define the source columns, add them to the internal sample export, and decide whether Procedure is its own index or fields on cohort_samples.

## Comments

### Markus Mar — 2026-08-27T14:16:56Z

Design decision pre-recorded in `docs/SAMPLE_VARIANT_INDEX.md` (2026-08-27): Procedure is **flattened onto `cohort_samples`** as `test` / `instrument` / `ref_genome` rather than given its own index — the relationship is 1:1 with the sample, and a separate index would cost a join on every carrier query for three keyword fields.

Reopen this if a sample can have more than one procedure (re-sequencing on a different instrument, or a WES and a WGS run under the same sample_id). That case makes the flattening wrong and the separate entity right, and it also changes OLI-8: `ref_genome` stops being a per-sample constant.
