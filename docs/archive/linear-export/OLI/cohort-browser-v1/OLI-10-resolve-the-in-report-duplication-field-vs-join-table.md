# OLI-10: Resolve the In Report duplication (field vs join table)

- Status: Decision pending
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 2
- Labels: decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-10/resolve-the-in-report-duplication-field-vs-join-table

## Current assessment

The supplied specification has one-to-many sample/variant report rows and the input tokens Unknown, None, or a variant ID. No report importer or authoritative flag derivation exists. Preserve Unknown versus None; their exact semantics and storage contract are not explicitly defined by the table. See docs/SAMPLE_METADATA_SOURCE.md.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Prepared pilot branch

`feat/internal-sample-browser` (not merged/deployed): The pilot imports report rows separately, keeps None/Unknown distinct, derives nullable in_report membership, and counts/skips malformed IDs. This is report-list membership, not a clinical diagnosis.
See [the internal server handoff](../../../DRAGON_SAMPLE_PILOT_HANDOFF.md).

## Original exported description

The new ERD carries in_report both as a column on Genomic data (VCF) and as a separate In Report table keyed on sample_id + variant_id. Decide which is authoritative, define the source of the clinical-report flag, and document the load path.

## Comments

None.
