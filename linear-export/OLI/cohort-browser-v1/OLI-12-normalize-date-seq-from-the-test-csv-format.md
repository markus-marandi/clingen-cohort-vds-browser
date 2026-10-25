# OLI-12: Normalize date_seq from the test CSV format

- Status: Remaining
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Medium
- Estimate: 2
- Labels: metadata, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-12/normalize-date-seq-from-the-test-csv-format

## Current assessment

Metadata fields pass through as CSV strings. Date parsing, accepted input formats, invalid-date handling, and sequencing-date versus ingestion-date semantics remain.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Open item in TODO.md > Metadata. date_seq is now an explicit schematic field, so it needs a stable date representation and an ES date mapping. Define the input format(s), the parse, and the failure behavior for unparseable values.

## Comments

None.
