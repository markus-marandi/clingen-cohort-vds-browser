# OLI-16: Add a metadata validation command

- Status: Remaining
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: metadata, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-16/add-a-metadata-validation-command

## Current assessment

There is no dedicated metadata validation command. Required fields, sample relationships, dates/prenatal tokens, vocabularies, repeated panels, HPO IDs, and report tokens still need validation.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Open item in TODO.md > Local Demo. Validate sample IDs, required columns, date formats (date_of_birth, date_seq), panel cardinality, and HPO ID well-formedness before annotation runs. Should fail loudly rather than producing null columns downstream.

## Comments

None.
