# OLI-11: Clarify assay-conditional metadata (Panels = WES, HPO = WGS)

- Status: Decision pending
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Medium
- Estimate: 2
- Labels: decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-11/clarify-assay-conditional-metadata-panels-wes-hpo-wgs

## Current assessment

Clarify whether Panels=WES and HPO=WGS are enforced constraints or descriptions of current data. The internal-only scope does not answer this assay question.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

The new ERD labels Panels as '(WES sample)' and HPO IDs as '(WGS sample)'. Decide whether this is a hard constraint (validate and reject cross-assay rows) or descriptive of current data only, and document the answer. Affects validation and the UI's empty states.

Droppable if scope compresses.

## Comments

None.
