# OLI-26: Add carrier-list views for UC-1, UC-2, UC-3 (internal build only)

- Status: Remaining
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 5
- Labels: ui, internal, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-26/add-carrier-list-views-for-uc-1-uc-2-uc-3-internal-build-only

## Current assessment

Carrier/sample views for variant, gene, and HPO lookups do not exist. All authenticated internal users may see sample IDs. A public build is outside v1 scope.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Prepared pilot branch

`feat/internal-sample-browser` (not merged/deployed): Variant/gene carrier panels and sample list/detail/variant/report-link views are prepared. Synthetic API/UI checks pass; real browser acceptance is pending on the internal server. HPO is deferred.
See [the internal server handoff](../../../DRAGON_SAMPLE_PILOT_HANDOFF.md).

## Original exported description

Variant page shows carrier samples; gene page shows samples with a variant in the gene; HPO lookup shows matching samples. Must be absent from the public build, not merely hidden.

## Comments

None.
