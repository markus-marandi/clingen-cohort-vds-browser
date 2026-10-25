# OLI-24: Expose the annotation fields through GraphQL

- Status: Implemented in main
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: graphql, browser
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-24/expose-the-annotation-fields-through-graphql

## Current assessment

d776a06 exposes the existing VEP, predictor, ClinVar review-status, and population/constraint fields through GraphQL and the variant page. Missing condition and requested field renames remain OLI-4/5/6, not a reason to call the entire API field set absent.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Open item in browser/TODO.md > GraphQL. Decide and implement how gnomad_nonfin, the agreed predictor set, ClinVar fields, and VEP fields surface in the API. Blocked by the predictor-set decision.

## Comments

None.
