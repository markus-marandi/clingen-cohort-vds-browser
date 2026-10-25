# OLI-23: Implement internal carrier resolvers behind a DEPLOYMENT_MODE gate

- Status: Rescoped
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 5
- Labels: graphql, internal, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-23/implement-internal-carrier-resolvers-behind-a-deployment-mode-gate

## Current assessment

The carrier/HPO resolvers remain unimplemented and required. All authenticated internal users may see sample IDs. A separate public schema/deployment-mode gate is not a v1 acceptance requirement while public deployment is excluded.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Prepared pilot branch

`feat/internal-sample-browser` (not merged/deployed): Sample/detail/called-variant and variant/gene carrier GraphQL resolvers are prepared with configurable indices, cursor paging and visible errors. HPO and a public deployment gate are outside this pilot.
See [the internal server handoff](../../../DRAGON_SAMPLE_PILOT_HANDOFF.md).

## Original exported description

cohort_variant_carriers (UC-1), cohort_gene_carriers (UC-2), cohort_hpo_samples (UC-3). Registered only when DEPLOYMENT_MODE=internal, per docs/SAMPLE_VARIANT_INDEX.md section 4. The gate must be enforced at schema-build time so the fields are not introspectable on the public deployment.

Non-negotiable for the public release.

## Comments

None.
