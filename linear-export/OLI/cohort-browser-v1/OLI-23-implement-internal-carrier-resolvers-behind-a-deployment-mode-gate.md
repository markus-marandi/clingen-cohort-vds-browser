# OLI-23: Implement internal carrier resolvers behind a DEPLOYMENT_MODE gate

- Status: Backlog
- Priority: Urgent
- Estimate: 5
- Labels: graphql, internal, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-23/implement-internal-carrier-resolvers-behind-a-deployment-mode-gate

## Description

cohort_variant_carriers (UC-1), cohort_gene_carriers (UC-2), cohort_hpo_samples (UC-3). Registered only when DEPLOYMENT_MODE=internal, per docs/SAMPLE_VARIANT_INDEX.md section 4. The gate must be enforced at schema-build time so the fields are not introspectable on the public deployment.

Non-negotiable for the public release.

## Comments

None.
