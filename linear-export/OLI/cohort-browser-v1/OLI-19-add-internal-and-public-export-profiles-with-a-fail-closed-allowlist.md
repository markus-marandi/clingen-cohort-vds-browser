# OLI-19: Add internal and public export profiles with a fail-closed allowlist

- Status: Deferred outside v1
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 5
- Labels: export, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-19/add-internal-and-public-export-profiles-with-a-fail-closed-allowlist

## Current assessment

Public deployment is outside the user-confirmed v1 scope. Public export profiles and a public-release allowlist are deferred, not counted as current release blockers. Internal sample exports remain active under OLI-17/18.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Open item in browser/TODO.md > Export. The public profile must emit only allowlisted variant-level fields and fail if any patient-linked field (sample_id, hpo_ids, genotype, gq, depth, var_pct, in_report, date_of_birth, date_seq, run_id, care_site) is present. See docs/ARCHITECTURE.md two-VM model.

Non-negotiable for the public release.

## Comments

None.
