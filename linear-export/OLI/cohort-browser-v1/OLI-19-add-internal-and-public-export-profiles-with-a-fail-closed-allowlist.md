# OLI-19: Add internal and public export profiles with a fail-closed allowlist

- Status: Backlog
- Priority: Urgent
- Estimate: 5
- Labels: export, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-19/add-internal-and-public-export-profiles-with-a-fail-closed-allowlist

## Description

Open item in browser/TODO.md > Export. The public profile must emit only allowlisted variant-level fields and fail if any patient-linked field (sample_id, hpo_ids, genotype, gq, depth, var_pct, in_report, date_of_birth, date_seq, run_id, care_site) is present. See docs/ARCHITECTURE.md two-VM model.

Non-negotiable for the public release.

## Comments

None.
