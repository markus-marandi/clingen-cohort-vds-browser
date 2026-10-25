# OLI-30: Decide the privacy threshold for low aggregate counts

- Status: Deferred outside v1
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 2
- Labels: decision, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-30/decide-the-privacy-threshold-for-low-aggregate-counts

## Current assessment

A public low-count suppression policy is outside v1. All authenticated internal users may see sample IDs; do not add a count-based gate that conceals required internal carrier lists.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Before any internet-facing release, decide whether very low ac_total/hom_count values are suppressed, rounded, bucketed, or kept internal-only. Applies to the public tier and possibly to internal carrier lists. Open item in TODO.md > Deployment.

Non-negotiable for the public release.

## Comments

None.
