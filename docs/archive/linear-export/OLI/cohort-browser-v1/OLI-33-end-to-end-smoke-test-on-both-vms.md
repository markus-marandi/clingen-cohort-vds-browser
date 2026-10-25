# OLI-33: End-to-end smoke test on both VMs

- Status: Partial
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 3
- Labels: qa, deployment, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-33/end-to-end-smoke-test-on-both-vms

## Current assessment

The reviewed branch 47cab9f has reported isolated live checks for aggregate variant, gene, region, filter, and CSV paths. Main has not incorporated that branch. Carrier/HPO/report end-to-end checks remain, and the original second-public-VM requirement is deferred.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Known variant and gene query through the internal browser with carrier lists, and the same variant through the public browser with aggregates only and no sample linkage. Verify the public deployment cannot introspect internal fields.

## Comments

None.
