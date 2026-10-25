# OLI-28: Provision the two-VM production split

- Status: Deferred outside v1
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 5
- Labels: deployment, security
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-28/provision-the-two-vm-production-split

## Current assessment

The user excludes public deployment from v1. Provisioning a second public VM and enforcing the two-VM public split are deferred. Internal deployment readiness remains active work.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Internal patient-linked VM and public/sanitized browser VM, per docs/ARCHITECTURE.md. The public VM must have no network or filesystem path back to the internal VM. Related: VM migration to larger RAM+SSD host.

## Comments

None.
