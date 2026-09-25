# OLI-10: Resolve the In Report duplication (field vs join table)

- Status: Backlog
- Priority: High
- Estimate: 2
- Labels: decision, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-10/resolve-the-in-report-duplication-field-vs-join-table

## Description

The new ERD carries in_report both as a column on Genomic data (VCF) and as a separate In Report table keyed on sample_id + variant_id. Decide which is authoritative, define the source of the clinical-report flag, and document the load path.

## Comments

None.
