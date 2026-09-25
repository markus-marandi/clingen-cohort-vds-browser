# OLI-14: Build the hpo_terms dimension table with versioning

- Status: Backlog
- Priority: High
- Estimate: 3
- Labels: metadata, schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-14/build-the-hpo-terms-dimension-table-with-versioning

## Description

New ERD adds an HPO terms entity keyed on hpo_id: hpo_term, hpo_version, valid_from. This resolves the open HPO versioning question in TODO.md > Metadata. Create a standalone hpo_terms index loaded from the HPO release, carrying the release version and validity date.

## Comments

None.
