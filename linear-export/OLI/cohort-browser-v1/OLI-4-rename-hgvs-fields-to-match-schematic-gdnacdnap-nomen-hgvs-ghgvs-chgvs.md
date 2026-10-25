# OLI-4: Rename HGVS fields to match schematic (gdna/cdna/p_nomen -> HGVS_g/HGVS_c/HGVS_p)

- Status: Remaining
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: browser, schema, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-4/rename-hgvs-fields-to-match-schematic-gdnacdnap-nomen-hgvs-ghgvs-chgvs

## Current assessment

HGVS values exist as gdna, cdna, and p_nomen. The specified HGVS_g/HGVS_c/HGVS_p names or documented compatibility aliases remain to be implemented.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../docs/IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

New ERD names these HGVS_g, HGVS_c, HGVS_p. cohort_export.py:78-80 maps them as gdna, cdna, p_nomen and the GraphQL formatVariant reads cdna/p_nomen. Rename across annotate_cohort.py, cohort_export.py, cohort-variant-queries.ts, and the UI. Requires a reindex.

## Comments

None.
