# OLI-2: Download dbNSFP v5.3.1 on VM and verify field names

- Status: Backlog
- Priority: Urgent
- Estimate: 3
- Labels: annotation, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-2/download-dbnsfp-v531-on-vm-and-verify-field-names

## Description

Code migration to dbNSFP done 2026-07-14; VM data download and field-name verification still pending. Verify every plugin field name parsed in annotate_cohort.py:333-341 (CADD_phred, REVEL_score, SIFT_score, LOEUF, MOEUF, gnomAD_genomes_AF) against the actual v5.3.1 release header. See annotation_sources.md status block.

## Comments

None.
