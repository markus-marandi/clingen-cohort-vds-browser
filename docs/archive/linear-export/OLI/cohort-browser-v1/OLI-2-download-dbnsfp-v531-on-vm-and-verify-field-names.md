# OLI-2: Download dbNSFP v5.3.1 on VM and verify field names

- Status: Implemented in main
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: Urgent
- Estimate: 3
- Labels: annotation, pipeline
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-2/download-dbnsfp-v531-on-vm-and-verify-field-names

## Current assessment

439c025 records dbNSFP 5.3.1a field verification and end-to-end annotation. Main includes the consolidated GRCh37 file, predictor parsing, and the separate gene-constraint join. Do not count the original download/field-name task as wholly unimplemented; current production-run verification is OLI-34.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Code migration to dbNSFP done 2026-07-14; VM data download and field-name verification still pending. Verify every plugin field name parsed in annotate_cohort.py:333-341 (CADD_phred, REVEL_score, SIFT_score, LOEUF, MOEUF, gnomAD_genomes_AF) against the actual v5.3.1 release header. See annotation_sources.md status block.

## Comments

None.
