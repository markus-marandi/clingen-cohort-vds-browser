# OLI-9: Align sample schema: chromosomal_sex -> sex_chr, add date_seq and run_id

- Status: Backlog
- Priority: High
- Estimate: 2
- Labels: schema
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-9/align-sample-schema-chromosomal-sex-sex-chr-add-date-seq-and-run-id

## Description

New ERD Sample data is: sample_id, sex_assigned, sex_chr, date_of_birth, date_seq, run_id, material, care_site, health_status. The design in docs/SAMPLE_VARIANT_INDEX.md uses chromosomal_sex and has neither date_seq nor run_id. Update the design doc and the cohort_samples mapping before the index is built.

## Comments

### Markus Mar — 2026-08-27T14:17:01Z

Doc half done (2026-08-27). `docs/SAMPLE_VARIANT_INDEX.md` `cohort_samples` mapping now reads: sample_id, sex_assigned, **sex_chr**, date_of_birth, **date_seq** (date), **run_id**, material, care_site, health_status, test, instrument, ref_genome, panels, hpo_ids. `hpo_terms` dropped from the sample doc — labels come from the versioned lookup (OLI-14/OLI-15).

`docs/ARCHITECTURE.md` gains a "Schematic vs. implementation" table listing every ERD-vs-code delta with its issue number.

Still open here: the code half — `chromosomal_sex` → `sex_chr` in `annotate_cohort.py` and the metadata loader, plus wiring `date_seq` and `run_id` through the metadata join. Stale `chromosomal_sex` references remain in `MEMORY.md`, `AGENTS.md`, `annotation_sources.md` and `docs/pipeline/MEMORY.md`; those are accurate to the code today and should flip in the same commit as the rename, not before.
