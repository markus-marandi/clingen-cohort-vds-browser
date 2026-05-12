# docs/pipeline/MEMORY.md

Scoped memory for Hail, VDS, MatrixTable, metadata, and cohort frequency work.

## Storage Model

- Raw and filtered GVCFs are large inputs and should not be committed.
- Hail VDS stores genotype data across incremental cohort loads.
- Annotated MatrixTable stores row annotations, column metadata, and fields needed for export.
- Elasticsearch receives flattened variant documents from MatrixTable or VDS fallback export.

## Current Scripts

- `parallel_ingest_cohort.py` filters contigs with bcftools, renames chromosomes, and combines
  GVCFs into versioned VDS outputs.
- `ingest_manifest.py` records completed, failed, and in-progress VDS runs.
- `annotate_cohort.py` densifies VDS, converts `LGT` to `GT`, computes `variant_qc()`, runs VEP,
  joins gnomAD, optionally joins metadata, and writes an annotated MatrixTable.
- `browser/data-pipeline/cohort_export.py` indexes row-level variant documents into Elasticsearch.

## Cohort Frequency Fields

These are database-level cohort fields from `hl.variant_qc()`:

- `ac_total`: alternate allele count.
- `an_total`: total called alleles.
- `af_total`: alternate allele frequency.
- `hom_count`: alternate homozygote count.

## Metadata Inputs

Sample CSV fields (updated schema — CSV files not yet updated):

- `sample_id`
- `chromosomal_sex` (was `sex` — inferred from genotype; XX, XY, ambiguous)
- `sex_assigned` (was `sex` — from clinical metadata; Male, Female, Other, Unknown)
- `date_of_birth` (was `age` — ISO date string YYYY-MM-DD)
- `date_seq`
- `run_id`
- `care_site`
- `health_status`
- `material` (new — blood, tissue, saliva, buccal swab, etc.)
- `test` (future — WES / WGS)
- `instrument` (future — sequencer model)

Panel CSV fields (unchanged):

- `sample_id`
- `panel`

HPO data from two sources joined by `HPO_ID`:

- Per-sample HPO assignments: `sample_id`, `HPO_ID` (one-to-many)
- HPO lookup table: `HPO_ID`, `HPO_termin`, `HPO_version`, `date_valid_from`

## Validation Risks

- Panel rows can reference samples missing from the sample CSV.
- Some sample IDs may contain typos and need explicit reporting.
- `date_seq` and `date_of_birth` may arrive in non-ISO formats; normalize to YYYY-MM-DD before load.
- `chromosomal_sex` and `sex_assigned` are separate fields and must never be merged or conflated.
- One sample can have multiple ordered panels.

## Annotation Decisions

- GRCh37/hg19, VEP 108, gnomAD v2.1.1 exomes.
- Annotation sources decided: VEP + CADD 1.6 (VEP plugin CADD.pm) + dbNSFP 4.x + ClinVar
  (local VCF join) + gnomAD v2.1.1 (local HT join).
- CADD 1.6 TSV data files still need to be downloaded to `/mnt/sdb/reference/cadd_v1.6/`.
- dbNSFP integration method pending (VEP plugin vs. Hail join); data files not yet downloaded.
- See `annotation_sources.md` for per-field paths and configuration details.
