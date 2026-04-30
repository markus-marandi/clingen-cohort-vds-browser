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

Sample CSV fields from the current test file:

- `sample_id`
- `sex`
- `age`
- `date_seq`
- `run_id`
- `care_site`
- `health_status`

Panel CSV fields from the current test file:

- `sample_id`
- `panel`

Expected future metadata also includes:

- `test`
- `instrument`
- chromosomal sex (inferred from genotypes, stored separately from social `sex`)

HPO data comes from two sources joined by `HPO_ID`:

- Per-sample HPO assignments: `sample_id`, `HPO_ID` (one-to-many)
- HPO lookup table: `HPO_ID`, `HPO_termin`, `HPO_version`, `date/valid from`

## Validation Risks

- Panel rows can reference samples missing from the sample CSV.
- Some sample IDs may contain typos and need explicit reporting.
- `date_seq` may arrive as `M/D/YY`; normalize before downstream use.
- Social self-defined sex is not the same as chromosomal sex.
- One sample can have multiple ordered panels.

## Annotation Assumptions

- Current docs assume GRCh37/hg19.
- VEP config is `vep_settings.json`.
- VEP version in docs is 108.
- gnomAD source in docs is v2.1.1 exomes with `AF` and `AF_nfe`.
- CADD and ClinVar are expected through VEP/custom annotation paths, but exact production setup
  still needs confirmation.
