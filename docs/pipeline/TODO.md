# docs/pipeline/TODO.md

Pipeline and metadata backlog.

## Local Demo

- [ ] Choose fixture policy for tiny non-sensitive VCF/GVCF files.
- [ ] Add or document local demo input paths for sample and panel CSVs.
- [ ] Add a metadata validation command that checks required columns and sample ID joins.
- [ ] Add a tiny Hail command path that can run on a laptop.
- [ ] Verify `variant_qc()` output fields on the tiny dataset.
- [ ] Document exact demo output paths for VDS, MatrixTable, and Elasticsearch export.

## Metadata Merge

- [ ] Define a combined metadata shape from sample CSV and panel CSV.
- [ ] Preserve one-to-many panels without losing per-panel rows.
- [ ] Add fields for both social self-defined sex and chromosomal sex; decide destination (MatrixTable column annotation, separate Elasticsearch field, or both).
- [ ] Decide how `health_status` affects downstream filtering or display.
- [ ] Decide whether HPO terms are in local demo v1.

## VDS And MatrixTable

- [ ] Keep all production paths configurable with CLI flags.
- [ ] Decide whether local demo starts from tiny GVCF, tiny VCF, or a generated MatrixTable.
- [ ] Define internal MatrixTable outputs versus public sanitized export outputs.
- [ ] Add a schema/field check that blocks public exports containing `sample_id`, patient IDs,
      column metadata, per-sample genotypes, report flags, dates, HPO assignments, run IDs, or care
      sites.
- [ ] Add smoke checks for manifest resume behavior if ingest logic changes.
- [ ] Document expected resource ranges for 2000 WES/WGS samples.

## Annotation

- [ ] Confirm production VEP binary, cache path, and plugin paths.
- [ ] Confirm gnomAD v2.1.1 exome source path and `AF_nfe` semantics.
- [ ] Define dbNSFP v5.3.1 field allowlist, including CADD, REVEL, SIFT, PolyPhen2, MetaRNN,
      ClinPred, AlphaMissense, selected population AF, and gene constraint fields.
- [ ] Replace the current standalone CADD VEP config/parsing with dbNSFP-derived fields once the
      dbNSFP field allowlist and file branch are confirmed.
- [ ] Confirm ClinVar snapshot/versioning and join key.
- [ ] Define gene-level aggregation from variant rows.
