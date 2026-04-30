# TODO.md

Top-level roadmap for agents. Keep detailed work items in scoped `TODO.md` files.

## Local Demo

- [ ] Decide whether tiny VCF/GVCF fixtures can be committed or must stay local.
- [ ] Add or document demo inputs for sample metadata and panel metadata.
- [ ] Add a metadata validation command for sample IDs, required columns, date formats, and panel
      cardinality.
- [ ] Run a tiny Hail path that produces VDS or MatrixTable output locally.
- [ ] Verify `variant_qc()` fields: `ac_total`, `an_total`, `af_total`, `hom_count`.
- [ ] Export demo variants to local Elasticsearch index `cohort_variants`.
- [ ] Smoke test browser at `http://localhost:3000` with a known variant and gene query.

## Metadata

- [ ] Define the production GE server metadata contract.
- [ ] Split social self-defined sex and chromosomal sex into separate fields.
- [ ] Normalize `date_seq` from test CSV format into a stable date representation.
- [ ] Validate panels-only samples and suspicious sample IDs before annotation.
- [ ] Decide HPO cardinality, versioning, and whether HPO filters are in v1.

## Hail Pipeline

- [ ] Keep VDS as the genotype storage layer and MatrixTable as the annotation/export layer.
- [ ] Make metadata loading compatible with one sample having multiple panels.
- [ ] Add or document a small fixture path for local Hail smoke tests.
- [ ] Define gene-level frequency aggregation from variant-level genotype statistics.
- [ ] Track long-running production paths and machine-specific defaults outside committed docs.

See `docs/pipeline/TODO.md` for pipeline-specific details.

## Browser

- [ ] Keep `browser/` patches aligned with generated `gnomad-browser/`.
- [ ] Expose cohort fields in Elasticsearch, GraphQL, and UI consistently.
- [ ] Verify variant, region, gene, transcript, and autocomplete queries.
- [ ] Decide how gene-level frequency summaries should appear in the browser.

See `browser/TODO.md` for browser-specific details.

## Deployment

- [ ] Define server paths for VDS, MatrixTable, annotations, plugins, and Elasticsearch storage.
- [ ] Document resource sizing for 2000 WES/WGS samples.
- [ ] Define refresh policy for new samples and metadata.
- [ ] Keep credentials, private URLs, and PHI out of tracked docs.
