# browser/TODO.md

Browser and Elasticsearch backlog.

## Export

- [ ] Verify `cohort_export.py` writes `ac_total`, `an_total`, `af_total`, and `hom_count` for
      annotated MatrixTables.
- [ ] Decide whether to remove or keep legacy fallback fields `ac`, `an`, `af`, and `n_hom`.
- [ ] Add a tiny export fixture or documented command that indexes a known demo variant.
- [ ] Add validation for required fields before bulk indexing.
- [ ] Decide whether per-sample VCF fields (`depth`, `gq`, `var%`) are indexed in Elasticsearch or
      kept only in VDS/MatrixTable.

## GraphQL

- [ ] Update cohort variant formatting to prefer annotated cohort fields over legacy fallback
      fields.
- [ ] Test `fetchVariantById` with variant ID and rsID.
- [ ] Test region, gene, transcript, and autocomplete queries against the local demo index.
- [ ] Decide how to expose `gnomad_nonfin`, CADD, ClinVar, and VEP fields through GraphQL.

## Browser UI

- [ ] Confirm the cohort dataset appears in the dataset selector as `Cohort`.
- [ ] Smoke test a known local demo variant in the UI.
- [ ] Decide how gene-level frequency summaries should appear.
- [ ] Decide which clinical metadata fields are filterable or display-only in v1.

## Local Stack

- [ ] Confirm `docker compose up --build` works after `./setup.sh`.
- [ ] Document any Podman-specific command differences if Docker is unavailable.
- [ ] Keep generated `gnomad-browser/` out of tracked source.
