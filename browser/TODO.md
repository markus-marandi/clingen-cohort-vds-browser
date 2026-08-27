# browser/TODO.md

Browser and Elasticsearch backlog.

## Export

- [x] Verify `cohort_export.py` writes `ac_total`, `an_total`, `af_total`, and `hom_count` for
      annotated MatrixTables. (confirmed 2026-04-30)
- [x] Decide whether to remove or keep legacy fallback fields `ac`, `an`, `af`, and `n_hom`.
      Decision (2026-07-14): **KEEP**. They are the output of the VDS-only fallback export
      (`export_vds_to_es`, used before the annotation pipeline has run), and `formatVariant`
      already coalesces annotated `*_total`/`hom_count` over them. Cost is 4 nullable fields.
- [ ] Add export profiles for `internal` and `public`; public must use an explicit allowlist and
      fail if patient-linked fields are present.
- [ ] Define production index names for internal and public variant databases.
- [ ] Add a tiny export fixture or documented command that indexes a known demo variant.
- [ ] Add validation for required fields before bulk indexing.
- [ ] Decide whether per-sample VCF fields (`depth`, `gq`, `var%`) are indexed in Elasticsearch or
      kept only in VDS/MatrixTable.
- [x] Add `revel_score` to ES mapping and `cohort_export.py` export (needed for UC-5B). (done
      2026-07-14, together with the full dbNSFP predictor set)
- [~] Add `hpo_ids` and `hpo_terms` to internal ES mapping and export (needed for UC-3, internal only).
      `annotate_cohort.py` HPO sample-join done 2026-07-14 (`--hpo-path` / `--hpo-lookup-path`); the
      internal ES mapping + export land with the sample-variant index (docs/SAMPLE_VARIANT_INDEX.md).

## GraphQL

- [x] Update cohort variant formatting to prefer annotated cohort fields over legacy fallback
      fields. (done 2026-05-06)
- [ ] Test `fetchVariantById` with variant ID and rsID.
- [ ] Test region, gene, transcript, and autocomplete queries against the local demo index.
- [ ] Decide how to expose `gnomad_nonfin`, dbNSFP-derived predictor fields, ClinVar, and VEP
      fields through GraphQL.
- [ ] Add direct `gene_symbol` term query (currently gene query uses region overlap only).
- [ ] Add combined filter queries for use cases (see `docs/BROWSER_USE_CASES.md`):
  - [ ] `gene_symbol` + `clinvar_sig` (UC-4)
  - [ ] Multi-gene `terms` + `clinvar_sig` with per-gene aggregations (UC-5A)
  - [ ] Multi-gene `terms` + `revel_score` threshold with per-gene aggregations (UC-5B)
  - [ ] `consequence` + `gene_symbol` (UC-6A)
  - [ ] `impact` + `chrom` + `pos` range (UC-6B)
  - [ ] `hpo_ids` / `hpo_terms` sample lookup (UC-3, internal only)

## Browser UI

- [ ] Confirm the cohort dataset appears in the dataset selector as `Cohort`.
- [ ] Smoke test a known local demo variant in the UI.
- [ ] Decide how gene-level frequency summaries should appear.
- [ ] Decide which clinical metadata fields are filterable or display-only in v1.
- [ ] Add ClinVar significance filter dropdown on gene pages (UC-4).
- [ ] Add consequence / impact filter on gene and region pages (UC-6).
- [ ] Add REVEL score threshold filter (UC-5B).

## Sample-Level Queries (Internal Only)

These use cases return per-sample results and must not be exposed through the public browser.

- [ ] Design internal sample-variant index or MT query path for carrier lookups (UC-1, UC-2, UC-3).
- [ ] Implement sample list response for variant lookup — which samples carry this variant? (UC-1).
- [ ] Implement sample list response for gene query — which samples have a variant in this gene? (UC-2).
- [ ] Implement HPO-based sample lookup (UC-3).

## Local Stack

- [x] Runtime-agnostic entry point: `scripts/stack.sh {doctor|up|down|logs|ps}` resolves docker/podman and the compose implementation on every run, and preflights the host (/etc/mtab, podman storage roots, fuse-overlayfs, TMPDIR, port conflicts) before starting anything.
- [x] Fully qualify image names (`docker.io/library/redis:7-alpine`, `docker.io/library/node:20-slim`) so Podman does not need `unqualified-search-registries`.
- [ ] Run `scripts/stack.sh doctor` on oligo-VM (needs VPN) and confirm the /etc/mtab verdict. If it reports a dangling symlink: `sudo ln -sf /proc/self/mounts /etc/mtab`.
- [ ] Confirm `scripts/stack.sh up` builds and serves on the server. Note the native (non-compose) path in DEMO.md is the currently working deployment.
- [ ] Keep generated `gnomad-browser/` out of tracked source.
