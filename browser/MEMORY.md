# browser/MEMORY.md

Scoped memory for the patched gnomAD browser layer.

## Dataset

- Dataset key: `cohort`
- Display label: `Cohort`
- Reference genome: `GRCh37`
- Current demo Elasticsearch index: `cohort_variants`
- Production split: `cohort_variants_internal` or equivalent stays on the internal patient-linked
  VM; `cohort_variants_public` or equivalent lives on the separate public/browser VM and contains
  sanitized variant-level documents only.

## Local Services

- Elasticsearch: `http://localhost:9200`
- Redis: `localhost:6379`
- GraphQL API: `http://localhost:8000`
- Browser UI: `http://localhost:3000`

## Use Cases

See `docs/BROWSER_USE_CASES.md` for the six target query scenarios (UC-1 through UC-6),
required ES fields, example queries, and gap analysis.

## Elasticsearch Fields

Core variant identity:

- `variant_id`
- `chrom`
- `pos`
- `ref`
- `alt`
- `rsids`
- `filters`

Cohort frequencies:

- `ac_total`
- `an_total`
- `af_total`
- `hom_count`

Legacy VDS-only fallback fields:

- `ac`
- `an`
- `af`
- `n_hom`

Annotation fields (indexed):

- `gene_symbol`
- `consequence`
- `impact`
- `gdna` (source VEP field: `HGVS_g`)
- `cdna` (source VEP field: `HGVS_c`)
- `p_nomen` (source VEP field: `HGVS_p`)
- `transcript`
- `cadd_score` (source: dbNSFP CADD PHRED field)
- `clinvar_sig`
- `clinvar_clnrevstat`
- `gnomad_af`
- `gnomad_nonfin`

Annotation fields (planned, not yet in ES):

- `revel_score` (source: dbNSFP `REVEL_score`) — needed for UC-5B
- `sift_score` (source: dbNSFP)
- `polyphen_score` (source: dbNSFP)
- `metarnn_score` (source: dbNSFP)
- `clinpred_score` (source: dbNSFP)
- `alphamissense_score` (source: dbNSFP)

Internal-only fields (not yet in ES, needed for UC-3):

- `hpo_ids` (keyword array — from HPO assignment join table)
- `hpo_terms` (keyword array — from HPO lookup table)

## Query Behavior

- Variant lookup searches `variant_id` or `rsids`.
- Region lookup filters by `chrom` and `pos`.
- Gene and transcript queries currently use upstream gene/transcript coordinates and return
  overlapping cohort variants.
- Autocomplete searches variant ID prefixes and exact rsID terms.
- GraphQL variant formatting prefers annotated MatrixTable fields and falls back to VDS-only fields.
- Public GraphQL/browser responses must be backed by the sanitized public index only.

Combined filter queries (needed, not yet implemented):

- `gene_symbol` + `clinvar_sig` — ClinVar pathogenic variants in a gene (UC-4)
- `gene_symbol` (multi-value) + `clinvar_sig` or `revel_score` threshold — panel frequency (UC-5)
- `consequence` + `gene_symbol` — missense/LOF in a gene (UC-6A)
- `impact` + `chrom` + `pos` range — high-impact variants in a region (UC-6B)
- `hpo_ids` or `hpo_terms` — samples by HPO (UC-3, internal only)

## Known Gaps

- Gene-level frequency summaries are not yet defined.
- Metadata-derived filtering is not yet represented in the Elasticsearch mapping.
- `gnomad_nonfin` source semantics must be confirmed before exposing it prominently.
- Exact public export field allowlist, index name, and low-count privacy policy are not yet defined.
- `revel_score` is not yet in the ES mapping or export (needed for UC-5B).
- HPO fields (`hpo_ids`, `hpo_terms`) are not in the ES index (needed for UC-3).
- Sample-level carrier queries are not supported (needed for UC-1, UC-2, UC-3).
- Combined filter GraphQL queries are not implemented (UC-4, UC-5, UC-6).
- Direct `gene_symbol` term query is not exposed (current gene query uses region overlap).
