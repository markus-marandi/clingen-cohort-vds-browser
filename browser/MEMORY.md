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

Annotation fields:

- `gene_symbol`
- `consequence`
- `impact`
- `gdna` (source VEP field: `HGVS_g`)
- `cdna` (source VEP field: `HGVS_c`)
- `p_nomen` (source VEP field: `HGVS_p`)
- `transcript`
- `cadd_score` (source: dbNSFP CADD PHRED field)
- `revel_score`
- `sift_score`
- `polyphen_score`
- `metarnn_score`
- `clinpred_score`
- `alphamissense_score`
- `clinvar_sig`
- `clinvar_clnrevstat`
- `gnomad_af`
- `gnomad_nonfin`

## Query Behavior

- Variant lookup searches `variant_id` or `rsids`.
- Region lookup filters by `chrom` and `pos`.
- Gene and transcript queries currently use upstream gene/transcript coordinates and return
  overlapping cohort variants.
- Autocomplete searches variant ID prefixes and exact rsID terms.
- GraphQL variant formatting prefers annotated MatrixTable fields and falls back to VDS-only fields.
- Public GraphQL/browser responses must be backed by the sanitized public index only.

## Known Gaps

- Gene-level frequency summaries are not yet defined.
- Metadata-derived filtering is not yet represented in the Elasticsearch mapping.
- `gnomad_nonfin` source semantics must be confirmed before exposing it prominently.
- Exact public export field allowlist, index name, and low-count privacy policy are not yet defined.
