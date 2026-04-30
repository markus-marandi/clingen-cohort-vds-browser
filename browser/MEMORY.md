# browser/MEMORY.md

Scoped memory for the patched gnomAD browser layer.

## Dataset

- Dataset key: `cohort`
- Display label: `Cohort`
- Reference genome: `GRCh37`
- Elasticsearch index: `cohort_variants`

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
- `cadd_score`
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

## Known Gaps

- GraphQL formatting currently maps to the basic gnomAD variant shape and may need to prefer
  `ac_total`, `an_total`, `af_total`, and `hom_count` over fallback fields.
- Gene-level frequency summaries are not yet defined.
- Metadata-derived filtering is not yet represented in the Elasticsearch mapping.
- `gnomad_nonfin` source semantics must be confirmed before exposing it prominently.
