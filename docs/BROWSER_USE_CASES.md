# Browser Use Cases

_Target query scenarios for the cohort variant browser._

Each use case describes the clinical question, the required Elasticsearch fields,
the query pattern, and the current implementation status.

Internal deployment only — use cases 1–3 return sample-linked results and must not
be exposed through the public/sanitized browser.

---

## UC-1: Find all samples carrying a specific variant

**Clinical question:** Leia kõik proovid, kellel esineb konkreetne muutus.

**Search fields:** `variant_id` (or `rsids`)

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": { "term": { "variant_id": "17-41276045-G-C" } }
    }
  }
}
```

**Expected result:** Variant document with cohort frequency (`ac_total`, `an_total`,
`af_total`, `hom_count`) and, on the internal deployment, the list of carrier samples.

**Implementation status:**

| Component | Status |
|---|---|
| ES field `variant_id` | indexed |
| ES field `rsids` | indexed |
| GraphQL `fetchVariantById` | implemented |
| Sample-level carrier list | **not implemented** — requires internal sample-variant index or MT query |

**Gap:** The current ES index stores one document per variant with aggregate counts only.
Returning the list of carrier sample IDs requires either:

- A separate internal `cohort_sample_variants` index with per-sample documents, or
- A direct MatrixTable row filter at query time (slower, no caching).

---

## UC-2: Find all samples with a variant in a gene

**Clinical question:** Leia kõik proovid, kellel esineb muutus selles geenis.

**Search fields:** `gene_symbol`

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": { "term": { "gene_symbol": "BRCA1" } }
    }
  },
  "sort": [{ "pos": "asc" }],
  "size": 10000
}
```

**Expected result:** All variants in the gene, each with cohort frequencies. On the
internal deployment, each variant also lists carrier samples.

**Implementation status:**

| Component | Status |
|---|---|
| ES field `gene_symbol` | indexed (keyword) |
| GraphQL `fetchVariantsByGene` | implemented (uses region coordinates from gene table, not `gene_symbol` term) |
| Direct `gene_symbol` term query | **not yet exposed** in GraphQL |
| Sample-level carrier list | **not implemented** (same gap as UC-1) |

**Gap:** Current `fetchVariantsByGene` resolves gene → chrom/start/stop and queries by
region. A direct `gene_symbol` term filter is simpler and more precise for this use case.
Add a `fetchVariantsByGeneSymbol` query or extend the existing one.

---

## UC-3: Find all samples associated with an HPO term

**Clinical question:** Leia kõik proovid mis on seotud selle HPOga.

**Search fields:** `hpo_id` or `hpo_term`

**ES query (internal index):**

```json
{
  "query": {
    "bool": {
      "filter": { "term": { "hpo_ids": "HP:0001915" } }
    }
  }
}
```

**Expected result:** List of samples that have the given HPO assignment, with their
associated variants and clinical metadata.

**Implementation status:**

| Component | Status |
|---|---|
| HPO fields in ES mapping | **not present** |
| HPO export in `cohort_export.py` | **not implemented** |
| HPO-to-sample join in `annotate_cohort.py` | **not implemented** |
| GraphQL HPO query | **not implemented** |

**Gap:** HPO data lives in a separate join table (`sample_id` × `HPO_ID`) and an HPO
lookup table (`HPO_ID` → `HPO_termin`). Neither is exported to Elasticsearch. This use
case requires:

1. Join HPO assignments to sample metadata in `annotate_cohort.py` or a dedicated step.
2. Add `hpo_ids` (keyword array) and `hpo_terms` (keyword array) to the internal ES index.
3. Add a GraphQL query for HPO-based sample lookup.
4. Keep HPO fields out of the public/sanitized index.

---

## UC-4: ClinVar pathogenic variants in a gene

**Clinical question:** Millised ClinVar patogeensed variandid esinevad BRCA1 geenis?

**Search fields:** `gene_symbol` + `clinvar_sig`

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "gene_symbol": "BRCA1" } },
        { "term": { "clinvar_sig": "Pathogenic" } }
      ]
    }
  },
  "sort": [{ "pos": "asc" }],
  "size": 10000
}
```

**Expected result:** All variants in BRCA1 with ClinVar significance "Pathogenic",
showing cohort frequency and annotation details.

**Implementation status:**

| Component | Status |
|---|---|
| ES field `gene_symbol` | indexed |
| ES field `clinvar_sig` | indexed |
| Combined filter query | **not yet exposed** in GraphQL |
| UI filter for ClinVar significance | **not implemented** |

**Gap:** Both fields exist in ES. Needs a new GraphQL query or filter parameter that
combines `gene_symbol` + `clinvar_sig`. The browser UI needs a ClinVar significance
filter dropdown on gene pages.

**Note:** `clinvar_sig` values come from VEP's `ClinVar_CLNSIG` field and may contain
multi-value strings (e.g. `Pathogenic/Likely_pathogenic`). The ES query may need a
`match` or `wildcard` query instead of exact `term` for partial matches.

---

## UC-5: Pathogenic variant frequency in a gene panel

**Clinical question:** Millise sagedusega esinevad patogeensed variandid
hüperkolesteroleemia geenides?

**Search fields:** multiple `gene_symbol` values + `clinvar_sig`, or multiple
`gene_symbol` values + `revel_score` threshold

### Variant A — ClinVar filter

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": [
        { "terms": { "gene_symbol": ["APOB", "LDLR", "LDLRAP1", "PCSK9"] } },
        { "term": { "clinvar_sig": "Pathogenic" } }
      ]
    }
  },
  "aggs": {
    "by_gene": {
      "terms": { "field": "gene_symbol" },
      "aggs": {
        "total_variants": { "value_count": { "field": "variant_id" } },
        "mean_af": { "avg": { "field": "af_total" } },
        "max_af": { "max": { "field": "af_total" } }
      }
    }
  },
  "size": 10000
}
```

### Variant B — REVEL score filter

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": [
        { "terms": { "gene_symbol": ["APOB", "LDLR", "LDLRAP1", "PCSK9"] } },
        { "range": { "revel_score": { "gte": 0.9 } } }
      ]
    }
  },
  "aggs": {
    "by_gene": {
      "terms": { "field": "gene_symbol" },
      "aggs": {
        "total_variants": { "value_count": { "field": "variant_id" } },
        "mean_af": { "avg": { "field": "af_total" } },
        "max_af": { "max": { "field": "af_total" } }
      }
    }
  },
  "size": 10000
}
```

**Expected result:** Per-gene summary of pathogenic/damaging variant counts and
cohort allele frequencies across the specified gene panel.

**Implementation status:**

| Component | Status |
|---|---|
| ES field `gene_symbol` | indexed |
| ES field `clinvar_sig` | indexed |
| ES field `revel_score` | **not indexed** — not in ES mapping or export |
| Multi-gene `terms` query | **not yet exposed** in GraphQL |
| Per-gene aggregation | **not implemented** |

**Gap:** `revel_score` is defined in the annotation spec but not yet exported to
Elasticsearch. Needs:

1. Add `revel_score` to `cohort_export.py` export fields and ES mapping.
2. Add `revel_score` to the annotated MatrixTable (from dbNSFP).
3. Add a multi-gene + filter GraphQL query with per-gene aggregations.
4. Consider a gene-panel preset endpoint that accepts a panel name and resolves to
   gene symbols.

---

## UC-6: Missense / LOF variants in a gene or region

**Clinical question:** Leia missense/LOF vms variandid selles geenis/regioonis.

**Search fields:** `consequence` + `gene_symbol`, or `impact` = "HIGH" + genomic
region (chrom + pos range, derived from HGVS_g)

### Variant A — consequence + gene

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "gene_symbol": "BRCA1" } },
        { "term": { "consequence": "missense_variant" } }
      ]
    }
  },
  "sort": [{ "pos": "asc" }],
  "size": 10000
}
```

### Variant B — high impact + genomic region

**ES query:**

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "impact": "HIGH" } },
        { "term": { "chrom": "17" } },
        { "range": { "pos": { "gte": 41196312, "lte": 41277500 } } }
      ]
    }
  },
  "sort": [{ "pos": "asc" }],
  "size": 10000
}
```

**Expected result:** Filtered list of variants matching the consequence or impact
criteria within the specified gene or region.

**Implementation status:**

| Component | Status |
|---|---|
| ES field `consequence` | indexed (keyword) |
| ES field `gene_symbol` | indexed (keyword) |
| ES field `impact` | indexed (keyword) |
| ES field `chrom` + `pos` | indexed |
| ES field `gdna` (HGVS_g) | indexed (keyword) — not suitable for range queries |
| Combined consequence + gene query | **not yet exposed** in GraphQL |
| Combined impact + region query | **not yet exposed** in GraphQL |
| UI consequence filter | **not implemented** |

**Gap:** All required ES fields exist. Needs:

1. GraphQL query parameters for `consequence`, `impact`, and multi-filter combinations.
2. For HGVS_g range queries: convert HGVS_g notation to chrom + pos range before
   querying (the `gdna` field is a keyword string and cannot be range-queried directly).
3. Browser UI consequence/impact filter on gene and region pages.

---

## Summary: Required ES Fields

| Field | ES Mapping | Export | GraphQL Query |
|---|---|---|---|
| `variant_id` | ✓ | ✓ | ✓ |
| `rsids` | ✓ | ✓ | ✓ |
| `gene_symbol` | ✓ | ✓ | partial (region-based) |
| `consequence` | ✓ | ✓ | ✗ |
| `impact` | ✓ | ✓ | ✗ |
| `clinvar_sig` | ✓ | ✓ | ✗ |
| `revel_score` | **✗** | **✗** | ✗ |
| `cadd_score` | ✓ | ✓ | ✗ |
| `chrom` + `pos` | ✓ | ✓ | ✓ (region) |
| `gdna` (HGVS_g) | ✓ | ✓ | ✗ |
| `hpo_ids` | **✗** | **✗** | ✗ |
| `hpo_terms` | **✗** | **✗** | ✗ |
| `ac_total` / `an_total` / `af_total` / `hom_count` | ✓ | ✓ | ✓ |

## Summary: Implementation Priorities

1. **Add `revel_score` to ES export** — needed for UC-5B, already in annotation spec.
2. **Add combined filter queries to GraphQL** — gene + clinvar_sig, gene + consequence,
   impact + region, multi-gene + filter with aggregations (UC-4, UC-5, UC-6).
3. **Add direct `gene_symbol` term query** — simpler than region-based for UC-2, UC-4, UC-5, UC-6.
4. **Add HPO fields to internal ES index** — needed for UC-3, requires pipeline + export changes.
5. **Add sample-variant index or MT query path** — needed for UC-1, UC-2, UC-3 carrier lists
   (internal deployment only).
6. **Add browser UI filters** — ClinVar significance, consequence, impact, REVEL threshold.
