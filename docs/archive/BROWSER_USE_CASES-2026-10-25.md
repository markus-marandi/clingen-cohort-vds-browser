# Browser Use Cases

> Main-baseline/design assessment: the separate `feat/internal-sample-browser`
> branch now prepares TSV/MT export, sample/carrier APIs and browser views.
> See [the pilot handoff](DRAGON_SAMPLE_PILOT_HANDOFF.md) for its current contract,
> which preserves raw dates and nullable report membership. Real-data integration
> is still pending. The broader design below is not the current pilot task list.

_Target query scenarios for the cohort variant browser._

Each use case describes the clinical question, the required Elasticsearch fields,
the query pattern, and the current implementation status.

Reviewed 2026-10-25 against local main at `d776a06`. V1 is GRCh37-only and
internal-only. All authenticated internal users may see sample IDs. Public deployment
is excluded. See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for the full
issue inventory and the separate, unmerged browser review branch.

The ES examples below illustrate target queries; implementation status and remaining
acceptance limits are stated separately for each use case.

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
| Direct gene-symbol query | implemented in `fetchVariantsByGeneSymbol` and `fetchVariantsByGene` |
| GraphQL gene/multi-gene filters | implemented via `cohort_filtered_variants` |
| Sample-level carrier list | not implemented; needs the internal sample/variant export and query path |

**Remaining:** Add carrier samples and their qualifying genotypes. The direct
variant-level gene-symbol query is already implemented (OLI-21), so this is not a
missing gene-filter task. Normal gene-page/reference-index runtime fixes are on
the separate review branch, not this main baseline.

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
| HPO-to-sample import | implemented in `annotate_cohort.py` with optional assignment and label CSVs |
| Versioned HPO lookup | not implemented |
| HPO/sample export to internal ES | not implemented |
| GraphQL HPO-to-sample query | not implemented |
| Sample/HPO browser view | not implemented |

**Remaining:** Build the versioned HPO lookup and internal sample export, then
expose ID/label lookup with sample results and variant drill-down. The current
helper copies both IDs and labels onto sample columns; the design to keep labels
in a versioned lookup is not implemented. All authenticated internal users may
see the matching sample IDs. Public export is outside v1.

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
| ES fields `gene_symbol` and `clinvar_sig` | indexed |
| Combined filter query | implemented via `cohort_filtered_variants` |
| ClinVar significance control | implemented on the dedicated `/filter` page |
| Filter controls embedded on gene pages | not implemented |

**Remaining:** Agree whether the dedicated filter page meets v1 UI acceptance,
and validate pathogenic/likely-pathogenic/conflicting classifications and review
status semantics. The existing implementation matches ClinVar significance by
substring; it does not define the complete classification policy.

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
| ES fields `gene_symbol`, `clinvar_sig`, and `revel_score` | mapped and exported |
| Multi-gene combined filters | implemented via `cohort_filtered_variants` |
| ClinVar and REVEL UI controls | implemented on `/filter` |
| Per-gene aggregation | implemented: variant count and mean/max variant AF |
| Distinct-sample carrier frequency | not implemented or fully specified |

**Remaining:** Define whether the question asks for per-variant AF or the proportion
of eligible samples carrying at least one qualifying variant. Mean/max variant AF
does not calculate distinct-sample carrier prevalence. The example uses REVEL
`>0.9`, while the implemented minimum filter is inclusive (`>=`); agree the boundary.
REVEL export and combined queries themselves are already present.

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
| ES fields `consequence`, `gene_symbol`, `impact`, `chrom`, and `pos` | mapped and exported |
| Combined consequence + gene query | implemented via `cohort_filtered_variants` |
| Combined impact + region query | implemented via `cohort_filtered_variants` |
| Consequence/impact UI controls | implemented on `/filter` |
| HGVS_g interval parser | not implemented; the UI accepts chromosome/start/stop |

**Remaining:** Define LoF and multi-consequence matching. Annotation can join
consequence terms with `&`, while the current filter matches a single exact keyword.
Specify coordinate conventions and whether HGVS_g interval input is actually
required. Confirm whether filters must also be embedded on gene/region pages.
Pagination/result totals beyond the current 10,000-result limit remain open.

---

## Summary: Required ES Fields

| Field | Main implementation | Remaining boundary |
|---|---|---|
| `variant_id` | mapped/exported/queryable | sample carrier linkage missing |
| `rsids` | mapped and supported by lookup code | annotated export does not populate rsids; fallback emits an empty array |
| `gene_symbol` | direct term query and combined filters | sample carrier linkage missing |
| `consequence` | exported and filterable | combined-term semantics need validation |
| `impact` | exported and filterable | UI placement/acceptance |
| `clinvar_sig` | exported and filterable | classification/review policy |
| `revel_score` | exported and filterable | inclusive versus exclusive threshold |
| `cadd_score` | exported and exposed on variant details | no dedicated CADD threshold query |
| `chrom` + `pos` | exported and region-filterable | coordinate and pagination acceptance |
| `gdna` (HGVS_g) | exported/displayed | specified naming and optional HGVS interval parsing |
| `hpo_ids` / `hpo_terms` | sample columns can be annotated | no internal sample index/API/UI; versioned lookup remains |
| `ac_total` / `an_total` / `af_total` / `hom_count` | exported and exposed | genotype-quality, ploidy, and cohort denominator acceptance |

## Summary: Remaining Priorities

1. Build internal sample and carrier exports/indices and sample-linked queries.
2. Add sample/carrier/HPO/report browser workflows for authenticated internal users.
3. Validate metadata/report input, including prenatal and Unknown/None tokens;
   see [SAMPLE_METADATA_SOURCE.md](../SAMPLE_METADATA_SOURCE.md).
4. Complete the versioned HPO lookup and agreed field naming/annotation requirements.
5. Resolve search semantics, result completeness, and frequency definitions.
6. Close the separate browser review findings and complete internal/full-cohort acceptance.

Combined filters, REVEL export, direct gene-symbol queries, HPO import, and the
existing annotation API field set must not be counted as wholly missing features.
