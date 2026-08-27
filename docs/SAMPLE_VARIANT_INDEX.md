# Internal Sample-Variant Index (Design)

_Design for the internal, patient-linked indices that answer sample-level questions
(UC-1, UC-2, UC-3). Card #82._

> **INTERNAL ONLY.** Everything described here is patient-linked and must live on the
> internal VM. None of these indices, fields, or GraphQL queries may be exposed through
> the public/sanitized browser. See `docs/ARCHITECTURE.md` (two-VM split) and
> `docs/BROWSER_USE_CASES.md` (UC-1/2/3).

---

## 1. Why a separate index

The public-facing `cohort_variants` index stores **one document per variant** with
**aggregate** cohort counts (`ac_total`, `an_total`, `af_total`, `hom_count`). It has no
sample linkage by design, so it cannot answer:

| Use case | Question |
|---|---|
| UC-1 | Which samples carry variant X? |
| UC-2 | Which samples have a variant in gene Y? |
| UC-3 | Which samples are associated with HPO term Z? |

These require per-sample rows, which come from the annotated MatrixTable **entries**
(genotypes) and **columns** (clinical metadata + HPO). They are kept in dedicated
internal indices, never merged into `cohort_variants`.

---

## 2. Indices

Two internal indices, normalized to avoid repeating clinical metadata on every carrier row.

### 2a. `cohort_samples` — one doc per sample

Serves UC-3 (HPO → samples) and holds the clinical metadata used to describe carriers.

```json
{
  "mappings": {
    "properties": {
      "sample_id":     { "type": "keyword" },
      "sex_assigned":  { "type": "keyword" },
      "sex_chr":       { "type": "keyword" },
      "date_of_birth": { "type": "date" },
      "date_seq":      { "type": "date" },
      "run_id":        { "type": "keyword" },
      "material":      { "type": "keyword" },
      "care_site":     { "type": "keyword" },
      "health_status": { "type": "keyword" },
      "test":          { "type": "keyword" },
      "instrument":    { "type": "keyword" },
      "ref_genome":    { "type": "keyword" },
      "panels":        { "type": "keyword" },
      "hpo_ids":       { "type": "keyword" }
    }
  }
}
```

Field set follows the ERD in `docs/ARCHITECTURE.md`. `sex_chr` was `chromosomal_sex`
(OLI-9). `test` / `instrument` / `ref_genome` are the Procedure entity, flattened onto the
sample doc rather than given their own index — one procedure per sample (OLI-7).

`hpo_ids` and `panels` are keyword arrays; the schematic draws both wide (`hpo_id1`,
`hpo_id2`, `Panel1`…`Panel3`). HPO *labels* are deliberately absent: they live in the
versioned `hpo_terms` lookup index (OLI-14) and are joined at query time, so a term rename
does not require re-annotating samples (OLI-15).

### 2b. `cohort_sample_variants` — one doc per (sample, variant) carrier

Serves UC-1 and UC-2. One row for each non-ref genotype call.

```json
{
  "mappings": {
    "properties": {
      "sample_id":   { "type": "keyword" },
      "variant_id":  { "type": "keyword" },
      "chrom":       { "type": "keyword" },
      "pos":         { "type": "integer" },
      "ref":         { "type": "keyword" },
      "alt":         { "type": "keyword" },
      "genotype":    { "type": "keyword" },   // "het" | "hom"
      "gene_symbol": { "type": "keyword" },
      "consequence": { "type": "keyword" },
      "gq":          { "type": "integer" },
      "depth":       { "type": "integer" },
      "var_pct":     { "type": "float" },     // alt fraction from entry.AD
      "in_report":   { "type": "boolean" }
    }
  }
}
```

`var_pct` and `in_report` are the two fields the ERD's *Genomic data (VCF)* entity carries
that this design originally omitted (OLI-18). `in_report` is written from the clinical
report source; whether that source is this column or the separate *In Report* join table
is OLI-10.

Clinical metadata is intentionally **not** denormalized here — join to `cohort_samples`
by `sample_id` when a query needs it. This keeps the (larger) carrier index compact and
means an HPO or metadata correction only rewrites `cohort_samples`.

---

## 3. Export path

A new internal export (extend `cohort_export.py` with an `internal` profile, or a
sibling `cohort_sample_export.py`) reads the annotated MT:

```
annotated MT
  ├── cols  → cohort_samples          (mt.cols(): metadata + hpo_ids + panels)
  └── entries (non-ref) → cohort_sample_variants
              mt.filter_entries(mt.GT.is_non_ref())
                .entries()                          # explodes to sample×variant rows
                .select(sample_id = mt.s,
                        variant_id, chrom, pos, ref, alt,
                        genotype = hl.if_else(mt.GT.is_het(), 'het', 'hom'),
                        gene_symbol = mt.vep.SYMBOL,
                        consequence = mt.vep.Consequence,
                        gq = mt.GQ, depth = mt.DP,
                        var_pct = mt.AD[1] / hl.sum(mt.AD))
```

Sizing note: `cohort_sample_variants` has ~Σ(carriers per variant) docs. For ~2000
WES/WGS samples this is millions of docs — well within a single-node ES, but larger than
`cohort_variants`; bulk-index in the same batched way as `cohort_export.py`.

---

## 4. GraphQL (internal deployment only)

Add cohort resolvers, registered **only** when the API runs in internal mode:

| Query | UC | ES |
|---|---|---|
| `cohort_variant_carriers(variant_id)` | UC-1 | `cohort_sample_variants` term on `variant_id` |
| `cohort_gene_carriers(gene_symbol)` | UC-2 | `cohort_sample_variants` term on `gene_symbol` |
| `cohort_hpo_samples(hpo_id)` | UC-3 | `cohort_samples` term on `hpo_ids`; optionally join carriers |

Each returns sample IDs (+ their variants for UC-1/2, + variants for UC-3 via a second
lookup on the returned `sample_id`s).

---

## 5. Privacy guarantees

- These indices and resolvers are **never** created on the public VM. Gate them behind an
  explicit `DEPLOYMENT_MODE=internal` env check in the GraphQL API and in the export
  script.
- The public export allowlist (see `docs/ARCHITECTURE.md`) must **fail closed** if any of
  `sample_id`, `hpo_ids`, `genotype`, `gq`, `depth`, `var_pct`, `in_report`, `date_of_birth`,
  `date_seq`, `run_id` or `care_site` are present.
- Consider a small-count privacy threshold before returning carrier lists even internally,
  to match the public rare-variant threshold policy.

---

## 6. Open questions

- Single `internal` export profile in `cohort_export.py` vs. a separate script.
- Whether UC-3 should eagerly return each sample's carrier variants or lazily on drill-down.
- Minimum GQ/DP threshold for a genotype to count as a carrier call.
- Carrier-count privacy threshold (internal).
