# AI Retrieval Layer (Design)

_Last updated: 2026-08-27_

Design for semantic retrieval and natural-language querying on top of the cohort browser.
Covers where retrieval-augmented generation (RAG) belongs, where it does not, and the
Elasticsearch mappings and query shapes to use.

> **Scope guard.** Nothing here replaces the structured query path. Cohort counts,
> frequencies, and carrier lists are always returned by exact Elasticsearch filters over
> `cohort_variants` / `cohort_sample_variants`, never by a language model and never by
> vector similarity. Retrieval only ever touches free text.

> **INTERNAL/PUBLIC split applies.** See `docs/ARCHITECTURE.md`. Any vector index derived
> from patient-linked fields is internal-VM-only and inherits every restriction that
> applies to `cohort_samples`. See §7.

---

## 1. Why not RAG on the main query path

The browser's hot path is structured data with exact semantics:

| Query | Field | Correct mechanism |
|---|---|---|
| UC-1 variant lookup | `variant_id`, `rsids` | `term` filter |
| UC-2 gene carriers | `gene_symbol` | `term` filter |
| UC-4/5/6 filtering | `cadd_score`, `af_total`, `impact`, `clinvar_sig` | `range` / `terms` filter |

Vector search on these is strictly worse than a term filter: approximate where the answer
must be exact, higher latency, and no way to guarantee a variant is not silently missed.
Adding embeddings to this path buys nothing and costs recall guarantees on clinical data.

The project's actual scaling constraints for 2000 WES/WGS samples are Hail/VDS compute,
`cohort_sample_variants` document count, and Elasticsearch heap — none of which retrieval
addresses. Those stay tracked in `TODO.md` and `docs/SAMPLE_VARIANT_INDEX.md`.

**RAG earns its place only where the source is genuinely unstructured text.** Three layers
below; layers 1 and 2 are worth building, layer 0 is the existing structured path they sit on.

---

## 2. Layer model

```
Layer 0  structured query          cohort_variants / cohort_sample_variants
         exact term + range filters, deterministic, authoritative
              ▲
Layer 1  text-to-filter            NL question → validated filter object → Layer 0
         schema-constrained tool call; model emits filters, never values
              ▲
Layer 2  semantic retrieval        curation_text index (kNN)
         free-text sources: HPO definitions, ClinVar narratives, gene-disease evidence
```

Layer 1 output is executed by existing resolvers in
`browser/graphql-api/src/graphql/resolvers/cohort.ts`. Layer 2 results are shown as
citations beside results, never merged into count fields.

---

## 3. Layer 1 — natural-language to structured filter

Not RAG. A schema-constrained tool call: the model receives the field catalog and emits a
JSON filter object. The API validates it against an allowlist and runs it as an ordinary
Elasticsearch query.

### 3a. Filter allowlist

Derived from the `cohort_variants` mapping in `browser/data-pipeline/cohort_export.py`.
Anything not on this list is rejected before it reaches Elasticsearch.

| Field | Type | Allowed operators |
|---|---|---|
| `variant_id`, `rsids` | keyword | `eq`, `in` |
| `gene_symbol` | keyword | `eq`, `in` |
| `chrom` | keyword | `eq`, `in` |
| `pos` | integer | `eq`, `gte`, `lte` |
| `consequence`, `impact` | keyword | `eq`, `in` |
| `clinvar_sig`, `clinvar_clnrevstat` | keyword | `eq`, `in` |
| `cadd_score`, `revel_score`, `sift_score`, `polyphen_score` | float | `gte`, `lte` |
| `metarnn_score`, `clinpred_score`, `alphamissense_score` | float | `gte`, `lte` |
| `af_total`, `gnomad_af`, `gnomad_nonfin`, `dbnsfp_popmax_af` | float | `gte`, `lte` |
| `ac_total`, `an_total`, `hom_count` | integer | `gte`, `lte` |
| `gnomad_loeuf`, `gnomad_moeuf` | float | `gte`, `lte` |

Internal deployment additionally allows `sample_id`, `hpo_ids`, `panels`, `genotype`,
`gq`, `depth` — gated behind the same `DEPLOYMENT_MODE=internal` check as the resolvers
in `docs/SAMPLE_VARIANT_INDEX.md` §5.

### 3b. Emitted shape

Input: `"pathogenic BRCA1 variants with CADD over 25 and cohort AF under 1%"`

```json
{
  "filters": [
    { "field": "gene_symbol", "op": "eq",  "value": "BRCA1" },
    { "field": "clinvar_sig", "op": "in",  "value": ["Pathogenic", "Likely_pathogenic"] },
    { "field": "cadd_score",  "op": "gte", "value": 25 },
    { "field": "af_total",    "op": "lte", "value": 0.01 }
  ],
  "sort": { "field": "cadd_score", "order": "desc" },
  "unresolved": []
}
```

Validation rules:

1. Reject any `field` not in the allowlist for the current deployment mode.
2. Reject any `op` not permitted for that field's type.
3. Coerce `value` to the mapped type; reject on failure.
4. Anything the model could not map goes in `unresolved[]` and is shown to the user as
   "not applied" — never silently dropped, never guessed.
5. Render the resulting filter set in the UI before running it, so the clinician sees the
   interpretation. `CohortFilterPage.tsx` already renders an equivalent filter state.

### 3c. When this becomes RAG

Only when the field catalog outgrows the prompt budget. The table above is ~30 fields and
fits comfortably, so ship it as a static schema in the system prompt first. If the catalog
grows past a few hundred fields, retrieve relevant field definitions from the data-model
tables in `docs/ARCHITECTURE.md` instead of inlining all of them.

---

## 4. Layer 2 — semantic retrieval over curation text

### 4a. Sources

All public, non-patient-linked:

| Source | Text unit | Use |
|---|---|---|
| HPO `hp.obo` | term definition + synonyms | fuzzy phenotype match ("seizures" → HP:0001250) |
| ClinVar VCF / submission summary | condition + review narrative | interpretation context for a variant |
| GenCC / OMIM gene-disease | curated gene-disease statement | gene-level evidence |
| ACMG/AMP criteria text | criterion description | interpretation support |

HPO is already loaded by `annotate_cohort.py` (`--hpo-path`, `_load_hpo_ht`) and ClinVar
by the local VCF join — both are re-usable as text sources without new downloads.

### 4b. Index mapping — `curation_text`

Separate index. Does not touch `cohort_variants`.

```json
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "doc_id":       { "type": "keyword" },
      "source":       { "type": "keyword" },
      "source_version": { "type": "keyword" },
      "entity_type":  { "type": "keyword" },
      "entity_id":    { "type": "keyword" },
      "gene_symbol":  { "type": "keyword" },
      "variant_id":   { "type": "keyword" },
      "title":        { "type": "text" },
      "text":         { "type": "text" },
      "embedding": {
        "type": "dense_vector",
        "dims": 768,
        "index": true,
        "similarity": "cosine",
        "index_options": { "type": "hnsw", "m": 16, "ef_construction": 100 }
      }
    }
  }
}
```

Notes:

- `source_version` is mandatory. Ontology and ClinVar releases change; a retrieved
  statement is only interpretable against the release it came from.
- `entity_id` carries `HP:0001250`, a ClinVar VCV accession, an OMIM MIM number — the key
  that lets a semantic hit be turned back into an exact structured filter.
- `gene_symbol` / `variant_id` are the join keys back to `cohort_variants`, and double as
  kNN pre-filters (§5).
- `dims: 768` matches the candidate models in §6; change together with the model.

### 4c. Chunking

One document per ontology term or per curation statement. These are already short and
self-contained — do not window-chunk them. Splitting an HPO definition across chunks
destroys exactly the semantics being matched.

For longer narrative sources (ACMG criteria, OMIM clinical synopses), chunk on the
existing section boundaries and keep `entity_id` on every chunk.

---

## 5. Hybrid query shape

**Term filter narrows first, vector reranks the remainder. Never vector-first.**

Elasticsearch kNN applies `filter` during graph traversal (pre-filter), so the structured
constraint is enforced, not applied as a post-hoc trim:

```json
{
  "knn": {
    "field": "embedding",
    "query_vector": [ /* 768 floats */ ],
    "k": 20,
    "num_candidates": 200,
    "filter": {
      "bool": {
        "filter": [
          { "term":  { "source": "hpo" } },
          { "terms": { "gene_symbol": ["BRCA1", "BRCA2"] } }
        ]
      }
    }
  },
  "_source": ["doc_id", "source", "source_version", "entity_id", "title", "text"]
}
```

### 5a. Combining lexical and vector scores

Reciprocal rank fusion is the standard combiner. On 8.13.4 the syntax is a top-level
`rank` block alongside `query` and `knn`:

```json
{
  "query": {
    "bool": {
      "must":   [ { "match": { "text": "childhood onset seizures" } } ],
      "filter": [ { "term": { "source": "hpo" } } ]
    }
  },
  "knn": {
    "field": "embedding",
    "query_vector": [ /* 768 floats */ ],
    "k": 20,
    "num_candidates": 200,
    "filter": [ { "term": { "source": "hpo" } } ]
  },
  "rank": { "rrf": { "window_size": 50, "rank_constant": 20 } }
}
```

**License caveat — verify before building on this.** RRF (`rank.rrf`) and ELSER
(`text_expansion`) are subscription features on 8.x; `dense_vector` and plain kNN are
available on the free Basic license. `browser/docker-compose.yml:3` runs 8.13.4 with no
license configuration, i.e. Basic. Confirm with `GET /_license` on the target cluster
before depending on RRF.

If the cluster is Basic, fuse in the resolver instead — two searches, RRF combined in
TypeScript. Deterministic, free, and roughly ten lines:

```ts
// rank fusion over two ES result sets; k = rank constant (60 is the common default)
const rrf = (lists: string[][], k = 60): string[] => {
  const scores = new Map<string, number>()
  for (const list of lists) {
    list.forEach((id, i) => scores.set(id, (scores.get(id) ?? 0) + 1 / (k + i + 1)))
  }
  return [...scores.entries()].sort((a, b) => b[1] - a[1]).map(([id]) => id)
}
```

### 5b. Grounding rule

A semantic hit is a pointer, not an answer. The pipeline is always:

```
NL text → kNN over curation_text → entity_id (e.g. HP:0001250)
        → exact term filter on cohort_samples.hpo_ids
        → counts from Elasticsearch
```

Numbers shown to a user come from step 3. The model never restates a count, frequency, or
carrier number in prose.

---

## 6. Embedding model

Must run locally (§7). Candidates, to be benchmarked against a held-out set of phenotype
paraphrases before committing:

| Model | Dims | Notes |
|---|---|---|
| `cambridgeltl/SapBERT-from-PubMedBERT-fulltext` | 768 | trained for biomedical entity linking; strongest fit for HPO/OMIM term matching |
| `FremyCompany/BioLORD-2023` | 768 | clinical concept sentence similarity |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | general-purpose baseline; smaller and faster, weaker on ontology terms |

Treat the ranking above as a starting hypothesis, not a result — pick by measured
recall@10 on real HPO paraphrases from the cohort, not by reputation.

Serve via `sentence-transformers` in the existing Python environment. Embedding happens at
index build time in a sibling of `cohort_export.py`; only the query embedding is computed
at request time.

Pin the model name and revision next to `source_version`. Changing either invalidates the
whole index — re-embed, do not mix.

---

## 7. Privacy and PHI constraints

This section is normative. The internal VM holds patient-linked data: sample IDs, HPO
assignments, dates of birth, care sites, sequencing dates.

**Embeddings are not anonymization.** A vector computed from patient text is still
patient-derived data. Embedding-inversion research has repeatedly shown that substantial
portions of the source text can be reconstructed from its embedding, so a vector field
must be classified exactly as the text it was derived from.

Three rules follow:

1. **No hosted inference on internal data.** Content from the internal VM must not be sent
   to any external embedding or LLM API. Run the embedding model locally on the internal
   VM, or do not embed that content. This preserves the existing property that the
   internal VM has no outbound path for patient-linked data.
2. **Vector fields inherit their source's tier.** Any index built from patient-linked
   fields is internal-only, gated behind the same `DEPLOYMENT_MODE=internal` check as the
   resolvers in `docs/SAMPLE_VARIANT_INDEX.md` §4. The public export allowlist must fail
   closed on `embedding` and on any `curation_text` document whose `source` is not on the
   public source list — the same way it fails closed on `sample_id`.
3. **Start with public sources only.** The `curation_text` index as specified in §4a
   contains HPO, ClinVar, GenCC, and ACMG content exclusively. All of it is public
   reference data, it carries no PHI, and it is deployable on either VM. It also covers
   most of the intended value. Embedding patient-linked text is a separate, later decision
   that needs an explicit review — not an incremental extension of this design.

Layer 1 (§3) has a matching constraint: the model sees the *field catalog*, never field
*values*. Question text typed by a clinician may itself contain identifiers, so on the
internal deployment the Layer 1 model must also be locally hosted, or Layer 1 must be
restricted to the public deployment.

---

## 8. Sizing

`browser/docker-compose.yml` currently sets `ES_JAVA_OPTS=-Xms2g -Xmx2g` on a single node.

HNSW graphs are held off-heap and want to fit in the OS page cache. Rough vector footprint
is `dims × 4 bytes × docs`, plus graph overhead:

| Source | Approx. docs | Vector bytes @768 dims |
|---|---|---|
| HPO terms | ~19k | ~58 MB |
| ClinVar statements (subset joined to cohort) | ~100k–1M | ~0.3–3 GB |
| Gene-disease statements | ~10k | ~31 MB |

HPO alone is trivial and a fine first target. A full ClinVar embedding needs a container
memory-limit review first — restrict to variants present in the cohort, or to
`clinvar_sig` values that matter clinically, before embedding the whole release.

---

## 9. Build order

1. Structured filter queries for UC-1…UC-6 (`TODO.md`, `docs/BROWSER_USE_CASES.md`).
   Retrieval on top of a missing query layer has nothing to ground against.
2. Layer 1 text-to-filter with the static schema, filter preview in the UI, no vectors.
3. `curation_text` with HPO only — smallest source, clearest win, zero PHI.
4. Hybrid kNN + term-filter search, fused in the resolver (§5a) unless a license check
   says RRF is available.
5. ClinVar and gene-disease sources, subject to the sizing review in §8.
6. Patient-linked embedding — only with a local model and an explicit privacy review.

---

## 10. Open questions

- Which deployment gets Layer 1: public only, or internal with a locally hosted model?
- Embedding model choice, pending the recall@10 benchmark in §6.
- Whether `curation_text` lives on both VMs or only public, given it is PHI-free either way.
- Re-embedding policy when HPO or ClinVar releases change (`source_version` bump).
- Whether ES license on the production VM permits RRF and ELSER, or resolver-side fusion
  is permanent.
- Cache strategy for query embeddings — Redis is already in the stack.
