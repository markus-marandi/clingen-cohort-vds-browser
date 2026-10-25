# Architecture Specification

> Broader design/reference material. For the current internal pilot, use
> [release status](RELEASE_STATUS.md), [operations](OPERATIONS.md) and
> [operator workflows](WORKFLOWS.md). Historical implementation assessments
> below are not the current pilot's remaining task list.

_Scope and implementation assessment updated: 2026-10-25_

Cohort variant browser for internal WGS/WES data. Concatenates and stores genotypes in Hail VDS,
annotates variants with VEP + dbNSFP + ClinVar + gnomAD, and serves results through a
patched gnomAD browser UI. The first release is GRCh37-only and internal-only. All
authenticated internal users may see sample IDs. Public deployment is outside v1.
See [release status](RELEASE_STATUS.md) for current implementation and acceptance checks.

---

## Pipeline Overview

```
raw GVCFs
  → bcftools filter (contig filter, chr rename)
  → Hail VDS                              parallel_ingest_cohort.py
  → Hail MatrixTable                      annotate_cohort.py
      ├── VEP (HGVS, consequence, gene symbol)
      ├── dbNSFP v5.3.1 (predictors including CADD, population AF, gene annotations)
      ├── ClinVar (local VCF join via hl.import_vcf)
      ├── gnomAD v2.1.1 exomes (local HT join)
      ├── Cohort AF via hl.variant_qc()
      └── Sample metadata join (annotate_cols)
  → Elasticsearch index                   browser/data-pipeline/cohort_export.py
  → gnomAD browser UI (patched)           docker-compose / nginx + auth proxy
```

---

## Internal access and release scope

The internal analysis VM is the source of truth for raw/filtered GVCFs, VDS,
annotated MatrixTables, clinical metadata, aggregate variant indices, and planned
sample/carrier indices. The browser and API serve authenticated internal users.
All such users may see sample IDs in sample, carrier, HPO, and report workflows.

V1 uses GRCh37/hg19 coordinates only. Build metadata must agree with the genomic
inputs and reference resources; adding a GRCh38 label does not provide GRCh38 support.

Public deployment, a second public VM, public export profiles, one-way transfer,
and public low-count policies are deferred outside v1. The former two-tier plan
remains in the original exported issue descriptions as historical context only.
Internal authentication, HTTPS, service isolation, and protection of credentials
and patient data remain required; this is scope, not proof of deployed controls.

---

## Data Model

This section is the canonical ERD. Field names here are authoritative; where the code
still uses an older name, the mapping table at the end of this section records the delta
and the issue that closes it.

### VEP annotation — row-level on MatrixTable

| Field | Source | Notes |
|---|---|---|
| `variant_id` | VCF | `{chrom}-{pos}-{ref}-{alt}` |
| `HGVS_g` | VEP | genomic HGVS |
| `HGVS_c` | VEP | coding HGVS |
| `transcript` | VEP | MANE Select or NM_ RefSeq |
| `HGVS_p` | VEP | protein HGVS |
| `gene_symbol` | VEP | |
| `consequence` | VEP | most severe consequence term |
| `impact` | VEP | HIGH / MODERATE / LOW / MODIFIER |
| `cadd_score` | dbNSFP CADD PHRED field | PHRED-scaled; ≥20 = top 1% |
| `revel_score` | dbNSFP | 0–1; ≥0.5 suggests pathogenicity |
| `sift_score` | dbNSFP | <0.05 = damaging |
| `polyphen_score` | dbNSFP | `Polyphen2_HDIV_score` |
| `metarnn_score` | dbNSFP | ≥0.5 = damaging |
| `clinpred_score` | dbNSFP | >0.5 likely pathogenic |
| `alphamissense_score` | dbNSFP | score/prediction per dbNSFP README |
| `dbnsfp_popmax_af` | dbNSFP | max curated population AF, if selected for export |
| `gnomad_loeuf` | dbNSFP gene annotations | LoF constraint; lower is more constrained |
| `gnomad_moeuf` | dbNSFP gene annotations | missense constraint |
| `gnomad_af` | gnomAD v2.1.1 exomes local HT | `AF` (all populations) |
| `gnomad_nonfin` | gnomAD v2.1.1 exomes local HT | `AF_nfe` (Non-Finnish European) |

The ERD names only `cadd_score`, `revel_score` and `sift_score`. The rows above them —
polyphen, metarnn, clinpred, alphamissense, popmax AF, LOEUF, MOEUF — are produced by the
current dbNSFP pass and kept here as a documented superset pending OLI-3. Whatever that
decision lands on governs the ES mapping and the GraphQL exposure, not this table alone.

### Variant local frequency — from `hl.variant_qc()`

| Field | Hail expression |
|---|---|
| `ac_total` | `variant_qc.AC[1]` |
| `an_total` | `variant_qc.AN` |
| `af_total` | `variant_qc.AF[1]` |
| `hom_count` | `variant_qc.homozygote_count[1]` |

### ClinVar annotation — row-level join from clinvar.vcf.gz

| Field | Notes |
|---|---|
| `variant_id` | join key |
| `clinvar_sig` | clinical significance (Pathogenic, VUS, Benign, …) |
| `clinvar_condition` | associated condition / disease |
| `clinvar_review_status` | review status (criteria provided, expert panel, …) |

### Genomic data per sample — entry-level on MatrixTable

| Field | Source | Notes |
|---|---|---|
| `sample_id` | column key | |
| `variant_id` | row key | |
| `chrom`, `pos`, `ref`, `alt` | locus / alleles | |
| `genotype` | `entry.GT` | |
| `depth` | `entry.DP` | |
| `gq` | `entry.GQ` | |
| `var_pct` | `entry.AD` | alt allele fraction in this sample |
| `in_report` | clinical metadata | `yes` / `no` — whether this variant is included in the clinical report for this sample |

### In Report — join table (sample × variant)

| Field | Notes |
|---|---|
| `sample_id` | |
| `variant_id` | |

Populated from the laboratory information system. One row per (sample, variant) pair that
appears in a signed-out clinical report.

### Sample data — column annotations

| Field | Notes |
|---|---|
| `sample_id` | join key across all tables |
| `sex_assigned` | sex from clinical metadata (social/assigned); values: Male, Female, Other, Unknown |
| `sex_chr` | chromosomal sex inferred from genotype (X heterozygosity / chrY coverage); values: Male, Female, ambiguous |
| `date_of_birth` | Source accepts YYYY-MM-DD or the literal `prenatal`; storage/conversion remains unresolved (see `SAMPLE_METADATA_SOURCE.md`) |
| `date_seq` | date of sequencing run |
| `run_id` | sequencing run identifier |
| `material` | biological source material: blood, tissue, saliva, buccal swab, etc. |
| `care_site` | ordering institution (TUH, etc.) |
| `health_status` | Affected / Non-affected |

### Procedure / input parameters — per sample-run

| Field | Notes |
|---|---|
| `test` | WES or WGS |
| `instrument` | sequencer model (NovaSeq X, etc.) |
| `ref_genome` | GRCh37 / hg19 only in v1; validate compatibility with the input and annotation resources |
| `sample_id` | |

### Panels — one-to-many (WES samples)

| Field | Notes |
|---|---|
| `sample_id` | |
| `panel` | ordered gene panel name (e.g. ANEEMIA, SKIN) |

The schematic draws this wide (`Panel1`, `Panel2`, `Panel3`). Stored long: one row per
ordered panel, indexed as a `panels` keyword array on `cohort_samples`. The user
confirmed `whole exome` for samples without separately ordered panels. Unknown or
unmatched assignments must not silently become whole exome; validate source completeness
first. This grouping/import behavior remains to be implemented.

### HPO assignments — one-to-many (WGS samples)

| Field | Notes |
|---|---|
| `sample_id` | |
| `hpo_id` | e.g. HP:0001915 |

Drawn wide in the schematic (`hpo_id1`, `hpo_id2`); stored long and indexed as an
`hpo_ids` keyword array on `cohort_samples`, same as panels.

### HPO terms — lookup table from the HPO release file

| Field | Notes |
|---|---|
| `hpo_id` | join key |
| `hpo_term` | human-readable label (e.g. Aplastic anemia) |
| `hpo_version` | release version (e.g. v01-2024) |
| `valid_from` | ISO date when this version took effect |

The label lives here, not on the sample. A term rename is a new row in this table, not a
re-annotation of every sample that carries the ID.

### Schematic vs. implementation

Known schema differences and incomplete paths reviewed against main at `d776a06`.
This is not an exhaustive implementation checklist; see `IMPLEMENTATION_STATUS.md`.

| ERD field / entity | Current code | Where | Issue |
|---|---|---|---|
| `HGVS_g`, `HGVS_c`, `HGVS_p` | `gdna`, `cdna`, `p_nomen` | `cohort_export.py`, `cohort-variant-queries.ts` | OLI-4 |
| `clinvar_condition` | absent — only CLNSIG + CLNREVSTAT extracted | `annotate_cohort.py` | OLI-5 |
| `clinvar_review_status` | `clinvar_clnrevstat` | `annotate_cohort.py`, `cohort_export.py` | OLI-6 |
| Procedure (`test`, `instrument`, `ref_genome`) | `test`/`instrument` ride along in the metadata CSV; no `ref_genome` | metadata loader | OLI-7 |
| `ref_genome` per procedure | hardcoded `reference_genome: 'GRCh37'` | `cohort-variant-queries.ts:29` | OLI-8 |
| `sex_chr`, `date_seq`, `run_id` | `chromosomal_sex`; no `date_seq`, no `run_id` | `docs/SAMPLE_VARIANT_INDEX.md`, metadata loader | OLI-9 |
| In Report as its own table | join table and Boolean are design-only; no authoritative report importer | — | OLI-10 |
| Panels 1..N | CSV pass-through; no one-to-many panel grouping | `annotate_cohort.py` | OLI-13 |
| HPO terms table (`hpo_version`, `valid_from`) | no version, no validity date | `_load_hpo_ht` | OLI-14 |
| `hpo_term` on the lookup only | `hpo_terms` denormalized onto every sample | `annotate_cohort.py` | OLI-15 |
| `var%`, `in_report` on genomic data | absent from the `cohort_sample_variants` design | `docs/SAMPLE_VARIANT_INDEX.md` | OLI-18 |
| VEP predictors: cadd/revel/sift only | seven further predictors + constraint scores | `cohort_export.py` | OLI-3 |

Two naming conventions differ deliberately: the ERD's `GQ` and `var%` are indexed as `gq`
and `var_pct`, because `%` is not usable in an Elasticsearch field name.

---

## Annotation Sources

### VEP

- Binary: `/mnt/sdb/projects/ensembl-vep/vep`
- Cache: `/mnt/sdb/VEP/VEP_cache/` — GRCh37, v108, merged
- Reference FASTA: `/mnt/sdb/VEP/ref_fasta/ucsc.hg19.fasta`
- Hail call: `hl.vep(mt, config='vep_settings.json')`
- Flags: `--pick --canonical --symbol --HGVS --MAX_AF`

### dbNSFP v5.3.1

dbNSFP is a pre-computed database of functional annotations for all possible non-synonymous SNVs
and splice-site SNVs in the human genome. Use dbNSFP as the single source for functional
predictors, including CADD, instead of maintaining a separate CADD plugin/data download.

- Current release: dbNSFP v5.3.1 (January 1, 2026)
- Based on GENCODE Human release 49 / Ensembl 115
- Variant coverage: 83,049,507 nsSNVs and 2,446,464 splice-site SNVs
- Data files: `dbNSFP5.3.1_variant.chr*.gz` + `.tbi` or VEP-ready equivalent
- Download: `https://www.dbnsfp.org/download`
- License: confirm the correct academic/commercial branch before production use

Contents to retain in notes/schema planning:

- Prediction algorithms: SIFT, SIFT4G, PROVEAN, PolyPhen2-HDIV/HVAR, MutationTaster2021,
  MutationAssessor, FATHMM-XF coding, CADD, VEST4, DANN, MetaSVM, MetaLR, MetaRNN, Eigen,
  Eigen-PC, M-CAP, REVEL, MutPred2, MVP, gMVP, MPC, PrimateAI, DEOGEN2, ALoFT, BayesDel,
  ClinPred, LIST-S2, VARITY, ESM1b, AlphaMissense, PHACTboost, MutFormer, MutScore, MisFit,
  and popEVE.
- Conservation scores: PhyloP, phastCons, GERP++, GERP_92_mammals, and bStatistic.
- Population AF: 1000 Genomes, gnomAD v4.1, gnomAD v2.1.1, TOPMed, All of Us, RGC Million Exome,
  and ALFA.
- Gene-level annotations: HGNC IDs, GenCC, OMIM, Orphanet, HPO, GWAS Catalog, ClinGen Dosage
  Sensitivity, Human Protein Atlas, UniProt, Gene Ontology, IntAct, LOEUF/MOEUF from gnomAD 4.1,
  ConsensusPathDB, KEGG, MGI, and ZFIN.

**Integration option A — VEP plugin (recommended for pipeline consistency):**
```
--plugin dbNSFP,/mnt/sdb/reference/dbnsfp/dbNSFP5.3.1_variant.chr%s.gz,<allowlisted_fields>
```

**Integration option B — Hail Table join (more explicit, easier to update independently):**
Load dbNSFP flat file as a Hail Table and join on `locus` + `alleles`.

Fields to extract:

| dbNSFP field | Meaning | Threshold |
|---|---|---|
| CADD PHRED field | CADD score from dbNSFP | ≥20 = top 1% |
| `REVEL_score` | Rare variant pathogenicity ensemble | ≥0.5 suggestive; ≥0.75 strong |
| `SIFT_score` | Sequence-based tolerance | <0.05 damaging |
| `Polyphen2_HDIV_score` | Structure-based pathogenicity | >0.908 probably damaging |
| `MetaRNN_score` | Deep learning ensemble | ≥0.5 damaging |
| `ClinPred_score` | Trained on ClinVar P/LP | >0.5 likely pathogenic |
| `AlphaMissense_score` / prediction | Deep learning missense pathogenicity | use dbNSFP README thresholds |
| gnomAD v4.1 / v2.1.1 AF fields | population context | exact field names from README |
| gnomAD LOEUF / MOEUF fields | gene constraint | lower LOEUF = stronger LoF constraint |

### ClinVar

- File: `/mnt/sdb/reference/clinvar/clinvar.vcf.gz` (182 MB, in place)
- Strategy: `hl.import_vcf` then join on `locus` + `alleles`
- Update cadence: download new release every 3–6 months

### gnomAD v2.1.1 exomes

- Local Hail Table: `/mnt/sdb/reference/gnomad/gnomad.exomes.r2.1.1.sites.ht` (59 GB, in place)
- Join fields: `AF` (all pops), `AF_nfe` (Non-Finnish European)
- Reference genome: GRCh37 — matches current pipeline

---

## Storage Layer

| Layer | Path pattern / index | Role |
|---|---|---|
| Internal VDS | `/mnt/sdb/data/vds/cohort_*.vds` on internal VM | Raw genotype store; never modified post-ingest |
| Internal MatrixTable | `/mnt/sdb/data/mt/cohort_annotated.mt` on internal VM | Annotation, metadata, patient-linked query, and export layer; rebuilt on schema change |
| Internal aggregate Elasticsearch | `cohort_variants` | Current variant-level annotation and cohort-count queries |
| Planned internal sample indices | `cohort_samples`, `cohort_sample_variants` | Sample metadata and per-sample carriers/reports; design only |
| Reference data | `/mnt/sdb/reference/` | Immutable annotation sources; not inside git repo |

---

## Browser and Deployment

### Target

One internal browser/API deployment for GRCh37 cohort investigation. All
authenticated internal users may see sample IDs. Public deployment is excluded
from v1. Sample-linked export/query/view paths are still planned work.

### Network architecture

```
Internal user laptop
  ─── HTTPS (port 443) ───────────────────────────────────────────────────────►
                                              internal VM nginx reverse proxy
                                              ├── TLS termination
                                              ├── authentication
                                              ├── browser UI
                                              └── GraphQL API

                  Elasticsearch :9200  ◄── host access restricted to loopback;
                                           container networking isolated
```

This is the target deployment. The tracked Compose stack is a development stack;
the required production controls are not established by this diagram.

### Authentication

nginx HTTP basic auth — `.htpasswd` file, managed manually for the small known user group.

### Security requirements

- Elasticsearch binds to `127.0.0.1` only — never reachable from outside the host
- All user-facing traffic over HTTPS; no plain HTTP in production
- Firewall: only ports 443 (HTTPS) and 22 (SSH) open inbound on the VM
- All authenticated internal users may see sample IDs and the sample-linked results required by their workflows
- The browser/API must not be made public as part of v1
- No internal file paths (VDS, MT, reference dirs) in GraphQL error messages or API responses
- Session timeout: configure in the auth proxy (≤8 h idle)
- TLS certificate: use internal CA cert or a self-signed cert accepted by internal browsers

### nginx additions needed

- `nginx` service with TLS config, HTTP basic auth, and proxy pass to `:3000` / `:8000`
- Elasticsearch `network.host: 127.0.0.1` in config
- Healthcheck asserting Elasticsearch is not reachable from outside the Docker bridge network

See `docs/DEPLOYMENT.md` for the browser stack deployment steps.

---

## Key Scripts

| Script | Purpose |
|---|---|
| `parallel_ingest_cohort.py` | bcftools filter + incremental Hail VDS combine |
| `annotate_cohort.py` | VDS → MatrixTable, VEP, dbNSFP, ClinVar, gnomAD, cohort AF, metadata |
| `ingest_manifest.py` | Run tracking and resumability |
| `browser/data-pipeline/cohort_export.py` | Export variant documents to Elasticsearch |
| `setup.sh` | Clone upstream gnomAD browser and overlay `browser/` patches |

---

## Current VM State (as of 2026-05-14)

```
/mnt/sdb/projects/clingen-cohort-vds-browser/   ← code (latest main)
/mnt/sdb/reference/clinvar/                      ← ClinVar VCF (182 MB) in place
/mnt/sdb/reference/gnomad/                       ← gnomAD HT + VCF (59 GB) in place
/mnt/sdb/reference/dbnsfp/                       ← dbNSFP files still need downloading
/mnt/sdb/data/vds/                               ← cohort_10k.vds + cohort_2026-03-11_run001.vds
/mnt/sdb/data/mt/                                ← cohort_annotated.mt
/mnt/sdb/data/raw_gvcfs/ + filtered_gvcfs/       ← GVCF inputs
```

## Open Decisions

- Whether a sample may have multiple sequencing procedures and how those runs affect counting
- Authoritative report source, Unknown/None semantics, and prenatal storage representation
  (source tokens are preserved in `SAMPLE_METADATA_SOURCE.md`)
- Which of the existing predictor superset fields are required for v1; variant fields are already
  implemented, and 439c025 records the dbNSFP field verification and separate constraint join
- TLS certificate source: internal CA vs. self-signed
- Whether `sex_chr` inference runs inside `annotate_cohort.py` or as a pre-processing step
- Sample/carrier/HPO/report UI acceptance and genotype quality thresholds

Resolved scope: GRCh37-only, internal-only, with sample IDs visible to all
authenticated internal users. Public export/deployment decisions do not block v1.
