# Architecture Specification

_Last updated: 2026-05-29_

Cohort variant browser for internal WGS/WES data. Concatenates and stores genotypes in Hail VDS,
annotates variants with VEP + dbNSFP + ClinVar + gnomAD, and serves results through a
patched gnomAD browser UI. Patient-linked data and public-facing variant data are deployed as two
separate database tiers, preferably on separate VMs.

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

## Two Database Security Model

The pipeline must produce two different serving databases from the VDS/MatrixTable layer:

| Tier | Location | Contents | Access |
|---|---|---|---|
| Internal patient-linked database | Internal analysis VM | Raw/filtered GVCFs, VDS, full annotated MatrixTable, sample metadata, patient or sample IDs, per-sample genotype fields, internal Elasticsearch indexes | Restricted operators and authenticated clinical/internal users only |
| Public/sanitized variant database | Separate public/browser VM | De-identified variant-level documents only: variant identity, annotations, and approved aggregate cohort frequency fields | Public-facing or broader-access browser |

The internal VM is the source of truth. It performs ingest, annotation, metadata joins, and any
patient-linked queries. The public VM must not contain raw GVCFs, VDS, full MatrixTables, sample
metadata files, patient IDs, sample IDs, per-sample genotypes, report flags, HPO assignments, dates
of birth, sequencing dates, care sites, run IDs, or internal paths.

The only data movement from the internal VM to the public VM should be a sanitized export bundle or
bulk Elasticsearch load generated from an explicit allowlist of public fields. That export should
fail closed if any disallowed patient-linked field is present. The public VM should not have network
or filesystem access back to the internal VM.

Before truly internet-facing release, define a privacy threshold for aggregate cohort frequencies
in small cohorts. For example, decide whether very low counts are suppressed, rounded, bucketed, or
kept internal-only.

---

## Data Model

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

### Cohort frequency — from `hl.variant_qc()`

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
| `date_of_birth` | ISO date string YYYY-MM-DD |
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
| `ref_genome` | hg19 / GRCh37 |
| `sample_id` | |

### Panels — one-to-many (WES samples)

| Field | Notes |
|---|---|
| `sample_id` | |
| `panel` | ordered gene panel name (e.g. ANEEMIA, SKIN) |

One sample may appear on multiple rows (one per ordered panel).

### HPO assignments — one-to-many (WGS samples)

| Field | Notes |
|---|---|
| `sample_id` | |
| `HPO_ID` | e.g. HP:0001915 |

### HPO lookup table — from HPO release file

| Field | Notes |
|---|---|
| `HPO_ID` | join key |
| `HPO_termin` | human-readable label (e.g. Aplastic anemia) |
| `HPO_version` | release version (e.g. v01-2024) |
| `date_valid_from` | ISO date when this version took effect |

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
| Internal Elasticsearch | `cohort_variants_internal` or equivalent on internal VM | Optional patient-linked/internal browser queries; may contain restricted fields |
| Public Elasticsearch | `cohort_variants_public` or equivalent on public VM | Sanitized variant-level docs only; no patient/sample identifiers or per-sample fields |
| Reference data | `/mnt/sdb/reference/` | Immutable annotation sources; not inside git repo |

---

## Browser and Deployment

### Target

There are two target deployments:

- Internal browser/API on the internal VM for patient-linked investigation.
- Public/sanitized browser/API on a separate VM for de-identified variant-level search.

The internal deployment contains patient-linked genomic variants and must be gated even on the
internal network. The public deployment is built only from the sanitized public Elasticsearch index.

### Network architecture

```
Internal user laptop
  ─── HTTPS (port 443) ───────────────────────────────────────────────────────►
                                              internal VM nginx reverse proxy
                                              ├── TLS termination
                                              ├── auth check (see options)
                                              ├── → :3000  gnomAD browser UI
                                              └── → :8000  GraphQL API

                  Elasticsearch :9200  ◄── bound to 127.0.0.1 only, not exposed

Internal VM sanitized export
  ─── one-way transfer of allowlisted public variant docs ────────────────────►
                                              public/browser VM
                                              ├── nginx / TLS
                                              ├── public GraphQL API
                                              ├── public browser UI
                                              └── public Elasticsearch, localhost only
```

### Authentication

nginx HTTP basic auth — `.htpasswd` file, managed manually for the small known user group.

### Security requirements

- Elasticsearch binds to `127.0.0.1` only — never reachable from outside the host
- All user-facing traffic over HTTPS; no plain HTTP in production
- Firewall: only ports 443 (HTTPS) and 22 (SSH) open inbound on the VM
- `sample_id`, patient IDs, and per-sample genotype fields must not appear in public browser results
- No internal file paths (VDS, MT, reference dirs) in GraphQL error messages or API responses
- Session timeout: configure in the auth proxy (≤8 h idle)
- TLS certificate: use internal CA cert or a self-signed cert accepted by internal browsers
- Public export uses an allowlist, not a blocklist
- Public VM has no raw GVCFs, VDS, MatrixTables, clinical metadata, or internal credentials
- Public VM cannot initiate network connections to internal data services

### nginx additions needed

- `nginx` service with TLS config, HTTP basic auth, and proxy pass to `:3000` / `:8000`
- Elasticsearch `network.host: 127.0.0.1` in config
- Healthcheck asserting Elasticsearch is not reachable from outside the Docker bridge network

See `browser/TODO.md` for the outstanding browser stack tasks.

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

- Public export profile: exact allowlisted fields, index name, transfer method, and release checklist
- Public privacy thresholds for small cohort counts and rare variants
- dbNSFP integration method: VEP plugin (single VEP pass) vs. standalone Hail Table join
- Exact dbNSFP v5.3.1 field allowlist for MatrixTable, internal ES, and public ES export
- TLS certificate source: internal CA vs. self-signed
- Whether `sex_chr` inference runs inside `annotate_cohort.py` or as a pre-processing step
- HPO filter UI exposure: first demo or later phase
