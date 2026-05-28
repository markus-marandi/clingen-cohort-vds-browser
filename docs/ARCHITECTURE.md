# Architecture Specification

_Last updated: 2026-05-14_

Cohort variant browser for internal WGS/WES data. Concatenates and stores genotypes in Hail VDS,
annotates variants with VEP + CADD 1.6 + dbNSFP + ClinVar + gnomAD, and serves results through a
patched gnomAD browser UI on the internal network with authenticated access.

---

## Pipeline Overview

```
raw GVCFs
  → bcftools filter (contig filter, chr rename)
  → Hail VDS                              parallel_ingest_cohort.py
  → Hail MatrixTable                      annotate_cohort.py
      ├── VEP (HGVS, consequence, gene symbol)
      ├── CADD 1.6 (VEP plugin → local TSV)
      ├── dbNSFP 4.x (VEP plugin → REVEL, SIFT, PolyPhen, MetaRNN)
      ├── ClinVar (local VCF join via hl.import_vcf)
      ├── gnomAD v2.1.1 exomes (local HT join)
      ├── Cohort AF via hl.variant_qc()
      └── Sample metadata join (annotate_cols)
  → Elasticsearch index                   browser/data-pipeline/cohort_export.py
  → gnomAD browser UI (patched)           docker-compose / nginx + auth proxy
```

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
| `cadd_score` | CADD 1.6 via VEP CADD.pm plugin | PHRED-scaled; ≥20 = top 1% |
| `revel_score` | dbNSFP via VEP plugin | 0–1; ≥0.5 suggests pathogenicity |
| `sift_score` | dbNSFP via VEP plugin | <0.05 = damaging |
| `polyphen_score` | dbNSFP via VEP plugin | `Polyphen2_HDIV_score` |
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

### CADD 1.6

- Plugin: `/mnt/sdb/projects/ensembl-vep/Plugins/CADD.pm`
- Data files: `/mnt/sdb/reference/cadd_v1.6/` — `.tbi` placeholders in place; TSV files need downloading
  - `whole_genome_SNVs.tsv.gz` + `.tbi`
  - `InDels.tsv.gz` + `.tbi`
- VEP config line: `--plugin CADD,/mnt/sdb/reference/cadd_v1.6/whole_genome_SNVs.tsv.gz,/mnt/sdb/reference/cadd_v1.6/InDels.tsv.gz`
- Fields: `CADD_RAW` (raw score), `CADD_PHRED` (scaled; use this for thresholds)

### dbNSFP 4.8

dbNSFP is a pre-computed database of functional annotations for all possible non-synonymous SNVs
in the human genome. A single join provides REVEL, SIFT, PolyPhen-2, and many other scores,
avoiding the need to run each predictor separately.

- Data files: `dbNSFP4.8_variant.chr*.gz` + `.tbi` (one per chromosome)
- Download: `https://sites.google.com/site/jpopgen/dbNSFP` (academic use, free)
- License: free for non-commercial academic research

**Integration option A — VEP plugin (recommended for pipeline consistency):**
```
--plugin dbNSFP,/mnt/sdb/reference/dbnsfp/dbNSFP4.8_variant.chr%s.gz,\
REVEL_score,SIFT_score,Polyphen2_HDIV_score,MetaRNN_score,ClinPred_score
```

**Integration option B — Hail Table join (more explicit, easier to update independently):**
Load dbNSFP flat file as a Hail Table and join on `locus` + `alleles`.

Fields to extract:

| dbNSFP field | Meaning | Threshold |
|---|---|---|
| `REVEL_score` | Rare variant pathogenicity ensemble | ≥0.5 suggestive; ≥0.75 strong |
| `SIFT_score` | Sequence-based tolerance | <0.05 damaging |
| `Polyphen2_HDIV_score` | Structure-based pathogenicity | >0.908 probably damaging |
| `MetaRNN_score` | Deep learning ensemble | ≥0.5 damaging |
| `ClinPred_score` | Trained on ClinVar P/LP | >0.5 likely pathogenic |

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

| Layer | Path pattern | Role |
|---|---|---|
| VDS | `/mnt/sdb/data/vds/cohort_*.vds` | Raw genotype store; never modified post-ingest |
| MatrixTable | `/mnt/sdb/data/mt/cohort_annotated.mt` | Annotation + export layer; rebuilt on schema change |
| Elasticsearch | `http://localhost:9200` index `cohort_variants` | Flattened variant docs for browser queries |
| Reference data | `/mnt/sdb/reference/` | Immutable annotation sources; not inside git repo |

---

## Browser and Deployment

### Target

The gnomAD browser UI and GraphQL API run on the VM and are accessible to users on the internal
network only. The deployment is not public-facing. Because the data contains patient-linked
genomic variants, access must be gated even on the internal network.

### Network architecture

```
User laptop (internal network)
  ─── HTTPS (port 443) ───────────────────────────────────────────────────────►
                                              nginx reverse proxy
                                              ├── TLS termination
                                              ├── auth check (see options)
                                              ├── → :3000  gnomAD browser UI
                                              └── → :8000  GraphQL API

                  Elasticsearch :9200  ◄── bound to 127.0.0.1 only, not exposed
```

### Authentication

nginx HTTP basic auth — `.htpasswd` file, managed manually for the small known user group.

### Security requirements

- Elasticsearch binds to `127.0.0.1` only — never reachable from outside the host
- All user-facing traffic over HTTPS; no plain HTTP in production
- Firewall: only ports 443 (HTTPS) and 22 (SSH) open inbound on the VM
- `sample_id` values must not appear in variant query results returned to the browser
- No internal file paths (VDS, MT, reference dirs) in GraphQL error messages or API responses
- Session timeout: configure in the auth proxy (≤8 h idle)
- TLS certificate: use internal CA cert or a self-signed cert accepted by internal browsers

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
| `annotate_cohort.py` | VDS → MatrixTable, VEP, CADD, dbNSFP, ClinVar, gnomAD, cohort AF, metadata |
| `ingest_manifest.py` | Run tracking and resumability |
| `browser/data-pipeline/cohort_export.py` | Export variant documents to Elasticsearch |
| `setup.sh` | Clone upstream gnomAD browser and overlay `browser/` patches |

---

## Current VM State (as of 2026-05-14)

```
/mnt/sdb/projects/clingen-cohort-vds-browser/   ← code (latest main)
/mnt/sdb/projects/ensembl-vep/Plugins/CADD.pm   ← plugin in place
/mnt/sdb/reference/clinvar/                      ← ClinVar VCF (182 MB) in place
/mnt/sdb/reference/gnomad/                       ← gnomAD HT + VCF (59 GB) in place
/mnt/sdb/reference/cadd_v1.6/                    ← .tbi placeholders only; TSV files need downloading
/mnt/sdb/data/vds/                               ← cohort_10k.vds + cohort_2026-03-11_run001.vds
/mnt/sdb/data/mt/                                ← cohort_annotated.mt
/mnt/sdb/data/raw_gvcfs/ + filtered_gvcfs/       ← GVCF inputs
```

## Open Decisions

- dbNSFP integration method: VEP plugin (single VEP pass) vs. standalone Hail Table join
- CADD 1.6 TSV files still need downloading to `/mnt/sdb/reference/cadd_v1.6/`
- TLS certificate source: internal CA vs. self-signed
- Whether `sex_chr` inference runs inside `annotate_cohort.py` or as a pre-processing step
- HPO filter UI exposure: first demo or later phase
