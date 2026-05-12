# Annotation Sources

Reference document for the annotation pipeline. For each field in the data model, this lists
the source type, the path or endpoint, and configuration details.

See `docs/ARCHITECTURE.md` for the full data model and pipeline overview.

---

## Storage architecture

**VDS** is the raw genotype store (combiner output). Annotations go on a **Hail MatrixTable**:

```
VDS  →  hl.vds.to_dense_mt()  →  annotate rows/cols  →  write annotated .mt
```

- `mt.annotate_rows(...)` — variant-level annotations (VEP, CADD, dbNSFP, ClinVar, gnomAD, cohort freqs)
- `mt.annotate_cols(...)` — sample-level annotations (clinical metadata)
- `mt.write('cohort_annotated.mt')` — persist for downstream export

VDS itself does not change. The annotated MT feeds the export pipeline.

---

## Fields by source

### 1. VCF — extracted during ingestion, no external call needed

| Field | Notes |
|---|---|
| `sample_id` | from VCF filename or `##SAMPLE` header |
| `variant_id` | assembled: `{chrom}-{pos}-{ref}-{alt}` |
| `chrom` | `locus.contig` |
| `pos` | `locus.position` |
| `ref` | `alleles[0]` |
| `alt` | `alleles[1]` |
| `genotype` | `entry.GT` |
| `depth` | `entry.DP` |
| `gq` | `entry.GQ` |

---

### 2. Cohort frequencies — computed by Hail from the MT itself

| Field | Hail expression |
|---|---|
| `ac_total` | `variant_qc.AC[1]` |
| `an_total` | `variant_qc.AN` |
| `af_total` | `variant_qc.AF[1]` |
| `hom_count` | `variant_qc.homozygote_count[1]` |

Computed with `hl.variant_qc(mt)` after densification. No external endpoint needed.

---

### 3. VEP — variant annotation

Use Hail built-in `hl.vep()` with a local VEP installation.

```python
mt = hl.vep(mt, config='vep_settings.json')
```

**Paths on VM:**
- Binary: `/mnt/sdb/projects/ensembl-vep/vep`
- Cache: `/mnt/sdb/VEP/VEP_cache/` (GRCh37, v108, merged)
- FASTA: `/mnt/sdb/VEP/ref_fasta/ucsc.hg19.fasta`
- Plugins dir: `/mnt/sdb/projects/ensembl-vep/Plugins/`

**VEP flags:** `--pick --canonical --symbol --HGVS --MAX_AF --assembly GRCh37 --cache_version 108 --merged --offline`

**Fields from VEP response:**

| Field | VEP output key | Notes |
|---|---|---|
| `HGVS_g` | `HGVSg` | genomic HGVS |
| `HGVS_c` | `HGVSc` | coding HGVS |
| `transcript` | `transcript_id` | MANE Select / NM_ RefSeq |
| `HGVS_p` | `HGVSp` | protein HGVS |
| `gene_symbol` | `SYMBOL` | |
| `consequence` | `consequence_terms[0]` | most severe |
| `impact` | `IMPACT` | HIGH / MODERATE / LOW / MODIFIER |

CADD scores and dbNSFP scores are added through VEP plugins (see sections 4 and 5 below).

---

### 4. CADD 1.6 — deleteriousness score (decided)

CADD (Combined Annotation Dependent Depletion) integrates hundreds of annotations into a single
PHRED-scaled deleteriousness score. v1.6 is the current production release for GRCh37.

**Plugin:** `CADD.pm` — copy to `/mnt/sdb/projects/ensembl-vep/Plugins/CADD.pm`

**Data files** (download to `/mnt/sdb/reference/cadd_v1.6/`):
```
whole_genome_SNVs.tsv.gz      # ~200 GB
whole_genome_SNVs.tsv.gz.tbi
InDels.tsv.gz
InDels.tsv.gz.tbi
```
Download from: `https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh37/`

**VEP config line:**
```
"--plugin", "CADD,/mnt/sdb/reference/cadd_v1.6/whole_genome_SNVs.tsv.gz,/mnt/sdb/reference/cadd_v1.6/InDels.tsv.gz"
```

**Fields:**

| Field | Notes |
|---|---|
| `cadd_raw` | raw CADD score |
| `cadd_score` | PHRED-scaled score (use this for thresholds); ≥20 = top 1% most deleterious |

> **Status:** CADD.pm plugin exists on VM. TSV data files need to be downloaded (v1.4 remote URL
> was broken; v1.6 local files resolve the plugin warning seen in `STDOUT_warnings.txt`).

---

### 5. dbNSFP — functional annotation database (decided)

dbNSFP is a pre-computed resource covering all possible non-synonymous SNVs in the human genome.
A single join provides REVEL, SIFT, PolyPhen-2, MetaRNN, and ClinPred scores without running
each predictor separately. Recommended version: 4.8 (GRCh37 build available).

**License:** free for non-commercial academic research.

**Data files** (download to `/mnt/sdb/reference/dbnsfp/`):
```
dbNSFP4.8_variant.chr1.gz
dbNSFP4.8_variant.chr1.gz.tbi
... (one file per chromosome)
```
Download from: `https://sites.google.com/site/jpopgen/dbNSFP` (requires registration)

**Integration — Option A: VEP plugin (recommended)**

```
"--plugin", "dbNSFP,/mnt/sdb/reference/dbnsfp/dbNSFP4.8_variant.chr%s.gz,REVEL_score,SIFT_score,Polyphen2_HDIV_score,MetaRNN_score,ClinPred_score"
```

**Integration — Option B: Hail Table join**

Load the dbNSFP flat files as a Hail Table and join on `locus` + `alleles`. More explicit and
easier to update independently of VEP, but requires an extra pipeline step.

**Fields to extract:**

| dbNSFP field | Meaning | Threshold |
|---|---|---|
| `REVEL_score` | Rare variant pathogenicity ensemble | ≥0.5 suggestive; ≥0.75 strong |
| `SIFT_score` | Sequence-based tolerance | <0.05 = damaging |
| `Polyphen2_HDIV_score` | Structure-based pathogenicity | >0.908 = probably damaging |
| `MetaRNN_score` | Deep learning ensemble | ≥0.5 = damaging |
| `ClinPred_score` | Trained on ClinVar P/LP variants | >0.5 = likely pathogenic |

> **Status:** integration method not yet decided (VEP plugin vs. Hail join). Data files not yet
> downloaded.

---

### 6. ClinVar — clinical significance (decided: local VCF join)

| Field | Notes |
|---|---|
| `clinvar_sig` | clinical significance string (Pathogenic, VUS, Benign, …) |
| `clinvar_condition` | associated disease / condition |
| `clinvar_review_status` | evidence level (criteria provided, expert panel, …) |

**Data file:** `/mnt/sdb/reference/clinvar/clinvar.vcf.gz` (move from `Plugins/clinvar.vcf.gz`)

**Hail join:**
```python
clinvar = hl.import_vcf('/mnt/sdb/reference/clinvar/clinvar.vcf.gz', reference_genome='GRCh37')
clinvar_ht = clinvar.rows()
mt = mt.annotate_rows(clinvar=clinvar_ht[mt.locus, mt.alleles])
```

Update cadence: download a fresh release every 3–6 months from
`https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz`

---

### 7. gnomAD v2.1.1 exomes (decided: local Hail Table)

| Field | gnomAD HT field | Notes |
|---|---|---|
| `gnomad_af` | `freq[0].AF` | all-population AF |
| `gnomad_nonfin` | `freq[...].AF` where pop=`nfe` | Non-Finnish European AF |

**Data:** `/mnt/sdb/reference/gnomad/gnomad.exomes.r2.1.1.sites.ht` (move from `Plugins/`)

**Hail join:**
```python
gnomad_ht = hl.read_table('/mnt/sdb/reference/gnomad/gnomad.exomes.r2.1.1.sites.ht')
mt = mt.annotate_rows(gnomad=gnomad_ht[mt.locus, mt.alleles])
```

Reference genome GRCh37 matches the current pipeline.

---

### 8. Clinical metadata — sample-level, from internal GE server

These annotate **columns** (samples), not rows (variants). Joined by `sample_id`.

**Updated sample CSV fields:**

| Field | Previous field | Notes |
|---|---|---|
| `sample_id` | same | join key |
| `chromosomal_sex` | — | **new** — inferred from genotype data (X het rate / chrY coverage); values: XX, XY, ambiguous |
| `sex_assigned` | `sex` | **renamed** — from clinical metadata; values: Male, Female, Other, Unknown |
| `date_of_birth` | `age` | **renamed** — ISO date string (YYYY-MM-DD); normalize on load |
| `date_seq` | same | |
| `run_id` | same | |
| `care_site` | same | ordering institution |
| `health_status` | same | Affected / Non-affected |
| `material` | — | **new** — biological source: blood, tissue, saliva, buccal swab, etc. |
| `test` | (future) | WES / WGS |
| `instrument` | (future) | sequencer model |

**Panels CSV** (one row per sample per panel, unchanged):
- `sample_id`, `panel`

**HPO CSV** (WGS samples, one row per sample per HPO term):
- `sample_id`, `HPO_ID`

**HPO lookup table** (from HPO release):
- `HPO_ID`, `HPO_termin`, `HPO_version`, `date_valid_from`

> **TODO:**
> - Internal metadata table location / API endpoint: ___
> - Format (CSV / database / REST): ___
> - Confirm `chromosomal_sex` inference step location (annotate_cohort.py or pre-processing)

---

## Summary checklist

| Source | Status | Path / action needed |
|---|---|---|
| VCF fields | ready | no config needed |
| Cohort frequencies | ready | `hl.variant_qc()` |
| VEP — local `hl.vep()` | ready | paths confirmed on VM |
| CADD 1.6 | plugin ready, **data needed** | download v1.6 TSV to `/mnt/sdb/reference/cadd_v1.6/` |
| dbNSFP | **TODO** | decide VEP plugin vs Hail join; download data |
| ClinVar | data on VM, **move needed** | `mv Plugins/clinvar.vcf.gz* /mnt/sdb/reference/clinvar/` |
| gnomAD v2.1.1 | data on VM, **move needed** | `mv Plugins/gnomad.* /mnt/sdb/reference/gnomad/` |
| Sample metadata | **schema updated** | update CSV files with new field names |
