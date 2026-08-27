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

- `mt.annotate_rows(...)` — variant-level annotations (VEP, dbNSFP, ClinVar, gnomAD, cohort freqs)
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

Predictor scores are added from dbNSFP. CADD is retained as a dbNSFP-derived field, not as a
separate CADD plugin/download.

---

### 4. dbNSFP — functional annotation database (decided)

dbNSFP is a pre-computed resource covering all possible non-synonymous SNVs in the human genome.
Use dbNSFP as the single source for functional predictors, including CADD, rather than maintaining
a separate CADD plugin and data download.

**Current release:** dbNSFP v5.3.1, released January 1, 2026. v5.3 rebuilt the variant set from
GENCODE Human release 49 / Ensembl 115. v5.3.1 adds popEVE and hs1/T2T-CHM13 v2.0 coordinates.

**Coverage and contents to capture in notes/schema planning:**

- 83,049,507 non-synonymous SNVs and 2,446,464 splice-site SNVs.
- 36 deleteriousness prediction algorithms, including SIFT, SIFT4G, PROVEAN, PolyPhen2-HDIV,
  PolyPhen2-HVAR, MutationTaster2021, MutationAssessor, FATHMM-XF coding, CADD, VEST4, DANN,
  MetaSVM, MetaLR, MetaRNN, Eigen, Eigen-PC, M-CAP, REVEL, MutPred2, MVP, gMVP, MPC, PrimateAI,
  DEOGEN2, ALoFT, BayesDel, ClinPred, LIST-S2, VARITY, ESM1b, AlphaMissense, PHACTboost,
  MutFormer, MutScore, MisFit, and popEVE.
- Conservation scores: PhyloP, phastCons, GERP++, GERP_92_mammals, and bStatistic.
- Population frequencies: 1000 Genomes, gnomAD v4.1, gnomAD v2.1.1, TOPMed, All of Us, RGC
  Million Exome, and ALFA.
- Gene-level annotations: HGNC IDs, GenCC, OMIM, Orphanet, HPO, GWAS Catalog, ClinGen Dosage
  Sensitivity, Human Protein Atlas, UniProt, Gene Ontology, IntAct, LOEUF/MOEUF from gnomAD 4.1,
  ConsensusPathDB, KEGG, MGI, and ZFIN.

**License:** confirm branch/license before production download. Historically dbNSFP has had
academic and commercial branches; use the branch permitted for the deployment.

**Data files** (download to `/mnt/sdb/reference/dbnsfp/`):
```
dbNSFP5.3.1_variant.chr1.gz
dbNSFP5.3.1_variant.chr1.gz.tbi
... (one file per chromosome)
```
Download from: `https://www.dbnsfp.org/download` (requires registration).

**Integration — Option A: VEP plugin (recommended)**

```
"--plugin", "dbNSFP,/mnt/sdb/reference/dbnsfp/dbNSFP5.3.1_variant.chr%s.gz,<allowlisted_fields>"
```

**Integration — Option B: Hail Table join**

Load the dbNSFP flat files as a Hail Table and join on `locus` + `alleles`. More explicit and
easier to update independently of VEP, but requires an extra pipeline step.

**Initial fields to extract/export:**

| dbNSFP field | Meaning | Threshold |
|---|---|---|
| `CADD_phred` / branch-specific CADD PHRED field | CADD score from dbNSFP | ≥20 = top 1% most deleterious |
| `REVEL_score` | Rare variant pathogenicity ensemble | ≥0.5 suggestive; ≥0.75 strong |
| `SIFT_score` | Sequence-based tolerance | <0.05 = damaging |
| `Polyphen2_HDIV_score` | Structure-based pathogenicity | >0.908 = probably damaging |
| `MetaRNN_score` | Deep learning ensemble | ≥0.5 = damaging |
| `ClinPred_score` | Trained on ClinVar P/LP variants | >0.5 = likely pathogenic |
| `AlphaMissense_score` / prediction | Deep learning missense pathogenicity | use dbNSFP README thresholds |
| `gnomAD4` / `gnomAD2.1.1` population AF fields | population context | use exact fields from README |
| `gnomAD_LOEUF` / `gnomAD_MOEUF` gene constraint | gene-level constraint | lower LOEUF = stronger LoF constraint |

Keep the MatrixTable schema flexible enough to retain additional dbNSFP fields that are useful for
internal interpretation. The public Elasticsearch export should stay allowlisted and may expose a
smaller subset.

> **Status:** implemented as a VEP plugin (Option A) in `vep_settings.json`; the standalone CADD
> download/plugin is retired. `annotate_cohort.py` flattens the dbNSFP fields via the `_dbnsfp_float`
> helper and `cohort_export.py` indexes them (`cadd_score`, `revel_score`, `sift_score`,
> `polyphen_score`, `metarnn_score`, `clinpred_score`, `alphamissense_score`, `dbnsfp_popmax_af`,
> `gnomad_loeuf`, `gnomad_moeuf`).
>
> **Not yet verified on the VM** — before the next annotation run, confirm:
> - dbNSFP v5.3.1 data files are downloaded to `/mnt/sdb/reference/dbnsfp/` and tabix-indexed. The
>   plugin may require a single concatenated + bgzipped file rather than the per-chromosome `chr%s`
>   template currently in `vep_settings.json`.
> - The exact dbNSFP column names emitted into VEP JSON match the `vep_settings.json` allowlist and
>   `vep_json_schema`. In particular: the population-AF column mapped to `dbnsfp_popmax_af`
>   (`gnomAD_genomes_AF` is a placeholder — replace with the true popmax/grpmax column from the
>   v5.3.1 README) and the gene-constraint columns `LOEUF` / `MOEUF`.
> - VEP JSON emits these plugin fields as strings (handled by `_dbnsfp_float`); adjust the schema
>   types if any come through numeric.

---

### 5. ClinVar — clinical significance (decided: local VCF join)

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

### 6. gnomAD v2.1.1 exomes (decided: local Hail Table)

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

### 7. Clinical metadata — sample-level, from internal GE server

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
| dbNSFP v5.3.1 | code done, **data + VM verify pending** | VEP plugin wired in `vep_settings.json`; download data to `/mnt/sdb/reference/dbnsfp/` and verify field names |
| ClinVar | data on VM, **move needed** | `mv Plugins/clinvar.vcf.gz* /mnt/sdb/reference/clinvar/` |
| gnomAD v2.1.1 | data on VM, **move needed** | `mv Plugins/gnomad.* /mnt/sdb/reference/gnomad/` |
| Sample metadata | **schema updated** | update CSV files with new field names |
