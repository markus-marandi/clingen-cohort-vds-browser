# Annotation Sources

Reference document for the annotation pipeline. For each field in `andmebaasi_struktuur.xlsx`,
this lists the source type, the endpoint or library to use, and what needs to be configured.

---

## Storage architecture note

**VDS** is kept as the raw genotype store (combiner output). Annotations go on a **Hail MatrixTable**:

```
VDS  ->  hl.vds.to_dense_mt()  -> annotate rows/cols  ->  write annotated .mt
```

- `mt.annotate_rows(...)` — variant-level annotations (VEP, ClinVar, CADD, gnomAD, cohort freqs)
- `mt.annotate_cols(...)` — sample-level annotations (clinical metadata)
- `mt.write('cohort_annotated.mt')` — persist the annotated MT for downstream export

VDS itself does not need to change. The annotated MT is what feeds the export pipeline.

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

No external endpoint needed. These are computed with `hl.variant_qc(mt)` after densification.

---

### 3. VEP — variant annotation

All fields below come from VEP. Two options:

#### Option A — Hail built-in `hl.vep()` (recommended for large cohorts)

Requires a local VEP installation and a Hail VEP config JSON.

```python
# fill in your vep config path
mt = hl.vep(mt, config='/home/markus/gen-toolbox/src/config/vep_settings.json')
```

{
    "command": [
        "/mnt/sdb/projects/ensembl-vep/vep",
        "--format", "vcf",
        "__OUTPUT_FORMAT_FLAG__",
        "--fasta", "/mnt/sdb/VEP/ref_fasta/ucsc.hg19.fasta",
        "--dir_cache", "/mnt/sdb/VEP/VEP_cache/",
        "--no_stats",
        "--cache", "--offline",
        "--assembly", "GRCh37",
        "--cache_version", "108",
        "--merged",
        "--MAX_AF",
        "--symbol",
        "--fields", "IMPACT,SYMBOL,HGNC_ID,MAX_AF,MAX_AF_POPS",
        "--pick",
        "--canonical",
        "--use_given_ref",
        "--offline",
        "-o", "STDOUT"
    ],
    "env": {
        "PERL5LIB": "/vep_data/loftee"
    },
    "vep_json_schema": "Struct{IMPACT:String,SYMBOL:String,HGNC_ID:Int32,MAX_AF:Float64,MAX_AF_POPS:String,input:String}"
}

Hail's VEP config JSON points to:
- `vep` binary location
- plugins directory
- cache directory (GRCh37 or GRCh38)

> **TODO**: fill in VEP binary path and cache location for your environment

#### Option B — Ensembl REST API (smaller batches only, ~200 variants/request)

```
POST https://grch37.rest.ensembl.org/vep/human/region
Content-Type: application/json
{ "variants": ["17 7577556 . C A . . ."] }
```

Python library: `requests`

> **TODO**: decide batch vs local VEP based on cohort size

#### VEP fields and where they come from inside the VEP response

| Field | VEP output key | Notes |
|---|---|---|
| `gdna` | `HGVSg` | genomic HGVS |
| `cdna` | `HGVSc` | coding HGVS |
| `transcript` | `transcript_id` | filter to MANE Select / NM_ RefSeq |
| `p_nomen` | `HGVSp` | protein HGVS |
| `gene_symbol` | `gene_symbol` | |
| `consequence` | `consequence_terms[0]` | most severe |
| `impact` | `impact` | HIGH / MODERATE / LOW / MODIFIER |
| `clinvar_sig` | `ClinVar` plugin | requires ClinVar VEP plugin |
| `cadd_score` | `CADD` plugin | requires CADD VEP plugin |
| `gnomad_af` | `gnomAD` plugin — `AF` | gnomAD v2/v3 plugin |
| `gnomad_nonfin` | `gnomAD` plugin — `AF_nfe` | Non-Finnish European |

> **TODO**: confirm which VEP plugins are installed/available:
> - [ ] ClinVar (`--plugin ClinVar,...`)
> - [ ] CADD (`--plugin CADD,...`)
> - [ ] gnomAD (`--plugin gnomAD,...`)

---

### 4. ClinVar — if not using VEP plugin

Direct options if VEP ClinVar plugin is not available:

| Approach | Notes |
|---|---|
| Hail Table join | download `variant_summary.txt.gz` from NCBI, load as Hail Table, join on locus+alleles |
| NCBI E-utilities API | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=clinvar&term=...` |
| ClinVar FTP | `https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz` |

Python library: `hail` (for Table join) or `requests` (for API)

> **TODO**: choose approach — VEP plugin or separate join

---

### 5. gnomAD — if not using VEP plugin

| Approach | Notes |
|---|---|
| gnomAD public Hail Tables | GRCh37: `gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht` (requires GCS access) |
| gnomAD REST API | `https://gnomad.broadinstitute.org/api` — GraphQL, variant-level |
| Local VCF download | `https://gnomad.broadinstitute.org/downloads` — chr-split VCFs, join with Hail |

Python library: `hail` (Hail Table join) or `requests` (GraphQL API)

> **TODO**: fill in which gnomAD version (v2 exomes for GRCh37, v4 for GRCh38) and access method:
> - gnomAD version: ___
> - Access method: GCS Hail Table / REST API / local VCF

---

### 6. Clinical metadata — sample-level, from internal GE server

These annotate **columns** (samples), not rows (variants).

| Field | Notes |
|---|---|
| `test` | WES / WGS |
| `instrument` | sequencer model |
| `sex` | chromosomal or from metadata |
| `age` | age at sequencing |
| `care_site` | ordering institution (TUH, etc.) |
| `panel` | ordered gene panel(s) |
| `HPO_ID` | HPO term ID (WGS samples) |
| `HPO_termin` | HPO term label |
| `health_status` | Affected / Non-affected |

Source is a summary table on the GE server (or equivalent).

> **TODO**: fill in:
> - Internal metadata table location / API endpoint: ___
> - Format (CSV / database / REST): ___
> - Join key (sample_id / e-code): ___

---

## Summary checklist

| Source | Status | Endpoint / path to configure |
|---|---|---|
| VCF fields | ready | no config needed |
| Cohort frequencies | ready | `hl.variant_qc()` |
| VEP — local `hl.vep()` | **TODO** | VEP binary + cache path |
| VEP — ClinVar plugin | **TODO** | plugin path + ClinVar data |
| VEP — CADD plugin | **TODO** | plugin path + CADD scores |
| VEP — gnomAD plugin | **TODO** | plugin path + gnomAD data |
| gnomAD (if separate) | **TODO** | GCS / REST / local VCF |
| Clinical metadata | **TODO** | GE server table/API + join key |
