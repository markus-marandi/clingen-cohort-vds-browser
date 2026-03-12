# cohort-vds-browser

Incremental GVCF → Hail VDS ingest pipeline with a local gnomAD browser frontend for cohort variant exploration.

---

## Overview

```
raw GVCFs  →  bcftools filter  →  Hail VDS combiner  →  annotate (VEP / gnomAD / CADD / ClinVar)  →  Elasticsearch  →  gnomAD browser
```

- **bcftools preprocessing** runs in parallel across all samples, renaming chromosomes `chr1 → 1` and stripping non-autosomal/sex contigs.
- **Hail VDS combiner** aggregates filtered GVCFs into a single [Variant Dataset (VDS)](https://hail.is/docs/0.2/vds/index.html), incrementally appending new samples on each run.
- A **JSON manifest** (`ingest_manifest.json`) tracks every combiner run. If the pipeline crashes, re-running it resumes from the Hail checkpoint — no reprocessing of already-filtered files or already-included samples.
- The **export script** reads the latest VDS, computes per-variant allele counts/frequencies, and bulk-indexes them into Elasticsearch.
- The **browser** is a lightly patched fork of the [broadinstitute/gnomad-browser](https://github.com/broadinstitute/gnomad-browser) served locally via Docker Compose.

---

## Requirements

| tool | purpose |
|---|---|
| Python ≥ 3.11 | pipeline and export scripts |
| [Hail 0.2](https://hail.is) | VDS combiner and variant QC |
| bcftools + tabix | GVCF preprocessing |
| Docker + Docker Compose | browser stack |
| pnpm | browser/API node dependencies |
| git | cloning gnomad-browser |

---

## Setup

Clone this repo, then run the setup script once:

```bash
git clone https://github.com/markus-marandi/clingen-cohort-vds-browser.git
cd clingen-cohort-vds-browser
./setup.sh
```

`setup.sh` will:
1. Clone `https://github.com/broadinstitute/gnomad-browser.git` into `gnomad-browser/`
2. Overlay the cohort-specific patches from `browser/` (new files + modified `datasets.ts` / `variant-queries.ts`)
3. Run `pnpm install` for the graphql-api and browser packages

---

## Running the pipeline

```bash
python parallel_ingest_cohort.py \
    --raw-gvcf-dir     /mnt/sdb/gvcf_ustina/andmebaas_test_valim/ \
    --filtered-gvcf-dir /mnt/sdb/gvcf_ustina/temp/andmebaas_test_valim_filtered/ \
    --output-vds-dir   /mnt/sdb/gvcf_ustina/ \
    --temp-base        /mnt/sdb/tmp/combiner_temp \
    --manifest-path    /mnt/sdb/gvcf_ustina/ingest_manifest.json \
    --n-cores 16 \
    --memory-gb 64
```

### What happens on the first run

1. Every `.gvcf.gz` in `--raw-gvcf-dir` is processed by bcftools (rename chromosomes, filter to autosomes + X/Y, bgzip + tabix index). Already-processed files are skipped.
2. The Hail combiner merges all filtered GVCFs into a versioned VDS:
   `cohort_2026-03-11_run001.vds`
3. The manifest is updated:

```json
{
  "version": 1,
  "runs": [
    {
      "run_id": "2026-03-11_run001",
      "vds_path": "/mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds",
      "temp_path": "/mnt/sdb/tmp/combiner_temp/run_2026-03-11_run001",
      "status": "completed",
      "started_at": "2026-03-11T14:30:00",
      "completed_at": "2026-03-11T18:45:00",
      "n_samples": 1000,
      "gvcfs": ["sample_001.gvcf.gz", "..."]
    }
  ]
}
```

### Adding new samples (incremental run)

Drop new `.gvcf.gz` files into `--raw-gvcf-dir` and re-run the same command.

The pipeline reads the manifest, finds which GVCFs are not yet in any completed run, and passes them to the combiner alongside the previous VDS via `vds_paths=[previous_vds]`. The result is a new standalone VDS containing the full cumulative cohort. Previous VDSes are preserved.

### Resuming after a crash

Re-run the same command. An `in_progress` entry in the manifest causes the combiner to use `force=False` and restart from the Hail temp checkpoint in `--temp-base`. bcftools preprocessing also picks up where it left off via output file existence checks.

### Resource flags

| flag | default | notes |
|---|---|---|
| `--n-cores` | `16` | total cores; 75% go to bcftools, remainder to Spark |
| `--memory-gb` | `64` | 80% is allocated as Spark driver/executor memory |

For 2000 exomes on 16 cores / 64 GB, expect:
- bcftools preprocessing: ~2-4 h depending on file size
- VDS combination: ~6-12 h (single-node Spark is the bottleneck)

---

## Annotating and exporting to Elasticsearch

After a successful ingest run, annotate the VDS and then index to Elasticsearch.

### Step 1 — annotate

```bash
python annotate_cohort.py \
    --vds-path  /mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds \
    --output-mt /mnt/sdb/gvcf_ustina/cohort_annotated.mt \
    --n-cores 16 \
    --memory-gb 64
```

Annotation layers applied in order:

| layer | source |
|---|---|
| cohort AC / AN / AF / hom_count | `hl.variant_qc()` |
| consequence, IMPACT, gene, HGVSg/c/p, transcript | VEP 108 (GRCh37, offline cache) |
| CADD_PHRED score | CADD v1.4 plugin (remote HTTPS tabix) |
| ClinVar significance | local `clinvar.vcf.gz` (VEP `--custom`) |
| gnomAD AF + AF_nfe | gnomAD v2.1.1 exomes, local VCF (first run auto-converts to Hail Table) |

On **first run** the 59 GB gnomAD VCF is imported and written as a Hail Table alongside the VCF — this takes ~30 min and only happens once.

To add sample-level clinical metadata (sex, age, care_site, panel, HPO) once the GE server export is available:

```bash
python annotate_cohort.py \
    --vds-path      /mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds \
    --output-mt     /mnt/sdb/gvcf_ustina/cohort_annotated.mt \
    --overwrite
```
```bash
python annotate_cohort.py \
    --vds-path  /mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds \
    --output-mt /mnt/sdb/gvcf_ustina/cohort_annotated.mt \
    --n-cores 16 \
    --memory-gb 64
```

The CSV must have a `sample_id` column matching the e-codes in the VCF filenames (e.g. `E01230000`).

### Step 2 — export

```bash
python browser/data-pipeline/cohort_export.py \
    --mt-path /mnt/sdb/gvcf_ustina/cohort_annotated.mt \
    --es-url  http://localhost:9200 \
    --index   cohort_variants
```

If the annotation step has not been run yet, the export script falls back to a VDS-only export (basic variant stats only):

```bash
python browser/data-pipeline/cohort_export.py \
    --vds-path /mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds
```

---

## Starting the browser

```bash
cd gnomad-browser
docker compose up --build
```

Services:
- `elasticsearch:9200` — variant index
- `redis:6379` — graphql-api cache
- `graphql-api:8000` — GraphQL backend
- `browser:3000` — React frontend

Open `http://localhost:3000`, use the dataset selector to choose **Cohort**, and search by variant ID (`1-55516888-G-GA`), rsID, gene, region, or transcript.

---

## Project layout

```
.
├── parallel_ingest_cohort.py     # main ingest pipeline (run this)
├── annotate_cohort.py            # VDS → annotated MatrixTable (VEP, gnomAD, CADD, ClinVar)
├── ingest_manifest.py            # manifest read/write helpers
├── setup.sh                      # one-time setup script
├── browser/                      # gnomad-browser patches (applied by setup.sh)
│   ├── data-pipeline/
│   │   └── cohort_export.py      # MT/VDS → Elasticsearch export
│   ├── docker-compose.yml
│   └── graphql-api/
│       └── src/
│           ├── datasets.ts                         # +cohort dataset
│           └── queries/
│               ├── variant-queries.ts              # +cohort routing
│               └── variant-datasets/
│                   └── cohort-variant-queries.ts   # cohort ES queries
└── gnomad-browser/               # cloned by setup.sh, not committed
```

---

## How the browser patches work

The gnomAD browser has a dataset abstraction where each dataset maps to a set of Elasticsearch query functions. Adding the cohort required three changes:

1. `datasets.ts` — registers `cohort` with label `'Cohort'` and reference genome `GRCh37`.
2. `cohort-variant-queries.ts` — implements the six query functions (`countVariantsInRegion`, `fetchVariantById`, `fetchVariantsByGene`, `fetchVariantsByRegion`, `fetchVariantsByTranscript`, `fetchMatchingVariants`) against the flat `cohort_variants` index.
3. `variant-queries.ts` — imports the cohort queries and routes `dataset: 'cohort'` to them.

The rest of the browser (gene annotations, transcript models, coverage tracks) continues to work normally for other datasets. Gene/transcript variant queries for the cohort fall back to positional range queries since the cohort index does not store gene annotations.
