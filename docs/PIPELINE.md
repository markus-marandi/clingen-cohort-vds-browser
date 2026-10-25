# Processing GVCFs into a searchable cohort

The [README](../README.md) gives the end-to-end workflow and TUI launch command.
This guide covers direct commands and the boundary between processing and browsing.
Examples use placeholder paths; keep deployment configuration and patient data out of Git.

## Inputs and outputs

| Stage | Required inputs | Output |
|---|---|---|
| Ingest | Indexed `.gvcf.gz` files with unique sample IDs | Processed GVCFs, a new `.vds` and run-manifest record |
| Annotate | Completed VDS, VEP configuration/cache/FASTA, dbNSFP, ClinVar, gnomAD HT | Annotated `.mt` with cohort counts and row annotations |
| Aggregate export | Annotated MT | One Elasticsearch document per ALT allele |
| Sample/carrier export | Same MT and four metadata TSVs | Sample documents and real non-reference sample/variant calls |

The ingest script standardises chromosome names and retains 1–22, X and Y.
This is preprocessing, not a complete sample/genotype QC stage. GRCh38 is not
implemented. New files are identified by filename through the manifest; keep it
with the cohort and do not silently replace already-ingested files.

## Environments and reference setup

`scripts/setup_env.sh conda` creates `clingen-pipeline` (Hail 0.2.135, Python 3.11,
Java 17, bcftools and tabix) and `clingen-tui` (Textual). The two environments are
separate because their `rich` dependencies differ. Set `CLINGEN_PIPELINE_PYTHON`
to the pipeline interpreter when launching the TUI.

The bootstrap does not install the VEP/reference datasets. An operator must
configure the annotation runtime first, including VEP/cache/plugin paths,
reference FASTA, the existing gnomAD HT, and Spark/Hail JAR/temp paths used by
`annotate_cohort.py`. Some defaults still refer to the original development
layout; the input and output flags alone do not make a new annotation installation
ready. Keep an environment-specific VEP JSON outside Git and point `--vep-config`
at it. See [annotation sources](ANNOTATION_SOURCES.md) for the actual source set.

A processing-container alternative is:

```sh
scripts/setup_env.sh docker
docker run --rm -it -v "$PWD":/work -w /work clingen-pipeline:latest \
  python parallel_ingest_cohort.py --help
```

Bind-mount input/output/reference directories separately when processing data.
The base pipeline image does not include VEP or reference databases.
`Dockerfile.annotate` builds the VEP/Hail runtime; its reference files and local
configuration must still be supplied. The TUI launches its selected Python
interpreter directly; it does not automatically wrap commands in Docker.

## Run each stage directly

Use the pipeline Python environment, with the configured annotation runtime for
step 2. The TUI runs these same scripts; examples below are alternatives to its
forms, not extra steps after completing them there.

### 1. Ingest

```sh
python parallel_ingest_cohort.py \
  --raw-gvcf-dir /path/to/gvcfs \
  --filtered-gvcf-dir /path/to/filtered \
  --output-vds-dir /path/to/vds \
  --temp-base /path/to/scratch \
  --manifest-path /path/to/ingest_manifest.json \
  --n-cores 16 \
  --memory-gb 64
```

The resource values are examples, not a minimum requirement or a capacity result.
Output VDS names include a run ID. Use the manifest/dashboard to identify the
completed output for annotation. Incremental runs combine new files with the last
completed VDS rather than editing that previous VDS in place.

### 2. Annotate

```sh
python annotate_cohort.py \
  --vds-path /path/to/completed.vds \
  --output-mt /path/to/cohort_annotated.mt \
  --vep-config /path/to/local-vep.json \
  --gnomad-ht /path/to/gnomad.sites.ht \
  --n-cores 16 \
  --memory-gb 64
```

Optional CLI inputs include `--metadata-path` (CSV), `--gene-table-path` for
LOEUF/MOEUF, and `--hpo-path` / `--hpo-lookup-path` (CSV). The HPO helper annotates
sample columns but does not provide browser HPO search. These optional fields
are not all exposed in the current TUI. Use a new output path for a new dataset;
`--overwrite` deliberately replaces an existing MT.

### 3. Export aggregate variants

```sh
python browser/data-pipeline/cohort_export.py \
  --mt-path /path/to/cohort_annotated.mt \
  --es-url http://127.0.0.1:9200 \
  --index cohort_variants_v1 \
  --batch-size 5000
```

Choose a new index and verify its results before selecting it in the API.
The annotated-MT export emits per-ALT AC/AF/homozygote counts with shared AN.
Multi-ALT rows currently lack trustworthy allele-specific annotations; these
fields remain null rather than being copied onto another ALT. Row-level filter
flags are retained. The VDS-only fallback (`--vds-path`) provides basic statistics
and does not replace the annotated workflow.

### 4. Export sample/carrier records

Use the command in the [README](../README.md#add-sample-metadata-and-carrier-records).
The four TSVs are described in [INPUT_DATA.md](INPUT_DATA.md). This exporter
preserves raw dates (including `prenatal`), groups panel rows, and distinguishes
report `None` from `Unknown`. It does not reannotate or modify the source MT.

Add `--dry-run` and omit `--mt-path` to inspect metadata counts without loading
Hail or writing Elasticsearch. For a real export, use fresh sample/carrier index
names; the default behaviour refuses existing indices.

### 5. Select and serve the accepted set

Configure the API with matching index names:

```text
COHORT_VARIANTS_INDEX=cohort_variants_v1
COHORT_SAMPLES_INDEX=cohort_samples_v1
COHORT_SAMPLE_VARIANTS_INDEX=cohort_sample_variants_v1
```

Start the configured browser/API installation as described in
[OPERATIONS.md](OPERATIONS.md). The existing lifecycle helper has deployment-local
container/image and accepted-count defaults. Adjust its documented environment
settings for a different cohort; it is not an automatic fresh-server installer.
Starting the browser does not import files, annotate variants or refresh indices.

## Resource planning

There is no hard-coded sample count in ingestion. CPU and RAM options control
single-server processing. The current aggregate exporter collects variant rows
into driver memory, whereas the carrier exporter streams entries. Neither a
small run nor increasing `--n-cores` establishes arbitrary-scale capacity.
Before a larger run, measure storage/scratch use, dense-MT memory, annotation time,
export memory and Elasticsearch size. Streaming aggregate export and larger-cohort
benchmarks are development work, not completed performance guarantees.
