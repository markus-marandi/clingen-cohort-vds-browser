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

## Adding and updating samples

The unit stored by this pipeline is a **sample ID**. It has no separate patient
entity or supported model for multiple sequencing procedures under one ID.
Decide whether a newly sequenced specimen is a new sample or a replacement before
running ingestion; do not assign a new ID merely to bypass replacement handling.

### Add one or multiple new samples

1. Place the new indexed GVCFs in the configured raw-input directory. Keep each
   already-ingested filename unchanged, and give new samples unique sample IDs
   and filenames. Add their rows to the metadata/procedure/panel/report sources.
2. Run **Ingest** with the existing cohort's manifest and preprocessing/output
   settings. The manifest identifies completed filenames; their existing processed
   outputs are reused. The combiner merges newly discovered files with the latest
   completed VDS and writes a new version.
3. Select that completed VDS in **Annotate** and write a fresh MT. The current
   annotation script processes the updated cohort and recalculates cohort counts;
   it does not implement annotation of only the newly added samples.
4. Export aggregate variants from the new MT to a fresh aggregate index. Export
   sample/carrier records from that same MT, using metadata for **all samples
   intended to remain browsable**, to a fresh sample/carrier index pair. Exporting
   metadata only for the new samples would produce a browser index missing the
   previous samples.
5. Check the expected sample total, document counts and several old/new sample
   links. Have the operator update the API to select all three matching indices,
   update the lifecycle helper's accepted-count settings and verify the release.

An incomplete or failed export is not ready to serve. Leave the old VDS, MT and
accepted indices in place until the new set has been checked.

### Update metadata, panels or reported variants

Edit the authoritative TSV rows for the same sample ID. Re-run the full
sample/carrier exporter with the **existing annotated MT**, using fresh index
names and the complete selected metadata set. Switch the API's sample and carrier
index settings together; the aggregate index can remain unchanged.

The exporter stores report membership on carrier records as well as report
information on samples, so a report correction needs both exports refreshed.
Re-exporting also removes obsolete panel/report assignments from the newly built
set. The source MT itself is not modified by this browser-metadata refresh.
If its optional metadata column annotations are used elsewhere, update those
separately; they do not synchronize automatically with the browser TSVs.

Do not use `--allow-existing` as a complete replacement operation. It upserts
new documents but does not remove stale ones. No ingest or VEP rerun is required
for browser-metadata-only changes.

### Replace a sample's GVCF

There is currently no supported single-sample replace/remove command. Ingestion
tracks filenames, not file-content checksums or sample-version identities:

- A changed file with an already-completed filename is skipped by the manifest;
  its old processed GVCF can also be reused from the preprocessing directory.
- A different filename does not remove the old sample from the existing VDS.
  It must not be used to force a replacement under the same sample ID.

The supported approach with the current scripts is a clean cohort rebuild:

1. Prepare a separate complete input set with exactly one current GVCF per
   sample, using the replacement instead of its old version. Preserve the
   original input set and its indexes.
2. In **Ingest**, choose new filtered-output, VDS-output, scratch and manifest
   paths. A new manifest starts a new cohort rather than appending to the old
   VDS. Do not delete or edit the original manifest to trick the existing run.
3. Run annotation on the rebuilt VDS into a new MT, then export a fresh aggregate
   index and complete sample/carrier pair. Recalculate cohort frequencies;
   changing only the sample's carrier records would leave aggregate counts wrong.
4. Confirm that the affected sample appears once, has the expected replacement
   calls, and that retained samples and cohort totals are consistent. Switch the
   API only after those checks, keeping the earlier version for rollback.

This rebuild can be expensive. Targeted sample replacement and incremental
annotation are future improvements, not available TUI actions.

### Make a refreshed dataset visible

The operator must update the API container/process configuration for the accepted
index names and recreate/restart it as required by the deployment. Running
`scripts/browser.sh start` alone does **not** change the environment of an already
created container. Keep the browser on the same API endpoint unless its deployment
also changes. Refresh any deployment data caches through the existing scoped
process; do not flush shared Redis indiscriminately.

Normal startup does not watch input directories, schedule processing or refresh
indices. Adding files and publishing a refreshed cohort are separate actions.

## Resource planning

There is no hard-coded sample count in ingestion. CPU and RAM options control
single-server processing. The current aggregate exporter collects variant rows
into driver memory, whereas the carrier exporter streams entries. Neither a
small run nor increasing `--n-cores` establishes arbitrary-scale capacity.
Before a larger run, measure storage/scratch use, dense-MT memory, annotation time,
export memory and Elasticsearch size. Streaming aggregate export and larger-cohort
benchmarks are development work, not completed performance guarantees.
