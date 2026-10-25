# Cohort VDS pipeline

Turn individual whole-exome or whole-genome GVCFs into a searchable cohort database.
The pipeline combines genotypes, adds variant annotations and cohort frequencies,
links sample metadata, and serves the results through a customised gnomAD browser.
A terminal dashboard manages the main processing stages without writing code.

The current implementation uses **GRCh37/hg19** and is intended for authenticated
internal users. New samples can be added to an existing cohort.

## How it works

```text
GVCFs → preprocessing → Hail VDS → annotation → MatrixTable
                                                  ↓
Sample metadata, panels and reports → export → Elasticsearch → browser
```

| Stage | What happens | Main output |
|---|---|---|
| **Prepare** | Standardise chromosome names, retain supported chromosomes and index GVCFs. | Processed GVCFs and indexes |
| **Combine** | Merge new samples with the existing cohort; record each run in a manifest. | Versioned Hail VariantDataset (`.vds`) |
| **Annotate** | Add gene/consequence/HGVS information, predictor scores, ClinVar, gnomAD and cohort allele counts/frequencies. | Annotated MatrixTable (`.mt`) |
| **Export** | Write one aggregate document per ALT allele, plus separate sample and carrier records. | Elasticsearch indices |
| **Explore** | Search variants, genes and regions; inspect carriers, sample metadata, panels and reported variants. | Browser tables and variant CSV downloads |

A **VDS** stores the combined genotypes. A **MatrixTable** connects variant rows,
sample columns and individual genotype calls with their annotations. Elasticsearch
holds the records used for fast browser searches.

## Requirements

- A Linux analysis server with storage for input files, reference databases,
  intermediate files and search indices. CPU cores and memory are configurable.
- Indexed GRCh37 GVCFs with sample IDs matching the supplied metadata.
- For annotation: a configured VEP installation/cache and reference FASTA, dbNSFP,
  ClinVar and a gnomAD Hail Table. See [annotation sources](docs/ANNOTATION_SOURCES.md).
- For browsing: Docker, the configured browser/API installation, Elasticsearch
  and Redis. [Operations](docs/OPERATIONS.md) describes the existing installation.

## Set up the processing environments

From the project directory, install micromamba and create the two environments:

```sh
scripts/setup_env.sh install-conda
export PATH="$HOME/.local/bin:$PATH"
scripts/setup_env.sh conda
scripts/setup_env.sh verify
```

This installs Hail, Python, Java, bcftools and tabix, plus a separate environment
for the terminal dashboard. Annotation tools, reference databases and their local
paths must be configured separately. A container alternative and command-line
examples are in [the pipeline guide](docs/PIPELINE.md).

## Run the terminal dashboard

Launch the TUI (terminal user interface) on the analysis server:

```sh
CLINGEN_TUI_PROJECT="$PWD" \
CLINGEN_TUI_PYTHON="$(micromamba run -n clingen-tui which python)" \
CLINGEN_PIPELINE_PYTHON="$(micromamba run -n clingen-pipeline which python)" \
scripts/clingen-tui
```

The dashboard shows processing stages, live logs, progress and run history.
Work through these tabs in order, checking paths and settings before **Launch**:

1. **Ingest:** select the GVCF and output directories; set CPU cores and memory.
2. **Annotate:** select the resulting VDS, reference configuration and output MT.
3. **Export:** select the annotated MT, Elasticsearch address and aggregate index.

Each stage is launched separately. Press **b** for the dashboard, **1–3** for a
stage, **Ctrl+C** to terminate active processing, or **q** to quit. Settings are
saved locally in `.tui_config.json` and excluded from Git.

## Add sample metadata and carrier records

Sample-level browsing uses a separate export from the same annotated MT and four
TSVs: sample data, procedures, panels and reported variants. Run it in the pipeline
environment, replacing the example paths and index names with your configuration:

```sh
micromamba run -n clingen-pipeline python browser/data-pipeline/cohort_sample_export.py \
  --mt-path /path/to/cohort_annotated.mt \
  --sample-data /path/to/sample_data.tsv \
  --procedure /path/to/procedure.tsv \
  --panels /path/to/panels.tsv \
  --in-report /path/to/in_report.tsv \
  --es-url http://127.0.0.1:9200 \
  --samples-index cohort_samples_v1 \
  --carriers-index cohort_sample_variants_v1
```

Use fresh index names. The API must select the matching aggregate, sample and
carrier indices. This step reads existing genotype calls and does not rerun VEP.
See [input formats](docs/INPUT_DATA.md) and [configuration](docs/OPERATIONS.md).

## Start the browser

On the configured server, from the project directory:

```sh
scripts/browser.sh doctor
scripts/browser.sh start
scripts/browser.sh status
```

These commands start the existing installation and report its local URLs and
index counts. They do not run the data pipeline. Open the internal browser address
provided by your administrator and select **Cohort**. Deployment addresses and
login details belong in private configuration, not in the repository.

## Search the cohort

| Question | Browser workflow |
|---|---|
| Which samples carry a variant? | Search `variant_id` → open the variant → **Carrier samples** → sample details. |
| Which samples carry variants in a gene? | Search `gene_symbol` → gene page → **Carrier samples**. Samples may have several variant rows. |
| Which samples have a particular phenotype? | HPO ID/term search is planned; see **Next development** below. |
| Which ClinVar pathogenic variants occur in BRCA1? | **Filters** → gene `BRCA1` + ClinVar significance. Search Pathogenic and Likely pathogenic separately as required. |
| How frequent are variants in several genes? | **Filters** → `APOB, LDLR, LDLRAP1, PCSK9` + ClinVar significance or REVEL threshold → variant AF and per-gene summary. |
| Which missense/LoF candidates occur in a gene or region? | **Filters** → consequence + gene, or impact + chromosome/start/stop. |

**AF** is allele frequency (AC / AN), not the percentage of samples carrying a
variant. The REVEL control uses `≥`, and region input uses GRCh37 coordinates
rather than HGVS interval text. Exact search behaviour and annotation limits are
explained in [the workflow guide](docs/WORKFLOWS.md).

## Growing the cohort

Ingestion identifies new GVCFs through its run manifest and combines them with the
previous completed VDS into a new version. Refresh annotation and exports before
serving the updated cohort. The sample count is not fixed in the pipeline;
practical capacity depends on compute, memory, storage and variant count. The
current scripts use local Spark, so larger cohorts require resource planning
and capacity testing.

## FAQ

### How do I add one or several new GVCFs?

1. Add the new indexed GVCFs and their metadata. Each new sample needs a unique
   sample ID and filename.
2. In the TUI, run **Ingest** using the existing cohort manifest and directories.
   It identifies new filenames and combines them with the last completed VDS.
3. Run **Annotate** on the new VDS, writing a new MT. Export aggregate variants
   and sample/carrier records for the **entire updated cohort** into fresh indices.
4. Have the operator select the new index set in the API and verify sample counts,
   frequencies and links. Keep the previous version for rollback.

One file and a batch follow the same flow. The browser updates after the new
indices are selected, not when files are copied into the input directory.

### How do I update an existing patient/sample?

- **Metadata, panels or report changes:** edit the source TSVs, keep the sample ID,
  and repeat the full sample/carrier export into fresh indices using the existing
  MT. No GVCF ingestion or VEP rerun is needed. The aggregate index stays unchanged.
- **Corrected or replacement GVCF:** there is no in-place replacement command yet.
  Build a new cohort version from the complete current GVCF set, including the
  replacement and excluding the old file. Use fresh preprocessing, VDS, scratch
  and manifest paths, then annotate and export again before switching the API.

Do not overwrite an ingested file or rename it to force an update: tracking is
filename-based, and another filename with the same sample ID is not a replacement.
See [the update procedure](docs/PIPELINE.md#adding-and-updating-samples) for details.

## Documentation and support

- [Pipeline guide](docs/PIPELINE.md): inputs, outputs, direct commands and configuration.
- [Operations](docs/OPERATIONS.md): browser startup, logs and recovery.
- [Search workflows](docs/WORKFLOWS.md) and [documentation index](docs/README.md).
- Report problems through [GitHub Issues](https://github.com/markus-marandi/clingen-cohort-vds-browser/issues), without sample identifiers or patient data.

## Next development

- **HPO search:** sample-to-HPO import, versioned term lookup and sample search by ID or label.
- **Allele-specific annotations:** retain annotations for each ALT so multi-ALT variants participate fully in gene and annotation searches.
- **QC and sample metadata:** genotype/sample quality checks, inferred chromosomal sex and explicit sample-replacement/resequencing workflows.
- **Search completeness:** compound consequences, strict thresholds, larger result sets and distinct-sample carrier frequencies.
- **Scale and refresh:** memory-bounded aggregate export, large-cohort benchmarks and reliable metadata/reference-data updates.
- **Deployment and usability:** portable configuration, broader TUI coverage, browser interaction checks and operational recovery verification.
