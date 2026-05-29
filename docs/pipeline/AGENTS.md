# docs/pipeline/AGENTS.md

Guidance for agents working on Hail, VDS, MatrixTable, metadata, and local demo tasks.

## Pipeline Shape

VDS is the storage layer. MatrixTable is the annotation and export layer.

```text
raw GVCFs -> filtered GVCFs -> VDS -> dense MatrixTable -> annotated MatrixTable
```

Production exports split into two database tiers:

- Internal patient-linked outputs stay on the internal VM.
- Public/sanitized variant-only outputs go to a separate public/browser VM through an explicit
  allowlisted export.

Main scripts:

- `parallel_ingest_cohort.py`: GVCF filtering and incremental VDS combine.
- `ingest_manifest.py`: run tracking and resumability.
- `annotate_cohort.py`: VDS to annotated MatrixTable.
- `sanity_check_mt.py`: manual inspection script with environment-specific paths.

## Local Demo Target

Build toward a tiny local path that can run before server deployment:

1. Load sample and panel metadata.
2. Validate sample IDs, required columns, date formats, and panel multiplicity.
3. Produce a tiny Hail table, MatrixTable, or VDS fixture from non-sensitive data.
4. Run `variant_qc()` and verify cohort AF fields.
5. Export to local Elasticsearch.
6. Smoke test a known variant or gene in the browser.

## Metadata Rules

- Join by `sample_id`.
- Keep social self-defined sex and chromosomal sex separate.
- Treat multiple panel rows per sample as valid.
- Do not collapse panels into comma-separated strings unless an export boundary requires it.
- Keep HPO modeling separate until its cardinality and versioning are confirmed.
- Public exports must not include patient IDs, `sample_id`, column metadata, per-sample genotypes,
  report flags, dates, HPO assignments, run IDs, care sites, or internal paths.

## Checks

- Use the smallest fixture that exercises the changed behavior.
- Do not run production-scale Hail jobs for routine edits.
- Keep absolute server paths configurable through flags, not hardcoded into new code.
- Do not commit VCF/GVCF, VDS, MatrixTable, Spark temp data, or Hail logs.

## VDS and Binary Data Rules

- Do NOT read the contents of `.vds` or `.mt` directories. These are binary Hail data stores and
  cannot be meaningfully read as text.
- You MAY read: Hail log files (`hail-*.log`), `ls`/`find`/`du` output, and schema metadata printed
  by `hl.describe()` or `print(mt.describe())` in a script.
- If a pipeline step fails, read the Hail log file for the error — not the VDS or MT itself.
- Do not pass VDS or MT directory paths to text-reading tools. This produces garbage output.
