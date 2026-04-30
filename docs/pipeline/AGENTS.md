# docs/pipeline/AGENTS.md

Guidance for agents working on Hail, VDS, MatrixTable, metadata, and local demo tasks.

## Pipeline Shape

VDS is the storage layer. MatrixTable is the annotation and export layer.

```text
raw GVCFs -> filtered GVCFs -> VDS -> dense MatrixTable -> annotated MatrixTable
```

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

## Checks

- Use the smallest fixture that exercises the changed behavior.
- Do not run production-scale Hail jobs for routine edits.
- Keep absolute server paths configurable through flags, not hardcoded into new code.
- Do not commit VCF/GVCF, VDS, MatrixTable, Spark temp data, or Hail logs.
