# Prenatal dates and report values in the supplied specification

Source: `Ustina VDS browser specs-2.docx`, supplied for review on 2026-10-25.
The local source file is `/Users/markus/Downloads/Ustina VDS browser specs-2.docx`.
This note transcribes the relevant requirements; it does not define a new storage schema.

On 2026-10-25 the user also supplied redacted TSV excerpts and the actual input
locations on `the internal server`; see [INPUT_DATA.md](INPUT_DATA.md). Sample IDs in those
excerpts were deliberately replaced and must not be used to infer duplicates.

## Birth date

The source's Sample data table says:

```text
date_of_birth
Previous: age
renamed — ISO date string; normalize to YYYY-MM-DD. Fetal sample value: prenatal
```

The user confirmed the literal `prenatal` value for fetal DOB on 2026-10-25.
The supplied input distinguishes a calendar date from that token.
The current loader passes supplied CSV columns through without implementing a
date/prenatal conversion. The sample-index design maps `date_of_birth` as a date,
so it does not yet represent the token. A separate prenatal/status field with a
nullable date is a possible design, not an agreed or implemented representation.

## Report entries

The highlighted In report table says:

```text
In report — one-to-many
sample_id
variant_id: Reported genetic variant
Values: Unknown,None, {chrom}-{pos}-{ref}-{alt}
One sample may appear on multiple rows (one per variant reported).
```

The source therefore distinguishes three kinds of input: `Unknown`, `None`, and
an actual variant ID. It does not explicitly define the first two tokens in prose.
Do not silently collapse them into one missing value or `in_report=false`.

The accompanying Word comment proposes a one-to-many variants-in-report table,
uses `negative` for no reported variants, and suggests a variant-level indication
of whether the variant has appeared in any sample's report. The table uses `None`
rather than `negative`; an input contract must reconcile these labels.

A possible interpretation to check against the actual metadata is:

| Input | Proposed interpretation, not yet confirmed |
|---|---|
| `Unknown` | Report information is unknown or unavailable. |
| `None` | Report information is known and contains no reported variants. |
| Variant ID | This variant was reported for the associated sample. |

The supplied `test_hg19_in_report_n20.tsv` excerpt contains both `None` and
`Unknown`, as well as actual variant IDs. These are real source tokens rather
than hypothetical placeholders. The excerpt does not add a definition of report
completeness or amended-report behavior. Preserve the distinctions on import.

`None` here concerns reported variants. It does not establish that the sample has
no genomic variants, is unaffected, or has a negative result for every clinical test.

The current repository has no report importer or sample-report browser workflow.
[OLI-10](archive/linear-export/OLI/cohort-browser-v1/OLI-10-resolve-the-in-report-duplication-field-vs-join-table.md)
tracks the authoritative relationship and flag decision. The Boolean in
[SAMPLE_VARIANT_INDEX.md](SAMPLE_VARIANT_INDEX.md) is design-only and does not
resolve Unknown versus None. Storage, token normalization, and the authoritative
report source remain to be defined. Repeated sequencing/report history is also
still an open data-model question.

## Panels

The user confirmed on 2026-10-25: if no separate panels were ordered for a sample,
the value is `whole exome`. Preserve that literal label. It differs from `Unknown`
and from a panel assignment that failed to join to its sample.

The example panel TSV has columns `sample_id` and `panel`; assignments are
one-to-many, and the supplied panel vocabulary is expandable. Do not infer that
every unmatched sample is `whole exome` unless the assignment source is known to
be complete for that cohort. This distinction must be checked against the full
sample and panel inputs rather than the redacted excerpts.
