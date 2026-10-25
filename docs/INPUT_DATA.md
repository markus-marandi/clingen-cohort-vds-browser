# Cohort input data on dragon

Locations and filenames supplied by the user on 2026-10-25. These are remote
paths on `dragon`, not paths verified from the local Mac. No genomic or patient
data was copied into the repository.

## Metadata

Directory: `/srv/Helix/NovaseqX_WES/GE_db`

| File | Supplied evidence | Remaining inspection |
|---|---|---|
| `test_hg19_in_report_n20.tsv` | Header `sample_id`, `variant_id`; entries include `None`, `Unknown`, and variant IDs | Validate IDs, relationships, and report-state consistency on the unredacted source |
| `test_hg19_panels_n20.tsv` | Header `sample_id`, `panel`; categorical panel names | Confirm completeness and one-to-many grouping |
| `test_hg19_procedure_n20.tsv` | Header `sample_id`, `test`, `instrument`, `ref_genome` | Validate sample relationships, values, and procedure cardinality |
| `test_hg19_sample_data_n20.tsv` | Eight-column sample header supplied below | Validate required values, date/prenatal domains, and relationships |
| `WES_GE_hg19_panels_n6705.tsv` | Filename supplied | Validate against the full sample set |
| `WES_GE_hg19_procedure_n6705.tsv` | Rows shown with `WES`, `NovaSeq X Plus`, `hg19` | Inspect the actual header and cardinality; the pasted beginning is incomplete |
| `WES_GE_hg19_sample_data_n6705.tsv` | Same eight-column sample header as the test file | Validate values and sample relationships |
| `WES_GE_hg19_samples_n6705.tsv` | Header `sample_id`, `date_seq`, `run_id` | Sequencing manifest; reconcile its dates/run IDs with sample data and its IDs with GVCF headers |

Headers supplied by the user from `head -n 1` on dragon:

```text
test_hg19_sample_data_n20.tsv
WES_GE_hg19_sample_data_n6705.tsv
  sample_id, sex_assigned, date_of_birth, date_seq, run_id, care_site, material, health_status

test_hg19_procedure_n20.tsv
  sample_id, test, instrument, ref_genome

WES_GE_hg19_samples_n6705.tsv
  sample_id, date_seq, run_id
```

The comma-separated header rendering above is for readability; the source files
are TSVs. The test/full sample-data schemas match. The `samples` file has no GVCF
path column; it is a sequencing manifest rather than an input-file path list.

`date_seq` and `run_id` occur in both the full sample-data and manifest files.
Check agreement for the matching sample/run; do not silently choose a conflicting
value. Procedure data is a separate relation and needs an explicit join. Its test
header has no run ID, so the header alone does not establish how to link multiple
procedures for the same sample. Validate cardinality before assuming a one-to-one
sample join.

`sex_chr` is absent from the source sample header. The intended inferred-sex
field requires the separate genotype-based calculation described in the
specification; it must not be fabricated from `sex_assigned`.

The numbers in filenames are labels, not independently verified row or unique
sample counts. All example sample IDs were replaced by the user with
`E00000000`; apparent duplication in the pasted text is not evidence of duplicate
source identifiers.

Only the n20 report file was listed. A full-cohort report source was not shown.
This does not establish that one is absent on the machine; identify its location
if full-cohort report browsing is required. Missing report information must not
silently become `None`/a negative report status.

## Genomic inputs

Current GVCF and TBI directory supplied by the user:

`/srv/Helix/NovaseqX_WES/andmebaas_test_valim`

Before ingestion, verify file/index pairing, the sample ID in each GVCF header,
the relationship to metadata IDs, and the expected selected cohort. File counts
and header-to-metadata identity matching have not been checked locally.

## Confirmed source rules

- First release: GRCh37/hg19 only, internal deployment only.
- All authenticated internal users may see sample IDs.
- Fetal DOB uses the literal `prenatal`.
- Samples without separately ordered panels use the literal `whole exome`.
- Report input distinguishes `Unknown`, `None`, and actual variant IDs.
- TSV is the supplied tabular format. Current optional metadata/HPO loaders use
  a comma delimiter; these files need a defined TSV import path.

See [SAMPLE_METADATA_SOURCE.md](SAMPLE_METADATA_SOURCE.md) for exact specification
wording and the remaining distinction between input tokens and storage design.

## Validation finding in the supplied excerpt

`17-7807252--G-T` does not match the specified four-part
`chrom-pos-ref-alt` representation: it has an extra separator/empty component.
Check it against the original report and called variant. A validator should flag
it for correction at source, not silently remove a separator or guess an allele.

## Remaining input questions

- Required/missing-value rules and observed field domains; sample-data, test
  procedure, and sequencing-manifest headers are now known. The full procedure
  header has not been shown, so matching the test header is not yet verified.
- Report completeness and conflicting states within the same real sample.
- Whether repeated sequencing reuses a sample ID and how a procedure is identified.
- Source of HPO assignments/versioned labels if HPO searches are required for the
  selected cohort; no HPO file was listed in this input directory.
- The user mentions further edits in Google Docs. No Google Docs URL or new export
  was supplied in this message, so unseen edits are not treated as reviewed requirements.

## Current pilot decision

The subsequent remote preflight reported 20 matching metadata/GVCF samples, one
procedure per sample, 27 panel assignments, report counts None=14 / Unknown=2 /
variant-bearing=4, and one malformed report relationship. These are remote-agent
reports, not a repeated local inspection. The user then explicitly deferred edge
case cleanup: preserve dates/material/panel strings, skip/count the malformed
report relationship, and do not require the full manifest to admit this pilot.
Use [the prepared handoff](DRAGON_SAMPLE_PILOT_HANDOFF.md), not the broader input
questions above, as the current pilot implementation scope.
