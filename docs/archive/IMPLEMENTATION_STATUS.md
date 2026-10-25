# Cohort browser implementation status

Reviewed 2026-10-25. This is the current repository assessment of the 34 historical
Linear issue records. It replaces treating all 34 records, the exported Backlog
labels, or the old 118-point/four-week plan as a current estimate of unfinished work.
Issue rows overlap and differ in scope; their count is not an effort estimate.

## Confirmed release scope

- The first release is GRCh37-only (hg19 coordinates). GRCh38 processing is outside v1.
- Deployment is internal-only. Public deployment, public export, a second public VM,
  one-way public transfer, and public low-count policies are deferred.
- All authenticated internal users may see sample IDs. Sample/carrier/HPO/report
  workflows must expose the sample identifiers needed by those users.
- Internal authentication and service isolation remain required. These decisions
  do not establish that the production controls are already implemented.
- Repeated sequencing under one sample ID is still an open data-model question.
- The supplied prenatal/report tokens are preserved in
  [SAMPLE_METADATA_SOURCE.md](../SAMPLE_METADATA_SOURCE.md); the prepared pilot preserves raw strings and nullable report membership.
- The user confirmed fetal DOB `prenatal` and the `whole exome` value when no
  separate panels were ordered. Actual TSV/GVCF paths and known input gaps are in
  [INPUT_DATA.md](../INPUT_DATA.md).

These scope decisions were confirmed by the user on 2026-10-25. Public requirements
in the original exported descriptions are retained as historical context only.

## Evidence and status meanings

The inventory below assesses main at `d776a0662c90829885461799cfe7c830bcb358c2`.
Implemented means the described code exists in main; it does not prove deployment,
complete clinical acceptance, or the absence of remaining defects.

The separate [review branch](https://github.com/markus-marandi/clingen-cohort-vds-browser/tree/fix/sync-validated-browser-overlay)
contains `5674d74` (overlay reconciliation) and
[`47cab9f`](https://github.com/markus-marandi/clingen-cohort-vds-browser/commit/47cab9fd390881407ffde1c08a55d64cd41f12fd)
(runtime gene/region/export fixes). Those changes are pushed but have not been merged
into main. The sample pilot branch described below includes both review commits. Reported live browser checks there are not independent
production verification. The later runtime changes did not receive another full
Jest run according to the handoff.

- **Implemented in main:** the original implementation task exists in the source.
- **Partial:** an implemented subset exists, with explicit acceptance or workflow gaps.
- **Design only:** a documented design exists without its complete implementation.
- **Remaining / Decision pending:** code or a specified decision is still needed.
- **Rescoped:** keep the internal/GRCh37 part; remove the excluded release requirement.
- **Deferred outside v1:** excluded public-release work, not completed work.
- **Needs host verification / Needs validation:** insufficient evidence to claim completion.

The issue files preserve the original exported status and description separately
from their current assessment. No live Linear records were changed.

## Reviewed issue inventory

| Issue | Current assessment | Implemented work or remaining boundary |
|---|---|---|
| [OLI-1](linear-export/OLI/cohort-browser-v1/OLI-1-unblock-container-runtime-on-server-podman-etcmtab.md) | Needs host verification | The original Podman /etc/mtab problem is host-specific. Main includes bootstrap/preflight work (1272d83, 963e66c), and the other machine reports a working Docker test stack. That does not establish that the original Podman host issue is resolved. |
| [OLI-2](linear-export/OLI/cohort-browser-v1/OLI-2-download-dbnsfp-v531-on-vm-and-verify-field-names.md) | Implemented in main | 439c025 records dbNSFP 5.3.1a field verification and end-to-end annotation. Main includes the consolidated GRCh37 file, predictor parsing, and the separate gene-constraint join. Do not count the original download/field-name task as wholly unimplemented; current production-run verification is OLI-34. |
| [OLI-3](linear-export/OLI/cohort-browser-v1/OLI-3-decide-the-v1-predictor-set-against-the-new-schematic.md) | Decision pending | The predictor superset already exists in annotation, export, GraphQL, and the variant page. Agreeing which fields are required for v1 is still a scope decision; this no longer blocks recognizing OLI-24 as implemented. |
| [OLI-4](linear-export/OLI/cohort-browser-v1/OLI-4-rename-hgvs-fields-to-match-schematic-gdnacdnap-nomen-hgvs-ghgvs-chgvs.md) | Remaining | HGVS values exist as gdna, cdna, and p_nomen. The specified HGVS_g/HGVS_c/HGVS_p names or documented compatibility aliases remain to be implemented. |
| [OLI-5](linear-export/OLI/cohort-browser-v1/OLI-5-add-clinvar-condition-clndn-to-annotation-and-export.md) | Remaining | ClinVar significance and review status exist. CLNDN/clinvar_condition is not requested or exported by the current main pipeline. |
| [OLI-6](linear-export/OLI/cohort-browser-v1/OLI-6-rename-clinvar-clnrevstat-to-clinvar-review-status.md) | Remaining | Review status is exposed as clinvar_clnrevstat. The requested clinvar_review_status naming remains. This concerns ClinVar metadata, not the status of a sample clinical report. |
| [OLI-7](linear-export/OLI/cohort-browser-v1/OLI-7-model-the-procedure-entity-test-instrument-ref-genome.md) | Design only | Procedure fields are designed but have no dedicated validated internal export. V1 is GRCh37-only. Whether one sample can have multiple sequencing procedures remains unresolved; do not assume resequencing is excluded. |
| [OLI-8](linear-export/OLI/cohort-browser-v1/OLI-8-drive-reference-genome-from-procedure-instead-of-hardcoding-grch37.md) | Rescoped | The user confirmed a GRCh37-only first release on 2026-10-25. Runtime switching to GRCh38 is not required for v1. Validate input/procedure build compatibility and document accepted GRCh37/hg19 aliases instead. |
| [OLI-9](linear-export/OLI/cohort-browser-v1/OLI-9-align-sample-schema-chromosomal-sex-sex-chr-add-date-seq-and-run-id.md) | Partial | sex_chr, date_seq, and run_id are present in the sample-index design, but there is no validated sample export or sex-inference implementation. The supplied prenatal date token also needs representation; see SAMPLE_METADATA_SOURCE.md. |
| [OLI-10](linear-export/OLI/cohort-browser-v1/OLI-10-resolve-the-in-report-duplication-field-vs-join-table.md) | Decision pending | The supplied specification has one-to-many sample/variant report rows and the input tokens Unknown, None, or a variant ID. No report importer or authoritative flag derivation exists. Preserve Unknown versus None; their exact semantics and storage contract are not explicitly defined by the table. See SAMPLE_METADATA_SOURCE.md. |
| [OLI-11](linear-export/OLI/cohort-browser-v1/OLI-11-clarify-assay-conditional-metadata-panels-wes-hpo-wgs.md) | Decision pending | Clarify whether Panels=WES and HPO=WGS are enforced constraints or descriptions of current data. The internal-only scope does not answer this assay question. |
| [OLI-12](linear-export/OLI/cohort-browser-v1/OLI-12-normalize-date-seq-from-the-test-csv-format.md) | Remaining | Metadata fields pass through as CSV strings. Date parsing, accepted input formats, invalid-date handling, and sequencing-date versus ingestion-date semantics remain. |
| [OLI-13](linear-export/OLI/cohort-browser-v1/OLI-13-support-multiple-panels-per-sample-panel1paneln.md) | Design only | The design has a panels array, but the loader has no one-to-many panel grouping step. The supplied panel list is explicitly expandable, so it must not become a permanently closed enum. The user confirmed that samples without separately ordered panels use whole exome. See INPUT_DATA.md for the TSV paths and completeness checks. |
| [OLI-14](linear-export/OLI/cohort-browser-v1/OLI-14-build-the-hpo-terms-dimension-table-with-versioning.md) | Remaining | HPO assignment/label import exists. A separate lookup preserving release/version and validity date is not implemented. |
| [OLI-15](linear-export/OLI/cohort-browser-v1/OLI-15-stop-denormalizing-hpo-terms-onto-samples-keep-hpo-ids-and-join.md) | Remaining | The implemented HPO helper still copies labels onto sample columns. Keeping only IDs and resolving labels against a versioned lookup depends on OLI-14. |
| [OLI-16](linear-export/OLI/cohort-browser-v1/OLI-16-add-a-metadata-validation-command.md) | Remaining | There is no dedicated metadata validation command. Required fields, sample relationships, dates/prenatal tokens, vocabularies, repeated panels, HPO IDs, and report tokens still need validation. |
| [OLI-17](linear-export/OLI/cohort-browser-v1/OLI-17-build-the-cohort-samples-index-and-export.md) | Design only | cohort_samples has a documented mapping, but the exporter still reads variant rows only. No sample-column export or sample index creation path exists in main. |
| [OLI-18](linear-export/OLI/cohort-browser-v1/OLI-18-build-the-cohort-sample-variants-index-and-export.md) | Design only | cohort_sample_variants has a documented mapping, including var_pct and in_report. Non-reference sample/variant export and report-source integration are not implemented. |
| [OLI-19](linear-export/OLI/cohort-browser-v1/OLI-19-add-internal-and-public-export-profiles-with-a-fail-closed-allowlist.md) | Deferred outside v1 | Public deployment is outside the user-confirmed v1 scope. Public export profiles and a public-release allowlist are deferred, not counted as current release blockers. Internal sample exports remain active under OLI-17/18. |
| [OLI-20](linear-export/OLI/cohort-browser-v1/OLI-20-define-production-index-names-for-internal-and-public-databases.md) | Rescoped | Choose/document names for the internal aggregate, sample, and carrier indices. Names distinguishing public and internal production tiers are not required for the internal-only release. |
| [OLI-21](linear-export/OLI/cohort-browser-v1/OLI-21-add-a-gene-symbol-term-query-to-graphql.md) | Implemented in main | ce2823a adds direct gene_symbol term queries and wires gene-symbol and multi-gene filtering into cohort GraphQL. This does not implement sample carrier lists; those remain OLI-23/26. |
| [OLI-22](linear-export/OLI/cohort-browser-v1/OLI-22-implement-combined-filter-queries-for-uc-4-uc-5a-uc-5b-uc-6a-uc-6b.md) | Implemented in main | ce2823a implements cohort_filtered_variants with gene(s), ClinVar significance, consequence, impact, REVEL threshold, and chrom/position filters, plus mean/max AF summaries. Consequence matching, threshold boundaries, and pagination still need acceptance checks; the query itself is not missing. |
| [OLI-23](linear-export/OLI/cohort-browser-v1/OLI-23-implement-internal-carrier-resolvers-behind-a-deployment-mode-gate.md) | Rescoped | The carrier/HPO resolvers remain unimplemented and required. All authenticated internal users may see sample IDs. A separate public schema/deployment-mode gate is not a v1 acceptance requirement while public deployment is excluded. |
| [OLI-24](linear-export/OLI/cohort-browser-v1/OLI-24-expose-the-annotation-fields-through-graphql.md) | Implemented in main | d776a06 exposes the existing VEP, predictor, ClinVar review-status, and population/constraint fields through GraphQL and the variant page. Missing condition and requested field renames remain OLI-4/5/6, not a reason to call the entire API field set absent. |
| [OLI-25](linear-export/OLI/cohort-browser-v1/OLI-25-add-clinvar-significance-consequenceimpact-and-revel-filters-to-the-ui.md) | Partial | CohortFilterPage already provides ClinVar, consequence, impact, REVEL, gene, and region controls. The original requirement to place controls directly on gene/region pages is not implemented; confirm whether the dedicated page meets v1 acceptance. |
| [OLI-26](linear-export/OLI/cohort-browser-v1/OLI-26-add-carrier-list-views-for-uc-1-uc-2-uc-3-internal-build-only.md) | Remaining | Carrier/sample views for variant, gene, and HPO lookups do not exist. All authenticated internal users may see sample IDs. A public build is outside v1 scope. |
| [OLI-27](linear-export/OLI/cohort-browser-v1/OLI-27-decide-and-implement-gene-level-frequency-summaries.md) | Partial | Multi-gene queries already return per-gene variant count and mean/max variant AF. Distinct-sample carrier frequency is a separate calculation whose numerator/denominator and UI acceptance remain to be specified. |
| [OLI-28](linear-export/OLI/cohort-browser-v1/OLI-28-provision-the-two-vm-production-split.md) | Deferred outside v1 | The user excludes public deployment from v1. Provisioning a second public VM and enforcing the two-VM public split are deferred. Internal deployment readiness remains active work. |
| [OLI-29](linear-export/OLI/cohort-browser-v1/OLI-29-define-the-one-way-public-transfer-process-and-release-checklist.md) | Deferred outside v1 | One-way public transfer and its release checklist are deferred because no public deployment is in scope. |
| [OLI-30](linear-export/OLI/cohort-browser-v1/OLI-30-decide-the-privacy-threshold-for-low-aggregate-counts.md) | Deferred outside v1 | A public low-count suppression policy is outside v1. All authenticated internal users may see sample IDs; do not add a count-based gate that conceals required internal carrier lists. |
| [OLI-31](linear-export/OLI/cohort-browser-v1/OLI-31-document-resource-sizing-for-2000-weswgs-samples.md) | Needs validation | Measure compute, storage, export behavior, and Elasticsearch requirements against the intended cohort and carrier index. Existing sizing prose and a small test run are not a production-capacity result. |
| [OLI-32](linear-export/OLI/cohort-browser-v1/OLI-32-define-the-refresh-policy-for-new-samples-and-metadata.md) | Remaining | Define internal sample additions, metadata/report corrections, reannotation, index refresh, and cache invalidation. Public-tier refresh is excluded from this release. |
| [OLI-33](linear-export/OLI/cohort-browser-v1/OLI-33-end-to-end-smoke-test-on-both-vms.md) | Partial | The reviewed branch 47cab9f has reported isolated live checks for aggregate variant, gene, region, filter, and CSV paths. Main has not incorporated that branch. Carrier/HPO/report end-to-end checks remain, and the original second-public-VM requirement is deferred. |
| [OLI-34](linear-export/OLI/cohort-browser-v1/OLI-34-full-production-run-and-output-validation.md) | Needs validation | A full target-cohort run with internal aggregate/sample/carrier output reconciliation remains. Public export validation is excluded from v1. Small isolated browser checks do not complete this task. |

## Immediate review follow-ups

These are findings on the unmerged review branch, not newly created Linear issues:

1. ClinVar resolvers must not convert arbitrary backend failures into empty results.
   Limit optional-data fallback to the expected missing-data condition and retain
   visibility of real failures.
2. The gene-reference importer must inspect Elasticsearch bulk-item failures and
   verify/report what was actually indexed before claiming success.
3. Setup must stop before applying an overlay to an incompatible existing upstream
   checkout and must not overwrite unrelated local work.

The runtime commit also includes an Assisted-by footer contrary to the user's commit
instructions. That existing history has not been rewritten; new commits must omit it. These follow-ups
remain outside the sample-pilot change. Use a fresh pinned checkout as the handoff specifies.

## Prepared internal sample pilot

Branch `feat/internal-sample-browser` builds on `47cab9f`. It adds a standalone
TSV + existing-MT exporter, configurable sample/carrier indices, paginated GraphQL
queries, variant/gene carrier tables, and sample list/detail pages. Synthetic
export/API/UI tests pass locally. The Hail adapter, full upstream build/Jest, and
real-data browser integration still require the internal server; this is not a deployed result.

This implements a pilot subset of OLI-7/9/10/13/17/18/23/26. It deliberately keeps
DOB/date strings raw (including `prenatal`), groups supplied panel rows, separates
report `None` from `Unknown`, and counts/skips the malformed report relationship.
Missing calls are not inferred from AF. One procedure per sample is a pilot
limitation, not a resolution of the longer-term resequencing model.

The user requested the shortest path to working internal functionality on
2026-10-25. HPO, QC, sex inference, date normalization, full-6705 manifest
reconciliation, and public deployment are deferred from this pilot; they must not
become prerequisites for testing its carrier/sample workflow. The historical issue
inventory above still describes main and must not be read as the pilot branch's
remaining task list.

Follow [the the internal server implementation handoff](DRAGON_SAMPLE_PILOT_HANDOFF.md) for the
bounded final steps. No patient files or genomic datasets are included in this branch.

## Next milestones

1. On the internal server, run the prepared export against the existing annotated MT and the
   four n20 TSVs, writing fresh indices only on the isolated test Elasticsearch.
2. Bind the isolated API to those indices and check the real carrier → sample →
   variant workflow, alongside existing aggregate gene/region/filter/export paths.
3. Review final test/build results and remaining limitations before deciding on
   internal deployment. Deferred requirements remain separate follow-up work.

The remaining work is a mixture of implementation integration and validation.
This inventory is not a revised promise of a delivery date or a summed point estimate.
