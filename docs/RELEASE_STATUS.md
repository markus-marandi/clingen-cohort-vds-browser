# Internal pilot handover status

Source reviewed on 2026-10-25: `feat/internal-sample-browser` at
`94123158973da7844bfb59a8f8b2f3ce3514dc71`.
The handover branch `docs/internal-browser-handover` now includes that fix and the
operator documentation cleanup. Main has not been merged or deployed by this work.

## Available

GRCh37/hg19, internal-only, 20 selected samples. Variant/gene/region browsing,
combined filters, gene/region CSV export, variant/gene carrier panels, sample
metadata/panels/report links and paged called variants are implemented.

The per-ALT fix exports matching AC/AF/homozygote counts for each ALT and retains
AN. Aggregate index selection is configurable through `COHORT_VARIANTS_INDEX`.
A local source review confirms that unassignable multi-ALT annotations are nulled
in both the aggregate and carrier exports. The UI displays them as unavailable.

The server agent reports:

- fresh indices: 20 sample documents, 939,314 carrier documents, 172,123 aggregate
  ALT documents; [index names](OPERATIONS.md#data-preparation-and-refresh);
- all 155,346 distinct carrier variant IDs found in the new aggregate index;
  the previous 9,568 missing IDs are resolved;
- the known 0/2 page now loads with AC=5, AN=10, AF=0.5 and 5 carrier links;
- build/typecheck/lint/format and full Jest passing: 81 suites, 1,925 tests,
  1,197 snapshots; live API/component checks reported passing;
- only the isolated pilot API was recreated; the pilot browser hot-reloaded;
  original services, previous indices and source MT/TSVs/GVCFs were preserved.

The handover branch also commits a reproducible pilot lifecycle script and the
exact installation configuration in [OPERATIONS.md](OPERATIONS.md). On the internal server,
`doctor`, `start`, `status` and `logs` were verified against the running
installation: API health 200, browser `/samples` 200, and the accepted index
counts matched. The script starts existing services and creates only a missing pilot
container; it does not create, delete or reindex data.

On the internal server, the source also passes all 7 exporter unit tests, the synthetic
GraphQL/query/UI integration check, typecheck, lint, format, the production
browser build and full Jest (81 suites, 1,925 tests, 1,197 snapshots). The real
Hail export and live API/component checks were run there. No new snapshot files
were introduced by the handover work.

## Limits and remaining handover work

- 7,184 multi-ALT MT rows expand to 28,648 aggregate ALT documents. Their VEP,
  HGVS, gene/consequence, predictors, ClinVar and external population annotations
  cannot be assigned to a specific ALT from the stored flattened row fields.
  They remain unavailable. Gene and annotation-filter searches exclude those
  variants; variant-ID lookup and sample call browsing still expose their calls.
- HPO-to-sample search is not implemented. Strict REVEL `>0.9`, compound
  consequence matching and filter result limits are explained in [WORKFLOWS.md](WORKFLOWS.md).
- Actual browser Back/Forward, a full sample → multi-ALT variant → sample
  click-through, and the full GenePage interactive carrier panel remain
  unverified. The carrier API/component and variant/sample pages were checked live
  against the isolated API, not through a real browser session.
- The committed startup script and exact images, mounts, network, Redis dependency,
  ports and accepted index settings are in [OPERATIONS.md](OPERATIONS.md). The
  script's `doctor`, `start`, `status` and `logs` commands were verified on
  the internal server. It does not create, delete or reindex data.

QC/inferred sex, full-cohort reconciliation, annotation expansion and public
deployment are outside this pilot's current work. Historical tasks and estimates
are under [archive/](archive/README.md); they do not measure the work remaining.
