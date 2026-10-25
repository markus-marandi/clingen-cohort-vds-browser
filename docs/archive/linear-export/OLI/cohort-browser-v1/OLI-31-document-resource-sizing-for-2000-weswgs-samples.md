# OLI-31: Document resource sizing for 2000 WES/WGS samples

- Status: Needs validation
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: deployment
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-31/document-resource-sizing-for-2000-weswgs-samples

## Current assessment

Measure compute, storage, export behavior, and Elasticsearch requirements against the intended cohort and carrier index. Existing sizing prose and a small test run are not a production-capacity result.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

Hail/VDS compute, MatrixTable storage, cohort_sample_variants document count, and Elasticsearch heap. Current compose sets ES_JAVA_OPTS=-Xms2g -Xmx2g on a single node, which will not hold the production carrier index. Open item in TODO.md > Deployment.

## Comments

None.
