# OLI-31: Document resource sizing for 2000 WES/WGS samples

- Status: Backlog
- Priority: High
- Estimate: 3
- Labels: deployment
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-31/document-resource-sizing-for-2000-weswgs-samples

## Description

Hail/VDS compute, MatrixTable storage, cohort_sample_variants document count, and Elasticsearch heap. Current compose sets ES_JAVA_OPTS=-Xms2g -Xmx2g on a single node, which will not hold the production carrier index. Open item in TODO.md > Deployment.

## Comments

None.
