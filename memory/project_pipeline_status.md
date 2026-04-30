---
name: Pipeline status — VDS ingest verified
description: Hail VDS ingest pipeline has been tested and confirmed working on the production server as of 2026-04-30
type: project
---

Hail VDS ingest pipeline (`parallel_ingest_cohort.py` + `annotate_cohort.py`) is tested and confirmed working on the server as of 2026-04-30.

**Why:** User ran a real cohort ingest on their server and verified output. This is no longer a speculative pipeline.

**How to apply:** The next phase of work is downstream of VDS — specifically getting a working annotated MatrixTable → Elasticsearch → browser path, and then packaging the whole stack for reuse.
