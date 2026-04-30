# browser/AGENTS.md

Guidance for agents working on the gnomAD browser patch layer.

## Source Of Truth

Edit files under `browser/`. Do not edit `gnomad-browser/` as durable source; it is cloned and
patched by root `setup.sh`.

Patch targets:

- `browser/docker-compose.yml`
- `browser/data-pipeline/cohort_export.py`
- `browser/graphql-api/src/datasets.ts`
- `browser/graphql-api/src/queries/variant-queries.ts`
- `browser/graphql-api/src/queries/variant-datasets/cohort-variant-queries.ts`

## Local Stack

After running root `./setup.sh`:

```bash
cd gnomad-browser
docker compose up --build
```

Services:

- Elasticsearch: `http://localhost:9200`
- GraphQL API: `http://localhost:8000`
- Browser UI: `http://localhost:3000`

## Data Flow

`browser/data-pipeline/cohort_export.py` creates or updates Elasticsearch index
`cohort_variants`. The GraphQL API routes dataset `cohort` to
`cohort-variant-queries.ts`, which reads that flat index.

## Checks

- For export changes, verify the index mapping and a small bulk export against local Elasticsearch.
- For GraphQL query changes, test variant ID, rsID, region, gene, transcript, and autocomplete
  paths when possible.
- For Compose changes, run the local stack and confirm the three services become reachable.

## Constraints

- Keep the cohort dataset compatible with GRCh37 unless the pipeline reference changes.
- Keep Elasticsearch field names aligned with `cohort_export.py`.
- Do not add internal URLs, credentials, or private sample data to browser docs or source.
