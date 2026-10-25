# Run the internal pilot

This is the existing 20-sample pilot on the internal server, not a fresh data installation.
The reproducible lifecycle script is [scripts/internal-pilot.sh](../scripts/internal-pilot.sh).
It starts the existing installation; it does not create, delete, or reindex data.

## Quick start

On the internal server, as the user that owns the rootless Docker daemon:

```sh
scripts/internal-pilot.sh doctor
scripts/internal-pilot.sh start
scripts/internal-pilot.sh status
```

`start` starts the existing Elasticsearch and Redis dependencies if stopped, then
starts the pilot API and browser. It creates only a missing pilot API/browser
container from the exact configuration below. It does not create or delete indices.
`status` prints container state, API index selection, health, accepted document
counts and local URLs. `logs` tails both pilot services; logs may contain
patient data, so keep them on the internal server.

```sh
scripts/internal-pilot.sh logs 50
scripts/internal-pilot.sh urls
scripts/internal-pilot.sh stop       # stops only the pilot browser and API
```

## Exact pilot configuration

Verified against the running installation on 2026-10-25.

| Item | Value |
|---|---|
| Parent source | Local project checkout; branch `docs/internal-browser-handover` |
| Generated source | `gnomad-browser/` inside the local project checkout |
| Upstream source | `db245371f1e572106bda8481ef0f9a6baac6c00d` plus the tracked overlay |
| Docker network | `gnomad-browser_default` (existing bridge network) |
| Elasticsearch | `cohort-gene-ref-es` |
| Redis | `cohort-gene-ref-redis` |
| API container | `cohort-sample-pilot-api` |
| Browser container | `cohort-sample-pilot-browser` |
| Elasticsearch image | `docker.elastic.co/elasticsearch/elasticsearch:8.13.4` |
| Redis image | `redis:7-alpine` |
| API image | `gnomad-browser-graphql-api:latest` |
| Browser image | `gnomad-browser-browser:latest` |
| API host port | `127.0.0.1:8002` → container `8001` |
| Browser host port | `127.0.0.1:8010` → container `8008` |
| Elasticsearch host port | `0.0.0.0:9201` → container `9200` |
| Redis host port | none; internal `6379` only |

The original stack on host port **9200** (`gnomad-browser-*`) is a different
installation. Leave it and its indices untouched.

### API environment

```text
NODE_ENV=development
ELASTICSEARCH_URL=http://cohort-gene-ref-es:9200
COHORT_SAMPLES_INDEX=cohort_samples_pilot20_peralt
COHORT_SAMPLE_VARIANTS_INDEX=cohort_sample_variants_pilot20_peralt
COHORT_VARIANTS_INDEX=cohort_variants_pilot20_peralt
REDIS_HOST=cohort-gene-ref-redis
REDIS_PORT=6379
PORT=8001
```

The API selects all three accepted indices. The browser must not be configured
with these index names directly; it reaches them through the API.

### Browser environment

```text
NODE_ENV=development
GNOMAD_API_URL=http://cohort-sample-pilot-api:8001/api
```

### Source mounts

| Container | Host source | Container path | Mode |
|---|---|---|---|
| API | `gnomad-browser/graphql-api/src` | `/app/graphql-api/src` | read-only |
| API | `gnomad-browser/dataset-metadata` | `/app/dataset-metadata` | read-only |
| Browser | `gnomad-browser/browser/src` | `/app/browser/src` | read-write |

The browser source mount is writable because `buildHelp` writes the generated
`src/help/helpTopics.js`. Do not edit source through the running container.

### Commands

API entrypoint:

```sh
pnpm ts-node ./src/app.ts
```

Browser entrypoint:

```sh
pnpm start
```

Start order: Elasticsearch → Redis → API → browser.

### Port and access boundary

- API and browser bind only to `127.0.0.1`.
- Elasticsearch publishes `9201` on all host interfaces. Restrict it with the host
  firewall and do not expose it publicly.
- Redis is not published on a host port; it is reachable only on the Docker network.
  The host `6379` listener belongs to the original stack, not the pilot Redis.
- The API and browser must remain internal-only.

## Accepted index settings

Verified counts on isolated Elasticsearch 9201:

| API environment variable | Index | Documents |
|---|---|---:|
| `COHORT_VARIANTS_INDEX` | `cohort_variants_pilot20_peralt` | 172,123 |
| `COHORT_SAMPLES_INDEX` | `cohort_samples_pilot20_peralt` | 20 |
| `COHORT_SAMPLE_VARIANTS_INDEX` | `cohort_sample_variants_pilot20_peralt` | 939,314 |

`scripts/internal-pilot.sh start` verifies these counts and refuses to continue
if they differ. It never creates, deletes or reindexes them. The older
`cohort_variants`, `cohort_samples_pilot20` and
`cohort_sample_variants_pilot20` indices are preserved but are not selected by
the pilot API.

`cohort_variants` and `genes_grch37` on 9201 and the original 9200 stack are
separate existing services. Do not delete or recreate them for a pilot restart.

## Browser access

Use the internal browser address and connection instructions supplied privately
by your administrator. This repository stores no remote hostname, SSH account,
or network address. The pilot's local URL is `http://127.0.0.1:8010/samples` on
the server itself. `scripts/internal-pilot.sh urls` prints local URLs only.

The existing browser service and the new pilot are separate. Do not assume an
existing browser address serves the new pilot until an operator has verified
its configuration. This script does not replace the existing service or alter
its routing.

Keep machine-specific paths and access instructions in local, untracked
configuration. Logs can contain patient data; keep them on the internal system.

## Container recovery

`scripts/internal-pilot.sh doctor` checks source, images, network, dependencies and
the existing API/browser configuration without changing anything. If an image is
missing, rebuild it from the generated checkout (recovery only, not normal startup):

```sh
docker build -t gnomad-browser-graphql-api:latest \
  -f gnomad-browser/graphql-api/Dockerfile gnomad-browser
docker build -t gnomad-browser-browser:latest \
  -f gnomad-browser/browser/Dockerfile gnomad-browser
```

If the existing Elasticsearch or Redis dependency container is missing, stop and
inspect the installation. Do not create a second unrelated dependency. If the pilot
API/browser container is missing, `start` creates it with the exact configuration
above; if it exists, `start` only starts it.

The tracked `setup.sh`, `browser/docker-compose.yml`, and `scripts/stack.sh` are
legacy development tooling, **not the current the internal server pilot startup commands**.
They target different ports and the original 9200 stack, and can conflict with the
pilot. See [overlay mechanics](BROWSER_OVERLAY.md) and the
[archived implementation handoff](archive/DRAGON_SAMPLE_PILOT_HANDOFF.md) for
fresh pinned-checkout preparation.

## Verification limits

`doctor`, `start`, `status`, `urls` and `logs` were run against the running
installation. `start` took the start-existing path; the create-missing
API/browser branch and the start-stopped-dependency branch were not exercised
because those containers were already running. `stop` was not run, to avoid
interrupting the pilot. `logs` exited 0; its output is not reproduced here
because it may contain patient data.

## Interactive checks still unverified

The startup script verifies process health, API index selection, accepted document
counts and that `/samples` serves HTML. The following still require a real browser
session and are **not** verified by the script:

- browser Back/Forward across sample, variant and gene pages;
- a full click-through from a sample page to a multi-ALT called variant and back;
- the full GenePage carrier-panel interaction (the carrier API and component were
  verified separately);
- any claim that a page is visually correct without response interception.

Do not treat a successful HTTP response as proof that data rendered. Confirm the
sample table and variant/carrier links in the browser.

## Data preparation and refresh

Normal browsing reads the existing indices. Data refresh is separate from startup
and is not performed by `scripts/internal-pilot.sh`.

Set the annotated MT path in local configuration (`/path/to/cohort_annotated.mt`). No reannotation is part of
normal startup. Deliberate refreshes use the existing Hail/VEP image
`clingen-annotate:latest` (Hail 0.2.135, Python 3.11, VEP 108) and the four
n20 metadata TSVs. A metadata edit does not automatically refresh the browser.

The per-ALT correction is committed as `9412315`. A deliberate aggregate refresh
writes one document per ALT, then a separate sample/carrier refresh writes the
sample and carrier indices. Both are operator actions, not startup actions. The
accepted set above is the result of that refresh.

The earlier [pipeline/TUI commands](archive/README-2026-10-25.md) and
[oligo-VM runbook](archive/DEPLOYMENT-oligo-VM.md) are historical. They use other
hosts, paths and ports and are not verified instructions for the current pilot.
Actual supplied input paths are in [INPUT_DATA.md](INPUT_DATA.md).
