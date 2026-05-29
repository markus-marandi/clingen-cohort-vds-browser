# Deployment Guide — Cohort gnomAD Browser

This document explains how to bring the current local/internal cohort browser up on `oligo-VM` and
access it from your laptop over SSH. No firewall changes are needed.

Production should not expose this patient-linked VM publicly. The public/browser VM should be a
separate machine populated only with sanitized variant-level Elasticsearch documents generated from
an explicit public export allowlist.

---

## What runs where

For this internal deployment, the VM is entirely self-contained. All data and services live on
`/mnt/sdb`.

| Service | Port | Purpose |
|---|---|---|
| Elasticsearch 8.13.4 | 9200 | Stores and serves the local/demo variant index (`cohort_variants`) |
| Redis 7 (Podman) | 6379 | GraphQL API rate-limiter and response cache |
| GraphQL API (Node/pnpm) | 8000 | Translates browser queries into ES searches |
| Browser UI (webpack dev) | **8008** | React frontend served by `webpack serve` |

The browser JS sends queries to `/api` on the same origin (8008); webpack proxies those to the
GraphQL API on 8000.

---

## SSH tunnel (how to access from your laptop)

Run this **on your laptop** and keep the terminal open:

```bash
ssh -L 8008:localhost:8008 -L 8000:localhost:8000 -N oligo-VM
```

Then open `http://localhost:8008` in your browser and select dataset **Cohort**.

For a tunnel that reconnects automatically if it drops:

```bash
autossh -M 0 -f -N \
  -L 8008:localhost:8008 \
  -L 8000:localhost:8000 \
  -o ServerAliveInterval=30 oligo-VM
```

---

## Starting all services (after a VM reboot)

SSH into the VM (`ssh oligo-VM`) and run the blocks below in order.

### 1. Elasticsearch

```bash
ES_JAVA_OPTS='-Xms2g -Xmx2g' nohup \
  /mnt/sdb/packages/elasticsearch/bin/elasticsearch \
  -Epath.data=/mnt/sdb/tmp/es-data \
  -Epath.logs=/mnt/sdb/tmp/es-logs \
  -Ediscovery.type=single-node \
  -Expack.security.enabled=false \
  -Ecluster.routing.allocation.disk.threshold_enabled=false \
  >> /mnt/sdb/tmp/es-logs/es.log 2>&1 &

# wait for green
until curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green"'; do
  sleep 3
done && echo "ES green"
```

> **Note:** `ES_JAVA_OPTS` must be an environment variable, not an `-E` flag — passing it as
> `-EES_JAVA_OPTS=...` causes ES to refuse to start.

Verify the variant index is intact:

```bash
curl -s 'http://localhost:9200/cohort_variants/_count' | python3 -m json.tool
# expect: {"count": 150659, ...}
```

If the count is 0 or the index is missing, re-run the export (see "Re-populating the index" below).

### 2. Redis

If a reboot has occurred, Podman's runtime state will be stale:

```bash
rm -rf /var/tmp/containers-run /var/tmp/podman-tmp
podman run -d --name redis --network=host redis:7-alpine
```

If the container already exists from before the reboot:

```bash
podman start redis
```

Check it:

```bash
podman exec redis redis-cli ping   # should reply PONG
```

### 3. GraphQL API

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser/gnomad-browser/graphql-api

PORT=8000 \
ELASTICSEARCH_URL=http://localhost:9200 \
CACHE_REDIS_URL=redis://localhost:6379/1 \
RATE_LIMITER_REDIS_URL=redis://localhost:6379/2 \
  nohup pnpm ts-node ./src/app.ts >> /mnt/sdb/tmp/graphql-api.log 2>&1 &

until curl -s http://localhost:8000/health/ready | grep -q ok; do sleep 3; done
echo "GraphQL API up"
```

### 4. Browser UI

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser/gnomad-browser/browser

GNOMAD_API_URL=http://localhost:8000/api/ \
  nohup pnpm start >> /mnt/sdb/tmp/browser.log 2>&1 &

until curl -s -o /dev/null -w '%{http_code}' http://localhost:8008/ | grep -q 200; do
  sleep 5
done && echo "Browser UI up"
```

webpack takes 20–60 s to compile on first start.

---

## Quick status check

```bash
echo "ES:       $(curl -s http://localhost:9200/_cluster/health \
                  | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')"
echo "Variants: $(curl -s http://localhost:9200/cohort_variants/_count \
                  | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')"
echo "API:      $(curl -s http://localhost:8000/health/ready)"
echo "Browser:  $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8008/)"
```

Expected output:

```
ES:       green
Variants: 150659
API:      ok
Browser:  200
```

---

## Re-populating the Elasticsearch index

The local/demo `cohort_variants` index is persisted on disk at `/mnt/sdb/tmp/es-data` and survives
reboots.
You only need to re-run the export if:

- the index is empty / missing after a fresh ES data directory, or
- the annotated MatrixTable has been updated with new samples or annotations.

**Full export from annotated MT (preferred — includes VEP, dbNSFP-derived predictor fields,
ClinVar, gnomAD):**

```bash
/mnt/sdb/venvs/hail-39/bin/python \
  /mnt/sdb/projects/clingen-cohort-vds-browser/browser/data-pipeline/cohort_export.py \
  --mt-path /mnt/sdb/data/mt/cohort_annotated.mt \
  --es-url http://localhost:9200 \
  --index cohort_variants
```

**Fallback — basic stats only from VDS (no VEP/dbNSFP):**

```bash
/mnt/sdb/venvs/hail-39/bin/python \
  /mnt/sdb/projects/clingen-cohort-vds-browser/browser/data-pipeline/cohort_export.py \
  --vds-path /mnt/sdb/data/vds/cohort_2026-03-11_run001.vds \
  --es-url http://localhost:9200 \
  --index cohort_variants
```

---

## First-time setup on a new machine

If `gnomad-browser/` is absent or you need to re-apply all cohort patches:

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser
./setup.sh
```

`setup.sh` will:
1. Clone the upstream gnomAD browser repository into `gnomad-browser/`
2. Overlay all cohort patch files from `browser/`
3. Install Node dependencies with pnpm (store on `/mnt/sdb/packages`)

After `setup.sh` completes, follow the "Starting all services" steps above.

---

## Troubleshooting

**ES won't start:**

```bash
tail -50 /mnt/sdb/tmp/es-logs/elasticsearch.log | grep -E 'ERROR|Exception'
df -h /mnt/sdb   # check disk space
```

**GraphQL API crashes on startup:**

```bash
tail -30 /mnt/sdb/tmp/graphql-api.log | grep -v esMetrics
```

Common cause: missing or mismatched Node modules after a `setup.sh` run.
Fix: `cd /mnt/sdb/projects/clingen-cohort-vds-browser/gnomad-browser && pnpm install`

**Browser shows "An unknown error occurred" or "Unable to load variant":**

Make sure the dataset is set to **Cohort** (not gnomAD v4 or v2). The gnomAD dataset indices do not
exist on this VM. Navigate to `http://localhost:8008/variant/<id>?dataset=cohort` or use the
dataset dropdown in the top-right corner.

**Podman "boot ID differs from cached boot ID" error:**

```bash
rm -rf /var/tmp/containers-run /var/tmp/podman-tmp
podman run -d --name redis --network=host redis:7-alpine
```

**Browser or API not responding after a while:**

```bash
ps aux | grep -E 'node|webpack|ts-node|elasticsearch' | grep -v grep
```

Restart whichever service is missing using the commands in "Starting all services" above.

---

## Log files

| Log | Path |
|---|---|
| Elasticsearch | `/mnt/sdb/tmp/es-logs/elasticsearch.log` |
| GraphQL API | `/mnt/sdb/tmp/graphql-api.log` |
| Browser UI | `/mnt/sdb/tmp/browser.log` |
