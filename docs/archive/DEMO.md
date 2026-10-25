# Demo Runbook

Order: **run the pipeline first** (or show that it already ran), then start the browser services, then open the UI.

---

## 0. SSH into VM

```bash
ssh oligo-VM
```

---

## 1. Run the pipeline

### Option A — Pipeline TUI (recommended for demo)

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser
scripts/clingen-tui
```

Keys inside the TUI:
- **`q`** — quit the TUI
- **`Ctrl+C`** — terminate the currently running pipeline step (ingest / annotate / export)

Navigate to the **Ingest**, **Annotate**, or **Export** tab, fill in paths (defaults are pre-filled), press **Launch**.

### Option B — Manual pipeline commands

**Step 1 — Ingest GVCFs into Hail VDS**

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser

/mnt/sdb/venvs/py310/bin/python3 parallel_ingest_cohort.py \
  --raw-gvcf-dir      /mnt/sdb/data/raw_gvcfs/andmebaas_test_valim \
  --filtered-gvcf-dir /mnt/sdb/data/filtered_gvcfs/andmebaas_test_valim_filtered \
  --output-vds-dir    /mnt/sdb/data/vds \
  --temp-base         /mnt/sdb/data/tmp/combiner_temp \
  --manifest-path     /mnt/sdb/data/logs/ingest_manifest.json \
  --n-cores 16 \
  --memory-gb 64
```

What each parameter does:

| Parameter | What it means |
|---|---|
| `--raw-gvcf-dir` | Your original `.gvcf.gz` files from the sequencer — read-only, never touched |
| `--filtered-gvcf-dir` | Where bcftools writes the pre-processed GVCFs (chr rename + contig filter). Already-done files are skipped automatically |
| `--output-vds-dir` | Where the Hail VDS is written, named `cohort_YYYY-MM-DD_runNNN.vds` |
| `--temp-base` | Hail combiner scratch space for checkpoints and shuffles — needs plenty of free disk |
| `--manifest-path` | JSON log of every run: which GVCFs were ingested, status, VDS path. Enables resuming a crashed run |
| `--n-cores / --memory-gb` | 75 % of cores go to parallel bcftools workers, the rest to Spark/Hail |

**Step 2 — Annotate VDS → MatrixTable**

```bash
/mnt/sdb/venvs/py310/bin/python3 annotate_cohort.py \
  --vds-path      /mnt/sdb/data/vds/cohort_2026-03-11_run001.vds \
  --output-mt     /mnt/sdb/data/mt/cohort_annotated.mt \
  --metadata-path /mnt/sdb/data/metadata.csv \
  --n-cores 16 \
  --memory-gb 64 \
  --overwrite
```

**Step 3 — Export MatrixTable → Elasticsearch**

```bash
/mnt/sdb/venvs/hail-39/bin/python \
  /mnt/sdb/projects/clingen-cohort-vds-browser/browser/data-pipeline/cohort_export.py \
  --mt-path /mnt/sdb/data/mt/cohort_annotated.mt \
  --es-url  http://localhost:9200 \
  --index   cohort_variants
```

---

## 2. Start browser services (after pipeline is done)

### Elasticsearch

```bash
ES_JAVA_OPTS='-Xms2g -Xmx2g' nohup \
  /mnt/sdb/packages/elasticsearch/bin/elasticsearch \
  -Epath.data=/mnt/sdb/tmp/es-data \
  -Epath.logs=/mnt/sdb/tmp/es-logs \
  -Ediscovery.type=single-node \
  -Expack.security.enabled=false \
  -Ecluster.routing.allocation.disk.threshold_enabled=false \
  >> /mnt/sdb/tmp/es-logs/es.log 2>&1 &

until curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green"'; do
  sleep 3
done && echo "ES green"
```

### Redis

```bash
podman start redis 2>/dev/null || \
  podman run -d --name redis --network=host redis:7-alpine
```

### GraphQL API

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser/gnomad-browser/graphql-api

PORT=8000 \
ELASTICSEARCH_URL=http://localhost:9200 \
CACHE_REDIS_URL=redis://localhost:6379/1 \
RATE_LIMITER_REDIS_URL=redis://localhost:6379/2 \
  nohup pnpm ts-node ./src/app.ts >> /mnt/sdb/tmp/graphql-api.log 2>&1 &

until curl -s http://localhost:8000/health/ready | grep -q ok; do sleep 3; done && echo "API up"
```

### Browser UI

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser/gnomad-browser/browser

GNOMAD_API_URL=http://localhost:8000/api/ \
  nohup pnpm start >> /mnt/sdb/tmp/browser.log 2>&1 &

until curl -s -o /dev/null -w '%{http_code}' http://localhost:8008/ | grep -q 200; do
  sleep 5
done && echo "Browser up"
```

### Alternative: containerised stack

The steps above start each service natively and are the currently verified path.
`scripts/stack.sh` runs the same services through compose under either docker or
podman:

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser
./scripts/stack.sh doctor   # host preflight only
./scripts/stack.sh up       # compose up --build -d
```

`doctor` fails loudly if `/etc/mtab` is missing or dangling, which aborts podman
builds. Fix once with `sudo ln -sf /proc/self/mounts /etc/mtab`. The compose
stack publishes the browser on 3000, not 8008 — adjust the tunnel accordingly.

---

## 3. SSH tunnel (run on your laptop, keep open)

```bash
ssh -L 8008:localhost:8008 -L 8000:localhost:8000 -N oligo-VM
```

Then open `http://localhost:8008` and select dataset **Cohort**.

---

## 4. Health check

```bash
echo "ES:       $(curl -s http://localhost:9200/_cluster/health \
                  | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')"
echo "Variants: $(curl -s http://localhost:9200/cohort_variants/_count \
                  | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')"
echo "API:      $(curl -s http://localhost:8000/health/ready)"
echo "Browser:  $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8008/)"
```

Expected: `green / 150659 / ok / 200`

---

## 5. Re-populate Elasticsearch index (if empty after reboot)

```bash
curl -s 'http://localhost:9200/cohort_variants/_count' | python3 -m json.tool
# if count is 0, re-run Step 3 above
```

---

## Logs

| Service       | Path                                  |
|---------------|---------------------------------------|
| Elasticsearch | `/mnt/sdb/tmp/es-logs/es.log`         |
| GraphQL API   | `/mnt/sdb/tmp/graphql-api.log`        |
| Browser UI    | `/mnt/sdb/tmp/browser.log`            |
| Hail pipeline | `/mnt/sdb/data/logs/hail-*.log`       |
