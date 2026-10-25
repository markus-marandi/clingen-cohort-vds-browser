#!/usr/bin/env bash
# Lifecycle commands for the configured internal browser installation. It starts the existing installation; it does
# not create, delete, or reindex data. Run on the internal server as the user that owns the
# rootless Docker daemon.
#
# usage:
#   scripts/browser.sh doctor
#   scripts/browser.sh start
#   scripts/browser.sh status
#   scripts/browser.sh logs [lines]
#   scripts/browser.sh urls
#   scripts/browser.sh stop

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="${PILOT_SOURCE_DIR:-${REPO_ROOT}/gnomad-browser}"

NETWORK="${PILOT_NETWORK:-gnomad-browser_default}"
ES="${PILOT_ES_CONTAINER:-cohort-gene-ref-es}"
REDIS="${PILOT_REDIS_CONTAINER:-cohort-gene-ref-redis}"
API="${PILOT_API_CONTAINER:-cohort-sample-pilot-api}"
BROWSER="${PILOT_BROWSER_CONTAINER:-cohort-sample-pilot-browser}"

ES_IMAGE="${PILOT_ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.13.4}"
REDIS_IMAGE="${PILOT_REDIS_IMAGE:-redis:7-alpine}"
API_IMAGE="${PILOT_API_IMAGE:-gnomad-browser-graphql-api:latest}"
BROWSER_IMAGE="${PILOT_BROWSER_IMAGE:-gnomad-browser-browser:latest}"

ES_PORT="${PILOT_ES_PORT:-9201}"
API_PORT="${PILOT_API_PORT:-8002}"
BROWSER_PORT="${PILOT_BROWSER_PORT:-8010}"

SAMPLES_INDEX="${COHORT_SAMPLES_INDEX:-cohort_samples_pilot20_peralt}"
CARRIERS_INDEX="${COHORT_SAMPLE_VARIANTS_INDEX:-cohort_sample_variants_pilot20_peralt}"
VARIANTS_INDEX="${COHORT_VARIANTS_INDEX:-cohort_variants_pilot20_peralt}"

EXPECTED_SAMPLES="${PILOT_EXPECTED_SAMPLES:-20}"
EXPECTED_CARRIERS="${PILOT_EXPECTED_CARRIERS:-939314}"
EXPECTED_VARIANTS="${PILOT_EXPECTED_VARIANTS:-172123}"

UPSTREAM_HEAD="${PILOT_UPSTREAM_HEAD:-db245371f1e572106bda8481ef0f9a6baac6c00d}"

if [ -z "${DOCKER_HOST:-}" ] && [ -S "/run/user/$(id -u)/docker.sock" ]; then
    export DOCKER_HOST="unix:///run/user/$(id -u)/docker.sock"
fi

DOCKER=(docker)
info() { printf '[pilot] %s\n' "$*"; }
warn() { printf '[pilot] warning: %s\n' "$*" >&2; }
die() { printf '[pilot] error: %s\n' "$*" >&2; exit 1; }

run() { "${DOCKER[@]}" "$@"; }
exists() { run inspect "$1" >/dev/null 2>&1; }
running() { [ "$(run inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" = "true" ]; }

require_docker() {
    command -v docker >/dev/null 2>&1 || die "docker is not on PATH"
    run version >/dev/null 2>&1 || die "cannot reach Docker daemon (DOCKER_HOST=${DOCKER_HOST:-unset})"
}

require_source() {
    [ -d "$SOURCE_DIR" ] || die "pilot source directory not found: $SOURCE_DIR"
    [ -d "$SOURCE_DIR/browser/src" ] || die "missing $SOURCE_DIR/browser/src"
    [ -d "$SOURCE_DIR/graphql-api/src" ] || die "missing $SOURCE_DIR/graphql-api/src"
    [ -d "$SOURCE_DIR/dataset-metadata" ] || die "missing $SOURCE_DIR/dataset-metadata"
    local head
    head="$(git -C "$SOURCE_DIR" rev-parse HEAD 2>/dev/null || true)"
    [ -n "$head" ] || die "pilot source is not a git checkout: $SOURCE_DIR"
    if [ "$head" != "$UPSTREAM_HEAD" ]; then
        warn "generated checkout HEAD is ${head:0:7}, expected upstream ${UPSTREAM_HEAD:0:7}"
    fi
}

require_image() {
    run image inspect "$1" >/dev/null 2>&1 || die "image not found: $1"
}

require_network() {
    run network inspect "$NETWORK" >/dev/null 2>&1 || die "Docker network not found: $NETWORK"
}

require_dependency() {
    local name="$1"
    exists "$name" || die "required dependency container is missing: $name"
    running "$name" || run start "$name" >/dev/null
}

wait_es() {
    local i
    for i in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${ES_PORT}/_cluster/health" 2>/dev/null |
            grep -Eq '"status":"(green|yellow)"'; then
            return 0
        fi
        sleep 2
    done
    die "Elasticsearch did not become ready on 127.0.0.1:${ES_PORT}"
}

wait_redis() {
    local i
    for i in $(seq 1 30); do
        if run exec "$REDIS" redis-cli ping 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 2
    done
    die "Redis dependency did not answer in $REDIS"
}

wait_api() {
    local i
    for i in $(seq 1 90); do
        if curl -fsS "http://127.0.0.1:${API_PORT}/health/ready" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    die "API did not become ready on 127.0.0.1:${API_PORT}"
}

wait_browser() {
    local i
    for i in $(seq 1 120); do
        if curl -fsS -H 'Accept: text/html' "http://127.0.0.1:${BROWSER_PORT}/samples" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    die "browser did not serve /samples on 127.0.0.1:${BROWSER_PORT}"
}

count_index() {
    curl -fsS "http://127.0.0.1:${ES_PORT}/$1/_count" 2>/dev/null |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null || true
}

check_indices() {
    local samples carriers variants
    samples="$(count_index "$SAMPLES_INDEX")"
    carriers="$(count_index "$CARRIERS_INDEX")"
    variants="$(count_index "$VARIANTS_INDEX")"
    info "accepted indices: samples=$SAMPLES_INDEX ($samples), carriers=$CARRIERS_INDEX ($carriers), variants=$VARIANTS_INDEX ($variants)"
    [ "$samples" = "$EXPECTED_SAMPLES" ] || return 1
    [ "$carriers" = "$EXPECTED_CARRIERS" ] || return 1
    [ "$variants" = "$EXPECTED_VARIANTS" ] || return 1
}

create_api() {
    info "creating $API from $API_IMAGE"
    run run -d --name "$API" --network "$NETWORK" \
        -p "127.0.0.1:${API_PORT}:8001" \
        -e NODE_ENV=development \
        -e "ELASTICSEARCH_URL=http://${ES}:9200" \
        -e "COHORT_SAMPLES_INDEX=${SAMPLES_INDEX}" \
        -e "COHORT_SAMPLE_VARIANTS_INDEX=${CARRIERS_INDEX}" \
        -e "COHORT_VARIANTS_INDEX=${VARIANTS_INDEX}" \
        -e "REDIS_HOST=${REDIS}" \
        -e REDIS_PORT=6379 \
        -e PORT=8001 \
        -v "${SOURCE_DIR}/graphql-api/src:/app/graphql-api/src:ro" \
        -v "${SOURCE_DIR}/dataset-metadata:/app/dataset-metadata:ro" \
        --entrypoint pnpm "$API_IMAGE" ts-node ./src/app.ts >/dev/null
}

create_browser() {
    info "creating $BROWSER from $BROWSER_IMAGE"
    run run -d --name "$BROWSER" --network "$NETWORK" \
        -p "127.0.0.1:${BROWSER_PORT}:8008" \
        -e NODE_ENV=development \
        -e "GNOMAD_API_URL=http://${API}:8001/api" \
        -v "${SOURCE_DIR}/browser/src:/app/browser/src" \
        --entrypoint pnpm "$BROWSER_IMAGE" start >/dev/null
}

check_container_config() {
    local name="$1" expected_image="$2"
    exists "$name" || { warn "$name is not created"; return 0; }
    local image
    image="$(run inspect -f '{{.Config.Image}}' "$name")"
    [ "$image" = "$expected_image" ] || warn "$name image is $image, expected $expected_image"
    info "$name mounts: $(run inspect -f '{{range .Mounts}}{{.Source}}->{{.Destination}} rw={{.RW}} {{end}}' "$name")"
    info "$name env: $(run inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$name" |
        grep -E 'COHORT|ELASTICSEARCH|REDIS|GNOMAD|NODE_ENV|PORT' | tr '\n' ' ' || true)"
}

doctor() {
    require_docker
    info "DOCKER_HOST=${DOCKER_HOST:-unset}"
    require_source
    info "source=$SOURCE_DIR upstream=$(git -C "$SOURCE_DIR" rev-parse --short HEAD)"
    require_image "$ES_IMAGE"
    require_image "$REDIS_IMAGE"
    require_image "$API_IMAGE"
    require_image "$BROWSER_IMAGE"
    require_network
    exists "$ES" || warn "Elasticsearch dependency is missing: $ES"
    exists "$REDIS" || warn "Redis dependency is missing: $REDIS"
    check_container_config "$API" "$API_IMAGE"
    check_container_config "$BROWSER" "$BROWSER_IMAGE"
    info "API port: $(run port "$API" 8001/tcp 2>/dev/null || echo not-created)"
    info "browser port: $(run port "$BROWSER" 8008/tcp 2>/dev/null || echo not-created)"
    info "ports: ES=${ES_PORT} API=${API_PORT} browser=${BROWSER_PORT}"
    info "indices: samples=${SAMPLES_INDEX} carriers=${CARRIERS_INDEX} variants=${VARIANTS_INDEX}"
    info "doctor passed (no data was changed)"
}

start() {
    require_docker
    require_source
    require_image "$ES_IMAGE"
    require_image "$REDIS_IMAGE"
    require_image "$API_IMAGE"
    require_image "$BROWSER_IMAGE"
    require_network
    require_dependency "$ES"
    require_dependency "$REDIS"
    wait_es
    wait_redis

    if exists "$API"; then
        running "$API" || run start "$API" >/dev/null
    else
        create_api
    fi
    wait_api

    if exists "$BROWSER"; then
        running "$BROWSER" || run start "$BROWSER" >/dev/null
    else
        create_browser
    fi
    wait_browser

    check_indices || die "accepted index counts do not match; do not recreate indices, inspect the installation"
    info "started; browser http://127.0.0.1:${BROWSER_PORT}/samples"
    info "interactive browser checks are not performed by this script"
}

status() {
    require_docker
    info "containers"
    run ps --filter "name=cohort-sample-pilot" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    info "source=${SOURCE_DIR} upstream=$(git -C "$SOURCE_DIR" rev-parse --short HEAD 2>/dev/null || echo missing)"
    info "API indices: $(run inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$API" 2>/dev/null |
        grep -E 'COHORT_.*_INDEX' | tr '\n' ' ' || true)"
    info "API health: $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${API_PORT}/health/ready" 2>/dev/null || true)"
    info "browser /samples: $(curl -s -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "http://127.0.0.1:${BROWSER_PORT}/samples" 2>/dev/null || true)"
    check_indices || warn "index counts differ from the accepted set"
    info "URL: http://127.0.0.1:${BROWSER_PORT}/samples"
}

logs() {
    require_docker
    local lines="${1:-50}"
    info "$API logs"
    run logs --tail "$lines" "$API" 2>&1 || true
    info "$BROWSER logs"
    run logs --tail "$lines" "$BROWSER" 2>&1 || true
}

urls() {
    cat <<EOF
local browser URLs (on the server):
  http://127.0.0.1:${BROWSER_PORT}/samples
  http://127.0.0.1:${BROWSER_PORT}/gene/OR4F5?dataset=cohort
  http://127.0.0.1:${BROWSER_PORT}/variant/1-69270-A-G?dataset=cohort

Use the internal browser address and access instructions supplied by your administrator.
Remote hostnames, login accounts, and network addresses are not stored in this repository.
EOF
}

stop_pilot() {
    require_docker
    running "$BROWSER" && run stop "$BROWSER" >/dev/null || true
    running "$API" && run stop "$API" >/dev/null || true
    info "stopped only the pilot browser and API"
}

case "${1:-}" in
    doctor) doctor ;;
    start) start ;;
    status) status ;;
    logs) logs "${2:-50}" ;;
    urls) urls ;;
    stop) stop_pilot ;;
    *)
        cat <<EOF
usage: $0 {doctor|start|status|logs [lines]|urls|stop}

  doctor   check source, images, network, dependencies and existing config
  start    start dependencies and the pilot API/browser; create only missing pilot containers
  status   show running containers, health, accepted index counts and local URLs
  logs     tail API and browser logs (default 50 lines)
  urls     print the SSH tunnel and browser URLs
  stop     stop only the pilot browser and API

No data is created, deleted, reindexed, or reannotated by this script.
EOF
        exit 2
        ;;
esac
