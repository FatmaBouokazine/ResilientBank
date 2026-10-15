#!/usr/bin/env bash
# Starts the ENTIRE ResilientBank stack in Docker with a single command: infrastructure,
# schema, application, and dashboards.
#
# This is the one command for "show me the system working". Everything else (make run,
# make relayer) is for development against a live stack.
#
# Usage:
#   bash scripts/docker-up.sh          # build if needed, then start everything
#   bash scripts/docker-up.sh --fresh  # wipe volumes first (destroys local data)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FRESH=0
if [[ "${1:-}" == "--fresh" ]]; then
    FRESH=1
fi

log() { echo "[docker-up] $*"; }

if ! docker info >/dev/null 2>&1; then
    log "ERROR: Docker Desktop is not running. Start it first."
    exit 1
fi

if (( FRESH )); then
    log "WARNING: --fresh wipes ALL local volumes (payments, outbox history)."
    read -r -p "Continue? [y/N] " answer
    [[ "$answer" == [yY] ]] || { log "aborted."; exit 1; }
    docker compose -f deploy/local/docker-compose.yml down -v
fi

log "building the service image (payments-api, relayer, dashboard, migrate)..."
docker build -t resilientbank:dev -f Dockerfile .

log "starting infrastructure..."
docker compose -f deploy/local/docker-compose.yml up -d postgres redis kafka prometheus grafana

log "waiting for postgres..."
for _ in $(seq 1 30); do
    if docker exec rb-postgres pg_isready -U resilient -d payments >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

log "starting application tier (migrate, kafka-init, api, relayer, dashboard)..."
docker compose -f deploy/local/docker-compose.yml up -d migrate kafka-init payments-api outbox-relayer dashboard

log "waiting for the payment service..."
for _ in $(seq 1 30); do
    if curl -sf http://localhost:2112/metrics >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo
log "system is up:"
log "  checkout dashboard : http://localhost:2114/checkout"
log "  status page        : http://localhost:2114/"
log "  Grafana            : http://localhost:3000  (admin/admin)"
log "  Prometheus         : http://localhost:9090"
log "  gRPC               : localhost:50051"
echo
log "logs: docker compose -f deploy/local/docker-compose.yml logs -f [service]"
log "stop: docker compose -f deploy/local/docker-compose.yml down"