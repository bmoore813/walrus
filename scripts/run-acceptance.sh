#!/usr/bin/env bash
# Self-contained PostgreSQL -> walrus-extractor -> walrus-transformer -> DuckLake acceptance run.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PROJECT="walrus-acceptance-$$"
COMPOSE=(docker compose -p "$PROJECT" -f deploy/docker/docker-compose.yml)
export SOURCE_PG_HOST_PORT=0
export CONTROL_PG_HOST_PORT=0
export MINIO_API_HOST_PORT=0
export MINIO_CONSOLE_HOST_PORT=0

cleanup() {
  local rc=$?
  trap - EXIT
  if [ "$rc" -ne 0 ]; then
    echo "=== acceptance backing-service logs ===" >&2
    "${COMPOSE[@]}" logs --no-color --tail=100 >&2 || true
  fi
  if [ "${WALRUS_E2E_KEEP_STACK:-0}" = "1" ]; then
    echo "acceptance stack retained: docker compose -p $PROJECT -f deploy/docker/docker-compose.yml down -v" >&2
  else
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required to allocate service listener ports" >&2; exit 1; }
docker info >/dev/null

echo "=== building real extractor and transformer binaries ==="
cargo build \
  -p extractor --bin walrus-extractor \
  -p transformer --bin walrus-transformer
export WALRUS_E2E_SKIP_BUILD=1

echo "=== starting isolated acceptance backing services ($PROJECT) ==="
"${COMPOSE[@]}" up --wait

source_addr="$("${COMPOSE[@]}" port source-pg 5432 | tail -n 1)"
control_addr="$("${COMPOSE[@]}" port control-pg 5432 | tail -n 1)"
minio_addr="$("${COMPOSE[@]}" port minio 9000 | tail -n 1)"
read -r extractor_port transformer_port < <(python3 - <<'PY'
import socket

sockets = []
ports = []
for _ in range(2):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
    ports.append(str(sock.getsockname()[1]))
print(" ".join(ports))
PY
)

export WALRUS_E2E_SOURCE_URL="postgres://postgres:postgres@${source_addr}/walrus"
export WALRUS_E2E_CONTROL_URL="postgres://postgres:postgres@${control_addr}/walrus_control"
export WALRUS_E2E_CATALOG_URL="postgres://postgres:postgres@${control_addr}/walrus_ducklake"
export WALRUS_E2E_S3_ENDPOINT="http://${minio_addr}"
export WALRUS_E2E_EXTRACTOR_HEALTH_ADDR="127.0.0.1:${extractor_port}"
export WALRUS_E2E_TRANSFORMER_HEALTH_ADDR="127.0.0.1:${transformer_port}"
export WALRUS_E2E_MINIO_CONTAINER="$("${COMPOSE[@]}" ps -q minio)"

echo "=== running exact source-to-DuckLake parity scenarios ==="
cargo test -p e2e --features it --test mirror_parity -- --ignored --test-threads=1
