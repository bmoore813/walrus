#!/usr/bin/env bash
# Compose smoke for the walrus-extractor pod lifecycle shell.
#
# Requires the dev stack up (`docker compose up --wait`). Asserts:
#   1. happy path — the process boots, /startup and /ready flip to 200, /healthz is 200,
#      and SIGTERM drives a graceful exit 0;
#   2. a *missing* control DB yields the mapped non-zero exit (ControlDb = 11) after the
#      startup deadline (transient-until-deadline-then-terminal).
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

cargo build -p extractor --bin walrus-extractor
BIN=target/debug/walrus-extractor
export WALRUS_CONFIG="$PWD/walrus.yaml"

# Shared config: MinIO + source PG from compose. Only control_db_url / deadline / port vary below.
export WALRUS_SOURCE_DB_URL="postgres://postgres:postgres@localhost:5432/walrus"
export WALRUS_OBJECT_STORE__BUCKET="walrus"
export WALRUS_OBJECT_STORE__ENDPOINT="http://localhost:9000"
export WALRUS_OBJECT_STORE__REGION="us-east-1"
export WALRUS_INSTANCE="walrus-extractor-smoke"
export WALRUS_SLOT_NAME="walrus_smoke_slot"
export WALRUS_PUBLICATION_NAME="walrus_pub"
export WALRUS_MAX_FILL="5s"
export WALRUS_MAX_ROWS="1000"
export WALRUS_MAX_BYTES="1000000"
export WALRUS_MAX_INFLIGHT_BYTES="2000000"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

wait_for() { # url expected_code timeout_secs
  local url="$1" code="$2" t="${3:-30}" got=""
  for _ in $(seq 1 "$((t * 2))"); do
    got=$(curl -s -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || true)
    [ "$got" = "$code" ] && return 0
    sleep 0.5
  done
  echo "!! $url never returned $code (last: ${got:-none})"
  return 1
}

COMPOSE="docker compose -f deploy/docker/docker-compose.yml"

echo "=== 0/2 apply source migrations (internal WAL tables + DDL triggers, idempotent) ==="
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - \
  <migrations/source/0001_publication.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - \
  <migrations/source/0002_ddl_triggers.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - \
  <migrations/source/0003_reload_signal.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - \
  <migrations/source/0004_reload_event.sql

echo "=== 1/2 happy path: source preflight passes, /startup -> /ready flip + graceful SIGTERM ==="
export WALRUS_CONTROL_DB_URL="postgres://postgres:postgres@localhost:5433/walrus_control"
export WALRUS_STARTUP_DEADLINE="30s"
export WALRUS_HEALTH_ADDR="127.0.0.1:8088"

"$BIN" &
PID=$!
trap 'kill "$PID" 2>/dev/null' EXIT

wait_for "http://127.0.0.1:8088/startup" 200 30 || exit 1
wait_for "http://127.0.0.1:8088/ready" 200 10 || exit 1
curl -sf "http://127.0.0.1:8088/healthz" >/dev/null || { echo "!! /healthz not 200"; exit 1; }
echo "  /startup, /ready, /healthz all 200"

kill -TERM "$PID"
wait "$PID"
rc=$?
trap - EXIT
echo "  SIGTERM exit code: $rc"
[ "$rc" -eq 0 ] || { echo "!! expected graceful exit 0, got $rc"; exit 1; }

echo "=== 2/2 missing control DB -> mapped ControlDb exit (11) ==="
export WALRUS_CONTROL_DB_URL="postgres://postgres:postgres@localhost:5599/nope" # nothing listens on 5599
export WALRUS_STARTUP_DEADLINE="2s"                                            # give up fast
export WALRUS_HEALTH_ADDR="127.0.0.1:8089"

"$BIN"
rc=$?
echo "  missing-control-DB exit code: $rc (expected 11 = ControlDb)"
[ "$rc" -eq 11 ] || { echo "!! expected exit 11 (ControlDb), got $rc"; exit 1; }

echo "extractor-smoke: PASS"
