#!/usr/bin/env bash
# image-smoke.sh — PID-1 SIGTERM smoke for the two container images.
#
# Proves the load-bearing property of the Dockerfiles: because the entrypoint is exec-form under
# `tini`, a SIGTERM (as Kubernetes sends on pod stop) reaches the Rust process, whose handler runs a
# graceful shutdown and exits 0 — it is NOT swallowed by a shell and SIGKILLed (exit 137).
#
# Why a full mini-pipeline and not `docker run; docker stop`: neither binary reaches a
# SIGTERM-handling state without its dependencies. The extractor only exits 0 on SIGTERM once past
# bootstrap and in the streaming decode loop (needs control-pg + MinIO + source-pg); the transformer owns
# no tables — and so has no apply loop to signal — until the extractor has established an epoch and
# registered them. So: boot compose, run the extractor to `streaming`, run the transformer to `apply loops`,
# then SIGTERM each and assert a clean exit 0.
#
# Also checks the runtime images are slim (no cargo/rustc) and carry the CA bundle, and — implicitly,
# by the transformer reaching its apply loop — that the bundled-DuckDB binary runs (libstdc++ present).
#
# Self-contained: builds the images if missing, owns the compose lifecycle. Run locally with just
#   bash scripts/image-smoke.sh
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

EXTRACTOR_IMG="${EXTRACTOR_IMG:-walrus-extractor:ci}"
TRANSFORMER_IMG="${TRANSFORMER_IMG:-walrus-transformer:ci}"
COMPOSE="docker compose -f deploy/docker/docker-compose.yml"
NET="walrus_default" # compose project name is `walrus` → its default network
GRACE=90             # docker stop grace: SIGTERM, then SIGKILL after this many seconds
EXTRACTOR_C="walrus-extractor-smoke"
TRANSFORMER_C="walrus-transformer-smoke"

fail() { echo "!! $*" >&2; exit 1; }

wait_log() { # container marker timeout_secs
  local c="$1" marker="$2" t="${3:-60}"
  for _ in $(seq 1 "$t"); do
    if docker logs "$c" 2>&1 | grep -q "$marker"; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" != "true" ]; then
      echo "-- $c exited before logging '$marker'; last logs:"; docker logs "$c" 2>&1 | tail -40
      return 1
    fi
    sleep 1
  done
  echo "-- $c never logged '$marker' within ${t}s; last logs:"; docker logs "$c" 2>&1 | tail -40
  return 1
}

assert_sigterm_zero() { # container
  local c="$1" rc
  echo "-- SIGTERM $c (docker stop --time $GRACE) ..."
  docker stop --time "$GRACE" "$c" >/dev/null
  rc="$(docker inspect -f '{{.State.ExitCode}}' "$c")"
  echo "   $c exit code: $rc"
  if [ "$rc" != "0" ]; then
    docker logs "$c" 2>&1 | tail -40
    fail "$c: expected graceful exit 0, got $rc (137 = SIGKILL = signal was swallowed, not handled)"
  fi
}

cleanup() { docker rm -f "$EXTRACTOR_C" "$TRANSFORMER_C" >/dev/null 2>&1; $COMPOSE down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# 1. Build the images if they aren't present (CI builds them explicitly first; this makes a bare
#    local run one command).
docker image inspect "$EXTRACTOR_IMG"   >/dev/null 2>&1 || docker build -f deploy/docker/Dockerfile.extractor -t "$EXTRACTOR_IMG" .
docker image inspect "$TRANSFORMER_IMG" >/dev/null 2>&1 || docker build -f deploy/docker/Dockerfile.transformer -t "$TRANSFORMER_IMG" .

# 2. Runtime images are slim and CA-equipped: no build toolchain leaked in, CA bundle present.
for img in "$EXTRACTOR_IMG" "$TRANSFORMER_IMG"; do
  docker run --rm --entrypoint sh "$img" -c 'test -f /etc/ssl/certs/ca-certificates.crt' \
    || fail "$img: CA bundle missing (ca-certificates not installed)"
  if docker run --rm --entrypoint sh "$img" -c 'command -v cargo >/dev/null || command -v rustc >/dev/null'; then
    fail "$img: build toolchain (cargo/rustc) leaked into the runtime image"
  fi
done
echo "runtime images: slim (no toolchain) + CA bundle present"

# 3. Boot the backing stack and apply the source migrations the extractor's preflight requires
#    (publication + internal WAL tables + ddl_audit triggers; idempotent — mirrors extractor-smoke).
$COMPOSE up --wait
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0001_publication.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0002_ddl_triggers.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0003_reload_signal.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0004_reload_event.sql

# Credentials + object-store config shared by both containers (they reach compose by service DNS on
# the compose network — portable across Linux CI and local Docker Desktop).
COMMON_ENV=(
  -v "$PWD/walrus.yaml:/etc/walrus/walrus.yaml:ro"
  -e WALRUS_CONFIG=/etc/walrus/walrus.yaml
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin
  -e WALRUS_OBJECT_STORE__BUCKET=walrus
  -e WALRUS_OBJECT_STORE__ENDPOINT=http://minio:9000
  -e WALRUS_OBJECT_STORE__REGION=us-east-1
)
DUCKLAKE_ENV=(
  -e WALRUS_DUCKLAKE__CATALOG_URL=postgres://postgres:postgres@control-pg:5432/walrus_ducklake
  -e WALRUS_DUCKLAKE__METADATA_SCHEMA=walrus_imgsmoke
  -e WALRUS_DUCKLAKE__DATA_PATH=s3://walrus/ducklake/imgsmoke/
  -e WALRUS_DUCKLAKE__EXTENSION_DIRECTORY=/opt/walrus/duckdb_extensions
  -e WALRUS_DUCKLAKE__INSTALL_EXTENSIONS=false
)

# Catalog changes are an explicit release step; the normal transformer below attaches with automatic
# migration disabled. This also proves the runtime image contains all pinned extension artifacts.
echo "=== migrating DuckLake catalog ==="
docker run --rm --network "$NET" "${COMMON_ENV[@]}" "${DUCKLAKE_ENV[@]}" \
  -e WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@control-pg:5432/walrus_control \
  -e WALRUS_INSTANCE=walrus-transformer-imgsmoke-0 \
  "$TRANSFORMER_IMG" --migrate-ducklake-catalog \
  || fail "DuckLake catalog migration failed"

# 4. Extractor: creates the slot + establishes the epoch, registers orders/customers/items, then streams.
echo "=== starting $EXTRACTOR_C ==="
docker run -d --name "$EXTRACTOR_C" --network "$NET" "${COMMON_ENV[@]}" \
  -e WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@control-pg:5432/walrus_control \
  -e WALRUS_SOURCE_DB_URL=postgres://postgres:postgres@source-pg:5432/walrus \
  -e WALRUS_INSTANCE=walrus-extractor-imgsmoke \
  -e WALRUS_SLOT_NAME=walrus_imgsmoke_slot \
  -e WALRUS_PUBLICATION_NAME=walrus_pub \
  -e WALRUS_HEALTH_ADDR=0.0.0.0:8088 \
  -e WALRUS_MAX_FILL=5s -e WALRUS_MAX_ROWS=1000 -e WALRUS_MAX_BYTES=1000000 -e WALRUS_MAX_INFLIGHT_BYTES=2000000 \
  -e WALRUS_STARTUP_DEADLINE=60s \
  "$EXTRACTOR_IMG" >/dev/null
wait_log "$EXTRACTOR_C" "streaming logical replication" 90 || fail "$EXTRACTOR_C never reached the streaming decode loop"

# 5. Transformer: owns the just-registered tables (epoch from the extractor), apply loops poll waiting on the
#    shutdown token. Reaching "starting apply loops" also proves the bundled-DuckDB binary runs and
#    the httpfs extension loaded in the image.
echo "=== starting $TRANSFORMER_C ==="
docker run -d --name "$TRANSFORMER_C" --network "$NET" "${COMMON_ENV[@]}" "${DUCKLAKE_ENV[@]}" \
  -e WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@control-pg:5432/walrus_control \
  -e WALRUS_INSTANCE=walrus-transformer-imgsmoke-0 \
  -e WALRUS_HEALTH_ADDR=0.0.0.0:8090 \
  -e WALRUS_POLL_INTERVAL=1s \
  "$TRANSFORMER_IMG" >/dev/null
wait_log "$TRANSFORMER_C" "starting apply loops" 90 || fail "$TRANSFORMER_C never reached its apply loops"

# 6. The assertion: SIGTERM each container; both must handle it and exit 0 within the grace window.
assert_sigterm_zero "$TRANSFORMER_C"
assert_sigterm_zero "$EXTRACTOR_C"

echo "image-smoke: PASS"
