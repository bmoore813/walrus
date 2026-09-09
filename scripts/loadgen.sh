#!/usr/bin/env bash
# Load generator — drives pgbench against the compose source-pg with the scenario scripts under
# scripts/loadgen/. pgbench ships in the postgres:16 image, so we run it *inside* the source-pg
# container (the script is piped in via stdin) — no pgbench needed on the host.
#
#   scripts/loadgen.sh <mixed|wide_text|large_txn> [duration_secs] [clients]
#
# `large_txn` runs exactly once (one 200k-row transaction → streaming); the others run for DURATION.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

SCENARIO="${1:-mixed}"
DURATION="${2:-60}"
CLIENTS="${3:-4}"
COMPOSE="docker compose -f deploy/docker/docker-compose.yml"
SCRIPT="scripts/loadgen/${SCENARIO}.sql"
[ -f "$SCRIPT" ] || { echo "loadgen: no such scenario script: $SCRIPT" >&2; exit 1; }

# `-n` skips pgbench's own vacuum (we drive our own tables); connect on the container's local socket.
if [ "$SCENARIO" = "large_txn" ]; then
  echo "loadgen: $SCENARIO — one large transaction (streaming)"
  $COMPOSE exec -T source-pg bash -c \
    'cat > /tmp/lg.sql; pgbench -n -f /tmp/lg.sql -t 1 -c 1 -U postgres walrus' < "$SCRIPT"
else
  echo "loadgen: $SCENARIO — ${CLIENTS} clients for ${DURATION}s"
  $COMPOSE exec -T source-pg bash -c \
    "cat > /tmp/lg.sql; pgbench -n -f /tmp/lg.sql -c ${CLIENTS} -T ${DURATION} -P 5 -U postgres walrus" < "$SCRIPT"
fi
