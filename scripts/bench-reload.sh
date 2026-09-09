#!/usr/bin/env bash
# Reproducible, non-CI performance harness for one-slot table reconciliation.
#
#   scripts/bench-reload.sh narrow workers
#   RELOAD_BENCH_WORKERS=1,2,4,8 scripts/bench-reload.sh wide workers
#   BASELINE_EXTRACTOR_BIN=/tmp/baseline/walrus-extractor scripts/bench-reload.sh narrow all
#
# The candidate and optional external baseline extractor are exercised against the same warm PostgreSQL
# fixture. Each raw sample and its median aggregate land under target/perf. Timing is never a CI
# gate; slot/walsender counts, exported rows, worker bounds, and exact source/mirror equality are.
set -Eeuo pipefail
cd "$(git rev-parse --show-toplevel)"

FIXTURE="${1:-narrow}"
MATRIX="${2:-workers}"
WORKERS_CSV="${RELOAD_BENCH_WORKERS:-1,2,4,8}"
TABLES_CSV="${RELOAD_BENCH_TABLES:-1,2,4}"
CHUNKS_CSV="${RELOAD_BENCH_CHUNKS:-1000,10000,100000}"
BASE_WORKERS="${RELOAD_BENCH_BASE_WORKERS:-4}"
BASE_TABLES="${RELOAD_BENCH_BASE_TABLES:-1}"
BASE_CHUNK_ROWS="${RELOAD_BENCH_BASE_CHUNK_ROWS:-10000}"
NARROW_ROWS="${RELOAD_BENCH_NARROW_ROWS:-10000000}"
NARROW_PAYLOAD_BYTES="${RELOAD_BENCH_NARROW_PAYLOAD_BYTES:-128}"
WIDE_ROWS="${RELOAD_BENCH_WIDE_ROWS:-500000}"
WIDE_PAYLOAD_BYTES="${RELOAD_BENCH_WIDE_PAYLOAD_BYTES:-4096}"
WARMUPS="${RELOAD_BENCH_WARMUPS:-1}"
SAMPLES="${RELOAD_BENCH_SAMPLES:-5}"
SAMPLE_INTERVAL="${RELOAD_BENCH_SAMPLE_INTERVAL:-0.1}"
TIMEOUT_SECONDS="${RELOAD_BENCH_TIMEOUT_SECONDS:-1800}"
MAX_BYTES=134217728
RELOAD_ROUTE_RESERVATION_BYTES=33554432

case "$FIXTURE" in
  narrow) FIXTURES_CSV=narrow ;;
  wide) FIXTURES_CSV=wide ;;
  all) FIXTURES_CSV=narrow,wide ;;
  *) echo "fixture must be narrow, wide, or all" >&2; exit 2 ;;
esac
case "$MATRIX" in
  workers|tables|chunks|all) ;;
  *) echo "matrix must be workers, tables, chunks, or all" >&2; exit 2 ;;
esac

ensure_serial_baseline() {
  case ",$1," in
    *,1,*) printf '%s\n' "$1" ;;
    *) printf '1,%s\n' "$1" ;;
  esac
}

# A speedup without the serial point is not meaningful. Treat the serial point as part of the
# selected matrix even when a caller supplies only the parallel values.
case "$MATRIX" in
  workers|all) WORKERS_CSV="$(ensure_serial_baseline "$WORKERS_CSV")" ;;
esac
case "$MATRIX" in
  tables|all) TABLES_CSV="$(ensure_serial_baseline "$TABLES_CSV")" ;;
esac

positive_int() {
  case "$2" in
    ''|*[!0-9]*|0) echo "$1 must be a positive integer (got $2)" >&2; exit 2 ;;
  esac
}

nonnegative_int() {
  case "$2" in
    ''|*[!0-9]*) echo "$1 must be a non-negative integer (got $2)" >&2; exit 2 ;;
  esac
}

positive_csv() {
  old_ifs=$IFS
  IFS=,
  set -- $2
  IFS=$old_ifs
  [ "$#" -gt 0 ] || { echo "$1 must not be empty" >&2; exit 2; }
  for number in "$@"; do
    positive_int "$1" "$number"
  done
}

positive_csv RELOAD_BENCH_WORKERS "$WORKERS_CSV"
positive_csv RELOAD_BENCH_TABLES "$TABLES_CSV"
positive_csv RELOAD_BENCH_CHUNKS "$CHUNKS_CSV"
positive_int RELOAD_BENCH_BASE_WORKERS "$BASE_WORKERS"
positive_int RELOAD_BENCH_BASE_TABLES "$BASE_TABLES"
positive_int RELOAD_BENCH_BASE_CHUNK_ROWS "$BASE_CHUNK_ROWS"
positive_int RELOAD_BENCH_NARROW_ROWS "$NARROW_ROWS"
positive_int RELOAD_BENCH_NARROW_PAYLOAD_BYTES "$NARROW_PAYLOAD_BYTES"
positive_int RELOAD_BENCH_WIDE_ROWS "$WIDE_ROWS"
positive_int RELOAD_BENCH_WIDE_PAYLOAD_BYTES "$WIDE_PAYLOAD_BYTES"
nonnegative_int RELOAD_BENCH_WARMUPS "$WARMUPS"
positive_int RELOAD_BENCH_SAMPLES "$SAMPLES"
positive_int RELOAD_BENCH_TIMEOUT_SECONDS "$TIMEOUT_SECONDS"

MAX_TABLES=$BASE_TABLES
TABLE_MATRIX_REQUESTED_TABLES=1
case "$MATRIX" in
  tables|all)
    old_ifs=$IFS
    IFS=,
    set -- $TABLES_CSV
    IFS=$old_ifs
    for table_count in "$@"; do
      if [ "$table_count" -gt "$TABLE_MATRIX_REQUESTED_TABLES" ]; then
        TABLE_MATRIX_REQUESTED_TABLES=$table_count
      fi
      if [ "$table_count" -gt "$MAX_TABLES" ]; then MAX_TABLES=$table_count; fi
    done
    ;;
esac

MAX_WORKERS=$BASE_WORKERS
case "$MATRIX" in
  workers|all)
    old_ifs=$IFS
    IFS=,
    set -- $WORKERS_CSV
    IFS=$old_ifs
    for worker_count in "$@"; do
      if [ "$worker_count" -gt "$MAX_WORKERS" ]; then MAX_WORKERS=$worker_count; fi
    done
    ;;
esac

MAX_ACTIVE_ROUTES=1
case "$MATRIX" in
  workers|all)
    worker_routes=$((BASE_TABLES * MAX_WORKERS))
    if [ "$worker_routes" -gt "$MAX_ACTIVE_ROUTES" ]; then MAX_ACTIVE_ROUTES=$worker_routes; fi
    ;;
esac
case "$MATRIX" in
  tables|all)
    table_routes=$((MAX_TABLES * BASE_WORKERS))
    if [ "$table_routes" -gt "$MAX_ACTIVE_ROUTES" ]; then MAX_ACTIVE_ROUTES=$table_routes; fi
    ;;
esac
case "$MATRIX" in
  chunks|all)
    chunk_routes=$((BASE_TABLES * BASE_WORKERS))
    if [ "$chunk_routes" -gt "$MAX_ACTIVE_ROUTES" ]; then MAX_ACTIVE_ROUTES=$chunk_routes; fi
    ;;
esac

# Each live route conservatively reserves 32 MiB. Keep a complete WAL batch of further headroom so
# the process-wide budget cannot silently serialize the configuration being measured. Never go
# below the production default, even for a one-route matrix.
minimum_inflight=$((MAX_ACTIVE_ROUTES * RELOAD_ROUTE_RESERVATION_BYTES + MAX_BYTES))
if [ "$minimum_inflight" -lt 536870912 ]; then minimum_inflight=536870912; fi
MAX_INFLIGHT_BYTES="${RELOAD_BENCH_MAX_INFLIGHT_BYTES:-$minimum_inflight}"
positive_int RELOAD_BENCH_MAX_INFLIGHT_BYTES "$MAX_INFLIGHT_BYTES"
if [ "$MAX_INFLIGHT_BYTES" -lt "$minimum_inflight" ] || [ "$MAX_INFLIGHT_BYTES" -lt "$MAX_BYTES" ]; then
  echo "RELOAD_BENCH_MAX_INFLIGHT_BYTES=$MAX_INFLIGHT_BYTES is too small; need at least $minimum_inflight for $MAX_ACTIVE_ROUTES active reload routes plus WAL headroom" >&2
  exit 2
fi

csv_union() {
  python3 -c 'import sys
seen=[]
for value in sys.argv[1:]:
    for item in value.split(","):
        if item and item not in seen: seen.append(item)
print(",".join(seen))' "$@"
}

EFFECTIVE_TABLE_CAPS=$BASE_TABLES
EFFECTIVE_WORKERS=$BASE_WORKERS
EFFECTIVE_CHUNKS=$BASE_CHUNK_ROWS
case "$MATRIX" in tables|all) EFFECTIVE_TABLE_CAPS="$(csv_union "$EFFECTIVE_TABLE_CAPS" "$TABLES_CSV")" ;; esac
case "$MATRIX" in workers|all) EFFECTIVE_WORKERS="$(csv_union "$EFFECTIVE_WORKERS" "$WORKERS_CSV")" ;; esac
case "$MATRIX" in chunks|all) EFFECTIVE_CHUNKS="$(csv_union "$EFFECTIVE_CHUNKS" "$CHUNKS_CSV")" ;; esac

absolute_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$PWD" "$1" ;;
  esac
}

CANDIDATE_EXTRACTOR_BIN="$(absolute_path "${CANDIDATE_EXTRACTOR_BIN:-target/release/walrus-extractor}")"
BASELINE_EXTRACTOR_BIN="${BASELINE_EXTRACTOR_BIN:-}"
if [ -n "$BASELINE_EXTRACTOR_BIN" ]; then
  BASELINE_EXTRACTOR_BIN="$(absolute_path "$BASELINE_EXTRACTOR_BIN")"
fi
TRANSFORMER_BIN="$PWD/target/release/walrus-transformer"
VERIFY_BIN="$PWD/target/release/reload_verify"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-reload-${FIXTURE}-${MATRIX}-$$"
RUN_DIR="${PERF_OUTPUT_DIR:-target/perf/$RUN_ID}"
RUN_DIR="$(absolute_path "$RUN_DIR")"
RUNTIME_ROOT="$(mktemp -d)"
EXTENSION_DIR="$RUNTIME_ROOT/duckdb-extensions"
COMPOSE=(docker compose -f deploy/docker/docker-compose.yml)
EXTRACTOR_ADDR=127.0.0.1:8188
TRANSFORMER_ADDR=127.0.0.1:8190
EXTRACTOR_PID=""
TRANSFORMER_PID=""
STACK_STARTED=false
FINALIZED=false
RUN_INITIALIZED=false

stop_process() {
  local pid=$1
  local label=$2
  local attempts=0
  local state
  [ -n "$pid" ] || return 0
  kill -TERM "$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null && [ "$attempts" -lt 100 ]; do
    state="$(ps -o stat= -p "$pid" 2>/dev/null | awk '{$1=$1};1')"
    case "$state" in Z*) break ;; esac
    sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    state="$(ps -o stat= -p "$pid" 2>/dev/null | awk '{$1=$1};1')"
    case "$state" in
      Z*) ;;
      *)
        echo "$label did not stop after SIGTERM; sending SIGKILL" >&2
        kill -KILL "$pid" 2>/dev/null || true
        ;;
    esac
  fi
  wait "$pid" 2>/dev/null || true
}

stop_extractor() {
  if [ -n "$EXTRACTOR_PID" ]; then
    stop_process "$EXTRACTOR_PID" extractor
    EXTRACTOR_PID=""
  fi
}

stop_transformer() {
  if [ -n "$TRANSFORMER_PID" ]; then
    stop_process "$TRANSFORMER_PID" transformer
    TRANSFORMER_PID=""
  fi
}

cleanup() {
  stop_extractor
  stop_transformer
  if [ "$STACK_STARTED" = true ]; then
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  fi
  rm -rf "$RUNTIME_ROOT" 2>/dev/null || true
}

on_exit() {
  exit_code=$?
  trap - EXIT
  if [ "$RUN_INITIALIZED" = true ] && [ "$FINALIZED" != true ]; then
    python3 scripts/perf_report.py reload-finish \
      --run-dir "$RUN_DIR" \
      --failure-reason "harness exited unexpectedly with status $exit_code" || true
  fi
  cleanup
  exit "$exit_code"
}
trap on_exit EXIT

source_psql() {
  "${COMPOSE[@]}" exec -T source-pg psql -X -qAt -v ON_ERROR_STOP=1 \
    -U postgres -d walrus -c "$1"
}

control_psql() {
  "${COMPOSE[@]}" exec -T control-pg psql -X -qAt -v ON_ERROR_STOP=1 \
    -U postgres -d walrus_control -c "$1"
}

wait_ready() {
  local addr=$1
  local timeout=$2
  local pid=$3
  local deadline=$((SECONDS + timeout))
  local state
  while [ "$SECONDS" -lt "$deadline" ]; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "$addr process $pid exited before readiness" >&2
      return 1
    fi
    state="$(ps -o stat= -p "$pid" 2>/dev/null | awk '{$1=$1};1')"
    case "$state" in
      Z*) echo "$addr process $pid exited before readiness" >&2; return 1 ;;
    esac
    if [ "$(curl --max-time 1 -s -o /dev/null -w '%{http_code}' "http://$addr/ready" 2>/dev/null)" = 200 ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "$addr did not become ready within ${timeout}s" >&2
  return 1
}

scrape() {
  curl --max-time 2 -fsS "http://$1/metrics" 2>/dev/null || true
}

mval() {
  awk -v n="$2" '$1 ~ ("^" n "([{ ]|$)") {s += $2} END {printf "%.6f", s+0}' <<EOF
$1
EOF
}

now_ns() {
  python3 -c 'import time; print(time.monotonic_ns())'
}

seconds_between() {
  python3 -c 'import sys; print((int(sys.argv[2])-int(sys.argv[1]))/1e9)' "$1" "$2"
}

cpu_seconds() {
  cpu_text="$(ps -o time= -p "$1" 2>/dev/null | awk '{$1=$1};1')"
  if [ -z "$cpu_text" ]; then printf '0\n'; return; fi
  python3 -c 'import sys; value=sys.argv[1]; days=0
if "-" in value: day,value=value.split("-",1); days=int(day)
parts=value.split(":")
if len(parts)==2: parts=["0",*parts]
print(days*86400+int(parts[0])*3600+int(parts[1])*60+float(parts[2]))' "$cpu_text"
}

rss_bytes() {
  rss="$(ps -o rss= -p "$1" 2>/dev/null | awk '{$1=$1};1')"
  if [ -z "$rss" ]; then printf '0\n'; else printf '%s\n' "$((rss * 1024))"; fi
}

sha256_file() {
  python3 -c 'import hashlib,sys
h=hashlib.sha256()
with open(sys.argv[1], "rb") as source:
    for block in iter(lambda: source.read(1024 * 1024), b""): h.update(block)
print(h.hexdigest())' "$1"
}

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export WALRUS_CONFIG="$PWD/walrus.yaml"
export WALRUS_SOURCE_DB_URL=postgres://postgres:postgres@localhost:5432/walrus
export WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@localhost:5433/walrus_control
export WALRUS_OBJECT_STORE__BUCKET=walrus
export WALRUS_OBJECT_STORE__ENDPOINT=http://localhost:9000
export WALRUS_OBJECT_STORE__REGION=us-east-1
export WALRUS_DUCKLAKE__CATALOG_URL=postgres://postgres:postgres@localhost:5433/walrus_ducklake
export WALRUS_DUCKLAKE__METADATA_SCHEMA=walrus_reload_bench
export WALRUS_DUCKLAKE__DATA_PATH=s3://walrus/ducklake/reload-bench/
export WALRUS_DUCKLAKE__EXTENSION_DIRECTORY="$EXTENSION_DIR"
export WALRUS_DUCKLAKE__INSTALL_EXTENSIONS=false

echo "=== reload benchmark: fixture=$FIXTURE matrix=$MATRIX bundle=$RUN_DIR ==="
echo "--- building release binaries"
if [ "$CANDIDATE_EXTRACTOR_BIN" = "$PWD/target/release/walrus-extractor" ]; then
  cargo build --release -p extractor --bin walrus-extractor
fi
cargo build --release -p transformer --bin walrus-transformer
cargo build --release -p e2e --bin reload_verify
[ -x "$CANDIDATE_EXTRACTOR_BIN" ] || { echo "candidate extractor is not executable: $CANDIDATE_EXTRACTOR_BIN" >&2; exit 2; }
if [ -n "$BASELINE_EXTRACTOR_BIN" ]; then
  [ -x "$BASELINE_EXTRACTOR_BIN" ] || { echo "baseline extractor is not executable: $BASELINE_EXTRACTOR_BIN" >&2; exit 2; }
fi
CANDIDATE_SHA256="$(sha256_file "$CANDIDATE_EXTRACTOR_BIN")"
BASELINE_SHA256=""
if [ -n "$BASELINE_EXTRACTOR_BIN" ]; then BASELINE_SHA256="$(sha256_file "$BASELINE_EXTRACTOR_BIN")"; fi

python3 scripts/perf_report.py reload-start \
  --run-dir "$RUN_DIR" \
  --matrix "$MATRIX" \
  --fixtures "$FIXTURES_CSV" \
  --narrow-rows "$NARROW_ROWS" \
  --narrow-payload-bytes "$NARROW_PAYLOAD_BYTES" \
  --wide-rows "$WIDE_ROWS" \
  --wide-payload-bytes "$WIDE_PAYLOAD_BYTES" \
  --workers "$WORKERS_CSV" \
  --tables "$TABLES_CSV" \
  --chunks "$CHUNKS_CSV" \
  --base-workers "$BASE_WORKERS" \
  --base-tables "$BASE_TABLES" \
  --base-chunk-rows "$BASE_CHUNK_ROWS" \
  --effective-max-concurrent-reloads "$EFFECTIVE_TABLE_CAPS" \
  --effective-workers-per-table "$EFFECTIVE_WORKERS" \
  --effective-chunk-rows "$EFFECTIVE_CHUNKS" \
  --max-bytes "$MAX_BYTES" \
  --max-rows 100000 \
  --max-fill 5s \
  --heartbeat-idle-after 1s \
  --max-inflight-bytes "$MAX_INFLIGHT_BYTES" \
  --warmups "$WARMUPS" \
  --samples "$SAMPLES" \
  --sample-interval "$SAMPLE_INTERVAL" \
  --timeout-seconds "$TIMEOUT_SECONDS" \
  --candidate-bin "$CANDIDATE_EXTRACTOR_BIN" \
  --candidate-sha256 "$CANDIDATE_SHA256" \
  --baseline-bin "$BASELINE_EXTRACTOR_BIN" \
  --baseline-sha256 "$BASELINE_SHA256"
RUN_INITIALIZED=true

echo "--- resetting the benchmark compose volumes"
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
STACK_STARTED=true
"${COMPOSE[@]}" up --wait
for migration in migrations/source/0001_publication.sql migrations/source/0002_ddl_triggers.sql \
  migrations/source/0003_reload_signal.sql migrations/source/0004_reload_event.sql; do
  "${COMPOSE[@]}" exec -T source-pg psql -X -U postgres -d walrus -v ON_ERROR_STOP=1 \
    -f - <"$migration"
done

echo "--- creating deterministic source fixtures (seeding is outside measurements)"
for fixture_name in narrow wide; do
  table_no=1
  while [ "$table_no" -le "$MAX_TABLES" ]; do
    source_psql "DROP TABLE IF EXISTS public.bench_${fixture_name}_${table_no};" >/dev/null
    table_no=$((table_no + 1))
  done
done

seed_fixture() {
  fixture_name=$1
  row_count=$2
  payload_bytes=$3
  source_psql "CREATE TABLE public.bench_${fixture_name}_1 (id bigint PRIMARY KEY, payload text NOT NULL);
    INSERT INTO public.bench_${fixture_name}_1
    SELECT g, left(repeat(md5(g::text), (($payload_bytes + 31) / 32)), $payload_bytes)
    FROM generate_series(1, $row_count) AS g;
    ANALYZE public.bench_${fixture_name}_1;" >/dev/null
  table_no=2
  while [ "$table_no" -le "$MAX_TABLES" ]; do
    source_psql "CREATE TABLE public.bench_${fixture_name}_${table_no}
      (LIKE public.bench_${fixture_name}_1 INCLUDING ALL);
      INSERT INTO public.bench_${fixture_name}_${table_no}
      SELECT * FROM public.bench_${fixture_name}_1;
      ANALYZE public.bench_${fixture_name}_${table_no};" >/dev/null
    table_no=$((table_no + 1))
  done
}

case "$FIXTURE" in
  narrow) seed_fixture narrow "$NARROW_ROWS" "$NARROW_PAYLOAD_BYTES" ;;
  wide) seed_fixture wide "$WIDE_ROWS" "$WIDE_PAYLOAD_BYTES" ;;
  all)
    seed_fixture narrow "$NARROW_ROWS" "$NARROW_PAYLOAD_BYTES"
    seed_fixture wide "$WIDE_ROWS" "$WIDE_PAYLOAD_BYTES"
    ;;
esac

"$TRANSFORMER_BIN" --install-duckdb-extensions "$EXTENSION_DIR"
env -u WALRUS_SOURCE_DB_URL \
  WALRUS_INSTANCE=reload-bench-transformer "$TRANSFORMER_BIN" --migrate-ducklake-catalog

start_extractor() {
  implementation=$1
  binary=$2
  concurrent_tables=$3
  workers=$4
  chunk_rows=$5
  stop_extractor
  echo "--- start $implementation extractor: tables=$concurrent_tables workers=$workers chunk=$chunk_rows"
  if [ "$implementation" = baseline ]; then
    # A genuinely pre-feature binary may reject an unknown workers key. Scrub any ambient value;
    # the baseline path is intrinsically the one-worker baseline recorded in the sample.
    env -u WALRUS_CONFIG -u WALRUS_RELOAD_WORKERS_PER_TABLE \
    -u WALRUS_DUCKLAKE__CATALOG_URL -u WALRUS_DUCKLAKE__METADATA_SCHEMA \
    -u WALRUS_DUCKLAKE__DATA_PATH -u WALRUS_DUCKLAKE__EXTENSION_DIRECTORY \
    -u WALRUS_DUCKLAKE__INSTALL_EXTENSIONS \
    WALRUS_INSTANCE=reload-bench-extractor WALRUS_SLOT_NAME=reload_bench_slot \
    WALRUS_PUBLICATION_NAME=walrus_pub WALRUS_MANAGE_PUBLICATION=false \
    WALRUS_HEALTH_ADDR="$EXTRACTOR_ADDR" WALRUS_STARTUP_DEADLINE=300s \
    WALRUS_MAX_FILL=5s WALRUS_MAX_ROWS=100000 WALRUS_MAX_BYTES="$MAX_BYTES" \
    WALRUS_MAX_INFLIGHT_BYTES="$MAX_INFLIGHT_BYTES" WALRUS_HEARTBEAT_IDLE_AFTER=1s \
    WALRUS_MAX_CONCURRENT_RELOADS="$concurrent_tables" \
    WALRUS_RELOAD_CHUNK_ROWS="$chunk_rows" \
      "$binary" >>"$RUN_DIR/extractor.log" 2>&1 &
  else
    env -u WALRUS_DUCKLAKE__CATALOG_URL -u WALRUS_DUCKLAKE__METADATA_SCHEMA \
    -u WALRUS_DUCKLAKE__DATA_PATH -u WALRUS_DUCKLAKE__EXTENSION_DIRECTORY \
    -u WALRUS_DUCKLAKE__INSTALL_EXTENSIONS \
    WALRUS_INSTANCE=reload-bench-extractor WALRUS_SLOT_NAME=reload_bench_slot \
    WALRUS_PUBLICATION_NAME=walrus_pub WALRUS_MANAGE_PUBLICATION=false \
    WALRUS_HEALTH_ADDR="$EXTRACTOR_ADDR" WALRUS_STARTUP_DEADLINE=300s \
    WALRUS_MAX_FILL=5s WALRUS_MAX_ROWS=100000 WALRUS_MAX_BYTES="$MAX_BYTES" \
    WALRUS_MAX_INFLIGHT_BYTES="$MAX_INFLIGHT_BYTES" WALRUS_HEARTBEAT_IDLE_AFTER=1s \
    WALRUS_MAX_CONCURRENT_RELOADS="$concurrent_tables" \
    WALRUS_RELOAD_WORKERS_PER_TABLE="$workers" \
    WALRUS_RELOAD_CHUNK_ROWS="$chunk_rows" \
      "$binary" >>"$RUN_DIR/extractor.log" 2>&1 &
  fi
  EXTRACTOR_PID=$!
  wait_ready "$EXTRACTOR_ADDR" 300 "$EXTRACTOR_PID"
}

start_transformer() {
  stop_transformer
  env -u WALRUS_SOURCE_DB_URL \
  WALRUS_INSTANCE=reload-bench-transformer WALRUS_HEALTH_ADDR="$TRANSFORMER_ADDR" \
  WALRUS_POLL_INTERVAL=250ms WALRUS_STARTUP_DEADLINE=300s \
    "$TRANSFORMER_BIN" >>"$RUN_DIR/transformer.log" 2>&1 &
  TRANSFORMER_PID=$!
  wait_ready "$TRANSFORMER_ADDR" 300 "$TRANSFORMER_PID"
}

# Bootstrap all benchmark tables through the candidate's real first-start path. This also warms the
# source heap before timed source-WAL reload requests.
start_extractor candidate "$CANDIDATE_EXTRACTOR_BIN" "$BASE_TABLES" "$BASE_WORKERS" "$BASE_CHUNK_ROWS"
start_transformer
bootstrap_expected=$MAX_TABLES
if [ "$FIXTURE" = all ]; then bootstrap_expected=$((MAX_TABLES * 2)); fi
bootstrap_deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
while :; do
  bootstrap_complete="$(control_psql "SELECT count(*) FROM public.walrus_table_reload
    WHERE source_table LIKE 'bench\\_%' ESCAPE '\\' AND status = 'complete';")"
  if [ "$bootstrap_complete" -ge "$bootstrap_expected" ]; then break; fi
  [ "$(date +%s)" -lt "$bootstrap_deadline" ] || {
    echo "initial non-empty reconciliation did not complete ($bootstrap_complete/$bootstrap_expected)" >&2
    exit 1
  }
  sleep 1
done

CONFIGS=()
add_config() {
  CONFIGS[${#CONFIGS[@]}]="$1|$2|$3|$4|$5|$6|$7|$8"
}

add_worker_matrix() {
  fixture_name=$1
  old_ifs=$IFS; IFS=,; set -- $WORKERS_CSV; IFS=$old_ifs
  for workers in "$@"; do
    add_config candidate "$fixture_name" workers "$BASE_TABLES" "$BASE_TABLES" "$workers" "$BASE_CHUNK_ROWS" "$CANDIDATE_EXTRACTOR_BIN"
  done
  if [ -n "$BASELINE_EXTRACTOR_BIN" ]; then
    add_config baseline "$fixture_name" workers "$BASE_TABLES" "$BASE_TABLES" 1 "$BASE_CHUNK_ROWS" "$BASELINE_EXTRACTOR_BIN"
  fi
}

add_table_matrix() {
  fixture_name=$1
  old_ifs=$IFS; IFS=,; set -- $TABLES_CSV; IFS=$old_ifs
  for concurrent_tables in "$@"; do
    add_config candidate "$fixture_name" tables "$TABLE_MATRIX_REQUESTED_TABLES" "$concurrent_tables" "$BASE_WORKERS" "$BASE_CHUNK_ROWS" "$CANDIDATE_EXTRACTOR_BIN"
    if [ -n "$BASELINE_EXTRACTOR_BIN" ]; then
      add_config baseline "$fixture_name" tables "$TABLE_MATRIX_REQUESTED_TABLES" "$concurrent_tables" 1 "$BASE_CHUNK_ROWS" "$BASELINE_EXTRACTOR_BIN"
    fi
  done
}

add_chunk_matrix() {
  fixture_name=$1
  old_ifs=$IFS; IFS=,; set -- $CHUNKS_CSV; IFS=$old_ifs
  for chunk_rows in "$@"; do
    add_config candidate "$fixture_name" chunks "$BASE_TABLES" "$BASE_TABLES" "$BASE_WORKERS" "$chunk_rows" "$CANDIDATE_EXTRACTOR_BIN"
    if [ -n "$BASELINE_EXTRACTOR_BIN" ]; then
      add_config baseline "$fixture_name" chunks "$BASE_TABLES" "$BASE_TABLES" 1 "$chunk_rows" "$BASELINE_EXTRACTOR_BIN"
    fi
  done
}

old_ifs=$IFS; IFS=,; set -- $FIXTURES_CSV; IFS=$old_ifs
for fixture_name in "$@"; do
  case "$MATRIX" in workers|all) add_worker_matrix "$fixture_name" ;; esac
  case "$MATRIX" in tables|all) add_table_matrix "$fixture_name" ;; esac
  case "$MATRIX" in chunks|all) add_chunk_matrix "$fixture_name" ;; esac
done

request_reload_group() {
  fixture_name=$1
  table_count=$2
  source_psql "WITH payload AS (
      SELECT gen_random_uuid() AS request_id,
             format('bench_${fixture_name}_%s', g) AS source_table
      FROM generate_series(1, $table_count) AS g
    ), inserted AS (
      INSERT INTO public.walrus_reload_event
        (event_id, request_id, event_kind, scope, source_schema, source_table)
      SELECT request_id, request_id, 'request', 'table', 'public', source_table FROM payload
      RETURNING request_id
    )
    SELECT string_agg(request_id::text, ',' ORDER BY request_id::text) FROM inserted;"
}

run_one() {
  implementation=$1
  fixture_name=$2
  matrix_name=$3
  tables_requested=$4
  max_concurrent_reloads=$5
  workers=$6
  chunk_rows=$7
  binary=$8
  iteration=$9
  warmup=${10}

  start_extractor "$implementation" "$binary" "$max_concurrent_reloads" "$workers" "$chunk_rows"
  extractor_metrics_start="$(scrape "$EXTRACTOR_ADDR")"
  rows_metric_start="$(mval "$extractor_metrics_start" walrus_reload_rows_exported_total)"
  chunks_metric_start="$(mval "$extractor_metrics_start" walrus_reload_chunks_total)"
  source_stats_start="$(source_psql "SELECT blks_read || '|' || blks_hit FROM pg_stat_database
    WHERE datname = current_database();")"
  source_read_start=${source_stats_start%%|*}
  source_hit_start=${source_stats_start#*|}
  cpu_start="$(cpu_seconds "$EXTRACTOR_PID")"
  request_ids="$(request_reload_group "$fixture_name" "$tables_requested")"
  request_started_ns="$(now_ns)"

  peak_copy=0
  peak_copy_tables=0
  peak_wal_lag=0
  extractor_peak_rss=0
  transformer_peak_rss=0
  slot_min=999999
  slot_max=0
  walsender_min=999999
  walsender_max=0
  failed_reason=""
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while :; do
    if ! kill -0 "$EXTRACTOR_PID" 2>/dev/null || ! kill -0 "$TRANSFORMER_PID" 2>/dev/null; then
      failed_reason="extractor or transformer exited before reload completion"
      break
    fi
    reload_state="$(control_psql "SELECT
        count(*) FILTER (WHERE status = 'complete') || '|' ||
        count(*) FILTER (WHERE status = 'failed') || '|' || count(*)
      FROM public.walrus_table_reload
      WHERE source_request_id = ANY(string_to_array('$request_ids', ',')::uuid[]);")"
    complete_count=${reload_state%%|*}
    state_tail=${reload_state#*|}
    failed_count=${state_tail%%|*}
    total_count=${state_tail#*|}

    source_state="$(source_psql "SELECT
        (SELECT count(*) FROM pg_replication_slots) || '|' ||
        (SELECT count(*) FROM pg_stat_replication) || '|' ||
        (SELECT count(*)
           FROM pg_stat_activity activity
          WHERE activity.backend_type = 'client backend'
            AND activity.state = 'active'
            AND upper(ltrim(activity.query)) LIKE 'COPY %'
            AND position('bench_${fixture_name}_' IN activity.query) > 0) || '|' ||
        (SELECT count(DISTINCT relation.oid)
           FROM pg_stat_activity activity
           JOIN pg_locks lock ON lock.pid = activity.pid
           JOIN pg_class relation ON relation.oid = lock.relation
           JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
          WHERE activity.backend_type = 'client backend'
            AND activity.state = 'active'
            AND upper(ltrim(activity.query)) LIKE 'COPY %'
            AND lock.granted AND lock.mode = 'AccessShareLock'
            AND namespace.nspname = 'public'
            AND relation.relname LIKE 'bench\_${fixture_name}\_%' ESCAPE '\') || '|' ||
        COALESCE((SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
          FROM pg_replication_slots WHERE slot_name = 'reload_bench_slot'), 0);")"
    slots=${source_state%%|*}; source_state=${source_state#*|}
    walsenders=${source_state%%|*}; source_state=${source_state#*|}
    active_copy=${source_state%%|*}; source_state=${source_state#*|}
    active_copy_tables=${source_state%%|*}; wal_lag=${source_state#*|}
    [ "$slots" -lt "$slot_min" ] && slot_min=$slots
    [ "$slots" -gt "$slot_max" ] && slot_max=$slots
    [ "$walsenders" -lt "$walsender_min" ] && walsender_min=$walsenders
    [ "$walsenders" -gt "$walsender_max" ] && walsender_max=$walsenders
    [ "$active_copy" -gt "$peak_copy" ] && peak_copy=$active_copy
    [ "$active_copy_tables" -gt "$peak_copy_tables" ] && peak_copy_tables=$active_copy_tables
    [ "$wal_lag" -gt "$peak_wal_lag" ] && peak_wal_lag=$wal_lag
    current_rss="$(rss_bytes "$EXTRACTOR_PID")"
    [ "$current_rss" -gt "$extractor_peak_rss" ] && extractor_peak_rss=$current_rss
    current_rss="$(rss_bytes "$TRANSFORMER_PID")"
    [ "$current_rss" -gt "$transformer_peak_rss" ] && transformer_peak_rss=$current_rss

    if [ "$failed_count" -gt 0 ]; then
      failed_reason="one or more reload attempts reached failed"
      break
    fi
    if [ "$complete_count" -eq "$tables_requested" ] && [ "$total_count" -eq "$tables_requested" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      failed_reason="reload did not complete within ${TIMEOUT_SECONDS}s"
      break
    fi
    sleep "$SAMPLE_INTERVAL"
  done
  published_ns="$(now_ns)"

  extractor_metrics_end="$(scrape "$EXTRACTOR_ADDR")"
  rows_metric_end="$(mval "$extractor_metrics_end" walrus_reload_rows_exported_total)"
  chunks_metric_end="$(mval "$extractor_metrics_end" walrus_reload_chunks_total)"
  rows_exported="$(awk -v a="$rows_metric_start" -v b="$rows_metric_end" 'BEGIN {printf "%.0f", b-a}')"
  chunks_exported="$(awk -v a="$chunks_metric_start" -v b="$chunks_metric_end" 'BEGIN {printf "%.0f", b-a}')"
  cpu_end="$(cpu_seconds "$EXTRACTOR_PID")"
  extractor_cpu="$(awk -v a="$cpu_start" -v b="$cpu_end" 'BEGIN {printf "%.6f", b-a}')"
  source_stats_end="$(source_psql "SELECT blks_read || '|' || blks_hit FROM pg_stat_database
    WHERE datname = current_database();")"
  source_read_end=${source_stats_end%%|*}
  source_hit_end=${source_stats_end#*|}
  source_blks_read=$((source_read_end - source_read_start))
  source_blks_hit=$((source_hit_end - source_hit_start))

  row_count=$NARROW_ROWS
  if [ "$fixture_name" = wide ]; then row_count=$WIDE_ROWS; fi
  rows_expected=$((row_count * tables_requested))
  source_bytes="$(source_psql "SELECT sum(pg_table_size(format('public.bench_${fixture_name}_%s', g)::regclass))
    FROM generate_series(1, $tables_requested) AS g;")"
  export_seconds="$(control_psql "SELECT COALESCE(EXTRACT(EPOCH FROM (
      max(m.created_at) FILTER (WHERE m.marker_kind = 'end') -
      min(m.created_at) FILTER (WHERE m.marker_kind = 'baseline'))), 0)
    FROM public.walrus_table_reload r JOIN public.walrus_table_reload_marker m USING (reload_id)
    WHERE r.source_request_id = ANY(string_to_array('$request_ids', ',')::uuid[]);")"
  publish_seconds="$(seconds_between "$request_started_ns" "$published_ns")"
  rows_per_second="$(awk -v rows="$rows_expected" -v secs="$export_seconds" 'BEGIN {if (secs>0) printf "%.6f", rows/secs; else print 0}')"
  source_mib_per_second="$(awk -v bytes="$source_bytes" -v secs="$export_seconds" 'BEGIN {if (secs>0) printf "%.6f", bytes/1048576/secs; else print 0}')"
  chunk_files="$(control_psql "SELECT COALESCE(sum(chunk_no), 0) FROM public.walrus_table_reload
    WHERE source_request_id = ANY(string_to_array('$request_ids', ',')::uuid[]);")"

  mirror_diff=""
  if [ "$warmup" = 0 ] && [ -z "$failed_reason" ]; then
    stop_transformer
    verify_args=()
    table_no=1
    while [ "$table_no" -le "$tables_requested" ]; do
      verify_args[${#verify_args[@]}]="bench_${fixture_name}_${table_no}"
      table_no=$((table_no + 1))
    done
    set +e
    verify_json="$("$VERIFY_BIN" "$EXTENSION_DIR" "${verify_args[@]}" 2>>"$RUN_DIR/verify.log")"
    verify_status=$?
    set -e
    if [ "$verify_status" -ne 0 ]; then
      failed_reason="exact source/mirror verification failed"
      mirror_diff=-1
    else
      mirror_diff="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["difference_rows"])' "$verify_json")"
    fi
    start_transformer
  fi

  status=success
  if [ -n "$failed_reason" ]; then status=failed; fi
  if [ "$rows_exported" -ne "$rows_expected" ]; then
    status=failed
    failed_reason="exported $rows_exported rows; expected $rows_expected"
  fi
  if [ -n "$mirror_diff" ] && [ "$mirror_diff" -ne 0 ]; then
    status=failed
    failed_reason="exact source/mirror verification found $mirror_diff differing rows"
  fi
  if ! awk -v seconds="$export_seconds" 'BEGIN {exit !(seconds > 0)}'; then
    status=failed
    failed_reason="F-to-H duration was unavailable or zero"
  fi
  active_tables=$max_concurrent_reloads
  if [ "$active_tables" -gt "$tables_requested" ]; then active_tables=$tables_requested; fi
  max_allowed_copy=$((active_tables * workers))
  if [ "$peak_copy" -gt "$max_allowed_copy" ]; then
    status=failed
    failed_reason="COPY connection cap exceeded: observed $peak_copy; allowed $max_allowed_copy"
  fi
  if [ "$max_allowed_copy" -gt 1 ] && [ "$peak_copy" -lt 2 ]; then
    status=failed
    failed_reason="parallel configuration never demonstrated concurrent active COPY routes (peak=$peak_copy)"
  fi
  if [ "$peak_copy_tables" -gt "$active_tables" ]; then
    status=failed
    failed_reason="active table cap exceeded: observed $peak_copy_tables; allowed $active_tables"
  fi
  if [ "$matrix_name" = tables ] && [ "$active_tables" -gt 1 ] && [ "$peak_copy_tables" -lt 2 ]; then
    status=failed
    failed_reason="parallel table-cap configuration never demonstrated concurrent active tables (peak=$peak_copy_tables)"
  fi
  if [ "$slot_min" -ne 1 ] || [ "$slot_max" -ne 1 ] || \
     [ "$walsender_min" -ne 1 ] || [ "$walsender_max" -ne 1 ]; then
    status=failed
    failed_reason="one-slot/one-walsender invariant failed"
  fi
  failed_reason=${failed_reason//$'\n'/ }
  failed_reason=${failed_reason//,/;}

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$implementation" "$fixture_name" "$matrix_name" "$tables_requested" "$max_concurrent_reloads" "$workers" "$chunk_rows" \
    "$iteration" "$warmup" "$status" "$failed_reason" "$rows_expected" "$rows_exported" \
    "$source_bytes" "$export_seconds" "$publish_seconds" "$rows_per_second" \
    "$source_mib_per_second" "$extractor_cpu" "$extractor_peak_rss" "$transformer_peak_rss" \
    "$source_blks_read" "$source_blks_hit" "$peak_copy" "$peak_copy_tables" "$peak_wal_lag" "$chunk_files" \
    "$slot_min" "$slot_max" "$walsender_min" "$walsender_max" "$mirror_diff" \
    >>"$RUN_DIR/reload-samples.csv"

  echo "    $status $implementation/$fixture_name/$matrix_name requested=$tables_requested table-cap=$max_concurrent_reloads workers=$workers chunk=$chunk_rows F→H=${export_seconds}s rows/s=$rows_per_second"
  [ "$status" = success ]
}

rounds=$((WARMUPS + SAMPLES))
config_count=${#CONFIGS[@]}
round=0
while [ "$round" -lt "$rounds" ]; do
  warmup=0
  iteration=$((round - WARMUPS + 1))
  if [ "$round" -lt "$WARMUPS" ]; then
    warmup=1
    iteration=$((round + 1))
  fi
  offset=0
  while [ "$offset" -lt "$config_count" ]; do
    index=$(( (round + offset) % config_count ))
    config=${CONFIGS[$index]}
    old_ifs=$IFS; IFS='|'; set -- $config; IFS=$old_ifs
    run_one "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$iteration" "$warmup"
    offset=$((offset + 1))
  done
  round=$((round + 1))
done

set +e
python3 scripts/perf_report.py reload-finish --run-dir "$RUN_DIR"
REPORT_STATUS=$?
set -e
FINALIZED=true
cleanup
trap - EXIT
exit "$REPORT_STATUS"
