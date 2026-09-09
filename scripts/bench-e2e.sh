#!/usr/bin/env bash
# Local end-to-end performance harness. This is intentionally not a CI performance gate: absolute
# numbers are hardware-relative. Every invocation writes a self-describing bundle under target/perf.
#
#   scripts/bench-e2e.sh <mixed|wide_text|large_txn>
#   PERF_MODE=cpu PERF_TARGET=transformer scripts/bench-e2e.sh wide_text
#   PERF_MODE=heap PERF_TARGET=extractor scripts/bench-e2e.sh large_txn
#   PERF_MODE=async PERF_TARGET=transformer scripts/bench-e2e.sh mixed
set -Eeuo pipefail
cd "$(git rev-parse --show-toplevel)"

SCENARIO="${1:-mixed}"
PERF_MODE="${PERF_MODE:-measure}"
PERF_TARGET="${PERF_TARGET:-transformer}"
DURATION="${DURATION:-60}"
CLIENTS="${CLIENTS:-4}"
SAMPLE_INTERVAL="${PERF_SAMPLE_INTERVAL:-1}"
ASYNC_ATTACH_DELAY="${PERF_ASYNC_ATTACH_DELAY:-5}"

MAX_FILL="${WALRUS_MAX_FILL:-2s}"
MAX_ROWS="${WALRUS_MAX_ROWS:-5000}"
MAX_BYTES="${WALRUS_MAX_BYTES:-2000000}"
MAX_INFLIGHT="${WALRUS_MAX_INFLIGHT_BYTES:-4000000}"
POLL_INTERVAL="${WALRUS_POLL_INTERVAL:-1s}"

case "$SCENARIO" in
  mixed|wide_text|large_txn) ;;
  *) echo "scenario must be mixed, wide_text, or large_txn" >&2; exit 2 ;;
esac
case "$PERF_MODE" in
  measure|cpu|heap|async) ;;
  *) echo "PERF_MODE must be measure, cpu, heap, or async" >&2; exit 2 ;;
esac
case "$PERF_TARGET" in
  extractor|transformer) ;;
  *) echo "PERF_TARGET must be extractor or transformer" >&2; exit 2 ;;
esac

PROFILE="release"
PROFILE_DIR="release"
if [ "$PERF_MODE" != measure ]; then
  PROFILE="profiling"
  PROFILE_DIR="profiling"
fi

RUN_SUFFIX="$PERF_MODE"
if [ "$PERF_MODE" != measure ]; then
  RUN_SUFFIX="${PERF_MODE}-${PERF_TARGET}"
fi
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SCENARIO}-${RUN_SUFFIX}-$$"
RUN_DIR="${PERF_OUTPUT_DIR:-target/perf/$RUN_ID}"
case "$RUN_DIR" in
  /*) ;;
  *) RUN_DIR="$PWD/$RUN_DIR" ;;
esac

COMPOSE=(docker compose -f deploy/docker/docker-compose.yml)
EXTRACTOR_ADDR="127.0.0.1:8188"
TRANSFORMER_ADDR="127.0.0.1:8190"
RUNTIME_ROOT=""
EXTENSION_DIR=""
EXTRACTOR_PID=""
TRANSFORMER_PID=""
RESOURCE_SAMPLER_PID=""
SAMPLY_PID=""
STACK_STARTED=false
FINALIZED=false

if [ "$PERF_MODE" != measure ]; then
  set -- --target "$PERF_TARGET"
else
  set --
fi
python3 scripts/perf_report.py start \
  --run-dir "$RUN_DIR" \
  --mode "$PERF_MODE" \
  "$@" \
  --profile "$PROFILE" \
  --scenario "$SCENARIO" \
  --duration "$DURATION" \
  --clients "$CLIENTS" \
  --max-fill "$MAX_FILL" \
  --max-rows "$MAX_ROWS" \
  --max-bytes "$MAX_BYTES" \
  --max-inflight "$MAX_INFLIGHT" \
  --poll-interval "$POLL_INTERVAL" \
  --sample-interval "$SAMPLE_INTERVAL"
RUNTIME_ROOT="$(mktemp -d)"
EXTENSION_DIR="$RUNTIME_ROOT/duckdb-extensions"

stop_resource_sampler() {
  if [ -n "$RESOURCE_SAMPLER_PID" ]; then
    kill -TERM "$RESOURCE_SAMPLER_PID" 2>/dev/null || true
    wait "$RESOURCE_SAMPLER_PID" 2>/dev/null || true
    RESOURCE_SAMPLER_PID=""
  fi
}

stop_samply() {
  if [ -n "$SAMPLY_PID" ]; then
    kill -INT "$SAMPLY_PID" 2>/dev/null || true
    wait "$SAMPLY_PID" 2>/dev/null || true
    SAMPLY_PID=""
  fi
}

stop_services() {
  if [ -n "$TRANSFORMER_PID" ]; then
    kill -TERM "$TRANSFORMER_PID" 2>/dev/null || true
  fi
  if [ -n "$EXTRACTOR_PID" ]; then
    kill -TERM "$EXTRACTOR_PID" 2>/dev/null || true
  fi
  if [ -n "$TRANSFORMER_PID" ]; then
    wait "$TRANSFORMER_PID" 2>/dev/null || true
    TRANSFORMER_PID=""
  fi
  if [ -n "$EXTRACTOR_PID" ]; then
    wait "$EXTRACTOR_PID" 2>/dev/null || true
    EXTRACTOR_PID=""
  fi
}

cleanup() {
  stop_resource_sampler
  stop_samply
  stop_services
  if [ "$STACK_STARTED" = true ]; then
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  fi
  if [ -n "$RUNTIME_ROOT" ]; then
    rm -rf "$RUNTIME_ROOT" 2>/dev/null || true
  fi
}

on_exit() {
  exit_code=$?
  trap - EXIT
  if [ "$FINALIZED" != true ]; then
    python3 scripts/perf_report.py fail \
      --run-dir "$RUN_DIR" \
      --reason "harness exited unexpectedly with status $exit_code" || true
  fi
  cleanup
  exit "$exit_code"
}
trap on_exit EXIT

scrape() {
  curl -fsS "http://$1/metrics" 2>/dev/null
}

mval() {
  awk -v n="$2" '$1 ~ ("^" n "([{ ]|$)") {s += $2} END {printf "%.6f", s+0}' <<<"$1"
}

wait_ready() {
  addr="$1"
  timeout_seconds="$2"
  for _ in $(seq 1 "$((timeout_seconds * 2))"); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://$addr/ready" 2>/dev/null)" = 200 ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "!! $addr/ready never reached 200" >&2
  return 1
}

echo "=== perf-e2e: scenario=$SCENARIO mode=$PERF_MODE target=$PERF_TARGET ==="
echo "--- run bundle: $RUN_DIR"

if [ "$PERF_MODE" = cpu ] && ! command -v samply >/dev/null 2>&1; then
  echo "samply is required for CPU profiling: cargo install --locked samply" >&2
  exit 2
fi

STACK_STARTED=true
"${COMPOSE[@]}" up --wait
"${COMPOSE[@]}" exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0001_publication.sql
"${COMPOSE[@]}" exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0002_ddl_triggers.sql
"${COMPOSE[@]}" exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0003_reload_signal.sql
"${COMPOSE[@]}" exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0004_reload_event.sql

echo "--- building $PROFILE binaries ---"
case "$PERF_MODE" in
  measure)
    cargo build --release -p extractor -p transformer
    ;;
  cpu)
    cargo build --profile profiling -p extractor -p transformer
    ;;
  heap)
    if [ "$PERF_TARGET" = extractor ]; then
      cargo build --profile profiling -p extractor -p transformer --features extractor/dhat-heap
    else
      cargo build --profile profiling -p extractor -p transformer --features transformer/dhat-heap
    fi
    ;;
  async)
    if [ "$PERF_TARGET" = extractor ]; then
      RUSTFLAGS="--cfg tokio_unstable" cargo build --profile profiling \
        -p extractor -p transformer --features extractor/tokio-console
    else
      RUSTFLAGS="--cfg tokio_unstable" cargo build --profile profiling \
        -p extractor -p transformer --features transformer/tokio-console
    fi
    ;;
esac

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export WALRUS_CONFIG="$PWD/walrus.yaml"
export WALRUS_OBJECT_STORE__BUCKET=walrus
export WALRUS_OBJECT_STORE__ENDPOINT=http://localhost:9000
export WALRUS_OBJECT_STORE__REGION=us-east-1
export WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@localhost:5433/walrus_control
export WALRUS_DUCKLAKE__CATALOG_URL=postgres://postgres:postgres@localhost:5433/walrus_ducklake
export WALRUS_DUCKLAKE__METADATA_SCHEMA=walrus_bench
export WALRUS_DUCKLAKE__DATA_PATH=s3://walrus/ducklake/bench/
export WALRUS_DUCKLAKE__EXTENSION_DIRECTORY="$EXTENSION_DIR"
export WALRUS_DUCKLAKE__INSTALL_EXTENSIONS=false
"target/$PROFILE_DIR/walrus-transformer" --install-duckdb-extensions "$EXTENSION_DIR"
WALRUS_INSTANCE=bench-transformer-0 \
  "target/$PROFILE_DIR/walrus-transformer" --migrate-ducklake-catalog
if [ "$PERF_MODE" = heap ]; then
  export DHAT_OUTPUT="$RUN_DIR/${PERF_TARGET}-dhat-heap.json"
fi
if [ "$PERF_MODE" = async ]; then
  export TOKIO_CONSOLE_BIND=127.0.0.1:6669
  export TOKIO_CONSOLE_RECORD_PATH="$RUN_DIR/${PERF_TARGET}-tokio-console"
fi

echo "--- starting walrus-extractor ($PROFILE) on $EXTRACTOR_ADDR ---"
env -u WALRUS_DUCKLAKE__CATALOG_URL \
    -u WALRUS_DUCKLAKE__METADATA_SCHEMA \
    -u WALRUS_DUCKLAKE__DATA_PATH \
    -u WALRUS_DUCKLAKE__EXTENSION_DIRECTORY \
    -u WALRUS_DUCKLAKE__INSTALL_EXTENSIONS \
WALRUS_SOURCE_DB_URL=postgres://postgres:postgres@localhost:5432/walrus \
WALRUS_INSTANCE=bench-extractor WALRUS_SLOT_NAME=bench_slot WALRUS_PUBLICATION_NAME=walrus_pub \
WALRUS_HEALTH_ADDR="$EXTRACTOR_ADDR" WALRUS_STARTUP_DEADLINE=60s \
WALRUS_MAX_FILL="$MAX_FILL" WALRUS_MAX_ROWS="$MAX_ROWS" WALRUS_MAX_BYTES="$MAX_BYTES" \
WALRUS_MAX_INFLIGHT_BYTES="$MAX_INFLIGHT" \
  "target/$PROFILE_DIR/walrus-extractor" >"$RUN_DIR/extractor.log" 2>&1 &
EXTRACTOR_PID=$!
wait_ready "$EXTRACTOR_ADDR" 60
echo "  extractor ready (pid=$EXTRACTOR_PID)"

echo "--- starting walrus-transformer ($PROFILE) on $TRANSFORMER_ADDR ---"
WALRUS_INSTANCE=bench-transformer-0 \
WALRUS_HEALTH_ADDR="$TRANSFORMER_ADDR" WALRUS_POLL_INTERVAL="$POLL_INTERVAL" \
  "target/$PROFILE_DIR/walrus-transformer" >"$RUN_DIR/transformer.log" 2>&1 &
TRANSFORMER_PID=$!
wait_ready "$TRANSFORMER_ADDR" 40
echo "  transformer ready (pid=$TRANSFORMER_PID)"

if [ "$PERF_MODE" = cpu ]; then
  PROFILE_PID="$TRANSFORMER_PID"
  if [ "$PERF_TARGET" = extractor ]; then
    PROFILE_PID="$EXTRACTOR_PID"
  fi
  echo "--- attaching Samply to $PERF_TARGET pid=$PROFILE_PID ---"
  samply record --save-only --output "$RUN_DIR/${PERF_TARGET}-samply.json.gz" \
    --pid "$PROFILE_PID" >"$RUN_DIR/samply.log" 2>&1 &
  SAMPLY_PID=$!
fi

if [ "$PERF_MODE" = async ]; then
  echo "--- tokio-console: connect to 127.0.0.1:6669 from another terminal ---"
  if ! command -v tokio-console >/dev/null 2>&1; then
    echo "    optional UI missing: cargo install --locked tokio-console"
  fi
  sleep "$ASYNC_ATTACH_DELAY"
fi

extractor0="$(scrape "$EXTRACTOR_ADDR")"
transformer0="$(scrape "$TRANSFORMER_ADDR")"
rows_start="$(mval "$extractor0" walrus_extractor_parquet_rows_written_total)"
flush_sum0="$(mval "$extractor0" walrus_extractor_batch_flush_latency_seconds_sum)"
flush_cnt0="$(mval "$extractor0" walrus_extractor_batch_flush_latency_seconds_count)"
spill0="$(mval "$extractor0" walrus_extractor_spill_total)"

python3 scripts/perf_report.py sample \
  --extractor-pid "$EXTRACTOR_PID" \
  --transformer-pid "$TRANSFORMER_PID" \
  --extractor-url "http://$EXTRACTOR_ADDR/metrics" \
  --transformer-url "http://$TRANSFORMER_ADDR/metrics" \
  --output "$RUN_DIR/samples.csv" \
  --interval "$SAMPLE_INTERVAL" &
RESOURCE_SAMPLER_PID=$!
t_start="$(date +%s)"

echo "--- applying load ---"
LOAD_FAILED=false
if ! bash scripts/loadgen.sh "$SCENARIO" "$DURATION" "$CLIENTS" \
    >"$RUN_DIR/loadgen.log" 2>&1; then
  LOAD_FAILED=true
  echo "!! load generator failed; preserving the run as failed" >&2
fi

echo "--- draining the complete pipeline ---"
DRAINED=false
zeros=0
for _ in $(seq 1 150); do
  if ! kill -0 "$EXTRACTOR_PID" 2>/dev/null || ! kill -0 "$TRANSFORMER_PID" 2>/dev/null; then
    echo "!! a Walrus process exited before drain completed" >&2
    break
  fi
  s="$(scrape "$EXTRACTOR_ADDR")"
  l="$(scrape "$TRANSFORMER_ADDR")"
  rlag="$(printf '%.0f' "$(mval "$s" walrus_extractor_replication_lag_bytes)")"
  raw_lag="$(mval "$l" walrus_transformer_raw_append_lag_bytes)"
  transform_lag="$(mval "$l" walrus_transformer_transform_lag_bytes)"
  files_ready="$(mval "$l" walrus_transformer_files_ready)"
  if [ "${raw_lag%.*}" = 0 ] && [ "${transform_lag%.*}" = 0 ] \
      && [ "${files_ready%.*}" = 0 ] && [ "$rlag" -lt 200000 ]; then
    zeros=$((zeros + 1))
    if [ "$zeros" -ge 3 ]; then
      DRAINED=true
      break
    fi
  else
    zeros=0
  fi
  sleep 2
done

extractor1="$(scrape "$EXTRACTOR_ADDR")"
transformer1="$(scrape "$TRANSFORMER_ADDR")"
rows_end="$(mval "$extractor1" walrus_extractor_parquet_rows_written_total)"
flush_sum1="$(mval "$extractor1" walrus_extractor_batch_flush_latency_seconds_sum)"
flush_cnt1="$(mval "$extractor1" walrus_extractor_batch_flush_latency_seconds_count)"
spill1="$(mval "$extractor1" walrus_extractor_spill_total)"
t_end="$(date +%s)"
elapsed=$((t_end - t_start))

sleep "$SAMPLE_INTERVAL"
stop_resource_sampler
stop_samply
stop_services

set --
if [ "$LOAD_FAILED" = true ]; then
  set -- "$@" --failure-reason "load generator failed"
fi
if [ "$DRAINED" != true ]; then
  set -- "$@" --failure-reason "pipeline did not drain before the timeout"
fi
if [ "$PERF_MODE" = cpu ] && [ ! -s "$RUN_DIR/${PERF_TARGET}-samply.json.gz" ]; then
  set -- "$@" --failure-reason "Samply did not produce a profile"
fi
if [ "$PERF_MODE" = heap ] && [ ! -s "$RUN_DIR/${PERF_TARGET}-dhat-heap.json" ]; then
  set -- "$@" --failure-reason "DHAT did not produce a heap profile"
fi
if [ "$PERF_MODE" = async ] \
    && ! find "$RUN_DIR" -maxdepth 1 -name "${PERF_TARGET}-tokio-console*" -type f -size +0c \
      -print -quit | grep -q .; then
  set -- "$@" --failure-reason "tokio-console did not produce a recording"
fi

set +e
python3 scripts/perf_report.py finish \
  --run-dir "$RUN_DIR" \
  --elapsed "$elapsed" \
  --rows-start "$rows_start" \
  --rows-end "$rows_end" \
  --flush-sum-start "$flush_sum0" \
  --flush-sum-end "$flush_sum1" \
  --flush-count-start "$flush_cnt0" \
  --flush-count-end "$flush_cnt1" \
  --spill-start "$spill0" \
  --spill-end "$spill1" \
  "$@"
REPORT_STATUS=$?
set -e
FINALIZED=true

echo "(interpret: extractor inflight points to decode/flush pressure; raw-append lag points to transformer"
echo " ingest; transform lag points to DuckDB reconciliation. Compare CPU seconds/1k rows, not only"
echo " wall time, before calling a code change an efficiency win.)"
exit "$REPORT_STATUS"
