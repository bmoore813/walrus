#!/usr/bin/env bash
# Resolve and profile one Criterion benchmark executable without sampling Cargo or compilation.
set -Eeuo pipefail
cd "$(git rev-parse --show-toplevel)"

PACKAGE="${1:-}"
BENCH="${2:-}"
FILTER="${3:-}"
SECONDS="${4:-10}"

case "$PACKAGE/$BENCH" in
  extractor/decode|pg-to-arrow/batch|transformer/transform|transformer/append) ;;
  *)
    echo "benchmark must be one of: extractor/decode, pg-to-arrow/batch, transformer/transform, transformer/append" >&2
    exit 2
    ;;
esac
case "$SECONDS" in
  ''|*[!0-9]*) echo "profile duration must be a positive integer" >&2; exit 2 ;;
  0) echo "profile duration must be greater than zero" >&2; exit 2 ;;
esac
if ! command -v samply >/dev/null 2>&1; then
  echo "samply is required: cargo install --locked samply" >&2
  exit 2
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-bench-${PACKAGE}-${BENCH}-cpu-$$"
RUN_DIR="${PERF_OUTPUT_DIR:-target/perf/$RUN_ID}"
case "$RUN_DIR" in
  /*) ;;
  *) RUN_DIR="$PWD/$RUN_DIR" ;;
esac

python3 scripts/perf_report.py start \
  --run-dir "$RUN_DIR" \
  --mode cpu \
  --profile profiling \
  --scenario "bench:$PACKAGE/$BENCH:$FILTER" \
  --duration "$SECONDS" \
  --clients 0 \
  --max-fill n/a \
  --max-rows 0 \
  --max-bytes 0 \
  --max-inflight 0 \
  --poll-interval n/a \
  --sample-interval 0

FINALIZED=false
on_exit() {
  exit_code=$?
  trap - EXIT
  if [ "$FINALIZED" != true ]; then
    python3 scripts/perf_report.py fail \
      --run-dir "$RUN_DIR" --reason "benchmark profiler exited with status $exit_code" || true
  fi
  exit "$exit_code"
}
trap on_exit EXIT

echo "--- building $PACKAGE/$BENCH with the profiling profile ---"
cargo bench --profile profiling -p "$PACKAGE" --bench "$BENCH" --no-run \
  --message-format=json-render-diagnostics >"$RUN_DIR/cargo-artifacts.json"
EXECUTABLE="$(python3 scripts/perf_report.py resolve-bench \
  --cargo-json "$RUN_DIR/cargo-artifacts.json" --bench "$BENCH")"

ARGS=(--profile-time "$SECONDS")
if [ -n "$FILTER" ]; then
  ARGS=("$FILTER" "${ARGS[@]}")
fi
echo "--- profiling $EXECUTABLE ${ARGS[*]} ---"
samply record --save-only --output "$RUN_DIR/profile.json.gz" -- "$EXECUTABLE" "${ARGS[@]}"

python3 scripts/perf_report.py complete-artifact \
  --run-dir "$RUN_DIR" --artifact profile.json.gz
FINALIZED=true
