#!/usr/bin/env bash
# check-no-os-threads.sh — production topology guard. Runtime sizing may query
# std::thread::available_parallelism; only OS-thread creation/import paths are forbidden.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

THREAD_PATTERN='std::thread::(spawn|scope|Builder)|use[[:space:]]+std::thread|use[[:space:]]+std::\{[^}]*\bthread\b'

check_sources() {
  local source_label=$1
  shift
  local output
  local status

  if command -v rg >/dev/null 2>&1; then
    output=$(rg -n --glob '*.rs' --glob '!*_test.rs' "$THREAD_PATTERN" "$@" 2>&1) && status=0 || status=$?
  else
    # GitHub's runner image does not guarantee ripgrep is installed. Keep the guard self-contained
    # instead of making a late CI step depend on an unrelated preinstalled package.
    output=$(grep -RInE --include='*.rs' --exclude='*_test.rs' "$THREAD_PATTERN" "$@" 2>&1) && status=0 || status=$?
  fi
  if [ "$status" -eq 0 ]; then
    while IFS=: read -r file line rest; do
      echo "::error file=${file},line=${line}::production OS-thread creation/import is forbidden: ${rest}"
    done <<<"$output"
    return 1
  fi
  if [ "$status" -ne 1 ]; then
    echo "$output" >&2
    return "$status"
  fi

  echo "OK: no OS-thread creation under ${source_label}"
}

self_test() {
  OS_THREAD_FIXTURE_DIR=$(mktemp -d)
  trap 'rm -rf "$OS_THREAD_FIXTURE_DIR"' EXIT
  mkdir -p "$OS_THREAD_FIXTURE_DIR/clean" "$OS_THREAD_FIXTURE_DIR/direct" "$OS_THREAD_FIXTURE_DIR/brace"

  cat >"$OS_THREAD_FIXTURE_DIR/clean/runtime.rs" <<'EOF'
fn workers() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from)
}
EOF
  cat >"$OS_THREAD_FIXTURE_DIR/clean/allowed_test.rs" <<'EOF'
fn proves_send() {
    std::thread::spawn(|| 1).join().unwrap();
}
EOF
  if ! check_sources "clean temporary source root" "$OS_THREAD_FIXTURE_DIR/clean" >/dev/null; then
    echo "check-no-os-threads self-test: allowed fixtures unexpectedly failed" >&2
    return 1
  fi
  echo "ok: available_parallelism and sibling test fixtures pass"

  cat >"$OS_THREAD_FIXTURE_DIR/direct/direct.rs" <<'EOF'
fn escapes_runtime() {
    std::thread::spawn(|| {});
}
EOF
  local output
  if output=$(check_sources "direct temporary source root" "$OS_THREAD_FIXTURE_DIR/direct" 2>&1); then
    echo "check-no-os-threads self-test: direct spawn unexpectedly passed" >&2
    return 1
  fi
  if ! grep -Fq "file=$OS_THREAD_FIXTURE_DIR/direct/direct.rs,line=2" <<<"$output"; then
    echo "$output"
    echo "check-no-os-threads self-test: direct rejection omitted fixture file and line" >&2
    return 1
  fi
  echo "ok: direct std::thread::spawn fixture is rejected with its file and line"

  cat >"$OS_THREAD_FIXTURE_DIR/brace/brace.rs" <<'EOF'
use std::{sync::Arc, thread};
fn escapes_runtime() {
    thread::spawn(|| Arc::new(1));
}
EOF
  if output=$(check_sources "brace temporary source root" "$OS_THREAD_FIXTURE_DIR/brace" 2>&1); then
    echo "check-no-os-threads self-test: brace import unexpectedly passed" >&2
    return 1
  fi
  if ! grep -Fq "file=$OS_THREAD_FIXTURE_DIR/brace/brace.rs,line=1" <<<"$output"; then
    echo "$output"
    echo "check-no-os-threads self-test: brace rejection omitted fixture file and line" >&2
    return 1
  fi
  echo "ok: brace-imported thread fixture is rejected with its file and line"
  echo "check-no-os-threads self-test: PASS"
}

case "${1:-}" in
  "")
    check_sources 'non-test crates/*/src' crates/*/src
    ;;
  --self-test)
    self_test
    ;;
  *)
    echo "usage: bash scripts/check-no-os-threads.sh [--self-test]" >&2
    exit 2
    ;;
esac
