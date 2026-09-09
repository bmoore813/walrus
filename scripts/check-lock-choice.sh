#!/usr/bin/env bash
# check-lock-choice.sh — lock-choice guard (`own-rwlock-readers`). Every Mutex/RwLock field in
# production code must justify its access pattern on the line immediately above the declaration.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PATTERN='^[[:space:]]*(pub[[:space:]]+)?[a-z_][a-z0-9_]*:[[:space:]]*((parking_lot|std::sync|tokio::sync)::)?(Mutex|RwLock)<'

check_lock_fields() {
  local source_label=$1
  shift
  local fail=0

  while IFS=: read -r file line _rest; do
    local previous=$((line - 1))
    if ! sed -n "${previous}p" "$file" | grep -q 'LOCK-CHOICE:'; then
      echo "::error file=${file},line=${line}::lock field declared without a '// LOCK-CHOICE:' justification"
      fail=1
    fi
  done < <(grep -rnE --include='*.rs' --exclude='*_test.rs' "$PATTERN" "$@")

  if [ "$fail" -eq 0 ]; then
    echo "OK: every Mutex/RwLock field under ${source_label} carries a // LOCK-CHOICE: justification"
  fi
  return "$fail"
}

self_test() {
  LOCK_CHOICE_FIXTURE_DIR=$(mktemp -d)
  trap 'rm -rf "$LOCK_CHOICE_FIXTURE_DIR"' EXIT
  mkdir -p "$LOCK_CHOICE_FIXTURE_DIR/crates/example/src"

  cat >"$LOCK_CHOICE_FIXTURE_DIR/crates/example/src/justified.rs" <<'EOF'
struct Justified {
    // LOCK-CHOICE: Mutex because every access writes and holds the guard for one operation.
    state: Mutex<u8>,
}
EOF

  local output
  if ! output=$(check_lock_fields "temporary source root" "$LOCK_CHOICE_FIXTURE_DIR/crates/example/src" 2>&1); then
    echo "$output"
    echo "check-lock-choice self-test: justified fixture unexpectedly failed" >&2
    return 1
  fi
  echo "ok: justified temporary lock field passes"

  cat >"$LOCK_CHOICE_FIXTURE_DIR/crates/example/src/unjustified.rs" <<'EOF'
struct Unjustified {
    state: RwLock<u8>,
}
EOF

  if output=$(check_lock_fields "temporary source root" "$LOCK_CHOICE_FIXTURE_DIR/crates/example/src" 2>&1); then
    echo "check-lock-choice self-test: unjustified fixture unexpectedly passed" >&2
    return 1
  fi
  if ! grep -Fq "file=$LOCK_CHOICE_FIXTURE_DIR/crates/example/src/unjustified.rs,line=2" <<<"$output"; then
    echo "$output"
    echo "check-lock-choice self-test: rejection did not name the temporary file and line" >&2
    return 1
  fi
  echo "ok: unjustified temporary lock field is rejected with its file and line"
  echo "check-lock-choice self-test: PASS"
}

case "${1:-}" in
  "")
    check_lock_fields 'crates/*/src' crates/*/src
    ;;
  --self-test)
    self_test
    ;;
  *)
    echo "usage: bash scripts/check-lock-choice.sh [--self-test]" >&2
    exit 2
    ;;
esac
