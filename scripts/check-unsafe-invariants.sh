#!/usr/bin/env bash
# check-unsafe-invariants.sh — repository guard. walrus never constructs a value from
# uninitialized memory: its replication buffer gets spare capacity safely through BytesMut +
# AsyncReadExt::read_buf. This script fails the build if a fake-initialization form comes back.
#
#   bash scripts/check-unsafe-invariants.sh
#   bash scripts/check-unsafe-invariants.sh --self-test
#
# Same command CI runs, so "green locally" predicts "green in CI".
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

UNINIT_PATTERN='MaybeUninit|mem::uninitialized|mem::zeroed|assume_init|\.set_len\('
# First-party Rust roots, relative to a tree root. Benches and integration tests are first-party
# too — a perf-motivated `set_len` over spare capacity is likeliest to appear in a bench — so they
# are scanned alongside library sources. Dependency and generated-code unsafe internals are outside
# this policy; legitimate reserve-only calls such as `with_capacity` are not in the pattern.
SCOPE_PATTERNS=('crates/*/src' 'crates/*/tests' 'crates/*/benches' 'tests/*/src' 'tests/*/tests')
POLICY_HINT="workspace unsafe policy requires unsafe_code = forbid and per-member lint inheritance"
WALRUS_SELF_TEST_DIR=""

cleanup_self_test() {
  if [[ -n "$WALRUS_SELF_TEST_DIR" && -d "$WALRUS_SELF_TEST_DIR" ]]; then
    rm -rf -- "$WALRUS_SELF_TEST_DIR"
  fi
}
trap cleanup_self_test EXIT

scan_uninit() {
  echo "== fake initialization (${UNINIT_PATTERN}) =="
  echo "   scope: $*"
  local hits
  hits="$(grep -rnE --include='*.rs' "$UNINIT_PATTERN" "$@" 2>/dev/null || true)"
  if [[ -n "$hits" ]]; then
    printf '%s\n' "$hits" >&2
    echo "FAIL: uninitialized-memory construction found in first-party sources." >&2
    echo "      walrus gets uninitialized spare capacity safely via BytesMut + read_buf." >&2
    echo "      Use safe spare-capacity APIs such as BytesMut + AsyncReadExt::read_buf." >&2
    return 1
  fi
  echo "   0 sites"
}

fail() {
  echo "FAIL: $*" >&2
  return 1
}

# Expand SCOPE_PATTERNS under a tree root. A pattern that matches nothing is skipped (a crate need
# not have benches), but an empty result means the layout moved and the scan would "pass" over zero
# files — the one way this guard could go quiet without anyone noticing.
resolve_scope() {
  local root="${1%/}"
  local pattern candidate
  local -a roots=()
  for pattern in "${SCOPE_PATTERNS[@]}"; do
    for candidate in "$root"/$pattern; do
      if [[ -d "$candidate" ]]; then
        roots+=("${candidate#./}")
      fi
    done
  done
  if [[ ${#roots[@]} -eq 0 ]]; then
    fail "no first-party Rust root under $root matched ${SCOPE_PATTERNS[*]}; the scan would pass on an empty file set. $POLICY_HINT."
    return 1
  fi
  printf '%s\n' "${roots[@]}"
}

has_workspace_unsafe_forbid() {
  awk '
    /^[[:space:]]*\[workspace\.lints\.rust\][[:space:]]*$/ { inside = 1; next }
    /^[[:space:]]*\[/ { inside = 0 }
    inside && /^[[:space:]]*unsafe_code[[:space:]]*=[[:space:]]*"forbid"([[:space:]]*(#.*)?)?$/ {
      found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "$1"
}

member_inherits_workspace_lints() {
  awk '
    /^[[:space:]]*\[lints\][[:space:]]*$/ { inside = 1; next }
    /^[[:space:]]*\[/ { inside = 0 }
    inside && /^[[:space:]]*workspace[[:space:]]*=[[:space:]]*true([[:space:]]*(#.*)?)?$/ {
      found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "$1"
}

workspace_members() {
  sed -nE 's/^[[:space:]]*members[[:space:]]*=[[:space:]]*\[([^]]*)\].*/\1/p' "$1" \
    | tr ',' '\n' \
    | tr -d '" '
}

check_unsafe_policy() {
  local root="${1%/}"
  local workspace_manifest="$root/Cargo.toml"
  echo "== unsafe policy still in force (workspace forbid + per-member inheritance) =="

  if ! has_workspace_unsafe_forbid "$workspace_manifest"; then
    fail "$workspace_manifest lost \`unsafe_code = \"forbid\"\`. $POLICY_HINT."
    return 1
  fi

  local members
  members="$(workspace_members "$workspace_manifest")"
  if [[ -z "$members" ]]; then
    fail "$workspace_manifest has no parseable one-line workspace members list. $POLICY_HINT."
    return 1
  fi

  local member
  while IFS= read -r member; do
    [[ -n "$member" ]] || continue
    local member_manifest="$root/$member/Cargo.toml"
    if ! member_inherits_workspace_lints "$member_manifest"; then
      fail "$member_manifest no longer inherits \`[lints] workspace = true\`; the workspace forbid does not reach it. $POLICY_HINT."
      return 1
    fi
    echo "   ok: $member inherits [lints] workspace = true"
  done < <(printf '%s\n' "$members")
}

write_policy_fixture() {
  local root="$1"
  local root_lint="$2"
  local member_lint="$3"
  mkdir -p "$root/crates/example"
  printf '%s\n' \
    '[workspace]' \
    'members = ["crates/example"]' \
    '' \
    '[workspace.lints.rust]' \
    "$root_lint" >"$root/Cargo.toml"
  printf '%s\n' \
    '[package]' \
    'name = "example"' \
    'version = "0.0.0"' \
    'edition.workspace = true' \
    '' \
    '[lints]' \
    "$member_lint" >"$root/crates/example/Cargo.toml"
}

self_test() {
  WALRUS_SELF_TEST_DIR="$(mktemp -d)"
  local clean_src="$WALRUS_SELF_TEST_DIR/clean/src"
  local rejected_src="$WALRUS_SELF_TEST_DIR/rejected/src"
  local rejected_log="$WALRUS_SELF_TEST_DIR/rejected.log"
  mkdir -p "$clean_src" "$rejected_src"

  printf '%s\n' \
    'fn clean() {' \
    '    let mut bytes = Vec::with_capacity(1);' \
    '    bytes.push(1_u8);' \
    '}' >"$clean_src/clean.rs"
  if ! scan_uninit "$clean_src" >/dev/null 2>&1; then
    echo "not ok: clean temporary source tree was rejected" >&2
    return 1
  fi
  echo "ok: clean temporary source tree passes"

  printf '%s\n' \
    'fn rejected(buffer: &mut Vec<u8>) {' \
    '    buffer.set_len(1);' \
    '}' >"$rejected_src/violation.rs"
  if scan_uninit "$rejected_src" >"$rejected_log" 2>&1; then
    echo "not ok: temporary .set_len( fixture unexpectedly passed" >&2
    return 1
  fi

  local violation_line
  violation_line="$(grep -F "$rejected_src/violation.rs:2:" "$rejected_log" || true)"
  if [[ -z "$violation_line" ]]; then
    echo "not ok: rejection did not print the fixture file and line" >&2
    return 1
  fi
  printf '%s\n' "$violation_line"
  echo "ok: temporary .set_len( fixture is rejected with its file and line"

  local scope_root="$WALRUS_SELF_TEST_DIR/scope"
  local scope_log="$WALRUS_SELF_TEST_DIR/scope.log"
  local empty_root="$WALRUS_SELF_TEST_DIR/scope-empty"
  local empty_log="$WALRUS_SELF_TEST_DIR/scope-empty.log"
  mkdir -p "$scope_root/crates/example/benches" "$scope_root/tests/e2e/tests" "$empty_root/crates"

  printf '%s\n' \
    'fn bench(buffer: &mut Vec<u8>) {' \
    '    buffer.set_len(1);' \
    '}' >"$scope_root/crates/example/benches/append.rs"

  local resolved
  resolved="$(resolve_scope "$scope_root")"
  local expected
  for expected in crates/example/benches tests/e2e/tests; do
    if ! grep -Fxq "$scope_root/$expected" <<<"$resolved"; then
      printf '%s\n' "$resolved"
      echo "not ok: resolved scope omitted $expected" >&2
      return 1
    fi
  done

  local -a resolved_dirs=()
  local dir
  while IFS= read -r dir; do
    if [[ -n "$dir" ]]; then
      resolved_dirs+=("$dir")
    fi
  done <<<"$resolved"

  if scan_uninit "${resolved_dirs[@]}" >"$scope_log" 2>&1; then
    echo "not ok: bench-only .set_len( fixture escaped the resolved scope" >&2
    return 1
  fi
  local bench_line
  bench_line="$(grep -F "$scope_root/crates/example/benches/append.rs:2:" "$scope_log" || true)"
  if [[ -z "$bench_line" ]]; then
    echo "not ok: bench rejection did not print the fixture file and line" >&2
    return 1
  fi
  printf '%s\n' "$bench_line"
  echo "ok: benches and integration tests are inside the resolved scope"

  if resolve_scope "$empty_root" >"$empty_log" 2>&1; then
    echo "not ok: a tree with no first-party Rust root unexpectedly resolved" >&2
    return 1
  fi
  if ! grep -F "$POLICY_HINT" "$empty_log" >/dev/null; then
    echo "not ok: empty-scope diagnostic omitted the unsafe-policy context" >&2
    return 1
  fi
  grep -F 'FAIL:' "$empty_log"
  echo "ok: a tree with no first-party Rust root is rejected instead of scanning nothing"

  local clean_policy="$WALRUS_SELF_TEST_DIR/policy-clean"
  local missing_root="$WALRUS_SELF_TEST_DIR/policy-missing-root"
  local missing_member="$WALRUS_SELF_TEST_DIR/policy-missing-member"
  local missing_root_log="$WALRUS_SELF_TEST_DIR/policy-missing-root.log"
  local missing_member_log="$WALRUS_SELF_TEST_DIR/policy-missing-member.log"
  write_policy_fixture "$clean_policy" 'unsafe_code = "forbid"' 'workspace = true'
  write_policy_fixture "$missing_root" 'warnings = "deny"' 'workspace = true'
  write_policy_fixture "$missing_member" 'unsafe_code = "forbid"' ''

  if ! check_unsafe_policy "$clean_policy" >/dev/null 2>&1; then
    echo "not ok: clean temporary policy fixture was rejected" >&2
    return 1
  fi
  echo "ok: clean temporary policy fixture passes"

  if check_unsafe_policy "$missing_root" >"$missing_root_log" 2>&1; then
    echo "not ok: temporary root without unsafe_code = \"forbid\" unexpectedly passed" >&2
    return 1
  fi
  if ! grep -F "$POLICY_HINT" "$missing_root_log" >/dev/null; then
    echo "not ok: missing-root diagnostic omitted the unsafe-policy context" >&2
    return 1
  fi
  grep -F 'FAIL:' "$missing_root_log"
  echo "ok: temporary root without unsafe_code = \"forbid\" is rejected"

  if check_unsafe_policy "$missing_member" >"$missing_member_log" 2>&1; then
    echo "not ok: temporary member without workspace = true unexpectedly passed" >&2
    return 1
  fi
  if ! grep -F "$POLICY_HINT" "$missing_member_log" >/dev/null; then
    echo "not ok: missing-member diagnostic omitted the unsafe-policy context" >&2
    return 1
  fi
  grep -F 'FAIL:' "$missing_member_log"
  echo "ok: temporary member without workspace = true is rejected"
  echo "check-unsafe-invariants self-test: PASS"
}

case "${1:-}" in
  "")
    # Command substitution keeps `resolve_scope`'s failure status, unlike a process substitution.
    SCOPE_LIST="$(resolve_scope ".")"
    SCOPE=()
    while IFS= read -r scope_root; do
      if [[ -n "$scope_root" ]]; then
        SCOPE+=("$scope_root")
      fi
    done <<<"$SCOPE_LIST"
    scan_uninit "${SCOPE[@]}"
    check_unsafe_policy "."
    echo "check-unsafe-invariants: PASS"
    ;;
  --self-test)
    self_test
    ;;
  *)
    echo "usage: $0 [--self-test]" >&2
    exit 2
    ;;
esac
