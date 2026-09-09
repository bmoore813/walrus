#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  --self-test|--check) mode="$1" ;;
  *) echo "usage: $0 --self-test|--check" >&2; exit 2 ;;
esac

python3 - "$mode" <<'PY'
from __future__ import annotations

import pathlib
import sys
import tempfile


def production_sources(roots: list[pathlib.Path]) -> list[pathlib.Path]:
    return sorted(
        path
        for root in roots
        for path in root.rglob("*.rs")
        if not path.name.endswith("_test.rs") and "src" in path.parts
    )


def documentation_errors(roots: list[pathlib.Path]) -> list[str]:
    errors: list[str] = []
    for path in production_sources(roots):
        first_line = path.read_text(encoding="utf-8").splitlines()[:1]
        if not first_line or not first_line[0].startswith("//!"):
            errors.append(f"{path}: production Rust module must start with `//!` documentation")
    return errors


def self_test() -> None:
    with tempfile.TemporaryDirectory(prefix="walrus-module-docs-") as tmp:
        root = pathlib.Path(tmp)
        src = root / "crate" / "src"
        src.mkdir(parents=True)
        (src / "good.rs").write_text("//! Documented module.\n", encoding="utf-8")
        (src / "good_test.rs").write_text("use super::*;\n", encoding="utf-8")
        if documentation_errors([root]):
            raise AssertionError("a documented module or excluded sibling test was rejected")

        (src / "attribute_first.rs").write_text(
            "#![allow(dead_code)]\n//! Displaced documentation.\n", encoding="utf-8"
        )
        (src / "missing.rs").write_text("pub fn undocumented() {}\n", encoding="utf-8")
        report = "\n".join(documentation_errors([root]))
        for expected in ("attribute_first.rs", "missing.rs"):
            if expected not in report:
                raise AssertionError(f"self-test did not report {expected!r}:\n{report}")


self_test()
if sys.argv[1] == "--self-test":
    print("SELF_TEST=PASS")
    raise SystemExit(0)

roots = [pathlib.Path("crates"), pathlib.Path("tests/e2e/src")]
errors = documentation_errors(roots)
if errors:
    print(*errors, sep="\n")
    raise SystemExit("module documentation check failed")

print(f"CHECK=PASS production_modules={len(production_sources(roots))}")
PY
