#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  --self-test|--check) mode="$1" ;;
  *) echo "usage: $0 --self-test|--check" >&2; exit 2 ;;
esac

python3 - "$mode" <<'PY'
from __future__ import annotations

import os
import pathlib
import re
import sys
import tempfile

DECLARATION = re.compile(
    r'#\[cfg\(test\)\]\s*#\[path\s*=\s*"([^"]+_test\.rs)"\]\s*mod\s+tests\s*;',
    re.MULTILINE,
)
INLINE = re.compile(r'^\s*mod\s+tests\s*\{', re.MULTILINE)
SUPER_IMPORT = re.compile(r'^\s*use\s+super(?:::|::\{)', re.MULTILINE)
TEST_FUNCTION = re.compile(
    r'#\[(?:test|tokio::test(?:\([^]]*\))?)\]'
    r'(?:\s*#\[[^]]+\])*\s*(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(',
    re.MULTILINE,
)
PLACEHOLDER_TEST_NAMES = {"test", "tests", "it_works", "works"}


def rust_sources(root: pathlib.Path) -> tuple[list[pathlib.Path], list[pathlib.Path]]:
    production: list[pathlib.Path] = []
    siblings: list[pathlib.Path] = []
    for path in sorted(root.rglob("*.rs")):
        if "src" not in path.parts:
            continue
        if path.name.endswith("_test.rs"):
            siblings.append(path)
        else:
            production.append(path)
    return production, siblings


def wiring_errors(root: pathlib.Path) -> list[str]:
    production, siblings = rust_sources(root)
    declared: set[pathlib.Path] = set()
    errors: list[str] = []

    for source in production:
        text = source.read_text(encoding="utf-8")
        if INLINE.search(text):
            errors.append(f"{source}: inline `mod tests {{` must be a sibling *_test.rs module")
        for relative in DECLARATION.findall(text):
            sibling = (source.parent / relative).resolve()
            declared.add(sibling)
            if not sibling.is_file():
                errors.append(f"{source}: declared sibling test file is missing: {relative}")

    for sibling in siblings:
        text = sibling.read_text(encoding="utf-8")
        if sibling.resolve() not in declared:
            errors.append(f"{sibling}: orphan sibling test file has no cfg(test) path declaration")
        if not SUPER_IMPORT.search(text):
            errors.append(
                f"{sibling}: sibling unit-test module must import its parent with `use super`"
            )
        for name in TEST_FUNCTION.findall(text):
            if name in PLACEHOLDER_TEST_NAMES:
                errors.append(f"{sibling}: placeholder test function name `{name}`")

    return errors


def self_test() -> None:
    with tempfile.TemporaryDirectory(prefix="walrus-test-hygiene-") as tmp:
        root = pathlib.Path(tmp)
        src = root / "crate" / "src"
        src.mkdir(parents=True)

        (src / "good.rs").write_text(
            '#[cfg(test)]\n#[path = "good_test.rs"]\nmod tests;\n', encoding="utf-8"
        )
        (src / "good_test.rs").write_text(
            "use super::*;\n#[test]\nfn empty_input_is_rejected() {}\n", encoding="utf-8"
        )
        if wiring_errors(root):
            raise AssertionError("a correctly wired, descriptively named fixture was rejected")

        (src / "good_test.rs").write_text("use std::fmt;\n", encoding="utf-8")
        (src / "names.rs").write_text(
            '#[cfg(test)]\n#[path = "names_test.rs"]\nmod tests;\n', encoding="utf-8"
        )
        (src / "names_test.rs").write_text(
            "use super::*;\n"
            "#[test]\nfn test() {}\n"
            "#[test]\nfn tests() {}\n"
            "#[tokio::test]\nasync fn it_works() {}\n"
            "#[tokio::test(start_paused = true)]\nasync fn works() {}\n",
            encoding="utf-8",
        )

        (src / "inline.rs").write_text(
            "#[cfg(test)]\nmod tests { use super::*; }\n", encoding="utf-8"
        )
        (src / "missing.rs").write_text(
            '#[cfg(test)]\n#[path = "missing_test.rs"]\nmod tests;\n', encoding="utf-8"
        )
        (src / "orphan_test.rs").write_text("use super::*;\n", encoding="utf-8")
        report = "\n".join(wiring_errors(root))
        for expected in (
            "inline `mod tests {`",
            "missing_test.rs",
            "orphan_test.rs",
            "must import its parent with `use super`",
            "placeholder test function name `test`",
            "placeholder test function name `tests`",
            "placeholder test function name `it_works`",
            "placeholder test function name `works`",
        ):
            if expected not in report:
                raise AssertionError(f"self-test did not report {expected!r}:\n{report}")


self_test()
if sys.argv[1] == "--self-test":
    print("SELF_TEST=PASS")
    raise SystemExit(0)

root = pathlib.Path(os.environ.get("TEST_HYGIENE_ROOT", "crates"))
errors = wiring_errors(root)
if errors:
    print(*errors, sep="\n")
    raise SystemExit("test hygiene check failed")

production, siblings = rust_sources(root)
print(f"CHECK=PASS sibling_test_modules={len(siblings)} production_sources={len(production)}")
PY
