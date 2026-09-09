#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  --self-test|--check) mode="$1" ;;
  *) echo "usage: $0 --self-test|--check" >&2; exit 2 ;;
esac

# The standard-library scanner masks Rust comments and literals before looking for `$name:tt`.
# Both modes are read-only; --check runs the fixtures before scanning first-party Rust sources.
python3 - "$mode" <<'PY'
import pathlib
import re
import sys

CAPTURE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\s*:\s*tt\b")


def blank(character: str) -> str:
    """Mask one character without changing physical line coordinates."""
    return "\n" if character == "\n" else " "


def raw_string_open(source: str, start: int) -> tuple[int, str] | None:
    """Return the raw-string content offset and closing delimiter, if one starts here."""
    for prefix in ("br", "cr", "r"):
        if not source.startswith(prefix, start):
            continue
        cursor = start + len(prefix)
        while cursor < len(source) and source[cursor] == "#":
            cursor += 1
        if cursor < len(source) and source[cursor] == '"':
            hashes = source[start + len(prefix) : cursor]
            return cursor + 1, '"' + hashes
    return None


def char_literal_end(source: str, start: int) -> int | None:
    """Return the exclusive end of a Rust char literal, without mistaking lifetimes for chars."""
    cursor = start + 1
    if cursor >= len(source) or source[cursor] == "\n":
        return None
    if source[cursor] != "\\":
        return cursor + 2 if cursor + 1 < len(source) and source[cursor + 1] == "'" else None

    cursor += 1
    if cursor >= len(source):
        return None
    if source[cursor] == "x":
        cursor += 3
    elif source[cursor] == "u" and cursor + 1 < len(source) and source[cursor + 1] == "{":
        close = source.find("}", cursor + 2)
        if close < 0:
            return None
        cursor = close + 1
    else:
        cursor += 1
    return cursor + 1 if cursor < len(source) and source[cursor] == "'" else None


def mask_non_code(source: str) -> str:
    """Blank comments and literals while preserving every physical newline."""
    masked: list[str] = []
    cursor = 0
    while cursor < len(source):
        if source.startswith("//", cursor):
            while cursor < len(source) and source[cursor] != "\n":
                masked.append(" ")
                cursor += 1
            continue

        if source.startswith("/*", cursor):
            depth = 0
            while cursor < len(source):
                if source.startswith("/*", cursor):
                    depth += 1
                    masked.extend((" ", " "))
                    cursor += 2
                    continue
                if source.startswith("*/", cursor):
                    depth -= 1
                    masked.extend((" ", " "))
                    cursor += 2
                    if depth == 0:
                        break
                    continue
                masked.append(blank(source[cursor]))
                cursor += 1
            continue

        raw = raw_string_open(source, cursor)
        if raw is not None:
            content_start, closing = raw
            while cursor < content_start:
                masked.append(blank(source[cursor]))
                cursor += 1
            while cursor < len(source) and not source.startswith(closing, cursor):
                masked.append(blank(source[cursor]))
                cursor += 1
            for _ in closing:
                if cursor >= len(source):
                    break
                masked.append(blank(source[cursor]))
                cursor += 1
            continue

        if source[cursor] == '"':
            masked.append(" ")
            cursor += 1
            while cursor < len(source):
                character = source[cursor]
                masked.append(blank(character))
                cursor += 1
                if character == "\\" and cursor < len(source):
                    masked.append(blank(source[cursor]))
                    cursor += 1
                elif character == '"':
                    break
            continue

        if source[cursor] == "'":
            end = char_literal_end(source, cursor)
            if end is not None:
                while cursor < end:
                    masked.append(blank(source[cursor]))
                    cursor += 1
                continue

        masked.append(source[cursor])
        cursor += 1

    return "".join(masked)


def unmarked_captures(path: pathlib.Path, source: str) -> list[str]:
    """Return path:line diagnostics for raw captures without a same-line exemption marker."""
    masked = mask_non_code(source)
    original_lines = source.splitlines()
    findings: list[str] = []
    for match in CAPTURE.finditer(masked):
        line_number = masked.count("\n", 0, match.start()) + 1
        original_line = original_lines[line_number - 1]
        if "tt-fallback-ok" not in original_line:
            findings.append(f"{path}:{line_number}")
    return findings


def self_test() -> None:
    """Pin capture recognition, lexical masking, and the narrow exemption."""
    path = pathlib.Path("<self-test>")
    cases = [
        ("macro_rules! m { ($bad:tt) => {} }", 1),
        ("macro_rules! m { ($bad : tt) => {} }", 1),
        ("macro_rules! m { ($bad:tt) => {} } // tt-fallback-ok", 0),
        ("// tt-fallback-ok\nmacro_rules! m { ($bad:tt) => {} }", 1),
        ("macro_rules! m { ($bad:tt) => {} }\n// tt-fallback-ok", 1),
        ("// $bad:tt\n/* $also_bad : tt */", 0),
        ("/* outer $bad:tt /* nested $also_bad:tt */ still outer */", 0),
        ('let text = "$bad:tt // /*";', 0),
        ('let raw = r###"$bad : tt // /*"###;', 0),
        ('let text = "//"; macro_rules! m { ($bad:tt) => {} }', 1),
        ('let raw = r"/*"; macro_rules! m { ($bad:tt) => {} }', 1),
        ("let slash = '/'; macro_rules! m { ($bad:tt) => {} }", 1),
    ]
    for source, expected in cases:
        actual = len(unmarked_captures(path, source))
        if actual != expected:
            raise AssertionError(f"fixture expected {expected} finding(s), got {actual}: {source!r}")


self_test()
if sys.argv[1] == "--self-test":
    print("SELF_TEST=PASS")
    raise SystemExit(0)

hits: list[str] = []
for root in (pathlib.Path("crates"), pathlib.Path("tests")):
    if root.exists():
        for path in sorted(root.rglob("*.rs")):
            hits.extend(unmarked_captures(path, path.read_text(encoding="utf-8")))
if hits:
    print(*hits, sep="\n")
    raise SystemExit("raw :tt macro captures require a same-line tt-fallback-ok marker")
print("CHECK=PASS")
PY
