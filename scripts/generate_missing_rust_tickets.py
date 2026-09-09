#!/usr/bin/env python3
"""Scaffold and audit the phase 28-34 one-rule Rust ticket corpus.

Generation is deliberately opt-in and non-destructive: ``--generate`` creates
missing files as ``Readiness: draft`` and never rewrites an existing ticket.
``--check`` is read-only and reports missing, extra, draft, and untracked files;
``--require-tracked`` makes untracked corpus files an error for CI/activation.

The task-specific contracts below are intentionally kept in the scaffold tool.
They stop a regenerated ticket from falling back to the old generic "grep and
decide what to do" boilerplate.  A maintainer must still audit a generated file
and flip its readiness before it can become selectable.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RULE_SKILL = ROOT / ".claude/skills/rust-skills/SKILL.md"

PHASES: dict[int, tuple[str, str, list[str]]] = {
    28: ("Rust testing", "phase-28-rust-testing", [
        "test-cfg-test-module", "test-use-super", "test-descriptive-names",
        "test-arrange-act-assert", "test-integration-dir", "test-fixture-raii",
        "test-tokio-async", "test-mock-traits", "test-mockall-mocking",
        "test-proptest-properties", "test-should-panic", "test-doctest-examples",
        "test-snapshot-testing", "test-criterion-bench", "test-loom-concurrency",
    ]),
    29: ("Rust documentation", "phase-29-rust-documentation", [
        "doc-module-inner", "doc-all-public", "doc-errors-section",
        "doc-panics-section", "doc-safety-section", "doc-question-mark",
        "doc-intra-links", "doc-examples-section", "doc-hidden-setup",
        "doc-link-types", "doc-cargo-metadata", "doc-crate-readme",
    ]),
    30: ("Rust observability", "phase-30-rust-observability", [
        "obs-tracing-over-log", "obs-library-facade", "obs-structured-fields",
        "obs-instrument-spans", "obs-levels-filter", "obs-error-chain",
        "obs-no-sensitive-data",
    ]),
    31: ("Rust performance patterns", "phase-31-rust-performance", [
        "perf-iter-over-index", "perf-iter-lazy", "perf-collect-once",
        "perf-entry-api", "perf-drain-reuse", "perf-extend-batch",
        "perf-chain-avoid", "perf-collect-into", "perf-black-box-bench",
        "perf-release-profile", "perf-profile-first", "perf-ahash",
        "perf-io-buffering",
    ]),
    32: ("Rust project structure", "phase-32-rust-project-structure", [
        "proj-lib-main-split", "proj-mod-by-feature", "proj-flat-small",
        "proj-mod-rs-dir", "proj-pub-crate-internal", "proj-pub-super-parent",
        "proj-pub-use-reexport", "proj-prelude-module", "proj-bin-dir",
        "proj-workspace-large", "proj-workspace-deps", "proj-feature-additive",
        "proj-msrv-declare", "proj-build-rs-minimal",
    ]),
    33: ("Rust linting", "phase-33-rust-linting", [
        "lint-deny-correctness", "lint-warn-suspicious", "lint-warn-style",
        "lint-warn-complexity", "lint-warn-perf", "lint-pedantic-selective",
        "lint-missing-docs", "lint-unsafe-doc", "lint-cargo-metadata",
        "lint-rustfmt-check", "lint-workspace-lints", "lint-cfg-check",
        "lint-clippy-nursery-selected",
    ]),
    34: ("Rust anti-patterns", "phase-34-rust-anti-patterns", [
        "anti-unwrap-abuse", "anti-expect-lazy", "anti-clone-excessive",
        "anti-lock-across-await", "anti-string-for-str", "anti-vec-for-slice",
        "anti-index-over-iter", "anti-panic-expected", "anti-empty-catch",
        "anti-over-abstraction", "anti-premature-optimize", "anti-type-erasure",
        "anti-format-hot-path", "anti-collect-intermediate",
        "anti-stringly-typed",
    ]),
}


@dataclass(frozen=True)
class Spec:
    outcome: str
    packages: str
    probe: str
    finding: str
    files: str
    action: str
    acceptance: str
    deferred: str


def spec(
    outcome: str,
    packages: str,
    probe: str,
    finding: str,
    files: str,
    action: str,
    acceptance: str,
    deferred: str,
) -> Spec:
    return Spec(outcome, packages, probe, finding, files, action, acceptance, deferred)


# Every rule has a predetermined disposition and a Walrus-specific contract.
# Commands use stable paths/symbols, never frozen line numbers.
SPECS: dict[str, Spec] = {
    "test-cfg-test-module": spec(
        "change", "—",
        r"rg -n '#\[path = \"[^\"]+_test\.rs\"\]|mod tests \{' crates --glob '*.rs' || true",
        "Walrus intentionally wires unit tests as sibling `*_test.rs` child modules; the convention is complete but unguarded.",
        "`scripts/test-hygiene.sh`, `justfile`, `.github/workflows/ci.yml`, and `docs/implementation/notes/rust-skills/test-cfg-test-module.md`.",
        "Add a deterministic gate that rejects inline `mod tests {`, orphan sibling files, and declarations whose sibling is missing; accept an optional `TEST_HYGIENE_ROOT` fixture root (default `crates`) and wire that one script into `just hygiene` and the CI gates job.",
        "The script reports equal sibling/declaration counts on the real tree, and isolated fixture directories prove each of its three failure modes without editing tracked Rust files.",
        "Do not move sibling tests inline or inspect integration-test directories.",
    ),
    "test-use-super": spec(
        "change", "—",
        r"for f in $(find crates -path '*/src/*_test.rs' -type f | sort); do sed -n '1p' \"$f\"; done | sort | uniq -c",
        "Sibling unit-test modules consistently import their parent, but PR 28.1's hygiene gate does not yet enforce it.",
        "`scripts/test-hygiene.sh` and `docs/implementation/notes/rust-skills/test-use-super.md`.",
        "Extend the hygiene gate to require the first non-comment, non-attribute Rust item in every sibling test file to be `use super::*;`; report every offending path in one run.",
        "The real corpus passes and an isolated sibling fixture lacking `use super::*;` fails with its path while a correctly wired fixture passes.",
        "Do not rewrite imports in integration tests or require wildcard imports outside sibling test modules.",
    ),
    "test-descriptive-names": spec(
        "change", "—",
        r"rg -n '^\s*(async\s+)?fn (test|tests|it_works|works)(_|\()' crates --glob '*_test.rs' || true",
        "The current test names are descriptive; a narrow regression guard can reject only the known placeholder-name family without policing prose.",
        "`scripts/test-hygiene.sh` and `docs/implementation/notes/rust-skills/test-descriptive-names.md`.",
        "Extend the hygiene gate to reject exact placeholder stems `test`, `tests`, `it_works`, and `works` in sibling `#[test]`/`#[tokio::test]` functions and print path plus function name.",
        "The real tree passes; isolated fixtures prove all four placeholder stems fail and a behavior-describing name passes.",
        "Do not impose a naming grammar, minimum word count, or rename existing descriptive tests.",
    ),
    "test-arrange-act-assert": spec(
        "change", "common,pg-to-arrow,extractor",
        r"rg -n '^fn (descriptor_|schema_|insert_ready_|begin_commit_)' crates/{common,pg-to-arrow,extractor}/src/*_test.rs || true",
        "Four multi-concern unit tests mix setup, the operation, and assertions; splitting or labeling those exact tests improves failure locality.",
        "`crates/common/src/type_descriptor_test.rs`, `crates/pg-to-arrow/src/schema_test.rs`, `crates/extractor/src/manifest_test.rs`, and `crates/extractor/src/replication_test.rs`.",
        "Split the descriptor, schema, and manifest tests at their independent behavior assertions; add Arrange/Act/Assert comments to the replication vector test without changing fixture bytes.",
        "Each resulting test name states one behavior, the original assertions remain represented once, and the three targeted packages pass.",
        "Do not label every small test or change production code and protocol fixtures.",
    ),
    "test-integration-dir": spec(
        "evidence", "—",
        r"find crates tests -path '*/tests/*.rs' -type f | sort",
        "Walrus already keeps cross-component and public-surface tests in crate-root `tests/` directories; sibling `src/*_test.rs` files are private unit tests by design.",
        "`docs/implementation/notes/rust-skills/test-integration-dir.md` only.",
        "Record the integration-test inventory by package, confirm every listed file is outside `src/`, and distinguish it from the sibling-unit-test convention established by PR 28.1.",
        "The note contains the exact inventory command, package counts, the unit/integration boundary, and a reversal condition for any cross-component test added under `src/`.",
        "Do not relocate tests merely to demonstrate the rule or duplicate an integration scenario.",
    ),
    "test-fixture-raii": spec(
        "change", "transformer",
        r"rg -n 'std::env::temp_dir\(\)|remove_dir_all' crates/transformer/src --glob '*_test.rs' || true",
        "The transformer's `epoch_test` and `duck_test` sibling modules own fixed temp paths and manually clean them; `tempfile` is already a transformer dev-dependency.",
        "`crates/transformer/src/epoch_test.rs` and `crates/transformer/src/duck_test.rs`.",
        "Replace each fixed path plus `remove_dir_all` pair with a held `tempfile::TempDir`; pass `TempDir::path()` into `TableDb` and keep the guard alive until the database handle drops.",
        "The probe has no hit in transformer sibling tests, parallel repeated transformer tests do not collide, and no manifest changes.",
        "Leave compose integration fixtures and the e2e `Harness` Drop implementation unchanged.",
    ),
    "test-tokio-async": spec(
        "change", "extractor",
        r"rg -n 'cap_of_two|tokio::time::(sleep|advance|pause)' crates/extractor/src/reload_test.rs",
        "The reload concurrency-cap test is hermetic but advances on wall clock even though `tokio` already enables `test-util` for extractor tests.",
        "`crates/extractor/src/reload_test.rs` only.",
        "Mark the concurrency-cap test `start_paused = true`, replace its sleeps with explicit `tokio::time::advance`, and retain channel/barrier assertions that prove permit ownership.",
        "The named test passes repeatedly without wall-clock sleeps and the full extractor package passes.",
        "Do not pause integration tests that talk to Postgres, MinIO, sockets, or child processes.",
    ),
    "test-mock-traits": spec(
        "change", "extractor,transformer",
        r"rg -n 'Arc<dyn ObjectStore>|InMemory|struct FailingStore' crates/{extractor,transformer}/src --glob '*.rs' || true",
        "Production already exposes an `Arc<dyn ObjectStore>` seam; hermetic success and failure coverage is missing at two call sites.",
        "`crates/extractor/src/staging_test.rs`, `crates/extractor/Cargo.toml`, `crates/transformer/src/bootstrap.rs`, `crates/transformer/src/bootstrap_test.rs`, and `Cargo.toml`.",
        "Use object_store's in-memory implementation for success paths and one minimal local failing `ObjectStore` stub for propagation; add only the dev-dependencies required to implement that existing trait.",
        "Tests prove extractor put/delete, store-error propagation, and transformer S3-read verification without network access.",
        "Do not introduce a Walrus-owned storage trait or mock Postgres/SQLx.",
    ),
    "test-mockall-mocking": spec(
        "superseded by PR 28.8", "—",
        r"rg -n 'mockall|automock|Mock[A-Z]' Cargo.toml crates --glob '*.toml' --glob '*.rs' || true",
        "PR 28.8 exercises the only useful trait seam with the dependency's real in-memory implementation and a tiny failure stub; mockall would duplicate that coverage and add a proc-macro dependency.",
        "`docs/implementation/notes/rust-skills/test-mockall-mocking.md` only.",
        "Record the zero-dependency audit, link the PR 28.8 seam and tests, and state that mockall is reconsidered only after a second complex Walrus-owned trait needs ordered expectations.",
        "The note names every production trait seam and demonstrates that no mockall dependency or generated mock remains necessary.",
        "Do not add mockall, `#[automock]`, or a trait created solely for mocking.",
    ),
    "test-proptest-properties": spec(
        "change", "common",
        r"rg -n 'proptest|lsn.*(round|order)|numeric.*text' Cargo.toml crates/common --glob '*.toml' --glob '*.rs' || true",
        "`Lsn` has load-bearing parse/display and lexical-order invariants currently covered only by examples; it is a bounded, deterministic property-test target.",
        "`Cargo.toml`, `crates/common/Cargo.toml`, and `crates/common/src/lsn_test.rs`.",
        "Add proptest as a workspace dev-dependency and generate arbitrary `u64` LSNs to prove display/parse round-trip and that fixed-width textual ordering equals numeric ordering; configure explicit case count without persistence files.",
        "Both properties run under the ordinary common test target, report a reproducible seed on failure, and no production dependency gains proptest.",
        "Do not property-test SQL, external services, time, or already exhaustive small enums.",
    ),
    "test-should-panic": spec(
        "evidence", "—",
        r"rg -n '#\[should_panic|panic!|unimplemented!|unreachable!' crates --glob '*.rs' || true",
        "By this phase PR 10.9 denies production panic macros except the documented deferred stub; Walrus has no public panic contract that should be normalized by a test.",
        "`docs/implementation/notes/rust-skills/test-should-panic.md` only.",
        "Record all remaining panic constructs, their lint expectation or test-only location, and why testing returned errors or typed state is the applicable contract.",
        "The note proves there is no callable production panic behavior lacking a test and states that a future deliberate panic contract must add `#[should_panic(expected = ...)]`.",
        "Do not introduce a panic just to exercise `#[should_panic]` or convert Result tests into panic tests.",
    ),
    "test-doctest-examples": spec(
        "evidence", "—",
        r"rg -n '^/// ```|^//! ```' crates --glob '*.rs' || true",
        "Executable examples are owned by PR 29.8; the existing `common::sql_literal` example is already a compiling doctest and there is no stale non-code example to convert now.",
        "`docs/implementation/notes/rust-skills/test-doctest-examples.md` only.",
        "Run library doc tests, inventory Rust versus `text` fences, and record why protocol diagrams and SQL transcripts must remain non-Rust while API examples compile.",
        "The note includes the fence inventory and `cargo test --doc --workspace` result, with PR 29.8 named as the owner of new API examples.",
        "Do not turn shell, SQL, wire bytes, or service-dependent examples into fake Rust doctests.",
    ),
    "test-snapshot-testing": spec(
        "change", "transformer",
        r"rg -n 'insta|assert_snapshot|fn render|fn render_rebuild|apply_additive' Cargo.toml crates/transformer --glob '*.toml' --glob '*.rs' || true",
        "Transformer transform and additive-DDL rendering produce large deterministic SQL strings whose whole shape is reviewable and regression-sensitive.",
        "`Cargo.toml`, `crates/transformer/Cargo.toml`, `crates/transformer/src/transform.rs`, `crates/transformer/src/transform_test.rs`, `crates/transformer/src/ddl.rs`, `crates/transformer/src/ddl_test.rs`, `crates/transformer/src/snapshots/*.snap`, `.github/workflows/ci.yml`, and `.gitignore`.",
        "Add insta as a dev-only dependency; snapshot three representative transform plans and one additive-DDL plan; set `INSTA_UPDATE=no` in CI and ignore only `.snap.new` files.",
        "Four reviewed snapshots are committed, a deliberate renderer change produces `.snap.new` and fails, and transformer tests pass with updates disabled.",
        "Do not snapshot SQLx query files, nondeterministic values, or error/debug strings.",
    ),
    "test-criterion-bench": spec(
        "change", "—",
        r"find crates -path '*/benches/*.rs' -type f -print | sort && rg -n '^bench:' justfile",
        "Criterion targets already exist in extractor, pg-to-arrow, and transformer, but the `just bench` recipe omits transformer and there is no documented save/compare workflow.",
        "`justfile` and `docs/benchmarks.md`.",
        "Add transformer to `just bench`, plus named baseline and compare recipes that forward Criterion's `--save-baseline` and `--baseline` flags to all three packages; document quiet-machine usage.",
        "`just --dry-run bench`, `bench-baseline`, and `bench-compare` expand to all three packages with the expected Criterion flags.",
        "Do not run benchmarks in shared-runner CI or change benchmark bodies in this ticket.",
    ),
    "test-loom-concurrency": spec(
        "change", "—",
        r"rg -n 'Atomic|compare_exchange|fetch_(add|sub|or|and)|loom::' crates --glob '*.rs' || true",
        "Walrus has no hand-rolled lock-free algorithm for loom to model; that fact can be guarded without adding loom.",
        "`scripts/test-hygiene.sh` and `docs/implementation/notes/rust-skills/test-loom-concurrency.md`.",
        "Record why Tokio/parking_lot integration is tested behaviorally, then extend the hygiene script to fail when first-party atomic mutation appears so loom applicability is revisited deliberately.",
        "The real tree passes, an isolated fixture containing `fetch_add` fails with the note path, and no loom dependency is added.",
        "Do not match dependency source, read-only atomic loads, or replace runtime synchronization with loom types.",
    ),
    "doc-module-inner": spec(
        "change", "common,control,transformer,extractor,pg-to-arrow,e2e",
        r"for f in $(find crates tests/e2e/src -path '*/src/*.rs' -type f ! -name '*_test.rs' | sort); do sed -n '1p' \"$f\" | grep -q '^//!' || echo \"$f\"; done",
        "Several production modules lack an inner module summary, while existing summaries vary from current contract to stale PR narration.",
        "Production `*.rs` files under `crates/*/src` and `tests/e2e/src`, `scripts/check-module-docs.sh`, `justfile`, and `.github/workflows/ci.yml`.",
        "Give every production module a first-line `//!` summary of its current responsibility and add a path-based gate that reports missing summaries; wire it to `just check-docs` and CI.",
        "The gate finds every production module recursively, ignores sibling tests, and all six package doc builds pass with broken links denied.",
        "Do not add historical PR narration, restate filenames, or document test-only helper modules here.",
    ),
    "doc-all-public": spec(
        "change", "common,control,transformer,extractor,pg-to-arrow,e2e",
        r"rg -n '^\s*pub (async )?(unsafe )?(fn|struct|enum|trait|type|mod|const|static)|^\s*pub [A-Za-z_][A-Za-z0-9_]*\s*:' crates tests/e2e/src --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "The service libraries intentionally expose integration seams, but many reachable items and fields still lack API-contract documentation.",
        "Production Rust files under `crates/{common,control,transformer,extractor,pg-to-arrow}/src` and `tests/e2e/src` only.",
        "Document every item reachable from the five library roots, including public fields and enum variants; describe invariants, units, ownership, and side effects, not implementation syntax.",
        "`RUSTDOCFLAGS='-D missing_docs -D rustdoc::broken_intra_doc_links' cargo doc --workspace --no-deps` succeeds after excluding binaries only through rustdoc's normal reachability rules.",
        "Do not widen/narrow visibility, add examples, or enable the permanent workspace lint before PR 33.7.",
    ),
    "doc-errors-section": spec(
        "change", "common,control,transformer,extractor,pg-to-arrow,e2e",
        r"rg -n '^\s*pub (async )?fn .*Result|^\s*pub fn .*$' crates/{common,control,transformer,extractor,pg-to-arrow}/src tests/e2e/src --glob '*.rs'",
        "Public fallible APIs are documented after PR 29.2, but their error conditions need explicit caller-facing contracts.",
        "Production Rust files under `crates/{common,control,transformer,extractor,pg-to-arrow}/src` and `tests/e2e/src` containing reachable functions that return `Result`.",
        "Add `# Errors` sections to every reachable fallible function and method, grouping variants by observable cause and linking named error types; include cancellation/partial-effect behavior where applicable.",
        "A rustdoc source audit finds no reachable Result-returning function without `# Errors`, and broken intra-doc links are denied.",
        "Do not enumerate private helper errors, promise third-party variant stability, or change error types.",
    ),
    "doc-panics-section": spec(
        "evidence", "—",
        r"rg -n 'panic!|unimplemented!|unreachable!|\.expect\(|\.unwrap\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' --glob '!**/benches/**' || true",
        "PRs 7.7 and 10.9 deny production panic paths; any remaining hits must be classified by visibility and reachability.",
        "`docs/implementation/notes/rust-skills/doc-panics-section.md` only.",
        "Classify every residual production panic construct by visibility and reachability, cite its narrow lint expectation, and confirm there is no public precondition requiring `# Panics`.",
        "The note accounts for every probe hit and names the exact condition that would require adding a `# Panics` section later.",
        "Do not document an impossible panic as supported behavior or weaken the panic lints.",
    ),
    "doc-safety-section": spec(
        "evidence", "—",
        r"rg -n '\bunsafe\b' crates --glob '*.rs' || true",
        "The first-party Rust tree contains no unsafe block, unsafe function, or manual unsafe impl after the Phase 12 policy work.",
        "`docs/implementation/notes/rust-skills/doc-safety-section.md` only.",
        "Record the zero-hit command, the workspace unsafe policy established in Phase 12, and the requirement that any future `unsafe fn` carries a caller contract before merge.",
        "The note separates first-party source from dependency internals and gives a concrete reversal condition.",
        "Do not add a vacuous `# Safety` heading to safe APIs.",
    ),
    "doc-question-mark": spec(
        "evidence", "—",
        r"rg -n '^\s*///.*(unwrap|expect)\(|^\s*//!.*(unwrap|expect)\(' crates --glob '*.rs' || true",
        "The existing executable API example is infallible and uses no unwrap/expect; new fallible examples are owned by PR 29.8 and must return Result.",
        "`docs/implementation/notes/rust-skills/doc-question-mark.md` only.",
        "Audit documentation code blocks for unwrap/expect, distinguish quoted anti-examples from runnable guidance, and record the Result-returning convention PR 29.8 must follow.",
        "The note accounts for all matches and the doc test suite passes without a runnable unwrap/expect example.",
        "Do not add `?` to infallible examples or edit test code under this documentation rule.",
    ),
    "doc-intra-links": spec(
        "change", "common,control,transformer,extractor,pg-to-arrow,e2e",
        r"RUSTDOCFLAGS='-D rustdoc::broken_intra_doc_links' cargo doc --workspace --no-deps",
        "PRs 29.1-29.3 create the complete prose surface; type and method references in that prose must now be resolvable links.",
        "Production Rust files under `crates/{common,control,transformer,extractor,pg-to-arrow}/src` and `tests/e2e/src` only.",
        "Convert references to reachable Walrus types, variants, traits, modules, and methods into intra-doc links using qualified paths where names are ambiguous; leave SQL identifiers and external concepts as code text.",
        "Rustdoc builds the workspace with broken intra-doc links denied and no link is silenced with an allow.",
        "Do not create reexports solely to shorten a documentation path or link every ordinary English noun.",
    ),
    "doc-examples-section": spec(
        "change", "common,pg-to-arrow",
        r"rg -n '^pub (struct Lsn|fn sql_literal)|^\s*pub fn (parse_rfc3339|parse_interval|parse_timetz|parse_uuid_bytes|parse_range)' crates/{common,pg-to-arrow}/src --glob '*.rs'",
        "The listed pure parsers and value helpers are stable, hermetic entry points where executable examples improve the API contract without starting services.",
        "`crates/common/src/lsn.rs`, `crates/common/src/sql.rs`, `crates/common/src/extractor_meta.rs`, `crates/pg-to-arrow/src/tier2.rs`, `crates/pg-to-arrow/src/uuid_enum.rs`, and `crates/pg-to-arrow/src/range.rs`.",
        "Add one runnable `# Examples` block per listed API, use Result-returning doctests for fallible calls, and assert a representative value or round trip.",
        "`cargo test --doc -p common -p pg-to-arrow` runs every new example with no ignored blocks.",
        "Do not add examples to service/database APIs that cannot be hermetic or duplicate examples on aliases.",
    ),
    "doc-hidden-setup": spec(
        "evidence", "—",
        r"rg -n '^\s*/// # |^\s*//! # ' crates --glob '*.rs' || true",
        "The PR 29.8 examples target small pure APIs and require no distracting fixture setup; hidden setup would obscure rather than clarify them.",
        "`docs/implementation/notes/rust-skills/doc-hidden-setup.md` only.",
        "Review every executable example introduced by PR 29.8, record its visible line count and imports, and explain why no line is incidental enough to hide.",
        "The note lists each example and states the threshold for revisiting the decision when a future example needs multi-line fixture construction.",
        "Do not hide the value construction or assertion that teaches the API.",
    ),
    "doc-link-types": spec(
        "superseded by PR 29.7", "—",
        r"RUSTDOCFLAGS='-D rustdoc::broken_intra_doc_links' cargo doc --workspace --no-deps",
        "This rule duplicates `doc-intra-links`; PR 29.7 already converts related type/function references and installs the broken-link proof.",
        "`docs/implementation/notes/rust-skills/doc-link-types.md` only.",
        "Record the rule equivalence, link PR 29.7's scope and rustdoc command, and audit only for residual plain-text `See also`/`Related` references introduced after that PR.",
        "The note reports zero residual actionable references or names each intentional external/non-item term.",
        "Do not churn link spelling that already resolves or repeat PR 29.7's edits.",
    ),
    "doc-cargo-metadata": spec(
        "change", "—",
        r"for f in crates/*/Cargo.toml tests/e2e/Cargo.toml; do printf '%s ' \"$f\"; rg -n '^(name|version|license|rust-version|publish|description|repository|readme)\s*=' \"$f\" || true; done",
        "Walrus is deployed as a monorepo application and none of its internal crates has a crates.io publishing contract, but five member manifests still inherit Cargo's publishable default.",
        "`Cargo.toml` and `crates/{common,control,transformer,extractor,pg-to-arrow}/Cargo.toml`.",
        "Set `publish = false` in `[workspace.package]` and add `publish.workspace = true` to the five crate packages; retain e2e's existing explicit false setting.",
        "`cargo metadata --no-deps` reports `publish: []` for all six members and no invented crates.io metadata is added.",
        "Do not invent keywords, homepage, authors, repository URLs, or docs.rs links.",
    ),
    "doc-crate-readme": spec(
        "evidence", "—",
        r"find crates tests/e2e -maxdepth 2 -name README.md -print | sort",
        "The internal crates are components of one deployed product, not independently published libraries; the root README and crate module docs serve different audiences and cannot be safely unified with include_str.",
        "`docs/implementation/notes/rust-skills/doc-crate-readme.md` only.",
        "Record the absence of per-crate READMEs, identify root README sections whose relative links would be wrong under rustdoc, and define publication as the reversal condition.",
        "The note demonstrates why `#![doc = include_str!(...)]` would duplicate product/operator material in internal API docs.",
        "Do not create five placeholder READMEs or include the repository README from every crate.",
    ),
    "obs-tracing-over-log": spec(
        "evidence", "—",
        r"rg -n 'println!|eprintln!|log::' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' --glob '!**/benches/**' || true",
        "Production diagnostics already use tracing; the only `eprintln!` calls are binary bootstrap failures that occur before a subscriber can be installed.",
        "`docs/implementation/notes/rust-skills/obs-tracing-over-log.md` only.",
        "Classify each stderr bootstrap fallback, confirm no library or post-init path prints directly, and record why those pre-subscriber errors must remain visible without tracing.",
        "The note accounts for every direct-print hit and identifies any post-init hit as a blocking contradiction rather than an optional cleanup.",
        "Do not route tracing-initialization failure through the subscriber that failed to initialize.",
    ),
    "obs-library-facade": spec(
        "evidence", "—",
        r"rg -n 'init_tracing|tracing_subscriber|\.init\(\)|\.try_init\(' crates --glob '*.rs'",
        "Subscriber construction is centralized in `common::telemetry::init_tracing` and invoked only from the two owning binaries; other library code only emits events.",
        "`docs/implementation/notes/rust-skills/obs-library-facade.md` only.",
        "Trace every initializer call from `main`, verify it is absent from reusable library operations/tests, and document common::telemetry as an internal application bootstrap adapter rather than a self-initializing library.",
        "The note lists the initializer definition and all call sites and confirms a second installation is handled without panic.",
        "Do not duplicate subscriber setup in binaries or remove the shared configuration policy.",
    ),
    "obs-structured-fields": spec(
        "change", "common,transformer,extractor",
        r"rg -n 'tracing::(trace|debug|info|warn|error)!\([^;]*\{[A-Za-z_][A-Za-z0-9_]*(?::[#?])?\}' crates/{transformer,extractor}/src --glob '*.rs' || true",
        "Several transformer and extractor events interpolate errors or identifiers into message text even though common::telemetry defines stable structured-field keys.",
        "Production Rust files under `crates/transformer/src` and `crates/extractor/src` containing probe hits, plus `crates/common/src/telemetry_test.rs`.",
        "Move every dynamic value in a matching tracing message into a named field, use canonical keys from `common::telemetry::fields` where defined, and leave the message as stable prose.",
        "The probe has zero production hits, telemetry tests assert representative JSON field names, and both packages pass.",
        "Preserve the pre-subscriber `eprintln!` fallbacks and do not log SQL, credentials, or full configuration.",
    ),
    "obs-instrument-spans": spec(
        "evidence", "—",
        r"rg -n '#\[(tracing::)?instrument|span!|\.instrument\(' crates --glob '*.rs' || true",
        "Walrus's long-lived loops multiplex many tables/transactions, so function-wide instrument spans would retain high-cardinality arguments and misstate request boundaries; events already carry explicit CDC keys.",
        "`docs/implementation/notes/rust-skills/obs-instrument-spans.md` only.",
        "Review apply_loop, replication consume, and reload export boundaries; record their lifetime/cardinality and the structured fields that already correlate work.",
        "The note names each candidate and a concrete future request-scoped boundary that would justify an instrument span.",
        "Do not instrument long-lived loops, database clients, record batches, credentials, or object-store handles.",
    ),
    "obs-levels-filter": spec(
        "evidence", "—",
        r"rg -n 'EnvFilter|DEFAULT_FILTER|RUST_LOG|with_env_filter' crates/common/src/telemetry.rs crates/*/src/main.rs",
        "The shared telemetry layer already implements explicit-config then RUST_LOG then safe-default precedence, with malformed directives tested.",
        "`docs/implementation/notes/rust-skills/obs-levels-filter.md` only.",
        "Record filter precedence, default level, malformed-input behavior, and the binary call sites; run the focused telemetry tests.",
        "The note cites focused test names for explicit, environment, default, and malformed filter cases.",
        "Do not add a second filtering layer or change production verbosity without an operator requirement.",
    ),
    "obs-error-chain": spec(
        "change", "transformer,extractor",
        r"rg -n 'tracing::(warn|error)!.*(error = %|\{e(?::[#?])?\})' crates/{transformer,extractor}/src --glob '*.rs' || true",
        "Warning/error events still render only the outer Display error at several retry and terminal boundaries, hiding anyhow/thiserror sources.",
        "Production Rust files under `crates/transformer/src` and `crates/extractor/src` containing probe hits, plus their existing telemetry tests.",
        "Represent the error as a structured Debug field (`error = ?e`) at warning/error boundaries after PR 30.3 and keep context messages stable.",
        "The probe has no `%`/interpolated error at warn/error sites, and both service packages pass their existing failure-path tests.",
        "Do not expose connection strings or switch informational events to error severity.",
    ),
    "obs-no-sensitive-data": spec(
        "evidence", "—",
        r"rg -ni '(trace|debug|info|warn|error)!.*(password|secret|credential|access_key|dsn|database_url|control_db_url)' crates --glob '*.rs' || true",
        "Credential-bearing configuration is passed to clients but current tracing events log only safe identifiers, addresses, states, and error objects.",
        "`docs/implementation/notes/rust-skills/obs-no-sensitive-data.md` only.",
        "Audit config structs and every probe hit, distinguish field names in validation errors from secret values, and enumerate the safe logging allowlist used by both services.",
        "The note accounts for all hits and states that any event logging a config Debug representation is a blocking failure.",
        "Do not add redaction wrappers without an actual logging boundary or log secrets in tests.",
    ),
    "perf-iter-over-index": spec(
        "evidence", "—",
        r"rg -n 'while [A-Za-z_][A-Za-z0-9_]* < .*\.len\(\)|for [A-Za-z_][A-Za-z0-9_]* in 0\.\.' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Residual indexing is confined to stateful byte/text parsers that advance by variable widths or correlate builder and field slices; a mechanical iterator rewrite would obscure bounds invariants.",
        "`docs/implementation/notes/rust-skills/perf-iter-over-index.md` only.",
        "Classify tier2, geometric, range, reload-signal, replication, and batch-builder loops by variable-step/correlated-slice need and cite their focused parser tests.",
        "The note accounts for every loop and names profiling plus a simpler equivalent implementation as the reversal criteria.",
        "Do not rewrite parsers or correlated indexes for style alone.",
    ),
    "perf-iter-lazy": spec(
        "evidence", "—",
        r"rg -n '\.collect::<|: (Vec|HashSet|HashMap)<.*> = .*\.collect\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Current collections cross an API, ownership, SQL bind, sorting, or repeated-use boundary; no collection exists solely to feed the next iterator adapter.",
        "`docs/implementation/notes/rust-skills/perf-iter-lazy.md` only.",
        "Classify each production collect by its consumer and lifetime, with special attention to reload SQL construction and transformer transform rendering.",
        "The note records every candidate as required materialization or identifies a blocking contradiction; no speculative rewrite is allowed.",
        "Do not make SQL/rendering iterators escape functions or change deterministic ordering.",
    ),
    "perf-collect-once": spec(
        "evidence", "—",
        r"rg -n '\.collect\(\)|\.collect::<[^>]+>\(\)' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "No production pipeline collects an intermediate collection and immediately consumes it into another collection; observed materializations have named reuse or boundary semantics.",
        "`docs/implementation/notes/rust-skills/perf-collect-once.md` only.",
        "Trace each collected local through its next uses and record whether it is reused, joined, bound to SQL, or returned.",
        "The note contains a consumer classification for every credible intermediate candidate and a zero-action conclusion.",
        "Do not fuse transforms that would alter error order, SQL column order, or borrow lifetimes.",
    ),
    "perf-entry-api": spec(
        "evidence", "—",
        r"rg -n 'HashMap|BTreeMap|\.entry\(|\.get\(|\.insert\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Insert-or-update paths in stream_txn, consume, memory, and ddl already use entry; remaining inserts are unconditional cache replacement or hydration.",
        "`docs/implementation/notes/rust-skills/perf-entry-api.md` only.",
        "Audit each map insertion with its preceding lookup and record whether replacement, duplicate rejection, or entry mutation is intended.",
        "The note proves every insert-or-update uses entry and names unconditional replacement sites as intentional.",
        "Do not convert replacement inserts to `or_insert` or change duplicate semantics.",
    ),
    "perf-drain-reuse": spec(
        "evidence", "—",
        r"rg -n 'mem::take|\.clear\(\)|\.drain\(|= Vec::new\(\)' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' || true",
        "Transaction and batch buffers transfer ownership into sealed work with `mem::take`; draining would retain borrows or copy elements and cannot reuse capacity across the ownership boundary.",
        "`docs/implementation/notes/rust-skills/perf-drain-reuse.md` only.",
        "Audit each resettable collection and identify who owns the elements after reset; record why move-out or clear is correct.",
        "The note names all reusable candidates and requires benchmark evidence before changing a move-out boundary.",
        "Do not replace `mem::take` where the old allocation belongs to the returned value.",
    ),
    "perf-extend-batch": spec(
        "evidence", "—",
        r"rg -n 'for .*\{|\.push\(|\.extend\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Repeated pushes in production either compute fallible/stateful values per iteration or update Arrow builders that do not expose equivalent Extend semantics.",
        "`docs/implementation/notes/rust-skills/perf-extend-batch.md` only.",
        "Inspect loops that only push, separate pure mapping from fallible/stateful mutation, and record the consumer type's actual batch API.",
        "The note lists every credible pure-push candidate and explains why none gains a simpler or faster extend call.",
        "Do not collect merely to call extend or hide per-item error propagation.",
    ),
    "perf-chain-avoid": spec(
        "evidence", "—",
        r"rg -n '\.chain\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' || true",
        "The remaining iterator chain pads at most six fractional timestamp digits in a parser; it is bounded, clear, and not a measured hot loop.",
        "`docs/implementation/notes/rust-skills/perf-chain-avoid.md` only.",
        "Record each chain, its maximum cardinality, benchmark coverage, and allocation behavior.",
        "The note demonstrates bounded work and identifies a measured regression as the only reversal condition.",
        "Do not replace a bounded iterator with manual indexing or allocation.",
    ),
    "perf-collect-into": spec(
        "evidence", "—",
        r"rg -n '\.collect\(|\.collect_into\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "No collected destination survives across calls with clear reusable ownership; destinations are returned, stored, or sized from new schema/input data.",
        "`docs/implementation/notes/rust-skills/perf-collect-into.md` only.",
        "For every repeated-call collect candidate, identify destination lifetime and capacity ownership and cite benchmark coverage where present.",
        "The note names any long-lived reusable buffer or concludes none exists; no unstable API assumption is made.",
        "Do not add mutable scratch buffers to public APIs or state structs without profiling.",
    ),
    "perf-black-box-bench": spec(
        "evidence", "—",
        r"rg -n 'black_box|bench_function|iter_batched' crates/*/benches --glob '*.rs'",
        "All Criterion targets already black-box benchmark inputs or outputs at the optimization boundary; transformer transform uses iter_batched inputs whose outputs Criterion consumes.",
        "`docs/implementation/notes/rust-skills/perf-black-box-bench.md` only.",
        "Inventory each benchmark closure, name the value preventing dead-code elimination, and run `cargo bench --no-run` for all benchmark packages.",
        "The note accounts for every benchmark and flags a closure only if neither Criterion nor explicit black_box consumes its result.",
        "Do not sprinkle black_box inside the operation being measured or change benchmark workloads.",
    ),
    "perf-release-profile": spec(
        "evidence", "—",
        r"sed -n '/^\[profile.release\]/,/^\[/p' Cargo.toml",
        "The release profile already enables thin LTO and explicitly records why codegen-units=1 is rejected for CI/link-time cost.",
        "`docs/implementation/notes/rust-skills/perf-release-profile.md` only.",
        "Record the current release settings, the Phase 5 benchmark/build-time decision, and artifact defaults inherited from Cargo.",
        "The note cites measured trade-offs and requires new benchmark plus link-time data before profile changes.",
        "Do not add abort panics, strip symbols, fat LTO, or codegen-units=1 without deployment/debugging evidence.",
    ),
    "perf-profile-first": spec(
        "evidence", "—",
        r"rg -n 'bench|baseline|profile|throughput' docs/benchmarks.md justfile scripts --glob '*.md' --glob '*.sh' --glob 'justfile' || true",
        "Walrus already has Criterion microbenchmarks, an e2e throughput harness, and documented baseline comparison; this rule is process policy rather than a code change.",
        "`docs/implementation/notes/rust-skills/perf-profile-first.md` only.",
        "Map each production hot-path claim to an existing benchmark or record the gap; state the required before/after evidence for future performance PRs.",
        "The note defines reproducible baseline, comparison, and hardware-context fields and names all current benchmark targets.",
        "Do not optimize a candidate found only by grep.",
    ),
    "perf-ahash": spec(
        "evidence", "—",
        r"rg -n 'HashMap|HashSet|AHash|FxHash' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Maps accept source-derived relation IDs, schema/table names, and transaction IDs; no benchmark attributes material cost to SipHash, and adversarial inputs are not categorically excluded.",
        "`docs/implementation/notes/rust-skills/perf-ahash.md` only.",
        "Inventory every map key, trust boundary, maximum cardinality, and benchmark coverage; record why the default hasher remains the safe baseline.",
        "The note requires a profile showing hashing as a bottleneck plus a documented DoS boundary before reconsidering.",
        "Do not add ahash/FxHash or globally alias HashMap without evidence.",
    ),
    "perf-io-buffering": spec(
        "evidence", "—",
        r"rg -n 'File::|std::fs::|\.read(_exact|_to_end)?\(|\.write(_all)?\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Production I/O is database/object-store API calls, whole protocol-frame AsyncWriteExt writes, Parquet writers, or one-shot filesystem removal; there is no many-small-read/write file loop.",
        "`docs/implementation/notes/rust-skills/perf-io-buffering.md` only.",
        "Classify each I/O call by buffering already supplied by its library and call frequency; identify any direct File loop as a blocking contradiction.",
        "The note accounts for every direct I/O hit and names a repeated small-operation profile as the reversal condition.",
        "Do not double-buffer Parquet, object_store, SQLx, Tokio sockets, or DuckDB.",
    ),
    "proj-lib-main-split": spec(
        "change", "transformer,extractor",
        r"wc -l crates/{transformer,extractor}/src/main.rs && rg -n '^async fn run|^fn main|^async fn main' crates/{transformer,extractor}/src/main.rs",
        "Both binary main files own substantial bootstrap/run-loop logic that cannot be exercised as one application function from tests.",
        "`crates/transformer/src/main.rs`, `crates/transformer/src/app.rs`, `crates/transformer/src/lib.rs`, `crates/extractor/src/main.rs`, `crates/extractor/src/app.rs`, and `crates/extractor/src/lib.rs`.",
        "Move each service's post-runtime application orchestration into `pub async fn run(config) -> Result` in an app module; keep argument/config loading, telemetry setup, runtime construction, exit reporting, and ExitCode mapping in main.",
        "Each main is a thin owner of process-global setup, both library app functions compile under package tests, and behavior/order of bootstrap and shutdown is unchanged.",
        "Do not merge service modules, redesign config, or move subscriber/runtime ownership into libraries.",
    ),
    "proj-mod-by-feature": spec(
        "evidence", "—",
        r"find crates -path '*/src/*.rs' -type f ! -name '*_test.rs' -print | sort",
        "Modules are already named for CDC/control/transformer capabilities such as replication, reload, checkpoint, transform, and health rather than generic models/services/utilities buckets.",
        "`docs/implementation/notes/rust-skills/proj-mod-by-feature.md` only.",
        "Inventory each crate's top-level modules and map them to one product capability; call out shared primitive modules in common as the intentional exception.",
        "The note finds no type-layer bucket requiring a move and gives a reversal condition for a future generic `models`/`utils` module.",
        "Do not rename modules or churn import paths for taxonomy alone.",
    ),
    "proj-flat-small": spec(
        "evidence", "—",
        r"find crates -path '*/src/*.rs' -type f | awk -F/ '{print NF, $0}' | sort -n",
        "Walrus is not a small single-crate project; its mostly flat per-crate trees already balance discoverability with one cohesive pgoutput submodule.",
        "`docs/implementation/notes/rust-skills/proj-flat-small.md` only.",
        "Record module depth and file counts per crate and explain why further flattening would cross package/capability boundaries.",
        "The note documents the only nested production module and its cohesion.",
        "Do not collapse the workspace or move pgoutput files to satisfy a small-project heuristic.",
    ),
    "proj-mod-rs-dir": spec(
        "evidence", "—",
        r"find crates -path '*/src/*/mod.rs' -type f -print | sort",
        "The multi-file pgoutput decoder already uses `pgoutput/mod.rs`; other feature modules are single files with sibling test files, not multi-file module directories.",
        "`docs/implementation/notes/rust-skills/proj-mod-rs-dir.md` only.",
        "Inventory module directories and distinguish sibling tests from production submodules.",
        "The note proves every multi-file production module has one module root and no orphan submodule directory.",
        "Do not create one-file directories or move sibling tests under mod.rs directories.",
    ),
    "proj-pub-crate-internal": spec(
        "evidence", "—",
        r"rg -n '^\s*pub(\(crate\))? (async )?(fn|struct|enum|trait|type|mod)' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "Broad visibility is primarily required by crate-root integration tests and cross-crate service composition; known private helpers already use pub(crate) or private visibility.",
        "`docs/implementation/notes/rust-skills/proj-pub-crate-internal.md` only.",
        "For each public module root, identify its external consumer in another package or integration test; record any unconsumed item as a blocking contradiction rather than choosing a replacement API.",
        "The note classifies the reachable surface and confirms `cargo test --all-targets` needs the retained integration seams.",
        "Do not narrow visibility until the owning integration test/public consumer is migrated by a dedicated API task.",
    ),
    "proj-pub-super-parent": spec(
        "evidence", "—",
        r"rg -n '^\s*pub\((crate|super)\)' crates --glob '*.rs' || true",
        "No item is consumed exclusively by its parent module: the only nested production module is pgoutput and its pieces are private or re-exported through that root.",
        "`docs/implementation/notes/rust-skills/proj-pub-super-parent.md` only.",
        "Audit nested-module visibility in pgoutput and list every pub(crate) consumer across sibling modules.",
        "The note finds no parent-only candidate or names it as a blocking contradiction.",
        "Do not use pub(super) in flat modules where it is equivalent to an unclear crate-level boundary.",
    ),
    "proj-pub-use-reexport": spec(
        "evidence", "—",
        r"rg -n '^pub use|^pub mod' crates/*/src/lib.rs",
        "Common, control, and pg-to-arrow already re-export their intended convenience surfaces; transformer and extractor retain module-qualified service APIs to avoid collisions and hide no stable façade.",
        "`docs/implementation/notes/rust-skills/proj-pub-use-reexport.md` only.",
        "Inventory current reexports, their downstream use, and duplicate type/function names that make wildcard flattening undesirable.",
        "The note distinguishes convenience façades from module-qualified integration seams and identifies an actual repeated downstream import as the reversal condition.",
        "Do not re-export every public module item or introduce ambiguous root names.",
    ),
    "proj-prelude-module": spec(
        "evidence", "—",
        r"rg -n 'prelude|use (common|control|transformer|extractor|pg_to_arrow)::\{' crates tests --glob '*.rs' || true",
        "No external consumer repeats a stable, coherent import set; a prelude would hide which CDC/control types a module depends on and expand silently.",
        "`docs/implementation/notes/rust-skills/proj-prelude-module.md` only.",
        "Measure repeated cross-crate import sets and record their frequency and semantic cohesion.",
        "The note concludes no prelude or identifies a repeated set as a blocker requiring a separate public-API decision.",
        "Do not add wildcard imports or a prelude for internal convenience.",
    ),
    "proj-bin-dir": spec(
        "evidence", "—",
        r"find crates -path '*/src/main.rs' -o -path '*/src/bin/*.rs' | sort && rg -n '^\[\[bin\]\]|^name = \"walrus-' crates/*/Cargo.toml",
        "Each service package owns exactly one explicitly named binary; `src/bin/` is useful only when one package owns multiple binaries.",
        "`docs/implementation/notes/rust-skills/proj-bin-dir.md` only.",
        "Inventory binary targets by package and record their manifest paths and product ownership.",
        "The note confirms one binary per service package and states a second binary in either package as the reversal condition.",
        "Do not move a single main into src/bin or merge both services into one package.",
    ),
    "proj-workspace-large": spec(
        "evidence", "—",
        r"cargo metadata --no-deps --format-version 1",
        "Walrus already uses a six-member resolver-2 workspace aligned to shared primitives, control, conversion, two services, and e2e.",
        "`docs/implementation/notes/rust-skills/proj-workspace-large.md` only.",
        "Record member names, package roles, dependency direction, and the single lockfile/target policy from cargo metadata.",
        "The note accounts for all members and finds no source package outside the workspace.",
        "Do not split crates further without an ownership/compile-boundary requirement.",
    ),
    "proj-workspace-deps": spec(
        "evidence", "—",
        r"rg -n '^[A-Za-z0-9_-]+ = (\"|\{ version)' crates/*/Cargo.toml tests/e2e/Cargo.toml || true",
        "Third-party versions are centralized in `[workspace.dependencies]`; member manifests inherit them, while path dependencies intentionally encode local package edges.",
        "`docs/implementation/notes/rust-skills/proj-workspace-deps.md` only.",
        "Audit every direct version declaration in members, classify path dependencies separately, and verify one resolved version per shared third-party crate with cargo metadata.",
        "The note has zero unexplained member-local third-party version pins.",
        "Do not move path dependencies into workspace.dependencies solely for visual uniformity.",
    ),
    "proj-feature-additive": spec(
        "evidence", "—",
        r"for f in crates/*/Cargo.toml tests/e2e/Cargo.toml; do echo \"$f\"; sed -n '/^\[features\]/,/^\[/p' \"$f\"; done",
        "The four workspace features only add SQLx impls, integration tests, DuckDB conformance, or e2e tests; none disables behavior or selects a mutually exclusive backend.",
        "`docs/implementation/notes/rust-skills/proj-feature-additive.md` only.",
        "Record each feature's dependency/cfg additions and run default plus all-features checks for its package.",
        "The note proves enabling each feature is a superset of the default surface and lists the exact cargo check commands.",
        "Do not add cargo-hack or a feature matrix until feature count/interaction justifies it.",
    ),
    "proj-msrv-declare": spec(
        "evidence", "—",
        r"rg -n 'rust-version|channel' Cargo.toml rust-toolchain.toml .github/workflows/ci.yml",
        "Workspace rust-version, the toolchain file, and the CI MSRV drift/build job already establish one Rust 1.96 contract inherited by every member.",
        "`docs/implementation/notes/rust-skills/proj-msrv-declare.md` only.",
        "Record all three version sources, inheritance in members, and the CI command that detects drift.",
        "The note demonstrates exact version agreement and a successful metadata resolution.",
        "Do not lower or raise MSRV in a rule-audit ticket.",
    ),
    "proj-build-rs-minimal": spec(
        "evidence", "—",
        r"find . -name build.rs -not -path './target/*' -print | sort",
        "Walrus has no first-party build script; native dependency build scripts are outside repository policy and cannot be rewritten here.",
        "`docs/implementation/notes/rust-skills/proj-build-rs-minimal.md` only.",
        "Record the zero first-party result and distinguish Cargo registry/git dependency build scripts from workspace sources.",
        "The note states that adding a first-party build.rs triggers deterministic inputs, rerun-if directives, and hermetic-output review.",
        "Do not add a build script or vendor dependency build logic.",
    ),
    "lint-deny-correctness": spec(
        "evidence", "—",
        r"sed -n '/^\[workspace.lints.clippy\]/,/^\[/p' Cargo.toml && rg -n '^\[lints\]|workspace = true' crates/*/Cargo.toml tests/e2e/Cargo.toml",
        "`clippy::all = deny` already includes correctness and is inherited by every workspace member; adding correctness repeats policy without changing diagnostics.",
        "`docs/implementation/notes/rust-skills/lint-deny-correctness.md` only.",
        "Record Clippy group inclusion, inheritance for all six members, and a clean all-targets/all-features run.",
        "The note demonstrates correctness cannot be opted out silently and identifies an allow as a blocking contradiction.",
        "Do not add a redundant group entry or crate-level attribute.",
    ),
    "lint-warn-suspicious": spec(
        "superseded by PR 33.1", "—",
        r"cargo clippy --workspace --all-targets --all-features -- -D warnings",
        "Clippy all already contains suspicious and is denied workspace-wide; PR 33.1 records that enforcement.",
        "`docs/implementation/notes/rust-skills/lint-warn-suspicious.md` only.",
        "Record the group membership, PR 33.1 policy, and clean full Clippy command.",
        "The note finds no narrower allow for suspicious and no member missing lint inheritance.",
        "Do not downgrade an already denied group to warn or duplicate configuration.",
    ),
    "lint-warn-style": spec(
        "superseded by PR 33.1", "—",
        r"cargo clippy --workspace --all-targets --all-features -- -D warnings",
        "Clippy all already contains style and is denied workspace-wide; PR 33.1 records that enforcement.",
        "`docs/implementation/notes/rust-skills/lint-warn-style.md` only.",
        "Record the group membership, PR 33.1 policy, and clean full Clippy command.",
        "The note finds no narrower allow for style and no member missing lint inheritance.",
        "Do not downgrade an already denied group to warn or duplicate configuration.",
    ),
    "lint-warn-complexity": spec(
        "superseded by PR 33.1", "—",
        r"cargo clippy --workspace --all-targets --all-features -- -D warnings",
        "Clippy all already contains complexity and is denied workspace-wide; PR 33.1 records that enforcement.",
        "`docs/implementation/notes/rust-skills/lint-warn-complexity.md` only.",
        "Record the group membership, PR 33.1 policy, and clean full Clippy command.",
        "The note finds no narrower allow for complexity and no member missing lint inheritance.",
        "Do not downgrade an already denied group to warn or duplicate configuration.",
    ),
    "lint-warn-perf": spec(
        "superseded by PR 33.1", "—",
        r"cargo clippy --workspace --all-targets --all-features -- -D warnings",
        "Clippy all already contains perf and is denied workspace-wide; PR 33.1 records that enforcement.",
        "`docs/implementation/notes/rust-skills/lint-warn-perf.md` only.",
        "Record the group membership, PR 33.1 policy, and clean full Clippy command.",
        "The note finds no narrower allow for perf and no member missing lint inheritance.",
        "Do not downgrade an already denied group to warn or duplicate configuration.",
    ),
    "lint-pedantic-selective": spec(
        "evidence", "—",
        r"rg -n 'clippy::|\[workspace.lints.clippy\]' Cargo.toml crates --glob '*.toml' --glob '*.rs'",
        "Earlier rule tickets already select narrow restriction/pedantic/nursery lints tied to concrete invariants; enabling the full pedantic group would create unrelated churn.",
        "`docs/implementation/notes/rust-skills/lint-pedantic-selective.md` only.",
        "Inventory every explicit non-all lint, link it to the task/invariant that justified it, and run `cargo clippy` with pedantic at warn solely to classify residual diagnostics in the note.",
        "The note records diagnostic counts/categories and adds no lint without a specific low-noise invariant.",
        "Do not enable clippy::pedantic as a group or fix diagnostics outside an owning rule.",
    ),
    "lint-missing-docs": spec(
        "change", "common,control,transformer,extractor,pg-to-arrow,e2e",
        r"RUSTDOCFLAGS='-D missing_docs -D rustdoc::broken_intra_doc_links' cargo doc --workspace --no-deps",
        "Phase 29 completes public and module documentation; the invariant can now be made permanent at workspace level.",
        "`Cargo.toml` only.",
        "Add `missing_docs = deny` and `rustdoc::broken_intra_doc_links = deny` to workspace lint policy using the supported rust/rustdoc tables; preserve inheritance in every member.",
        "Workspace doc, all-target Clippy, and tests pass with no crate-level missing-doc allow.",
        "Do not document generated/dependency code or weaken the lint for integration tests; a Phase 29 documentation regression blocks this task for re-audit.",
    ),
    "lint-unsafe-doc": spec(
        "evidence", "—",
        r"rg -n '\bunsafe\b|missing_safety_doc|undocumented_unsafe_blocks' Cargo.toml crates --glob '*.toml' --glob '*.rs' || true",
        "Phase 12 establishes a zero-unsafe first-party tree and safety policy; there is no unsafe site for an additional documentation lint to diagnose.",
        "`docs/implementation/notes/rust-skills/lint-unsafe-doc.md` only.",
        "Record the zero-site audit and the exact Phase 12 tripwire/lint that would make future unsafe require documentation.",
        "The note proves both unsafe functions and blocks are absent and names the reversal condition.",
        "Do not enable a redundant lint with no first-party target or inspect dependency source.",
    ),
    "lint-cargo-metadata": spec(
        "evidence", "—",
        r"cargo metadata --no-deps --format-version 1",
        "Clippy cargo is designed for crates.io packages, while every Walrus member is an internal application component and PR 29.11 records the non-publication decision.",
        "`docs/implementation/notes/rust-skills/lint-cargo-metadata.md` only.",
        "Record package publication intent and run clippy::cargo at warn to classify which diagnostics are publication-only versus real dependency issues.",
        "The note accounts for diagnostics and requires an explicit publish decision before enabling the group.",
        "Do not invent crates.io metadata or blanket-allow individual cargo lints.",
    ),
    "lint-rustfmt-check": spec(
        "evidence", "—",
        r"rg -n 'cargo fmt( --all)? --check|components:.*rustfmt|rustfmt' .github/workflows/ci.yml justfile rust-toolchain.toml",
        "The pinned toolchain includes rustfmt and CI runs cargo fmt --check before compile-heavy gates; `just fmt` mirrors it.",
        "`docs/implementation/notes/rust-skills/lint-rustfmt-check.md` only.",
        "Record toolchain pinning, CI step order, and local recipe equivalence; run the exact command.",
        "The note proves one command is shared semantically by local and CI workflows.",
        "Do not add a formatter that rewrites in CI or a duplicate job.",
    ),
    "lint-workspace-lints": spec(
        "evidence", "—",
        r"sed -n '/^\[workspace.lints/,/^\[workspace.dependencies\]/p' Cargo.toml && rg -n '^\[lints\]|workspace = true' crates/*/Cargo.toml tests/e2e/Cargo.toml",
        "Lint policy is centralized and all six members explicitly inherit it.",
        "`docs/implementation/notes/rust-skills/lint-workspace-lints.md` only.",
        "Inventory workspace lint tables and member inheritance; compare cargo metadata member count to inheritance count.",
        "The note proves all members inherit and no member-local lint table overrides policy.",
        "Do not copy workspace lint entries into member crates.",
    ),
    "lint-cfg-check": spec(
        "evidence", "—",
        r"rg -n '#\[cfg|unexpected_cfgs|check-cfg' crates tests Cargo.toml --glob '*.rs' --glob '*.toml' || true",
        "All first-party cfg conditions use Cargo-declared feature values or built-in `test`; warnings are denied, so rustc's unexpected_cfgs warning already fails CI without custom cfg declarations.",
        "`docs/implementation/notes/rust-skills/lint-cfg-check.md` only.",
        "Inventory cfg names/values, match feature values to manifests, and run a clean all-features check under denied warnings.",
        "The note has zero undeclared feature/custom cfg and states that a future custom cfg must add check-cfg metadata in the same PR.",
        "Do not add empty custom-cfg allowlists or duplicate Cargo's feature declarations.",
    ),
    "lint-clippy-nursery-selected": spec(
        "evidence", "—",
        r"cargo clippy --workspace --all-targets --all-features -- -W clippy::nursery",
        "Nursery diagnostics are intentionally unstable; earlier tasks select individual lints only when they enforce a measured Walrus invariant.",
        "`docs/implementation/notes/rust-skills/lint-clippy-nursery-selected.md` only.",
        "Run the nursery group at warn without editing, classify every diagnostic by false-positive/churn/stable-invariant potential, and record the Clippy version.",
        "The note includes the full diagnostic inventory and no whole-group configuration; any proposed individual lint must have zero current violations and a named invariant.",
        "Do not fix nursery diagnostics or enable the group in this evidence task.",
    ),
    "anti-unwrap-abuse": spec(
        "superseded by PR 7.7", "—",
        r"rg -n '\.unwrap\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' --glob '!**/benches/**' || true",
        "PRs 7.6-7.7 removed production unwrap and deny clippy::unwrap_used with explicit test/bench boundaries.",
        "`docs/implementation/notes/rust-skills/anti-unwrap-abuse.md` only.",
        "Record the zero production probe, lint configuration, and scoped test/bench exceptions.",
        "The note identifies any production hit as a blocking regression owned by the existing lint policy.",
        "Do not change test unwraps or add a second lint.",
    ),
    "anti-expect-lazy": spec(
        "superseded by PR 7.7", "—",
        r"rg -n '\.expect\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' --glob '!**/benches/**' || true",
        "PRs 7.6-7.7 deny production expect; the single metrics recorder invariant has a narrow documented allow because failure means conflicting process-global initialization.",
        "`docs/implementation/notes/rust-skills/anti-expect-lazy.md` only.",
        "Account for every production hit, verify the metrics allow reason and OnceLock guard, and record all test/bench exemptions separately.",
        "The note proves no expect handles a recoverable I/O, input, network, or database error.",
        "Do not replace the intentional invariant without an API-level recorder initialization design.",
    ),
    "anti-clone-excessive": spec(
        "superseded by PR 9.1", "—",
        r"rg -n '\.clone\(|Arc::clone|clone_from' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "PR 9.1 owns the borrow-over-clone audit and records remaining ownership-transfer/async/task-boundary clones; this anti-rule adds no new decision boundary.",
        "`docs/implementation/notes/rust-skills/anti-clone-excessive.md` only.",
        "Re-run the clone inventory, compare it to PR 9.1's retained exceptions, and record only new residual sites.",
        "The note reports zero unclassified post-9.1 clones or identifies a regression that must be fixed under the existing policy.",
        "Do not redesign APIs or lifetimes after the owning audit.",
    ),
    "anti-lock-across-await": spec(
        "superseded by PR 14.3", "—",
        r"rg -n '\.(lock|read|write)\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "PR 14.3 tightens the only candidate scope and denies Clippy's await-holding lock/refcell guards.",
        "`docs/implementation/notes/rust-skills/anti-lock-across-await.md` only.",
        "Re-run the lock-site inventory, link each guard lifetime to PR 14.3 proof, and run full Clippy.",
        "The note reports zero guard live across await and identifies any lint allow as a blocking regression.",
        "Do not replace parking_lot/Tokio synchronization or duplicate the lint.",
    ),
    "anti-string-for-str": spec(
        "superseded by PR 9.2", "—",
        r"rg -n '&String' crates --glob '*.rs' || true",
        "PR 9.2 owns the borrowed-container API audit and establishes &str at read-only string boundaries.",
        "`docs/implementation/notes/rust-skills/anti-string-for-str.md` only.",
        "Run the exact signature probe and compare any hit to PR 9.2's exception record.",
        "The note reports zero unclassified &String parameters.",
        "Do not change owned String fields or values that cross ownership boundaries.",
    ),
    "anti-vec-for-slice": spec(
        "superseded by PR 9.2", "—",
        r"rg -n '&Vec<' crates --glob '*.rs' || true",
        "PR 9.2 owns the borrowed-container API audit and establishes slices at read-only vector boundaries.",
        "`docs/implementation/notes/rust-skills/anti-vec-for-slice.md` only.",
        "Run the exact signature probe and compare any hit to PR 9.2's exception record.",
        "The note reports zero unclassified &Vec parameters.",
        "Do not change owned Vec fields or APIs that require capacity mutation.",
    ),
    "anti-index-over-iter": spec(
        "superseded by PR 31.1", "—",
        r"rg -n 'while [A-Za-z_][A-Za-z0-9_]* < .*\.len\(\)|for [A-Za-z_][A-Za-z0-9_]* in 0\.\.' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "PR 31.1 performs the identical iterator-versus-index audit and records variable-width parser/correlated-slice exceptions.",
        "`docs/implementation/notes/rust-skills/anti-index-over-iter.md` only.",
        "Re-run PR 31.1's probe and record only sites introduced after that task.",
        "The note reports zero new unclassified index loops and links every retained parser exception.",
        "Do not reopen measured exceptions without new profiling or safety evidence.",
    ),
    "anti-panic-expected": spec(
        "superseded by PR 10.9", "—",
        r"rg -n 'panic!|todo!|unimplemented!|unreachable!' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' --glob '!**/benches/**' || true",
        "PR 10.9 denies production panic macros, converts recoverable/impossible paths to Result/exhaustive matches, and pins only the deferred stub.",
        "`docs/implementation/notes/rust-skills/anti-panic-expected.md` only.",
        "Account for every residual macro against PR 10.9's narrow expectation and verify no runtime input reaches it.",
        "The note reports no panic for I/O, parsing, network, database, or user-controlled state.",
        "Do not remove the documented deferred-goal seam or weaken the lints.",
    ),
    "anti-empty-catch": spec(
        "change", "transformer,extractor",
        r"rg -n 'let _ =|if let Err\(_\)|Err\(_\) => \{\}|\.ok\(\)' crates/{transformer,extractor}/src --glob '*.rs' --glob '!**/*_test.rs'",
        "A small set of best-effort rollback, object deletion, and task-join paths discard errors without an event; parse-to-Option conversions are intentional and must remain.",
        "`crates/transformer/src/compaction.rs`, `crates/transformer/src/phase_b.rs`, `crates/transformer/src/main.rs`, `crates/extractor/src/stream_txn.rs`, and `crates/extractor/src/main.rs`.",
        "Replace discarded cleanup/join Results with explicit `if let Err(e)` warning events carrying operation and safe identifiers; retain parser `.ok()` conversions whose return type intentionally discards parse detail.",
        "The probe leaves only classified parse-to-Option/test sites, every retained discard has an adjacent rationale, and both service packages pass.",
        "Do not turn best-effort cleanup failure into a primary operation failure or log credentials/object payloads.",
    ),
    "anti-over-abstraction": spec(
        "evidence", "—",
        r"rg -n '^trait |<[^>]+>|impl (dyn|[A-Z])|Box<dyn|Arc<dyn' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' || true",
        "Walrus traits/generics correspond to real third-party seams, protocol writers, executors, clocks, and Arrow builders; there is no abstraction with a single speculative consumer after Phase 19.",
        "`docs/implementation/notes/rust-skills/anti-over-abstraction.md` only.",
        "Inventory first-party traits and generic public functions, list concrete implementations/callers, and classify third-party trait bounds separately.",
        "The note finds no first-party abstraction lacking a real second implementation or compile-time benefit.",
        "Do not collapse tested seams or change dispatch based on syntax count alone.",
    ),
    "anti-premature-optimize": spec(
        "superseded by PR 31.11", "—",
        r"rg -n 'bench|baseline|profile|throughput' docs/benchmarks.md justfile scripts --glob '*.md' --glob '*.sh' --glob 'justfile' || true",
        "PR 31.11 establishes profile-before-change evidence and maps current hot-path claims to benchmark targets.",
        "`docs/implementation/notes/rust-skills/anti-premature-optimize.md` only.",
        "Audit performance-oriented comments/config changes after PR 31.11 for a linked benchmark or profile.",
        "The note reports zero unmeasured optimization claims or names a blocking policy regression.",
        "Do not add or revert an optimization in this duplicate policy task.",
    ),
    "anti-type-erasure": spec(
        "superseded by PR 19.5", "—",
        r"rg -n '(Box|Arc)<dyn|&dyn|dyn [A-Za-z_]' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' || true",
        "PR 19.5 audits static versus dynamic dispatch; remaining ObjectStore/ArrayBuilder/stream objects require runtime heterogeneity or third-party APIs.",
        "`docs/implementation/notes/rust-skills/anti-type-erasure.md` only.",
        "Re-run the dynamic-dispatch inventory, map each site to PR 19.5's retained rationale, and record only newly introduced sites.",
        "The note reports zero unexplained Box/Arc/& dyn sites and includes implementation-count evidence.",
        "Do not monomorphize third-party/runtime-polymorphic seams without benchmark and binary-size data.",
    ),
    "anti-format-hot-path": spec(
        "evidence", "—",
        r"rg -n 'format!\(' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**' --glob '!**/benches/**'",
        "Format allocations build owned SQL, object keys, configuration errors, or infrequent lifecycle diagnostics; no profile identifies one as hot, and formatting is often the required output.",
        "`docs/implementation/notes/rust-skills/anti-format-hot-path.md` only.",
        "Classify each production format by call frequency, output ownership, and benchmark coverage; pay special attention to transform and reload rendering.",
        "The note names every loop-contained format and requires a profile plus reusable destination before any rewrite.",
        "Do not replace readable SQL/error construction with mutable buffers speculatively.",
    ),
    "anti-collect-intermediate": spec(
        "superseded by PR 31.3", "—",
        r"rg -n '\.collect\(\)|\.collect::<[^>]+>\(\)' crates --glob '*.rs' --glob '!**/*_test.rs' --glob '!**/tests/**'",
        "PR 31.3 performs the identical intermediate-collection consumer audit.",
        "`docs/implementation/notes/rust-skills/anti-collect-intermediate.md` only.",
        "Re-run PR 31.3's probe and inspect only collections introduced after that task.",
        "The note reports zero new immediate re-collection pipelines and links all retained ownership boundaries.",
        "Do not reopen SQL ordering or borrow-lifetime decisions without new evidence.",
    ),
    "anti-stringly-typed": spec(
        "superseded by PR 18.5", "—",
        r"rg -n 'match .*\.as_str\(\)|== \"(pending|ready|running|failed|complete|reload|resync|streaming)\"|status: String|kind: String' crates --glob '*.rs' || true",
        "PR 18.5 owns the stringly-domain audit; earlier manifest/reload/status tasks and the string_enum macro convert closed vocabularies while retaining protocol/database text at boundaries.",
        "`docs/implementation/notes/rust-skills/anti-stringly-typed.md` only.",
        "Re-run the closed-vocabulary probe, map residual strings to external protocol/database boundaries or PR 18.5 exceptions, and record only new domain-state strings.",
        "The note reports zero untyped closed-domain state introduced after PR 18.5.",
        "Do not wrap free-form SQL, identifiers, URLs, error messages, or protocol text.",
    ),
}


TITLES: dict[str, str] = {
    "test-cfg-test-module": "Guard sibling unit-test module wiring",
    "test-use-super": "Require `use super::*;` in sibling unit tests",
    "test-descriptive-names": "Reject placeholder names in sibling unit tests",
    "test-arrange-act-assert": "Split and label four multi-concern unit tests",
    "test-integration-dir": "Verify crate-root integration-test placement",
    "test-fixture-raii": "Replace fixed transformer test paths with `TempDir`",
    "test-tokio-async": "Run the reload concurrency test on paused Tokio time",
    "test-mock-traits": "Add hermetic object-store success and failure tests",
    "test-mockall-mocking": "Record PR 28.8 as the owner of object-store test doubles",
    "test-proptest-properties": "Property-test `Lsn` round trips and textual ordering",
    "test-should-panic": "Record why Walrus has no `#[should_panic]` contract",
    "test-doctest-examples": "Verify API doctests and explicit non-Rust fences",
    "test-snapshot-testing": "Snapshot transformer transform and additive-DDL output",
    "test-criterion-bench": "Add transformer and comparison workflows to Criterion recipes",
    "test-loom-concurrency": "Guard the absence of first-party atomic mutation",
    "doc-module-inner": "Document and gate production module responsibilities",
    "doc-all-public": "Document every reachable public API",
    "doc-errors-section": "Document caller-facing error contracts",
    "doc-panics-section": "Verify no public API requires a `# Panics` contract",
    "doc-safety-section": "Record the zero-unsafe documentation decision",
    "doc-question-mark": "Verify runnable docs avoid `unwrap` and `expect`",
    "doc-intra-links": "Link related API items with resolvable rustdoc links",
    "doc-examples-section": "Add runnable examples for pure public APIs",
    "doc-hidden-setup": "Record why PR 29.8 examples need no hidden setup",
    "doc-link-types": "Record PR 29.7 as the owner of related-item links",
    "doc-cargo-metadata": "Mark internal workspace crates as non-publishable",
    "doc-crate-readme": "Record why monorepo crates must not include the root README",
    "obs-tracing-over-log": "Verify tracing-only diagnostics after subscriber initialization",
    "obs-library-facade": "Verify subscriber initialization stays in binary bootstrap",
    "obs-structured-fields": "Move tracing values into stable structured fields",
    "obs-instrument-spans": "Record why long-lived loops use events instead of instrument spans",
    "obs-levels-filter": "Verify telemetry filter precedence and safe defaults",
    "obs-error-chain": "Preserve error source chains in warning and error events",
    "obs-no-sensitive-data": "Verify tracing emits only allowlisted non-secret data",
    "perf-iter-over-index": "Record why variable-step parsers retain indexed loops",
    "perf-iter-lazy": "Verify collections cross ownership or reuse boundaries",
    "perf-collect-once": "Verify no iterator pipeline collects an intermediate result",
    "perf-entry-api": "Verify map updates already use the correct entry semantics",
    "perf-drain-reuse": "Record why move-out beats drain for owned work buffers",
    "perf-extend-batch": "Record why stateful builder loops do not use `extend`",
    "perf-chain-avoid": "Verify the remaining iterator chain is bounded and cold",
    "perf-collect-into": "Record why collected destinations cannot reuse capacity",
    "perf-black-box-bench": "Verify every Criterion target prevents dead-code elimination",
    "perf-release-profile": "Verify the existing thin-LTO release-profile decision",
    "perf-profile-first": "Record the benchmark evidence required before optimization",
    "perf-ahash": "Record why the default hasher remains the safe baseline",
    "perf-io-buffering": "Verify existing I/O layers already provide appropriate buffering",
    "proj-lib-main-split": "Move service orchestration out of `main.rs`",
    "proj-mod-by-feature": "Verify top-level modules follow product capabilities",
    "proj-flat-small": "Record why the current per-crate module depth stays flat",
    "proj-mod-rs-dir": "Verify only the multi-file decoder needs `mod.rs`",
    "proj-pub-crate-internal": "Verify public visibility has external consumers",
    "proj-pub-super-parent": "Record why no item qualifies for `pub(super)`",
    "proj-pub-use-reexport": "Verify current re-exports form the intended public facades",
    "proj-prelude-module": "Record why Walrus should not add a prelude",
    "proj-bin-dir": "Verify one named binary per service package",
    "proj-workspace-large": "Verify the six-package workspace boundary",
    "proj-workspace-deps": "Verify third-party versions inherit from the workspace",
    "proj-feature-additive": "Verify every Cargo feature is additive",
    "proj-msrv-declare": "Verify one inherited Rust 1.96 MSRV contract",
    "proj-build-rs-minimal": "Record the absence of first-party build scripts",
    "lint-deny-correctness": "Verify `clippy::all = deny` already enforces correctness",
    "lint-warn-suspicious": "Record PR 33.1 as the owner of `clippy::suspicious` enforcement",
    "lint-warn-style": "Record PR 33.1 as the owner of `clippy::style` enforcement",
    "lint-warn-complexity": "Record PR 33.1 as the owner of `clippy::complexity` enforcement",
    "lint-warn-perf": "Record PR 33.1 as the owner of `clippy::perf` enforcement",
    "lint-pedantic-selective": "Record why Walrus selects lints instead of enabling all pedantic",
    "lint-missing-docs": "Deny missing docs and broken rustdoc links workspace-wide",
    "lint-unsafe-doc": "Record why zero-unsafe policy needs no extra doc lint",
    "lint-cargo-metadata": "Record why internal crates do not enable `clippy::cargo`",
    "lint-rustfmt-check": "Verify CI and `just fmt` enforce `cargo fmt --check`",
    "lint-workspace-lints": "Verify every workspace member inherits centralized lints",
    "lint-cfg-check": "Verify denied warnings already catch unexpected cfgs",
    "lint-clippy-nursery-selected": "Record the selective nursery-lint decision",
    "anti-unwrap-abuse": "Record PR 7.7 as the owner of production `unwrap` enforcement",
    "anti-expect-lazy": "Record PR 7.7 as the owner of production `expect` enforcement",
    "anti-clone-excessive": "Record PR 9.1 as the owner of the clone audit",
    "anti-lock-across-await": "Record PR 14.3 as the owner of lock-across-await enforcement",
    "anti-string-for-str": "Record PR 9.2 as the owner of borrowed string parameters",
    "anti-vec-for-slice": "Record PR 9.2 as the owner of borrowed slice parameters",
    "anti-index-over-iter": "Record PR 31.1 as the owner of the indexed-loop audit",
    "anti-panic-expected": "Record PR 10.9 as the owner of production panic enforcement",
    "anti-empty-catch": "Log discarded cleanup and task-join errors",
    "anti-over-abstraction": "Verify first-party abstractions have concrete consumers",
    "anti-premature-optimize": "Record PR 31.11 as the owner of profile-before-optimize policy",
    "anti-type-erasure": "Record PR 19.5 as the owner of dynamic-dispatch decisions",
    "anti-format-hot-path": "Record why profiled hot paths retain required formatting",
    "anti-collect-intermediate": "Record PR 31.3 as the owner of intermediate-collection audits",
    "anti-stringly-typed": "Record PR 18.5 as the owner of typed domain-state decisions",
}


def descriptions() -> dict[str, str]:
    text = RULE_SKILL.read_text()
    return dict(re.findall(r"^- \[`([^`]+)`\]\(rules/[^)]+\) - (.+)$", text, re.M))


def sequence() -> list[tuple[int, int, str]]:
    return [
        (phase, index, slug)
        for phase, (_, _, slugs) in PHASES.items()
        for index, slug in enumerate(slugs, 1)
    ]


def task_path(phase: int, index: int, slug: str) -> Path:
    return ROOT / "docs/implementation" / PHASES[phase][1] / f"pr-{phase}.{index}-{slug}.md"


def test_command(packages: str) -> str:
    if packages == "—":
        return "cargo test --workspace"
    args = " ".join(f"-p {package}" for package in packages.split(","))
    return f"cargo test {args}"


def nonempty_probe(command: str) -> str:
    """Make a search inventory succeed when the correct result is zero hits."""
    if command.startswith("rg ") and "|| true" not in command:
        return f"{command} || true"
    return command


ZERO_MATCH_PROBES = {
    "test-fixture-raii",
    "test-mockall-mocking",
    "doc-safety-section",
    "doc-question-mark",
    "doc-hidden-setup",
    "obs-structured-fields",
    "obs-instrument-spans",
    "obs-error-chain",
    "obs-no-sensitive-data",
    "proj-prelude-module",
    "proj-pub-super-parent",
    "lint-unsafe-doc",
    "anti-unwrap-abuse",
    "anti-string-for-str",
    "anti-vec-for-slice",
}


VERIFICATION_PROBE_OVERRIDES = {
    "test-cfg-test-module": "bash scripts/test-hygiene.sh --self-test",
    "test-use-super": "bash scripts/test-hygiene.sh --self-test",
    "test-descriptive-names": "bash scripts/test-hygiene.sh --self-test",
    "test-doctest-examples": "cargo test --doc --workspace",
    "test-criterion-bench": "rg -n '^bench|bench-baseline|bench-compare' justfile docs/benchmarks.md",
    "test-loom-concurrency": "bash scripts/test-hygiene.sh --self-test",
    "doc-module-inner": "bash scripts/check-module-docs.sh --check",
    "doc-intra-links": "cargo doc --workspace --no-deps",
    "doc-link-types": "cargo doc --workspace --no-deps",
    "doc-cargo-metadata": "cargo metadata --no-deps --format-version 1",
    "doc-crate-readme": "! rg -n '^readme\\s*=' crates/common/Cargo.toml crates/control/Cargo.toml crates/transformer/Cargo.toml crates/extractor/Cargo.toml crates/pg-to-arrow/Cargo.toml",
    "perf-release-profile": "rg -n '^\\[profile.release\\]|^lto\\s*=' Cargo.toml",
    "proj-lib-main-split": "rg -n '^pub async fn run' crates/transformer/src/app.rs crates/extractor/src/app.rs",
    "proj-workspace-deps": "cargo metadata --no-deps --format-version 1",
    "proj-feature-additive": "rg -n '^\\[features\\]|^[a-zA-Z0-9_-]+\\s*=\\s*\\[' crates/common/Cargo.toml crates/control/Cargo.toml crates/pg-to-arrow/Cargo.toml tests/e2e/Cargo.toml",
    "proj-build-rs-minimal": "! rg -n 'fn main\\(' . --glob build.rs --glob '!target/**'",
    "lint-deny-correctness": "rg -n '^\\[workspace.lints.clippy\\]|^all\\s*=\\s*\"deny\"|^\\[lints\\]|^workspace\\s*=\\s*true' Cargo.toml crates tests/e2e --glob Cargo.toml",
    "lint-missing-docs": "cargo doc --workspace --no-deps",
    "lint-workspace-lints": "rg -n '^\\[workspace.lints|^\\[lints\\]|^workspace\\s*=\\s*true' Cargo.toml crates tests/e2e --glob Cargo.toml",
    "anti-empty-catch": "! rg -n 'let _ =' crates/transformer/src/compaction.rs crates/transformer/src/phase_b.rs crates/transformer/src/main.rs crates/extractor/src/stream_txn.rs crates/extractor/src/main.rs",
}


def verification_probe(slug: str, command: str) -> str:
    """Return one validator-approved, read-only postcondition command."""
    if slug in VERIFICATION_PROBE_OVERRIDES:
        return VERIFICATION_PROBE_OVERRIDES[slug]
    value = command.removesuffix(" || true")
    if " && " in value:
        value = value.split(" && ", 1)[0]
    if " | " in value:
        value = value.split(" | ", 1)[0]
    if slug in ZERO_MATCH_PROBES and value.startswith("rg "):
        value = f"! {value}"
    return value


def crates_touched(item: Spec) -> str:
    if item.packages != "—":
        return ", ".join(f"`{package}`" for package in item.packages.split(","))
    if item.outcome == "change":
        return "none (workspace tooling, manifests, or documentation)"
    return "none (evidence note only)"


def estimate_size(slug: str, item: Spec) -> str:
    if slug in {"doc-all-public", "doc-errors-section", "doc-intra-links", "proj-lib-main-split"}:
        return "L"
    return "M" if item.outcome == "change" else "S"


def file_block(item: Spec, note: str) -> str:
    paths = re.findall(r"`([^`]+)`", item.files)
    return "\n".join(dict.fromkeys(paths))


def note_contract(item: Spec, note: str) -> str:
    if note not in re.findall(r"`([^`]+)`", item.files):
        return (
            "No evidence note is authorized by this change ticket. A baseline mismatch requires "
            "re-auditing and re-authoring the task before it can be selected again."
        )
    return (
        f"`{note}` records the audited base commit, every exact command and result, findings "
        "classified by stable path/symbol, the predetermined conclusion, and the concrete reversal condition."
    )


def mismatch_contract(item: Spec) -> str:
    if item.outcome == "change":
        return (
            "If a predecessor already satisfies the acceptance postcondition, or the probe no longer "
            "reproduces the audited finding, stop. Re-audit and re-author this ticket before selection; "
            "an implementer may not substitute another refactor, a no-op completion, or an evidence-only PR."
        )
    return (
        "The evidence disposition and rule-named note are fixed. A contradictory baseline blocks "
        "implementation and requires the ticket to be re-audited and re-authored before selection."
    )


def render(
    phase: int,
    index: int,
    slug: str,
    summary: str,
    previous: str,
    following: str,
) -> str:
    task_id = f"{phase}.{index}"
    phase_name = PHASES[phase][0]
    item = SPECS[slug]
    note = f"docs/implementation/notes/rust-skills/{slug}.md"
    probe = nonempty_probe(item.probe)
    check_probe = verification_probe(slug, item.probe)
    mismatch = mismatch_contract(item)
    note_output = note if note in re.findall(r"`([^`]+)`", item.files) else "none"
    unlock = f"PR {following}" if following != "—" else "—"
    return f'''<!-- Canonical one-rule Rust curriculum ticket. Generated tickets start draft; audit before activation. -->

# PR {task_id} — {TITLES[slug]}

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** draft · **Outcome:** {item.outcome}
>
> **Gates:** fmt,clippy,test · **Test packages:** {item.packages}

> **Phase:** {phase} — {phase_name} · **Crates touched:** {crates_touched(item)} · **Est. size:** {estimate_size(slug, item)} ·
> **Depends on:** PR {previous} · **Unlocks:** {unlock}

Apply [`{slug}`](../../../.claude/skills/rust-skills/rules/{slug}.md) to Walrus with the
predetermined disposition above. The rule is: **{summary.rstrip('.')}**. This is not an open-ended
cleanup ticket: the probe, allowed files, action, and baseline-mismatch handling below are the complete contract.

## Why — learning objectives

- Practise the `{slug}` rule at the exact Walrus boundary identified by the baseline, including the
  exceptions in the source rule rather than applying its example mechanically.
- Preserve the project-specific trade-off in this audit: {item.finding}

## Read first

- [`{slug}`](../../../.claude/skills/rust-skills/rules/{slug}.md), including its exceptions.
- [`task-conventions.md`](../../../.claude/skills/implementing-walrus-roadmap/reference/task-conventions.md).
- The complete modules/configuration named under Allowed files; grep matches are evidence, not edit instructions.

## Scope

**In scope**

- Capture the baseline below after every predecessor has merged.
- Modify only the canonical file list and perform this exact operation: {item.action}
- Prove this postcondition: {item.acceptance}

**Explicitly deferred** (do not build these here)

- {item.deferred}
- Preserve CDC ordering `(commit_lsn, lsn)`, SQL/query bytes, public behavior, and existing error
  classification unless the required action explicitly names them.

## Baseline and decision

```bash
{probe}
```

{item.finding}

**Baseline precondition:** the command must reproduce that finding after all predecessors merge.
{mismatch}

## Implementation contract

**Allowed files:** {item.files}

**Required action:** {item.action}

**Acceptance:** {item.acceptance}

**Baseline mismatch:** {mismatch}

**Evidence note schema:** {note_contract(item, note)}

## Files to create / modify

```text
{file_block(item, note)}
```

## Skeleton

```text
target = {item.files}
operation = {item.action}
proof = {item.acceptance}
baseline-mismatch = block-for-reauthoring
```

## Verification commands

```text
rule-probe = {check_probe}
fmt = cargo fmt --check
clippy = cargo clippy --workspace --all-targets --all-features -- -D warnings
test = {test_command(item.packages)}
docs = cargo doc --workspace --no-deps
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

- [ ] The baseline command and its complete output are recorded in the PR or evidence note.
- [ ] The diff contains only the allowed files and implements the required action.
- [ ] {item.acceptance}
- [ ] No deferred cleanup, dependency, abstraction, documentation, panic, or public API was invented.
- [ ] Every command in **Verification commands** passes and is reported by label.
- [ ] Status remains Planned and the roadmap checkbox remains unchecked until the separate mark-done PR.

## What completed looks like

```text
task={task_id}
rule={slug}
outcome={item.outcome}
rule-probe=pass
baseline=recorded-with-output
allowed-files=verified
acceptance=pass
fmt=pass
clippy=pass
test=pass
docs=pass
diff-check=pass
note={note_output}
```

## Hints & gotchas

- A search hit is a candidate, not permission to edit it; read the complete containing module.
- If the baseline contradicts the audited finding or already satisfies a change task, stop and
  re-author the ticket before it is selected again.
- Evidence and superseded outcomes create the named note only; a low-signal tripwire or dependency
  is not required to make the task substantial.

## References

- [Rust rule: `{slug}`](../../../.claude/skills/rust-skills/rules/{slug}.md)
- [Implementation curriculum](../README.md)
- [Task conventions](../../../.claude/skills/implementing-walrus-roadmap/reference/task-conventions.md)
'''


def validate_spec_coverage() -> None:
    expected = {slug for _, _, slug in sequence()}
    missing = sorted(expected - SPECS.keys())
    extra = sorted(SPECS.keys() - expected)
    missing_titles = sorted(expected - TITLES.keys())
    extra_titles = sorted(TITLES.keys() - expected)
    if missing or extra or missing_titles or extra_titles:
        raise SystemExit(
            "spec coverage mismatch: "
            f"missing={missing} extra={extra} "
            f"missing_titles={missing_titles} extra_titles={extra_titles}"
        )
    incoherent_titles: list[str] = []
    for slug in sorted(expected):
        outcome = SPECS[slug].outcome
        task_title = TITLES[slug]
        if outcome == "evidence" and not task_title.startswith(("Record ", "Verify ")):
            incoherent_titles.append(f"{slug}: evidence title must start with Record/Verify")
        elif outcome.startswith("superseded by PR "):
            owner = outcome.removeprefix("superseded by PR ")
            if not task_title.startswith(f"Record PR {owner} "):
                incoherent_titles.append(f"{slug}: superseded title must name PR {owner}")
        elif outcome == "change" and task_title.startswith(("Record ", "Verify ")):
            incoherent_titles.append(f"{slug}: change title must name the concrete adjustment")
    if incoherent_titles:
        raise SystemExit("title/outcome mismatch: " + "; ".join(incoherent_titles))


def generate() -> int:
    validate_spec_coverage()
    desc = descriptions()
    ordered = sequence()
    made = 0
    for offset, (phase, index, slug) in enumerate(ordered):
        path = task_path(phase, index, slug)
        if path.exists():
            continue
        if slug not in desc:
            raise SystemExit(f"missing SKILL.md summary for {slug}")
        previous = "27.16" if offset == 0 else f"{ordered[offset - 1][0]}.{ordered[offset - 1][1]}"
        following = "—" if offset + 1 == len(ordered) else f"{ordered[offset + 1][0]}.{ordered[offset + 1][1]}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render(phase, index, slug, desc[slug], previous, following))
        print(f"CREATED={path.relative_to(ROOT)}")
        made += 1
    print(f"GENERATED={made}")
    return 0


def task_contract_errors(phase: int, index: int, slug: str, text: str) -> list[str]:
    item = SPECS[slug]
    note = f"docs/implementation/notes/rust-skills/{slug}.md"
    required = (
        "**Status:** 📋 Planned", "**Readiness:**", "**Outcome:**",
        "**Gates:** fmt,clippy,test", "**Test packages:**", "**Crates touched:**",
        "**Est. size:**", "## Why — learning objectives", "## Read first", "## Scope",
        "## Files to create / modify", "## Skeleton", "## Verification commands",
        "## Definition of Done", "## What completed looks like", "## Hints & gotchas",
        "## References", "git diff --check origin/main...HEAD",
    )
    errors = [f"missing marker `{marker}`" for marker in required if marker not in text]
    expected_h1 = f"# PR {phase}.{index} — {TITLES[slug]}"
    if expected_h1 not in text.splitlines()[:40]:
        errors.append(f"H1 is not `{expected_h1}`")
    expected_metadata = f"**Readiness:** audited · **Outcome:** {item.outcome}"
    if expected_metadata not in text:
        errors.append(f"metadata is not `{expected_metadata}`")
    if "baseline precondition" not in text.lower():
        errors.append("missing explicit baseline precondition")
    if "baseline mismatch" not in text.lower():
        errors.append("missing explicit baseline-mismatch contract")
    if not re.search(r"\bstop\b", text, re.I) or "re-author" not in text.lower():
        errors.append("baseline mismatch does not stop and require re-authoring")

    files_match = re.search(
        r"^## Files to create / modify\s*$\n(?P<body>.*?)(?=^## )", text, re.M | re.S
    )
    files_body = files_match.group("body") if files_match else ""
    if item.outcome == "change":
        forbidden = (
            "predecessor fallback", "none-or-predecessor-fallback",
            "# predecessor fallback only", "explicit predecessor fallback",
        )
        for marker in forbidden:
            if marker in text.lower():
                errors.append(f"change task contains dynamic fallback marker `{marker}`")
    elif note not in files_body:
        errors.append(f"{item.outcome} task does not name deterministic note `{note}`")
    return errors


def check(require_tracked: bool) -> int:
    validate_spec_coverage()
    ordered = sequence()
    expected = {task_path(phase, index, slug).relative_to(ROOT) for phase, index, slug in ordered}
    actual = {
        path.relative_to(ROOT)
        for _, (_, directory, _) in PHASES.items()
        for path in (ROOT / "docs/implementation" / directory).glob("pr-*.md")
    }
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    draft = sorted(path for path in expected if (ROOT / path).exists() and "**Readiness:** draft" in (ROOT / path).read_text())
    malformed_reasons: dict[Path, list[str]] = {}
    for phase, index, slug in ordered:
        path = task_path(phase, index, slug).relative_to(ROOT)
        if not (ROOT / path).exists():
            continue
        errors = task_contract_errors(phase, index, slug, (ROOT / path).read_text())
        if errors:
            malformed_reasons[path] = errors
    malformed = sorted(malformed_reasons)

    roadmap_text = (ROOT / "docs/implementation/README.md").read_text()
    roadmap_mismatch: list[str] = []
    for phase, index, slug in ordered:
        task_id = f"{phase}.{index}"
        match = re.search(
            rf"^\| ☐ \| \[{re.escape(task_id)}\]\([^)]+\) \| (?P<title>.*?) "
            rf"\| `{re.escape(slug)}` \|$",
            roadmap_text,
            re.M,
        )
        if not match:
            roadmap_mismatch.append(f"{task_id}: missing row for `{slug}`")
        elif match.group("title") != TITLES[slug]:
            roadmap_mismatch.append(
                f"{task_id}: Delivers `{match.group('title')}` != `{TITLES[slug]}`"
            )
    tracked_raw = subprocess.run(
        ["git", "ls-files", "--", *[str(path) for path in sorted(expected)]],
        cwd=ROOT, check=True, text=True, capture_output=True,
    ).stdout.splitlines()
    tracked = {Path(line) for line in tracked_raw}
    untracked = sorted(expected - tracked)
    print(f"EXPECTED={len(expected)}")
    print(f"PRESENT={len(actual & expected)}")
    print(f"MISSING={len(missing)}")
    print(f"EXTRA={len(extra)}")
    print(f"DRAFT={len(draft)}")
    print(f"MALFORMED={len(malformed)}")
    print(f"ROADMAP_MISMATCH={len(roadmap_mismatch)}")
    print(f"UNTRACKED={len(untracked)}")
    for label, paths in (("missing", missing), ("extra", extra), ("draft", draft), ("malformed", malformed), ("untracked", untracked)):
        for path in paths:
            print(f"{label.upper()}_FILE={path}")
    for path in malformed:
        for reason in malformed_reasons[path]:
            print(f"MALFORMED_REASON={path}: {reason}")
    for reason in roadmap_mismatch:
        print(f"ROADMAP_REASON={reason}")
    failed = bool(
        missing or extra or draft or malformed or roadmap_mismatch
        or (require_tracked and untracked)
    )
    print(f"CORPUS_CHECK={'FAIL' if failed else 'PASS'}")
    return int(failed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate", action="store_true", help="create missing draft tickets; never overwrite")
    mode.add_argument("--check", action="store_true", help="read-only corpus/schema check")
    parser.add_argument("--require-tracked", action="store_true", help="with --check, fail if a ticket is not tracked")
    args = parser.parse_args()
    if args.require_tracked and not args.check:
        parser.error("--require-tracked requires --check")
    return check(args.require_tracked) if args.check else generate()


if __name__ == "__main__":
    raise SystemExit(main())
