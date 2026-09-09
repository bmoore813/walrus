#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              themselves repository-policy checks, not runtime I/O"
)]
//! Guard for `anti-panic-expected`: the last way the no-panic policy can be undone in silence.
//!
//! A recoverable failure — a bad config value, a short wire frame, a malformed range literal —
//! reaches `main` as a typed `Err` and leaves as a `common::ExitCode`. Nothing in the type system
//! requires that. What requires it is `[workspace.lints.clippy]` denying `panic`, `todo`,
//! `unimplemented`, `unreachable` and `panic_in_result_fn`, which
//! `tests/workspace_lints_inherited.rs` pins entry by entry. One vector walks past that pin: lint
//! levels are innermost-wins, so a single `#![allow(clippy::panic, reason = "…")]` at the top of a
//! production module reopens `panic!` for that whole file — manifest untouched, `clippy.toml`
//! untouched, build green. Nothing in the toolchain reports it, because the suppression *is* the
//! toolchain's answer. Only a source scan notices, which is what this file is.
//!
//! Scope is `crates/*/src/**/*.rs` minus the `*_test.rs` siblings, matching
//! `no_unwrap_suppression.rs`. Tests are the rule's own carve-out — asserting an impossible branch
//! is exactly where `unreachable!` belongs — so `crates/*/tests/` targets and the `*_test.rs`
//! siblings carry these allows on purpose. `clippy.toml`'s `allow-panic-in-tests` covers `panic`
//! under `#[cfg(test)]` and has no counterpart for `unreachable`, which is why
//! `crates/extractor/src/reload_test.rs` names that one for itself.
//!
//! There are no production exceptions. Both `#[allow(…)]` and `#[expect(…)]` weaken the workspace
//! deny at their local scope, so this scan rejects either spelling.

use std::path::{Path, PathBuf};

/// The five lints that keep an expected failure from aborting the process. A production file that
/// names any of them — in any attribute — is turning that lint off for itself, so the mention alone
/// is the offence.
const PANIC_LINTS: [&str; 5] = [
    "clippy::panic",
    "clippy::todo",
    "clippy::unimplemented",
    "clippy::unreachable",
    "clippy::panic_in_result_fn",
];

/// Repo root, derived from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every production `crates/*/src/**/*.rs` under `root`. Recurses (see `src/pgoutput/`) and skips
/// the Go-style sibling `*_test.rs` unit-test files.
fn production_sources(root: &Path) -> Vec<PathBuf> {
    fn visit(dir: &Path, sources: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read a source directory") {
            let path = entry.expect("read a source-directory entry").path();
            if path.is_dir() {
                visit(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with("_test.rs"))
            {
                sources.push(path);
            }
        }
    }

    let mut sources = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).expect("read the workspace crates") {
        let src = entry.expect("read a crate entry").path().join("src");
        if src.is_dir() {
            visit(&src, &mut sources);
        }
    }
    sources.sort();
    sources
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Whether `line` names `lint` as a whole lint path. `clippy::panic` is a prefix of
/// `clippy::panic_in_result_fn`, so a plain `contains` would report the longer entry twice — once
/// under a name nobody touched.
fn names_lint(line: &str, lint: &str) -> bool {
    let mut rest = line;
    while let Some(at) = rest.find(lint) {
        let tail = &rest[at + lint.len()..];
        if !tail.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
            return true;
        }
        rest = tail;
    }
    false
}

/// Every suppression of a [`PANIC_LINTS`] entry in `source`, as a reportable line. A line whose
/// first non-space characters are `//` is prose: a doc comment may name the lint it explains.
fn offences(relative: &str, source: &str) -> Vec<String> {
    let mut found = Vec::new();
    for (index, line) in source.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        for lint in PANIC_LINTS {
            if !names_lint(line, lint) {
                continue;
            }
            let line_number = index + 1;
            found.push(format!("{relative}:{line_number}: suppresses {lint}"));
        }
    }
    found
}

#[test]
fn no_production_source_reopens_the_panic_denies() {
    let root = repo_root();
    let sources = production_sources(&root);
    let relatives = sources
        .iter()
        .map(|path| display_path(&root, path))
        .collect::<Vec<_>>();

    assert!(
        !relatives.is_empty(),
        "the production-source scan is not vacuous"
    );

    let mut found = Vec::new();
    for (path, relative) in sources.iter().zip(&relatives) {
        let source = std::fs::read_to_string(path).expect("read a production source");
        found.extend(offences(relative, &source));
    }

    assert!(
        found.is_empty(),
        "a scoped suppression turns an expected error back into a crash for a whole module while \
         the manifest still reads as policy:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_scan_rejects_a_planted_suppression_and_spares_prose() {
    let planted = concat!(
        "#![allow(clippy::panic, reason = \"legacy module\")]\n",
        "//! Contrast `clippy::todo`, which this module never suppresses.\n",
        "pub fn port(raw: &str) -> u16 {\n",
        "    #[allow(clippy::unreachable, reason = \"validated at startup\")]\n",
        "    match raw.parse() {\n",
        "        Ok(port) => port,\n",
        "        Err(_) => unreachable!(),\n",
        "    }\n",
        "}\n",
    );

    let found = offences("crates/transformer/src/port.rs", planted);
    let report = found.join("\n");

    assert_eq!(found.len(), 2, "{report}");
    for expected in [
        "crates/transformer/src/port.rs:1: suppresses clippy::panic",
        "crates/transformer/src/port.rs:4: suppresses clippy::unreachable",
    ] {
        assert!(report.contains(expected), "{report}");
    }
}

#[test]
fn an_expect_is_also_a_suppression() {
    let seam = "#[expect(clippy::unimplemented, reason = \"deferred goal\")]\n";

    assert_eq!(offences("crates/transformer/src/port.rs", seam).len(), 1);
}

/// `clippy::panic` is a prefix of `clippy::panic_in_result_fn`. Reporting the longer entry under
/// the shorter name would send a reader to a deny that was never touched.
#[test]
fn the_lint_matcher_reads_whole_paths_not_prefixes() {
    let longer = "#[allow(clippy::panic_in_result_fn, reason = \"legacy\")]\n";
    let expected = "crates/transformer/src/port.rs:1: suppresses clippy::panic_in_result_fn";
    let found = offences("crates/transformer/src/port.rs", longer);

    assert_eq!(found, [expected]);
    assert!(names_lint("clippy::panic,", "clippy::panic"));
    assert!(!names_lint("clippy::panicky", "clippy::panic"));
}
