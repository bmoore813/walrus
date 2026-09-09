#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              themselves repository-policy checks, not runtime I/O"
)]
//! Guard for `anti-unwrap-abuse`: the last way the no-unwrap policy can be undone in silence.
//!
//! Three facts hold that policy up, and `tests/workspace_lints_inherited.rs` asserts each one:
//! `[workspace.lints.clippy]` denies `unwrap_used` and `expect_used`, `clippy.toml` re-allows them
//! only under keys ending `-in-tests`, and every member inherits the table. A fourth vector walks
//! past all three. Lint levels are innermost-wins, so one
//! `#![allow(clippy::unwrap_used, reason = "…")]` at the top of a production module reopens the
//! door for that whole file — manifest untouched, `clippy.toml` untouched, build green. Nothing in
//! the toolchain reports that, because the suppression *is* the toolchain's answer. Only a source
//! scan notices, which is what this file is.
//!
//! Scope is `crates/*/src/**/*.rs` minus the `*_test.rs` siblings. Benches, `crates/*/tests/` and
//! the compose-gated `tests/e2e` harness carry that header on purpose — they are the rule's own
//! test carve-out, not production — and the `*_test.rs` siblings need no header at all, since
//! `clippy.toml` already re-allows both lints inside a `#[cfg(test)]` module.
//!
//! walrus keeps exactly one production exception, and its *spelling* is load-bearing: [`CARVE_OUT`]
//! writes `#[expect(…)]`, not `#[allow(…)]`. An unfulfilled expectation is a warning and `warnings`
//! is denied, so the day `install_recorder` stops needing the exception the toolchain retires it for
//! us. An `#[allow]` in the same place would outlive its reason with nothing to say so.

use std::path::{Path, PathBuf};

/// The two restriction lints the policy rests on. A production file that names either one — in any
/// attribute — is turning that lint off for itself, so the mention alone is the offence.
const UNWRAP_LINT: &str = "clippy::unwrap_used";
const EXPECT_LINT: &str = "clippy::expect_used";
const SUPPRESSED_LINTS: [&str; 2] = [UNWRAP_LINT, EXPECT_LINT];

/// The one production module allowed to name one of them. `metrics::init` installs the process-wide
/// recorder inside a `OnceLock`, so its `expect` can only fire when a *foreign* recorder is already
/// installed — a programming error rather than a runtime condition.
const CARVE_OUT: &str = "crates/common/src/metrics.rs";

/// The attribute form that expires on its own. Written without the leading `#` so one needle matches
/// both the outer `#[expect(…)]` and the inner `#![expect(…)]` spelling.
const EXPECT_FORM: &str = "[expect(";

/// The form that does not expire: a suppression that outlives the reason for it.
const ALLOW_FORM: &str = "[allow(";

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

/// Every suppression of a [`SUPPRESSED_LINTS`] entry in `source`, as a reportable line. A line whose
/// first non-space characters are `//` is prose — a doc comment may name the lint it explains.
fn offences(relative: &str, source: &str) -> Vec<String> {
    if relative == CARVE_OUT {
        return Vec::new();
    }

    let mut found = Vec::new();
    for (index, line) in source.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        for lint in SUPPRESSED_LINTS {
            if line.contains(lint) {
                let line_number = index + 1;
                found.push(format!("{relative}:{line_number}: suppresses {lint}"));
            }
        }
    }
    found
}

/// The attribute enclosing the first mention of `lint` in `source`: which family it belongs to, and
/// its text up to the closing `)]`. `None` when `source` never names the lint, or names it outside
/// any attribute.
fn enclosing_attribute<'a>(source: &'a str, lint: &str) -> Option<(&'static str, &'a str)> {
    let head = &source[..source.find(lint)?];
    let (form, start) = match (head.rfind(EXPECT_FORM), head.rfind(ALLOW_FORM)) {
        (Some(expect_at), None) => (EXPECT_FORM, expect_at),
        (None, Some(allow_at)) => (ALLOW_FORM, allow_at),
        (Some(expect_at), Some(allow_at)) if expect_at > allow_at => (EXPECT_FORM, expect_at),
        (Some(_), Some(allow_at)) => (ALLOW_FORM, allow_at),
        (None, None) => return None,
    };
    let end = start + source[start..].find(")]")?;
    Some((form, &source[start..end]))
}

#[test]
fn no_production_source_reopens_the_unwrap_denies() {
    let root = repo_root();
    let sources = production_sources(&root);
    let relatives = sources
        .iter()
        .map(|path| display_path(&root, path))
        .collect::<Vec<_>>();

    assert!(
        relatives.iter().any(|path| path == CARVE_OUT),
        "the scan must reach {CARVE_OUT}, the one file it exempts — otherwise it is vacuous"
    );

    let mut found = Vec::new();
    for (path, relative) in sources.iter().zip(&relatives) {
        let source = std::fs::read_to_string(path).expect("read a production source");
        found.extend(offences(relative, &source));
    }

    assert!(
        found.is_empty(),
        "a scoped allow reopens unwrap/expect for a whole module while the manifest and \
         clippy.toml still read as policy; {CARVE_OUT} is the one exception:\n{}",
        found.join("\n")
    );
}

/// The other half of "one exception": the exception itself. It must stay an `#[expect]`, because
/// that is what makes it self-retiring — and it must stay scoped to `expect_used`, because
/// `metrics::init` has an invariant to assert, not an error to swallow.
#[test]
fn the_recorder_install_is_still_a_self_retiring_expect() {
    let source = std::fs::read_to_string(repo_root().join(CARVE_OUT))
        .expect("read the one production module with a carve-out");

    assert!(
        !source.contains(UNWRAP_LINT),
        "{CARVE_OUT} is exempt for the recorder install alone; it must never suppress unwrap_used"
    );

    let (form, attribute) = enclosing_attribute(&source, EXPECT_LINT)
        .expect("the carve-out module must still carry the install-once suppression");

    assert_eq!(
        form, EXPECT_FORM,
        "the carve-out must be #[expect(…)], so an unfulfilled expectation retires it the day \
         install_recorder stops needing one; #[allow(…)] would sit there forever"
    );
    assert!(
        attribute.contains("reason ="),
        "a suppression must say why (clippy::allow_attributes_without_reason):\n{attribute}"
    );
}

#[test]
fn the_scan_rejects_a_planted_suppression_and_spares_prose() {
    let planted = concat!(
        "#![allow(clippy::unwrap_used, reason = \"legacy module\")]\n",
        "//! Contrast `clippy::expect_used`, which this module never suppresses.\n",
        "pub fn port(raw: &str) -> u16 {\n",
        "    #[allow(clippy::expect_used, reason = \"parsed at startup\")]\n",
        "    raw.parse().expect(\"a port number\")\n",
        "}\n",
    );

    let found = offences("crates/transformer/src/port.rs", planted);
    let report = found.join("\n");

    assert_eq!(found.len(), 2, "{report}");
    assert!(report.contains("crates/transformer/src/port.rs:1"));
    assert!(report.contains("crates/transformer/src/port.rs:4"));
    assert!(offences(CARVE_OUT, planted).is_empty());
}

#[test]
fn the_attribute_reader_tells_a_self_retiring_expect_from_an_allow() {
    let retiring = concat!(
        "#[expect(\n",
        "    clippy::expect_used,\n",
        "    reason = \"install-once invariant\"\n",
        ")]\n",
        "let handle = build().expect(\"BUG\");\n",
    );
    let permanent = "#![allow(clippy::expect_used, reason = \"legacy\")]\n";
    let unsuppressed = "let handle = build().expect(\"BUG\");\n";

    let (form, attribute) = enclosing_attribute(retiring, EXPECT_LINT).unwrap();
    let (permanent_form, _) = enclosing_attribute(permanent, EXPECT_LINT).unwrap();

    assert_eq!(form, EXPECT_FORM);
    assert!(attribute.contains("reason = \"install-once invariant\""));
    assert_eq!(permanent_form, ALLOW_FORM);
    assert!(enclosing_attribute(unsuppressed, EXPECT_LINT).is_none());
}
