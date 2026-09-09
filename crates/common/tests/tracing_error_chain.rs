#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — filesystem reads and assertions implement a repository policy gate"
)]
//! Conformance gate for `obs-error-chain`: tracing events render error values with
//! `Debug`, which retains `anyhow`/`thiserror` source chains, rather than outer-only `Display`.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize the repository root")
}

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
    for service in ["common", "extractor", "transformer"] {
        visit(&root.join("crates").join(service).join("src"), &mut sources);
    }
    sources.sort();
    sources
}

/// Extract the complete parenthesized body of each tracing event macro. Fields can contain
/// nested calls such as `format_args!(...)`, so stopping at the first `)` would miss violations.
fn tracing_event_calls(source: &str) -> Vec<(usize, &str)> {
    let mut calls = Vec::new();
    for marker in [
        "tracing::trace!(",
        "tracing::debug!(",
        "tracing::info!(",
        "tracing::warn!(",
        "tracing::error!(",
    ] {
        let mut search_from = 0;
        while let Some(relative) = source[search_from..].find(marker) {
            let start = search_from + relative;
            let mut cursor = start + marker.len();
            let mut depth = 1_u32;
            let mut in_string = false;
            let mut escaped = false;
            let bytes = source.as_bytes();

            while cursor < bytes.len() && depth != 0 {
                let byte = bytes[cursor];
                if in_string {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        in_string = false;
                    }
                } else if byte == b'"' {
                    in_string = true;
                } else if byte == b'(' {
                    depth += 1;
                } else if byte == b')' {
                    depth -= 1;
                }
                cursor += 1;
            }

            assert_eq!(depth, 0, "unterminated tracing macro at byte {start}");
            let line = source[..start]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1;
            calls.push((line, &source[start..cursor]));
            search_from = cursor;
        }
    }
    calls.sort_by_key(|(line, _)| *line);
    calls
}

fn display_error_violation(call: &str) -> bool {
    let compact: String = call
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    if compact.contains("error=%") {
        return true;
    }

    call.match_indices('%').any(|(percent, _)| {
        let name: String = call[percent + 1..]
            .chars()
            .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
            .collect();
        name == "e" || name == "error" || name == "rollback" || name.ends_with("_error")
    })
}

#[test]
fn production_tracing_events_keep_error_source_chains() {
    let root = repo_root();
    let mut violations = Vec::new();

    for path in production_sources(&root) {
        let source = std::fs::read_to_string(&path).expect("read production Rust source");
        for (line, call) in tracing_event_calls(&source) {
            if display_error_violation(call) {
                violations.push(format!("{}:{line}: {call}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "tracing events must record errors as `error = ?value` so source chains survive:\n{}",
        violations.join("\n")
    );
}

#[test]
fn gate_distinguishes_error_values_from_safe_display_fields() {
    let fixture = r#"
        tracing::warn!(table = %table, error = ?error, "good");
        tracing::error!(error = %error, "bad named field");
        tracing::warn!(%abort_error, "bad shorthand field");
    "#;
    let violations = tracing_event_calls(fixture)
        .into_iter()
        .filter(|(_, call)| display_error_violation(call))
        .count();

    assert_eq!(violations, 2);
}
