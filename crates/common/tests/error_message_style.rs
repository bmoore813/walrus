#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              themselves repository-policy checks, not runtime I/O"
)]
//! Conformance gate for `err-lowercase-msg`: every production `#[error("…")]` literal
//! starts lowercase (or with an allow-listed acronym) and carries no trailing sentence punctuation,
//! so `{:#}` chains read as one sentence. Pure source scanning — no Docker, no new dependency.

use std::path::{Path, PathBuf};

/// Acronyms / proper nouns the rule permits at the start of a message. Keep this SHORT: every
/// entry is an exception, and a long list means the convention is being eroded, not enforced.
const ACRONYM_ALLOW: &[&str] = &["DuckDB", "DDL", "LSN", "JSON", "TLS", "S3", "UTF-8"];

/// Repo root, derived from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize the repository root")
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

/// Each `#[error("…")]` literal in `src`, as `(1-based line number, logical message)`.
/// `#[error(transparent)]` yields nothing; `\`-continuations are joined into one message.
fn error_literals(src: &str) -> Vec<(usize, String)> {
    const MARKER: &str = "#[error(";

    let bytes = src.as_bytes();
    let mut found = Vec::new();
    let mut search_from = 0;
    while let Some(offset) = src[search_from..].find(MARKER) {
        let attribute_start = search_from + offset;
        let line = src[..attribute_start]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        let mut cursor = attribute_start + MARKER.len();
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }

        if src[cursor..].starts_with("transparent") {
            search_from = cursor + "transparent".len();
            continue;
        }
        if bytes.get(cursor) != Some(&b'"') {
            search_from = cursor.max(attribute_start + MARKER.len());
            continue;
        }

        cursor += 1;
        let mut message = Vec::new();
        let mut closed = false;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'"' => {
                    cursor += 1;
                    closed = true;
                    break;
                }
                b'\\' if bytes.get(cursor + 1) == Some(&b'\n') => {
                    cursor += 2;
                    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                        cursor += 1;
                    }
                }
                b'\\'
                    if bytes.get(cursor + 1) == Some(&b'\r')
                        && bytes.get(cursor + 2) == Some(&b'\n') =>
                {
                    cursor += 3;
                    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                        cursor += 1;
                    }
                }
                b'\\' if bytes.get(cursor + 1) == Some(&b'"') => {
                    message.push(b'"');
                    cursor += 2;
                }
                b'\\' if bytes.get(cursor + 1).is_some() => {
                    message.extend_from_slice(&bytes[cursor..=cursor + 1]);
                    cursor += 2;
                }
                byte => {
                    message.push(byte);
                    cursor += 1;
                }
            }
        }
        if closed {
            found.push((
                line,
                String::from_utf8(message).expect("Rust source strings are valid UTF-8"),
            ));
        }
        search_from = cursor.max(attribute_start + MARKER.len());
    }
    found
}

/// `Err(reason)` when `msg` breaks the convention; `Ok(())` otherwise.
fn check(msg: &str) -> Result<(), String> {
    let message = msg.trim_end();
    if matches!(message.chars().last(), Some('.' | '!' | '?')) {
        return Err("ends with terminal punctuation".to_string());
    }

    if message.chars().next().is_some_and(char::is_uppercase) {
        let first_word: String = message
            .chars()
            .take_while(|character| character.is_alphanumeric() || *character == '-')
            .collect();
        if !ACRONYM_ALLOW.contains(&first_word.as_str()) {
            return Err(format!(
                "starts uppercase with non-allow-listed word {first_word:?}"
            ));
        }
    }
    Ok(())
}

#[test]
fn every_production_error_literal_follows_the_convention() {
    let root = repo_root();
    let sources = production_sources(&root);
    assert!(
        sources.len() >= 80,
        "source walk found only {} files",
        sources.len()
    );
    assert!(
        sources
            .iter()
            .any(|path| path.ends_with("crates/extractor/src/pgoutput/error.rs")),
        "source walk did not recurse into pgoutput"
    );
    assert!(
        sources.iter().all(|path| !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("_test.rs"))),
        "source walk included a sibling test"
    );

    let mut violations: Vec<String> = Vec::new();
    for path in sources {
        let src = std::fs::read_to_string(&path).expect("read a production source file");
        for (line, msg) in error_literals(&src) {
            if let Err(why) = check(&msg) {
                violations.push(format!("{}:{line}: {why} — {msg:?}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "err-lowercase-msg violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn the_gate_rejects_what_it_is_supposed_to_reject() {
    let fixture = r#"
        #[error("invalid configuration: {0}")]
        #[error("{0}")]
        #[error("DuckDB: {0}")]
        #[error("DDL capture not installed: {detail}")]
        #[error("Failed to read config")]
        #[error("failed to read config.")]
        #[error("invalid JSON format!")]
    "#;
    let results: Vec<_> = error_literals(fixture)
        .into_iter()
        .map(|(_, message)| check(&message))
        .collect();
    assert_eq!(results.len(), 7);
    assert!(results[..4].iter().all(Result::is_ok));
    assert!(results[4..].iter().all(Result::is_err));
    assert!(check("DuckDB: {0}").is_ok());
    assert!(check("Failed.").is_err());
}

#[test]
fn the_extractor_sees_multi_line_and_skips_transparent() {
    let src = r#"
        #[error(transparent)]
        Config(#[from] ConfigError),
        #[error(
            "publication {p} missing table {s}.{t} \
             (fix: ALTER PUBLICATION {p} ADD TABLE {s}.{t})"
        )]
        PublicationGap { p: String, s: String, t: String },
    "#;
    let found = error_literals(src);
    assert_eq!(found.len(), 1, "transparent must not be extracted");
    assert!(found[0].1.starts_with("publication "));
    assert!(
        found[0].1.contains("ALTER PUBLICATION"),
        "continuation must be joined"
    );
}
