#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
#![allow(
    clippy::disallowed_methods,
    reason = "the synchronous integration-test scan is a repository-policy check, not runtime I/O"
)]

//! Guard: walrus newtypes expose explicit accessors (`Lsn::as_u64`, `From<ManifestId> for i64`,
//! `DuckTable::as_str`), never `Deref`. `Deref` is for smart pointers and owned→borrowed containers
//! (API guideline C-DEREF); using it on a domain newtype re-exposes every inner method through
//! method resolution and quietly undoes the type distinction the newtype was created to enforce.

use std::{
    fs,
    path::{Path, PathBuf},
};

const DECISION_CONTEXT: &str = "domain newtypes expose explicit accessors, never Deref";

/// Types that legitimately implement `Deref`/`DerefMut`, with the reason.
///
/// A real smart pointer, an owned→borrowed container (`String` → `str`) or an RAII guard belongs
/// here. A domain newtype (`Lsn`, `ManifestId`, `EpochNo`, `SchemaVersionNo`, `ReloadId`,
/// `DuckTable<K>`) does **not** — that is exactly what this guard exists to catch.
///
/// Empty: `SubscribeGuard` was the one entry until it replaced its receiver `Deref` with an
/// inherent `try_recv`. An RAII guard still qualifies — but only when handing out the inner type's
/// whole API is the access it means to grant, which a subscription awaited through `Future` and
/// `try_recv` does not need.
const ALLOWED: &[(&str, &str, &str)] = &[];

#[test]
fn no_walrus_type_implements_deref() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/common/../.. is the repo root");
    let crates_dir = repo_root.join("crates");

    let mut offences = Vec::new();
    let mut files_scanned = 0_usize;
    for src in crate_src_dirs(&crates_dir) {
        for file in rust_files(&src) {
            files_scanned += 1;
            let rel = file
                .strip_prefix(repo_root)
                .expect("source file is beneath the repository root")
                .to_string_lossy()
                .replace('\\', "/");
            let source =
                fs::read_to_string(&file).expect("failed to read a production Rust source file");
            offences.extend(deref_offences(&rel, &source));
        }
    }

    assert!(
        files_scanned > 0,
        "the production source scan found no Rust files"
    );
    assert!(
        offences.is_empty(),
        "{DECISION_CONTEXT}:\n{}",
        offences.join("\n")
    );
}

#[test]
fn scanner_rejects_fabricated_deref() {
    let source = r#"
//! A comment that mentions impl std::ops::Deref for ManifestId must be ignored.
// impl std::ops::Deref for ManifestId also remains a comment.
impl std::ops::Deref for ManifestId {
    type Target = i64;
}
"#;

    let offences = deref_offences("crates/common/src/fabricated.rs", source);
    let rejection = rejection_message(&offences).expect("the fabricated Deref must be rejected");

    assert!(rejection.contains(DECISION_CONTEXT));
    assert!(rejection.contains("crates/common/src/fabricated.rs:4"));
    assert!(!rejection.contains(":2"));
    assert!(!rejection.contains(":3"));
}

/// The lookup is exercised against a fixture list because the live [`ALLOWED`] is empty: a future
/// entry must still match on BOTH the repo-relative path and the exact wrapper name, so a
/// same-named type in another module — or a longer name in the listed one — stays an offence.
#[test]
fn allow_list_requires_the_path_and_exact_type() {
    let allowed: &[(&str, &str, &str)] = &[(
        "crates/extractor/src/reload_signal.rs",
        "SubscribeGuard",
        "fixture: a hypothetical RAII guard entry",
    )];
    let implementation = "impl std::ops::Deref for SubscribeGuard<'_> {";

    assert!(is_allowed_in(
        allowed,
        "crates/extractor/src/reload_signal.rs",
        implementation
    ));
    assert!(!is_allowed_in(
        allowed,
        "crates/extractor/src/another_module.rs",
        implementation
    ));
    assert!(!is_allowed_in(
        allowed,
        "crates/extractor/src/reload_signal.rs",
        "impl std::ops::Deref for SubscribeGuardAdapter<'_> {"
    ));
}

/// `crates/<name>/src` for every member.
fn crate_src_dirs(crates_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = fs::read_dir(crates_dir)
        .expect("failed to read the workspace crates directory")
        .map(|entry| {
            entry
                .expect("failed to read crate directory entry")
                .path()
                .join("src")
        })
        .filter(|src| src.is_dir())
        .collect::<Vec<_>>();
    dirs.sort();
    dirs
}

/// Every `.rs` file under `dir`, recursively (no `walkdir` dependency).
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).expect("failed to read a production source directory") {
        let path = entry.expect("failed to read source directory entry").path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn deref_offences(rel: &str, source: &str) -> Vec<String> {
    source
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if !is_deref_impl(line) || is_allowed(rel, line) {
                return None;
            }

            Some(format!("{rel}:{}  {}", index + 1, line.trim()))
        })
        .collect()
}

/// A source line that actually implements `Deref`, ignoring doc comments and line comments —
/// `//! …never Deref…` in a module header must not trip the guard.
fn is_deref_impl(line: &str) -> bool {
    let trimmed = line.trim_start();
    !trimmed.starts_with("//")
        && trimmed.starts_with("impl")
        && trimmed.contains("Deref")
        && trimmed.contains(" for ")
}

/// The allow-list lookup, by repo-relative path and the exact wrapper named on the impl line.
fn is_allowed(rel: &str, line: &str) -> bool {
    is_allowed_in(ALLOWED, rel, line)
}

/// The lookup itself, over an explicit list so it stays testable while [`ALLOWED`] is empty.
fn is_allowed_in(allowed: &[(&str, &str, &str)], rel: &str, line: &str) -> bool {
    let Some(type_name) = impl_type_name(line) else {
        return false;
    };

    allowed
        .iter()
        .any(|(path, allowed_type, _reason)| *path == rel && *allowed_type == type_name)
}

fn impl_type_name(line: &str) -> Option<&str> {
    let (_, implemented_for) = line.split_once(" for ")?;
    implemented_for
        .trim_start()
        .split(|character: char| character == '<' || character == '{' || character.is_whitespace())
        .next()
        .and_then(|qualified| qualified.rsplit("::").next())
}

fn rejection_message(offences: &[String]) -> Option<String> {
    if offences.is_empty() {
        return None;
    }

    Some(format!("{DECISION_CONTEXT}:\n{}", offences.join("\n")))
}
