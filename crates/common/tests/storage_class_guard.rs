#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              repository-policy checks, not runtime I/O"
)]
//! Storage-class guard. Two invariants no compiler lint covers:
//! a mutable global is never declared, and every production global is a thread-safe one.
//!
//! A `thread_local!` static is exempt from the second invariant: it is one value per thread,
//! never shared, so `Cell`/`RefCell` inside one is the safe replacement for a mutable global
//! rather than a violation of it (`conc-thread-local`). A `#[global_allocator]` item is the other
//! narrow exception: Rust requires that allocator entry point to be a `static`, and the
//! `GlobalAlloc` contract is specifically for globally shared allocators.

use std::path::{Path, PathBuf};

const MUTABLE_GLOBAL_NEEDLE: &str = concat!("static", " mut ");
const THREAD_LOCAL_NEEDLE: &str = "thread_local!";
const ALLOWED_STATIC_HEADS: [&str; 4] = ["OnceLock", "LazyLock", "Atomic", "Mutex"];

/// Workspace root — this crate's manifest dir is `<root>/crates/common`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every `*.rs` under `dir`, recursively, skipping build and VCS output.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read a source directory") {
        let path = entry.expect("read a source-directory entry").path();
        if path.is_dir() {
            if !matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("target" | ".git")
            ) {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// Every production `crates/*/src/**/*.rs`, excluding Go-style sibling unit tests.
fn production_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).expect("read the workspace crates") {
        let src = entry.expect("read a crate entry").path().join("src");
        if src.is_dir() {
            rust_files(&src, &mut sources);
        }
    }
    sources.retain(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("_test.rs"))
    });
    sources.sort();
    sources
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// The `(name, type)` from a declaration-shaped line, after optional visibility.
fn static_declaration(line: &str) -> Option<(&str, &str)> {
    let mut declaration = line.trim_start();
    if declaration.starts_with("//") {
        return None;
    }

    if let Some(rest) = declaration.strip_prefix("pub ") {
        declaration = rest;
    } else if let Some(rest) = declaration.strip_prefix("pub(") {
        let (_, after_visibility) = rest.split_once(") ")?;
        declaration = after_visibility;
    }

    let declaration = declaration.strip_prefix("static ")?;
    let (name, type_and_value) = declaration.split_once(':')?;
    let type_name = type_and_value
        .split_once('=')
        .map_or(type_and_value, |(before_value, _)| before_value)
        .trim();
    Some((name.trim(), type_name))
}

/// Per-line flag: `true` where a line sits inside a `thread_local!` block, tracked by brace depth
/// from the macro invocation. Such a static is per-thread by construction, so the
/// thread-safe-global rule below does not apply to it.
fn thread_local_mask(source: &str) -> Vec<bool> {
    let mut mask = Vec::new();
    let mut depth = 0_usize;
    for line in source.lines() {
        let trimmed = line.trim_start();
        let opens_here = !trimmed.starts_with("//") && trimmed.contains(THREAD_LOCAL_NEEDLE);
        let inside = depth > 0 || opens_here;
        mask.push(inside);
        if inside {
            depth = (depth + line.matches('{').count()).saturating_sub(line.matches('}').count());
        }
    }
    mask
}

/// Per-line flag for the `static` item carrying `#[global_allocator]`. Attribute lines may be
/// separated from the item by other attributes, blank lines, or comments, but the exemption is
/// consumed by the next declaration and never leaks to a following ordinary static.
fn global_allocator_mask(source: &str) -> Vec<bool> {
    let mut mask = Vec::new();
    let mut pending = false;
    for line in source.lines() {
        let trimmed = line.trim();
        let is_declaration = static_declaration(line).is_some();
        mask.push(pending && is_declaration);

        if trimmed == "#[global_allocator]" {
            pending = true;
        } else if is_declaration
            || (!trimmed.is_empty() && !trimmed.starts_with("//") && !trimmed.starts_with("#["))
        {
            pending = false;
        }
    }
    mask
}

fn mutable_global_offences(path: &str, source: &str) -> Vec<String> {
    source
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if !line.contains(MUTABLE_GLOBAL_NEEDLE) {
                return None;
            }
            let (name, _) = static_declaration(line)?;
            let name = name.strip_prefix("mut ")?;
            Some(format!(
                "{path}:{}: mutable global {name} is banned; use an Atomic*, OnceLock, LazyLock, \
                 or Mutex for shared state, or thread_local! with Cell/RefCell for per-thread \
                 state",
                index + 1
            ))
        })
        .collect()
}

fn is_allowed_static_type(type_name: &str) -> bool {
    let qualified_head = type_name
        .split(['<', ';'])
        .next()
        .map(str::trim)
        .unwrap_or_default();
    let head = qualified_head.rsplit("::").next().unwrap_or_default();
    ALLOWED_STATIC_HEADS
        .iter()
        .any(|allowed| head == *allowed || (*allowed == "Atomic" && head.starts_with("Atomic")))
}

fn plain_static_offences(path: &str, source: &str) -> Vec<String> {
    source
        .lines()
        .zip(thread_local_mask(source))
        .zip(global_allocator_mask(source))
        .enumerate()
        .filter_map(|(index, ((line, in_thread_local), is_global_allocator))| {
            if in_thread_local || is_global_allocator {
                return None;
            }
            let (name, type_name) = static_declaration(line)?;
            if name.starts_with("mut ") || is_allowed_static_type(type_name) {
                return None;
            }
            Some(format!(
                "{path}:{}: plain addressed global {name}: {type_name}; use const for a small \
                 value, OnceLock, LazyLock, an Atomic*, or Mutex for shared state, or \
                 thread_local! with Cell/RefCell for per-thread state",
                index + 1
            ))
        })
        .collect()
}

#[test]
fn no_mutable_global_is_declared_anywhere() {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    rust_files(&root.join("tests"), &mut files);
    files.sort();

    let mut offences = Vec::new();
    for file in files {
        let relative = display_path(&root, &file);
        let source = std::fs::read_to_string(&file).expect("read a Rust source file");
        offences.extend(mutable_global_offences(&relative, &source));
    }

    assert!(
        offences.is_empty(),
        "mutable globals are banned — use an Atomic*, OnceLock, LazyLock, or Mutex for shared \
         state, or thread_local! for per-thread state:\n{}",
        offences.join("\n")
    );
}

#[test]
fn every_production_static_is_a_thread_safe_global() {
    let root = workspace_root();
    let sources = production_sources(&root);
    assert!(!sources.is_empty(), "the production source scan is empty");

    let mut offences = Vec::new();
    for file in sources {
        let relative = display_path(&root, &file);
        let source = std::fs::read_to_string(&file).expect("read a production Rust source file");
        offences.extend(plain_static_offences(&relative, &source));
    }

    assert!(
        offences.is_empty(),
        "plain addressed globals are banned — use const for small values, a thread-safe global, or \
         thread_local! for per-thread state:\n{}",
        offences.join("\n")
    );
}

#[test]
fn synthetic_mutable_global_is_rejected() {
    let source = format!("{} mut COUNTER: u64 = 0;", "static");
    let offences = mutable_global_offences("fixture/static_mut.rs", &source);
    let diagnostic = offences.join("\n");

    assert!(diagnostic.contains("fixture/static_mut.rs:1"));
    assert!(diagnostic.contains("Atomic*"));
    assert!(diagnostic.contains("OnceLock"));
    assert!(diagnostic.contains("LazyLock"));
    assert!(diagnostic.contains(THREAD_LOCAL_NEEDLE));
}

/// A per-thread static is not a shared global: the guard must accept `Cell`/`RefCell` inside a
/// `thread_local!` block, and must resume flagging plain statics once the block closes.
#[test]
fn synthetic_thread_local_is_accepted() {
    let source = concat!(
        "thread_local! {\n",
        "    static TS_SCRATCH: RefCell<String> = RefCell::new(String::new());\n",
        "    static CALLS: Cell<u32> = const { Cell::new(0) };\n",
        "}\n",
        "static TIMEOUT_MS: u64 = 5_000;\n",
    );
    let offences = plain_static_offences("fixture/thread_local.rs", source);
    let diagnostic = offences.join("\n");

    assert_eq!(
        offences.len(),
        1,
        "only the trailing plain static: {diagnostic}"
    );
    assert!(diagnostic.contains("fixture/thread_local.rs:5"));
    assert!(diagnostic.contains("TIMEOUT_MS"));
}

/// The brace tracking must also survive a single-line `thread_local!` invocation.
#[test]
fn synthetic_inline_thread_local_is_accepted() {
    let source = "thread_local! { static CALLS: Cell<u32> = const { Cell::new(0) }; }\n\
                  static TIMEOUT_MS: u64 = 5_000;";
    let offences = plain_static_offences("fixture/inline_thread_local.rs", source);
    let diagnostic = offences.join("\n");

    assert_eq!(
        offences.len(),
        1,
        "one line opens and closes it: {diagnostic}"
    );
    assert!(diagnostic.contains("fixture/inline_thread_local.rs:2"));
}

#[test]
fn synthetic_global_allocator_is_accepted_without_exempting_the_next_static() {
    let source = "#[cfg(feature = \"heap\")]\n\
                  #[global_allocator]\n\
                  // Rust requires this allocator entry point to be a static.\n\
                  static ALLOC: heap::Alloc = heap::Alloc;\n\
                  static TIMEOUT_MS: u64 = 5_000;";
    let offences = plain_static_offences("fixture/global_allocator.rs", source);
    let diagnostic = offences.join("\n");

    assert_eq!(offences.len(), 1, "only the ordinary static: {diagnostic}");
    assert!(!diagnostic.contains("ALLOC:"), "{diagnostic}");
    assert!(diagnostic.contains("TIMEOUT_MS"), "{diagnostic}");
}

#[test]
fn a_commented_global_allocator_attribute_does_not_exempt_a_static() {
    let source = "// #[global_allocator]\nstatic ALLOC: heap::Alloc = heap::Alloc;";
    let offences = plain_static_offences("fixture/commented_allocator.rs", source);
    let diagnostic = offences.join("\n");

    assert_eq!(offences.len(), 1, "{diagnostic}");
    assert!(diagnostic.contains("ALLOC"), "{diagnostic}");
}

#[test]
fn synthetic_plain_static_is_rejected() {
    let source = "static TIMEOUT_MS: u64 = 5_000;";
    let offences = plain_static_offences("fixture/plain_static.rs", source);
    let diagnostic = offences.join("\n");

    assert!(diagnostic.contains("fixture/plain_static.rs:1"));
    assert!(diagnostic.contains("const"));
}
