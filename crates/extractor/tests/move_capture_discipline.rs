#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              themselves repository-policy checks, not runtime I/O"
)]
//! Conformance guard for `closure-move-capture`: every production `move` closure and
//! `async move` block captures explicitly pre-bound locals, never `self`. Pure source scanning —
//! no Docker, no new dependency. Two clone-shaped guards ride along, because they answer the other
//! half of the same question — what a capture is allowed to duplicate, and what it never may.

use std::path::{Path, PathBuf};

/// Repo root, derived from this crate's manifest dir (`<root>/crates/extractor`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize the repository root")
}

/// Every production `crates/*/src/**/*.rs` under `root`; recurses (see `extractor/src/pgoutput/`)
/// and skips the Go-style sibling `*_test.rs` unit-test files.
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

fn blank_non_newlines(bytes: &mut [u8], start: usize, end: usize) {
    for byte in &mut bytes[start..end] {
        if *byte != b'\n' {
            *byte = b' ';
        }
    }
}

fn raw_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut cursor = start;
    if matches!(bytes.get(cursor), Some(b'b' | b'c')) {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'r') {
        return None;
    }
    cursor += 1;
    let hashes_start = cursor;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    let hashes = cursor - hashes_start;
    if bytes.get(cursor) != Some(&b'"') {
        return None;
    }
    cursor += 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && bytes
                .get(cursor + 1..cursor + 1 + hashes)
                .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'))
        {
            return Some(cursor + 1 + hashes);
        }
        cursor += 1;
    }
    Some(bytes.len())
}

fn quoted_string_end(bytes: &[u8], start: usize) -> usize {
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => cursor = (cursor + 2).min(bytes.len()),
            b'"' => return cursor + 1,
            _ => cursor += 1,
        }
    }
    bytes.len()
}

fn char_literal_end(src: &str, start: usize) -> Option<usize> {
    let bytes = src.as_bytes();
    let mut cursor = start + 1;
    match bytes.get(cursor)? {
        b'\\' => {
            cursor += 1;
            match bytes.get(cursor)? {
                b'x' => cursor += 3,
                b'u' if bytes.get(cursor + 1) == Some(&b'{') => {
                    cursor += 2;
                    while bytes.get(cursor).is_some_and(|byte| *byte != b'}') {
                        cursor += 1;
                    }
                    cursor += usize::from(bytes.get(cursor) == Some(&b'}'));
                }
                _ => cursor += 1,
            }
        }
        _ => {
            cursor += src[cursor..].chars().next()?.len_utf8();
        }
    }
    (bytes.get(cursor) == Some(&b'\'')).then_some(cursor + 1)
}

/// Replace literals and comments with spaces while preserving byte offsets and newlines.
fn sanitized_source(src: &str) -> String {
    let original = src.as_bytes();
    let mut clean = original.to_vec();
    let mut cursor = 0;
    while cursor < original.len() {
        let end = if original
            .get(cursor..)
            .is_some_and(|tail| tail.starts_with(b"//"))
        {
            original[cursor + 2..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(original.len(), |offset| cursor + 2 + offset)
        } else if original
            .get(cursor..)
            .is_some_and(|tail| tail.starts_with(b"/*"))
        {
            let mut end = cursor + 2;
            let mut depth = 1usize;
            while end < original.len() && depth > 0 {
                if original[end..].starts_with(b"/*") {
                    depth += 1;
                    end += 2;
                } else if original[end..].starts_with(b"*/") {
                    depth -= 1;
                    end += 2;
                } else {
                    end += 1;
                }
            }
            end
        } else if let Some(end) = raw_string_end(original, cursor) {
            end
        } else if original[cursor] == b'"' {
            quoted_string_end(original, cursor)
        } else if original[cursor] == b'\''
            && let Some(end) = char_literal_end(src, cursor)
        {
            end
        } else {
            cursor += 1;
            continue;
        };
        blank_non_newlines(&mut clean, cursor, end);
        cursor = end;
    }
    String::from_utf8(clean).expect("blanking valid UTF-8 preserves valid UTF-8")
}

const fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_keyword_at(bytes: &[u8], start: usize, keyword: &[u8]) -> bool {
    bytes.get(start..start + keyword.len()) == Some(keyword)
        && (start == 0 || !is_ident_byte(bytes[start - 1]))
        && bytes
            .get(start + keyword.len())
            .is_none_or(|byte| !is_ident_byte(*byte))
}

fn skip_whitespace(bytes: &[u8], mut cursor: usize) -> usize {
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor += 1;
    }
    cursor
}

fn braced_body(clean: &str, open: usize) -> Option<(String, usize)> {
    let bytes = clean.as_bytes();
    let mut depth = 0usize;
    for cursor in open..bytes.len() {
        match bytes[cursor] {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some((clean[open + 1..cursor].to_string(), cursor + 1));
                }
            }
            _ => {}
        }
    }
    None
}

fn expression_body(clean: &str, start: usize) -> String {
    let bytes = clean.as_bytes();
    let (mut parens, mut brackets, mut braces) = (0usize, 0usize, 0usize);
    let mut cursor = start;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'(' => parens += 1,
            b'[' => brackets += 1,
            b'{' => braces += 1,
            b')' if parens == 0 && brackets == 0 && braces == 0 => break,
            b']' if parens == 0 && brackets == 0 && braces == 0 => break,
            b'}' if parens == 0 && brackets == 0 && braces == 0 => break,
            b',' | b';' if parens == 0 && brackets == 0 && braces == 0 => break,
            b')' => parens -= 1,
            b']' => brackets -= 1,
            b'}' => braces -= 1,
            _ => {}
        }
        cursor += 1;
    }
    clean[start..cursor].to_string()
}

/// Every `move` capture body in `src`, as `(1-based line, kind, body text)`.
/// `kind` is `"async move"` for `async move { … }` and `"move ||"` for a `move |…|` closure.
/// Brace-matches from the opening `{`, skipping string literals and comments so braces inside SQL
/// cannot desynchronise the depth counter. Expression-bodied closures end at their outer delimiter.
fn move_bodies(src: &str) -> Vec<(usize, &'static str, String)> {
    let clean = sanitized_source(src);
    let bytes = clean.as_bytes();
    let mut found = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if is_keyword_at(bytes, cursor, b"async") {
            let after_async = skip_whitespace(bytes, cursor + "async".len());
            if is_keyword_at(bytes, after_async, b"move") {
                let open = skip_whitespace(bytes, after_async + "move".len());
                if bytes.get(open) == Some(&b'{')
                    && let Some((body, _)) = braced_body(&clean, open)
                {
                    let line = src[..cursor].bytes().filter(|byte| *byte == b'\n').count() + 1;
                    found.push((line, "async move", body));
                }
            }
        }

        if is_keyword_at(bytes, cursor, b"move") {
            let params_open = skip_whitespace(bytes, cursor + "move".len());
            if bytes.get(params_open) == Some(&b'|')
                && let Some(params_len) = bytes[params_open + 1..]
                    .iter()
                    .position(|byte| *byte == b'|')
            {
                let body_start = skip_whitespace(bytes, params_open + 1 + params_len + 1);
                let body = if bytes.get(body_start) == Some(&b'{') {
                    braced_body(&clean, body_start).map(|(body, _)| body)
                } else if is_keyword_at(bytes, body_start, b"async") {
                    let after_async = skip_whitespace(bytes, body_start + "async".len());
                    let open = if is_keyword_at(bytes, after_async, b"move") {
                        skip_whitespace(bytes, after_async + "move".len())
                    } else {
                        after_async
                    };
                    (bytes.get(open) == Some(&b'{'))
                        .then(|| braced_body(&clean, open).map(|(body, _)| body))
                        .flatten()
                } else {
                    Some(expression_body(&clean, body_start))
                };
                if let Some(body) = body {
                    let line = src[..cursor].bytes().filter(|byte| *byte == b'\n').count() + 1;
                    found.push((line, "move ||", body));
                }
            }
        }
        cursor += 1;
    }
    found
}

fn reaches_through_self(body: &str) -> bool {
    let bytes = body.as_bytes();
    (0..bytes.len()).any(|cursor| {
        is_keyword_at(bytes, cursor, b"self")
            && bytes.get(skip_whitespace(bytes, cursor + "self".len())) == Some(&b'.')
    })
}

fn clones_self(src: &str) -> bool {
    let clean = sanitized_source(src);
    let bytes = clean.as_bytes();
    (0..bytes.len()).any(|cursor| {
        if !is_keyword_at(bytes, cursor, b"self") {
            return false;
        }
        let dot = skip_whitespace(bytes, cursor + "self".len());
        let clone = skip_whitespace(bytes, dot + 1);
        bytes.get(dot) == Some(&b'.') && is_keyword_at(bytes, clone, b"clone")
    })
}

#[test]
fn no_production_move_body_reaches_through_self() {
    let root = repo_root();
    let (mut async_blocks, mut move_closures) = (0usize, 0usize);
    let mut violations: Vec<String> = Vec::new();
    for path in production_sources(&root) {
        let src = std::fs::read_to_string(&path).expect("read a production source file");
        for (line, kind, body) in move_bodies(&src) {
            match kind {
                "async move" => async_blocks += 1,
                "move ||" => move_closures += 1,
                other => violations.push(format!(
                    "{}:{line}: scanner returned unknown kind {other}",
                    path.display()
                )),
            }
            if reaches_through_self(&body) {
                violations.push(format!("{}:{line}: {kind}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "closure-move-capture violations (bind + clone the fields, do not move `self`):\n{}",
        violations.join("\n")
    );
    assert!(
        async_blocks >= 15,
        "only {async_blocks} `async move` blocks — walker is broken"
    );
    assert!(
        move_closures >= 2,
        "only {move_closures} `move |` closures — walker is broken"
    );
}

#[test]
fn production_never_clones_self_wholesale() {
    let root = repo_root();
    let violations: Vec<_> = production_sources(&root)
        .into_iter()
        .filter(|path| {
            clones_self(&std::fs::read_to_string(path).expect("read a production source file"))
        })
        .collect();
    assert!(
        violations.is_empty(),
        "production `self.clone()` calls: {violations:#?}"
    );
}

#[test]
fn redundant_clone_stays_denied() {
    let manifest = std::fs::read_to_string(repo_root().join("Cargo.toml"))
        .expect("read the workspace manifest");
    let clippy_table = manifest
        .split_once("[workspace.lints.clippy]")
        .expect("workspace clippy lint table")
        .1
        .split("\n[")
        .next()
        .expect("workspace clippy lint table body");
    assert_eq!(
        clippy_table
            .lines()
            .filter(|line| line.trim() == "redundant_clone = \"deny\"")
            .count(),
        1,
        "[workspace.lints.clippy] must deny redundant_clone exactly once"
    );
}

/// `own-clone-explicit`: a `Clone` nobody calls is still an invitation to call one.
/// `StreamedChange` buffers one decoded row of an open streamed transaction — pushed by
/// value, drained by `extract_if`, read through `iter_survivors` — and `InflightMeter` charges its
/// bytes exactly once against the spill ceiling. A derive would make an unmetered deep copy of that
/// whole buffer compile silently, so the buffered row stays move-only.
#[test]
fn the_buffered_streamed_row_stays_move_only() {
    let source = std::fs::read_to_string(repo_root().join("crates/extractor/src/stream_txn.rs"))
        .expect("read the streamed-transaction buffer source");
    let derive = source
        .split_once("struct StreamedChange")
        .expect("the StreamedChange definition")
        .0
        .lines()
        .rev()
        .find(|line| line.starts_with("#[derive("))
        .expect("the StreamedChange derive list");
    assert!(
        !derive.contains("Clone"),
        "StreamedChange derives: {derive}"
    );
}

#[test]
fn the_guard_rejects_what_it_is_supposed_to_reject() {
    let good =
        r#"let pool = self.pool.clone(); tokio::spawn(async move { use_it(&pool).await; });"#;
    let bad = r#"tokio::spawn(async move { use_it(&self.pool).await; });"#;
    let syntax_noise = r###"
        tokio::spawn(async move {
            let sql = "SELECT '{'";
            let raw = r#"{"#;
            let brace = '{';
            // } self.not_a_capture
            use_it(&pool, sql, raw, brace).await;
        });
    "###;

    let good_bodies = move_bodies(good);
    assert_eq!(good_bodies.len(), 1);
    assert!(!reaches_through_self(&good_bodies[0].2));

    let bad_bodies = move_bodies(bad);
    assert_eq!(bad_bodies.len(), 1);
    assert!(reaches_through_self(&bad_bodies[0].2));

    let noise_bodies = move_bodies(syntax_noise);
    assert_eq!(
        noise_bodies.len(),
        1,
        "strings/comments must not unbalance braces"
    );
    assert!(!reaches_through_self(&noise_bodies[0].2));
    assert!(
        noise_bodies[0].2.contains("use_it"),
        "body ended at a quoted brace"
    );
}
