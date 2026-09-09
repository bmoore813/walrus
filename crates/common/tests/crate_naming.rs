//! Guard for `name-crate-no-rs`: no first-party name announces the language it is written in.
//!
//! Cargo accepts `walrus-transformer-rs` as readily as `walrus-transformer`, and no rustc or Clippy lint has
//! an opinion about a package name — the whole rule lives in review. This test is what goes red
//! instead, across every name a member manifest declares (`[package]`, `[lib]`, `[[bin]]`,
//! `[[bench]]`) and the directory each member lives in.

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");

/// Every `[workspace] members` entry with its manifest. `include_str!` fixes the list at compile
/// time, so `every_workspace_member_is_covered` re-reads the declaration and fails when member
/// number seven arrives without a line here — the one moment this rule is actually decided.
const MEMBER_MANIFESTS: &[(&str, &str)] = &[
    ("crates/common", include_str!("../Cargo.toml")),
    (
        "crates/walrus-config",
        include_str!("../../walrus-config/Cargo.toml"),
    ),
    ("crates/control", include_str!("../../control/Cargo.toml")),
    (
        "crates/transformer",
        include_str!("../../transformer/Cargo.toml"),
    ),
    (
        "crates/extractor",
        include_str!("../../extractor/Cargo.toml"),
    ),
    (
        "crates/pg-to-arrow",
        include_str!("../../pg-to-arrow/Cargo.toml"),
    ),
    ("tests/e2e", include_str!("../../../tests/e2e/Cargo.toml")),
];

/// The endings and openings that make a name announce its language. Both separators count:
/// package names hyphenate, `[lib]` names use the underscore spelling.
const SUFFIXES: [&str; 4] = ["-rs", "_rs", "-rust", "_rust"];
const PREFIXES: [&str; 2] = ["rust-", "rust_"];

/// The body of `header` in `manifest`, up to the next line that opens a table. `None` if absent.
fn table_body<'a>(manifest: &'a str, header: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start: Option<usize> = None;

    for line in manifest.split_inclusive('\n') {
        let trimmed = line.trim();
        match body_start {
            Some(start) if trimmed.starts_with('[') => return Some(&manifest[start..offset]),
            None if trimmed == header => body_start = Some(offset + line.len()),
            _ => {}
        }
        offset += line.len();
    }

    body_start.map(|start| &manifest[start..])
}

/// The paths in `[workspace] members`, however the array is wrapped. Each entry is read as the
/// first quoted string in its comma-separated slot, so a comment between entries cannot hide one.
fn workspace_members(manifest: &str) -> Vec<&str> {
    let Some(body) = table_body(manifest, "[workspace]") else {
        return Vec::new();
    };
    let Some(key) = body.find("members") else {
        return Vec::new();
    };
    let Some((_, array)) = body[key..].split_once('[') else {
        return Vec::new();
    };
    let Some((entries, _)) = array.split_once(']') else {
        return Vec::new();
    };

    entries
        .split(',')
        .filter_map(|entry| {
            let (_, quoted) = entry.split_once('"')?;
            let (member, _) = quoted.split_once('"')?;
            Some(member)
        })
        .collect()
}

/// Every `name = "…"` a manifest declares. A dependency spells its own key first
/// (`sqlx = { workspace = true }`), so no upstream crate name is collected: walrus cannot rename
/// those, and the rule is about the names it chooses. `rust-version` is a key, not a name.
fn declared_names(manifest: &str) -> Vec<&str> {
    manifest
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with('#') {
                return None;
            }

            let (key, value) = line.split_once('=')?;
            if key.trim() != "name" {
                return None;
            }

            Some(value.split('#').next()?.trim().trim_matches('"'))
        })
        .collect()
}

/// The `-rs`/`-rust` suffix or `rust-` prefix that makes a name announce its language, if any. A
/// name that merely ends in those letters (`parsers`) carries no separator and is not a marker.
fn language_marker(name: &str) -> Option<&'static str> {
    let suffix = SUFFIXES.into_iter().find(|m| name.ends_with(*m));
    let prefix = PREFIXES.into_iter().find(|m| name.starts_with(*m));
    suffix.or(prefix)
}

fn last_segment(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[test]
fn no_declared_name_announces_its_language() {
    let mut all = Vec::new();

    for &(member, manifest) in MEMBER_MANIFESTS {
        let names = declared_names(manifest);
        assert!(
            !names.is_empty(),
            "{member} declares no name — scan is broken"
        );
        for name in &names {
            assert_eq!(
                language_marker(name),
                None,
                "{member} names a target {name:?} after the language it is written in"
            );
        }
        all.extend(names);
    }

    // The shipped binaries live in `[[bin]]`, not `[package]`. Finding them proves the scan
    // reaches every target table, so a `-rs` binary under a clean package name still fails above.
    for binary in ["walrus-transformer", "walrus-extractor"] {
        assert!(
            all.contains(&binary),
            "the scan missed the {binary} bin target"
        );
    }
}

#[test]
fn no_member_directory_announces_its_language() {
    for &(member, _) in MEMBER_MANIFESTS {
        let dir = last_segment(member);
        assert_eq!(
            language_marker(dir),
            None,
            "the {member} directory says Rust"
        );
    }
}

#[test]
fn every_workspace_member_is_covered() {
    let mut declared = workspace_members(WORKSPACE_MANIFEST);
    declared.sort_unstable();
    let mut covered: Vec<&str> = MEMBER_MANIFESTS.iter().map(|&(path, _)| path).collect();
    covered.sort_unstable();

    assert_eq!(
        declared, covered,
        "add each new member's manifest to MEMBER_MANIFESTS so its names are checked too"
    );
}

#[test]
fn declared_names_reads_every_target_table() {
    let manifest = concat!(
        "[package]\n",
        "name = \"extractor\" # the crate\n",
        "rust-version = \"1.95\"\n",
        "\n",
        "[lib]\n",
        "name = \"extractor\"\n",
        "\n",
        "[[bin]]\n",
        "name = \"walrus-extractor\"\n",
        "\n",
        "[dependencies]\n",
        "sqlx = { workspace = true, features = [\"postgres\"] }\n",
        "# name = \"commented-out-rs\"\n",
    );

    let expected = ["extractor", "extractor", "walrus-extractor"];
    assert_eq!(declared_names(manifest), expected);
}

#[test]
fn workspace_members_survives_a_wrapped_array() {
    let manifest = concat!(
        "[workspace]\n",
        "resolver = \"2\"\n",
        "members = [\n",
        "    \"crates/common\",\n",
        "    # a comment between entries must not hide a member\n",
        "    \"tests/e2e\",\n",
        "]\n",
        "\n",
        "[workspace.package]\n",
        "edition = \"2024\"\n",
    );
    let no_workspace = "[package]\nname = \"common\"\n";

    let members = workspace_members(WORKSPACE_MANIFEST);

    assert!(members.contains(&"crates/pg-to-arrow"));
    assert_eq!(workspace_members(manifest), ["crates/common", "tests/e2e"]);
    assert_eq!(workspace_members(no_workspace), Vec::<&str>::new());
}

#[test]
fn language_marker_rejects_fabricated_input() {
    let cases = [
        ("pg-to-arrow", None),
        ("walrus-transformer", None),
        ("extractor", None),
        // Ends in the letters, but carries no separator: a word, not a language tag.
        ("parsers", None),
        ("json-parser-rs", Some("-rs")),
        ("json_parser_rs", Some("_rs")),
        ("my-lib-rust", Some("-rust")),
        ("my_lib_rust", Some("_rust")),
        ("rust-sqlite", Some("rust-")),
        ("rust_sqlite", Some("rust_")),
    ];

    for (name, expected) in cases {
        assert_eq!(language_marker(name), expected, "name: {name}");
    }
}
