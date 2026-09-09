//! Guard for `doc-cargo-metadata`: nothing walrus builds goes to a registry, and every member
//! manifest has to say so.
//!
//! The rule's required fields — `description`, `repository`, `keywords`, `categories` — are what a
//! *published* package owes the readers who find it on crates.io. walrus ships two container
//! images instead, so it owes none of them. What it does owe is the other half of that trade, and
//! nothing enforces it: a member that omits `publish` is one `cargo publish` away from uploading a
//! deployment role under a name (`common`, `control`, `extractor`, `transformer`) that is not a
//! crates.io identity, and the fields above would then be missing in the one place they matter. Clippy is silent by
//! construction — `cargo_common_metadata` is the lint that would demand them, and it skips exactly
//! the packages that declare themselves unpublishable. This test is what goes red instead.

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");

/// Every `[workspace] members` entry with its manifest. `include_str!` fixes the list at compile
/// time, so `every_workspace_member_is_covered` re-reads the declaration and fails when member
/// number seven arrives without a line here.
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

/// The `publish` assignment in a table body as `(key, value)`, comments stripped. The key is
/// matched exactly so neither a commented-out line nor a longer name answers for it, and both
/// spellings are returned as written: a member inherits with `publish.workspace`, the workspace
/// itself states the value.
fn publish_assignment(body: &str) -> Option<(&str, &str)> {
    body.lines().find_map(|line| {
        let line = line.trim();
        if line.starts_with('#') {
            return None;
        }

        let (key, value) = line.split_once('=')?;
        let key = key.trim();
        if key != "publish" && key != "publish.workspace" {
            return None;
        }

        Some((key, value.split('#').next()?.trim()))
    })
}

/// Whether a member's `[package]` keeps it off every registry — by inheriting the workspace bar or
/// by stating it outright. `publish = ["some-registry"]` is a publish target like any other, so it
/// does not count; an absent key is the state this guard exists to fail on.
fn is_unpublishable(manifest: &str) -> bool {
    let Some(package) = table_body(manifest, "[package]") else {
        return false;
    };

    match publish_assignment(package) {
        Some(("publish.workspace", value)) => value == "true",
        Some((_, value)) => value == "false" || value == "[]",
        None => false,
    }
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

#[test]
fn the_workspace_states_the_publish_bar_for_members_to_inherit() {
    let package = table_body(WORKSPACE_MANIFEST, "[workspace.package]");

    assert_eq!(
        package.and_then(publish_assignment),
        Some(("publish", "false")),
        "[workspace.package] must carry the non-publication decision every member inherits"
    );
}

#[test]
fn no_member_can_be_uploaded_to_a_registry() {
    for &(member, manifest) in MEMBER_MANIFESTS {
        assert!(
            is_unpublishable(manifest),
            "{member} is publishable — add `publish.workspace = true` to its [package]"
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
        "add each new member's manifest to MEMBER_MANIFESTS so its publish key is checked too"
    );
}

#[test]
fn the_publish_key_is_read_from_fabricated_manifests() {
    let inherited = "[package]\nname = \"transformer\"\npublish.workspace = true\n\n[lints]\n";
    let stated = "[package]\npublish = false # never a registry package\n";
    let no_registries = "[package]\npublish = []\n";
    let commented = "[package]\n# publish = false\n";
    let one_registry = "[package]\npublish = [\"internal\"]\n";
    let another_key = "[package]\npublish-lsn = false\n";
    let no_package = "[workspace]\nmembers = []\n";

    assert!(is_unpublishable(inherited));
    assert!(is_unpublishable(stated));
    assert!(is_unpublishable(no_registries));
    assert!(!is_unpublishable(commented));
    assert!(!is_unpublishable(one_registry));
    assert!(!is_unpublishable(another_key));
    assert!(!is_unpublishable(no_package));
    assert_eq!(
        publish_assignment(stated),
        Some(("publish", "false")),
        "the trailing comment is not part of the value"
    );
}
