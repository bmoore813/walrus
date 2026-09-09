//! Guard for `proj-msrv-declare`: walrus declares one MSRV, and every member takes its floor from
//! there.
//!
//! The CI `msrv` job compares the declared `rust-version` with the channel `rust-toolchain.toml`
//! pins, so drift between those two is already a red build. What it reads is one line of the *root*
//! manifest — the first `rust-version` assignment, `head -1` — so a member that states its own
//! floor, or omits the key altogether, passes it untouched. Cargo is quiet in the same way: a
//! package with no `rust-version` has no MSRV to check, so the actionable "package `foo` requires
//! rustc 1.96" error never arrives and edition-2024 source fails on an old toolchain as a syntax
//! error far from the manifest that caused it. No lint covers the gap either:
//! `cargo_common_metadata` is the one Clippy lint that reads a manifest, it does not ask for
//! `rust-version`, and it skips a package that declares itself unpublishable — which every member
//! does (`crates/common/tests/publish_policy.rs`). This test is what goes red instead.

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

/// The `rust-version` assignment in a table body as `(key, value)`, comments stripped. The key is
/// matched exactly so neither a commented-out line nor a longer name answers for it, and both
/// spellings are returned as written: a member inherits with `rust-version.workspace`, the
/// workspace itself states the number.
fn msrv_assignment(body: &str) -> Option<(&str, &str)> {
    body.lines().find_map(|line| {
        let line = line.trim();
        if line.starts_with('#') {
            return None;
        }

        let (key, value) = line.split_once('=')?;
        let key = key.trim();
        if key != "rust-version" && key != "rust-version.workspace" {
            return None;
        }

        Some((key, value.split('#').next()?.trim()))
    })
}

/// What a member's own `[package]` says about its MSRV. Scoped to that table, so the root
/// declaration a member manifest cannot own is never mistaken for one it does.
fn package_msrv(manifest: &str) -> Option<(&str, &str)> {
    table_body(manifest, "[package]").and_then(msrv_assignment)
}

/// Whether `part` is a decimal number: at least one ASCII digit and nothing else.
const fn is_number(part: &str) -> bool {
    let digits = part.as_bytes();
    let mut i = 0;
    while i < digits.len() {
        if !digits[i].is_ascii_digit() {
            return false;
        }
        i += 1;
    }

    !digits.is_empty()
}

/// Whether a value states a floor: a quoted `major.minor` with an optional patch — the shape the
/// CI `msrv` job's `[0-9]+\.[0-9]+` comparison reads. The inheritance spelling's `true` is not one,
/// and neither is a channel name: the number lives at the root, exactly once.
fn states_a_version(value: &str) -> bool {
    let Some(version) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return false;
    };

    let mut parts = version.split('.');
    let (Some(major), Some(minor)) = (parts.next(), parts.next()) else {
        return false;
    };
    let patch = parts.next();

    is_number(major) && is_number(minor) && patch.is_none_or(is_number) && parts.next().is_none()
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
fn the_workspace_states_the_msrv_for_members_to_inherit() {
    let package = table_body(WORKSPACE_MANIFEST, "[workspace.package]");
    let declared = package.and_then(msrv_assignment);

    assert!(
        matches!(declared, Some(("rust-version", floor)) if states_a_version(floor)),
        "[workspace.package] must state one `rust-version = \"<major.minor>\"`, got {declared:?}"
    );
}

#[test]
fn every_member_inherits_the_workspace_msrv() {
    for &(member, manifest) in MEMBER_MANIFESTS {
        assert_eq!(
            package_msrv(manifest),
            Some(("rust-version.workspace", "true")),
            "{member} owns its MSRV — add `rust-version.workspace = true` to its [package]"
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
        "add each new member's manifest to MEMBER_MANIFESTS so its MSRV key is checked too"
    );
}

#[test]
fn the_msrv_key_is_read_from_fabricated_manifests() {
    let inherited = "[package]\nname = \"transformer\"\nrust-version.workspace = true\n\n[lints]\n";
    let stated = "[package]\nrust-version = \"1.95\" # in sync with rust-toolchain.toml\n";
    let commented = "[package]\n# rust-version.workspace = true\n";
    let another_key = "[package]\nrust-version-policy = \"inherit\"\n";
    let root_only = "[package]\nname = \"e2e\"\n[workspace.package]\nrust-version = \"1.95\"\n";

    assert_eq!(
        package_msrv(inherited),
        Some(("rust-version.workspace", "true"))
    );
    assert_eq!(
        package_msrv(stated),
        Some(("rust-version", "\"1.95\"")),
        "the trailing comment is not part of the value"
    );
    assert_eq!(package_msrv(commented), None);
    assert_eq!(package_msrv(another_key), None);
    assert_eq!(
        package_msrv(root_only),
        None,
        "a member cannot borrow the root declaration"
    );
}

#[test]
fn a_floor_is_a_number_not_a_flag() {
    assert!(states_a_version("\"1.95\""));
    assert!(states_a_version("\"1.95.0\""));
    // The inheritance spelling, a channel name, a lone major and a fourth component are all not.
    assert!(!states_a_version("true"));
    assert!(!states_a_version("\"stable\""));
    assert!(!states_a_version("\"1\""));
    assert!(!states_a_version("\"1.95.0.1\""));
    assert!(!states_a_version("\"1.95\"# unquoted"));
    assert!(!states_a_version("1.95"));
}
