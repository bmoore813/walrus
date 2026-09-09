//! Guard for `proj-workspace-deps`: every version walrus pins is pinned once, at the workspace
//! root, and every member inherits it.
//!
//! Cargo has no opinion here. A member may write `serde = "1.0.150"` beside a root `serde = "1"`
//! and the build stays green: semver-compatible requirements unify, so the drift surfaces first as
//! a longer compile, and only later — once the two requirements stop overlapping — as a second
//! `serde` in `Cargo.lock` whose derive output no longer agrees with the first. Nothing reads a
//! manifest on the way there: no rustc lint does, and the Clippy lint that comes closest,
//! `cargo_common_metadata`, skips a workspace that publishes nothing. This test is what goes red
//! instead, across every dependency table of every member.
//!
//! The internal edges (`common`, `control`, `extractor`, `pg-to-arrow`) are covered by the same scan.
//! They have no version to drift, but they did have a path: declarations across five manifests, and
//! `common` alone was spelled two ways — `../common` from a `crates/*` member,
//! `../../crates/common` from `tests/e2e` — so no crate could move without finding all of them. One
//! root-relative pin per crate is what replaced that, and the third test keeps it that way.

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

/// The dependency tables a member declares. All three are read: a dev- or build-dependency drifts
/// exactly as a normal one does, and `[dev-dependencies]` is where `criterion` and `proptest` live.
const DEPENDENCY_TABLES: [&str; 3] = [
    "[dependencies]",
    "[dev-dependencies]",
    "[build-dependencies]",
];

/// The five internal edges, in the order the root table pins them. A new crate that another
/// member depends on has to be added here — which is the moment its path spelling is decided.
const INTERNAL_CRATES: [&str; 5] = [
    "common",
    "walrus-config",
    "control",
    "extractor",
    "pg-to-arrow",
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

/// The characters a bare TOML key is made of, plus the `.` that spells a dotted inheritance key.
const fn is_bare_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')
}

/// Every `key = value` assignment in a table body, comments and trailing comments stripped. A
/// wrapped `features = [` array continues with a quoted string or a closing bracket, and neither
/// is a bare key, so no `"runtime-tokio",` line is mistaken for a dependency of that name.
fn entries(body: &str) -> Vec<(&str, &str)> {
    body.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with('#') {
                return None;
            }

            let (key, value) = line.split_once('=')?;
            let key = key.trim();
            if key.is_empty() || !key.chars().all(is_bare_key_char) {
                return None;
            }

            Some((key, value.split('#').next()?.trim()))
        })
        .collect()
}

/// The package a dependency key names. `sqlx = { workspace = true }` spells it directly;
/// `sqlx.workspace = true` spells it as the head of a dotted key.
fn package(key: &str) -> &str {
    key.split('.').next().unwrap_or(key)
}

/// Whether an entry takes its pin from `[workspace.dependencies]`. Both spellings inherit; an
/// inline `version` — or, for an internal crate, an inline `path` — is what does not.
fn inherits(key: &str, value: &str) -> bool {
    match key.split_once('.') {
        Some((_, suffix)) => suffix == "workspace" && value == "true",
        None => value.starts_with('{') && value.contains("workspace = true"),
    }
}

/// A dependency table this scan does not read. `[dependencies.serde]` and
/// `[target.'cfg(unix)'.dependencies]` both hold pins that `table_body` above would never see, so
/// the arrival of either has to fail rather than pass silently.
fn is_unread_dependency_table(line: &str) -> bool {
    let line = line.trim();
    line.starts_with('[') && line.contains("dependencies") && !DEPENDENCY_TABLES.contains(&line)
}

/// Every pin in the root `[workspace.dependencies]` table.
fn workspace_pins() -> Vec<(&'static str, &'static str)> {
    table_body(WORKSPACE_MANIFEST, "[workspace.dependencies]").map_or_else(Vec::new, entries)
}

/// Every package a member inherits, across all three dependency tables of all seven members.
fn inherited_packages() -> Vec<&'static str> {
    let mut packages = Vec::new();

    for &(_, manifest) in MEMBER_MANIFESTS {
        for table in DEPENDENCY_TABLES {
            let Some(body) = table_body(manifest, table) else {
                continue;
            };
            packages.extend(entries(body).into_iter().map(|(key, _)| package(key)));
        }
    }

    packages
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
    let Some((slots, _)) = array.split_once(']') else {
        return Vec::new();
    };

    slots
        .split(',')
        .filter_map(|entry| {
            let (_, quoted) = entry.split_once('"')?;
            let (member, _) = quoted.split_once('"')?;
            Some(member)
        })
        .collect()
}

#[test]
fn every_member_dependency_inherits_the_workspace_pin() {
    let mut scanned = 0;

    for &(member, manifest) in MEMBER_MANIFESTS {
        for line in manifest.lines() {
            assert!(
                !is_unread_dependency_table(line),
                "{member}: `{}` is a dependency table this scan cannot read — a pin could hide there",
                line.trim()
            );
        }

        for table in DEPENDENCY_TABLES {
            let Some(body) = table_body(manifest, table) else {
                continue;
            };
            for (key, value) in entries(body) {
                assert!(
                    inherits(key, value),
                    "{member} {table}: `{key} = {value}` states its own pin — declare it once in [workspace.dependencies] and inherit it here"
                );
                scanned += 1;
            }
        }
    }

    assert!(
        scanned >= 60,
        "only {scanned} dependency entries read — the scan is broken"
    );
}

#[test]
fn the_workspace_table_carries_no_pin_nobody_inherits() {
    let pins = workspace_pins();
    assert!(
        pins.len() >= 25,
        "only {} pins read — the scan is broken",
        pins.len()
    );

    let inherited = inherited_packages();
    for (name, _) in pins {
        assert!(
            inherited.contains(&name),
            "[workspace.dependencies] pins `{name}`, which no member inherits — a pin ahead of its first use is a version walrus does not actually build"
        );
    }
}

#[test]
fn each_internal_crate_is_pinned_once_with_a_root_relative_path() {
    let internal: Vec<(&str, &str)> = workspace_pins()
        .into_iter()
        .filter(|&(_, value)| value.contains("path ="))
        .collect();

    let names: Vec<&str> = internal.iter().map(|&(name, _)| name).collect();
    assert_eq!(
        names, INTERNAL_CRATES,
        "the internal edges walrus pins at the root"
    );

    for (name, value) in internal {
        assert_eq!(
            value,
            format!("{{ path = \"crates/{name}\" }}"),
            "an internal pin is resolved against the workspace root, not against a member"
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
        "add each new member's manifest to MEMBER_MANIFESTS so its pins are checked too"
    );
}

#[test]
fn the_scan_reads_a_fabricated_manifest() {
    let manifest = concat!(
        "[dependencies]\n",
        "# serde = \"1.0.150\"\n",
        "serde = { workspace = true, features = [\n",
        "    \"derive\",\n",
        "] }\n",
        "sqlx.workspace = true # the dotted spelling inherits too\n",
        "tokio = \"1.32\"\n",
        "common = { path = \"../common\" }\n",
        "\n",
        "[dev-dependencies]\n",
        "criterion = { workspace = true }\n",
    );

    // The commented-out pin, the two continuation lines and the blank line are all dropped.
    let expected = [
        ("serde", "{ workspace = true, features = ["),
        ("sqlx.workspace", "true"),
        ("tokio", "\"1.32\""),
        ("common", "{ path = \"../common\" }"),
    ];
    let deps = entries(table_body(manifest, "[dependencies]").unwrap_or_default());
    assert_eq!(deps, expected);

    assert_eq!(package("sqlx.workspace"), "sqlx");
    assert_eq!(package("serde"), "serde");

    // Both spellings inherit; an inline version and an inline path do not.
    assert!(inherits("serde", "{ workspace = true, features = ["));
    assert!(inherits("sqlx.workspace", "true"));
    assert!(!inherits("tokio", "\"1.32\""));
    assert!(!inherits("common", "{ path = \"../common\" }"));

    let dev = entries(table_body(manifest, "[dev-dependencies]").unwrap_or_default());
    assert_eq!(dev, [("criterion", "{ workspace = true }")]);
    assert_eq!(table_body(manifest, "[build-dependencies]"), None);
}

#[test]
fn the_unread_table_check_rejects_fabricated_headers() {
    assert!(is_unread_dependency_table("[dependencies.serde]"));
    assert!(is_unread_dependency_table("[dev-dependencies.criterion]"));
    assert!(is_unread_dependency_table(
        "[target.'cfg(unix)'.dependencies]"
    ));

    for table in DEPENDENCY_TABLES {
        assert!(!is_unread_dependency_table(table), "{table} is read");
    }
    assert!(!is_unread_dependency_table("[features]"));
    assert!(!is_unread_dependency_table(
        "# dependencies are pinned at the root"
    ));
}
