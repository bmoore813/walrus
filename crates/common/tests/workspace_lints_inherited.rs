#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous manifest and \
              clippy-config reads are themselves repository-policy checks, not runtime I/O"
)]
//! Guard for `err-no-unwrap-prod`. `[workspace.lints]` is a default a member must
//! *request*: without `[lints] workspace = true` in its own manifest, a crate silently opts out of
//! `deny(warnings)`, `clippy::all`, `unwrap_used` and `expect_used` — and CI stays green. This test
//! is the thing that goes red instead.

use std::path::{Path, PathBuf};

/// Repo root, from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("CARGO_MANIFEST_DIR must be inside the walrus workspace")
}

/// Every path in the root manifest's `[workspace] members = [ … ]`, in declaration order.
/// Parsed, never hard-coded: crate number seven must be covered the day it is added.
fn workspace_members(root_manifest: &str) -> Vec<String> {
    let workspace = section(root_manifest, "[workspace]")
        .expect("root manifest must define a [workspace] section");
    let mut offset = 0;
    let mut assignment = None;
    for line in workspace.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if let Some(after_key) = trimmed.strip_prefix("members")
            && after_key.trim_start().starts_with('=')
        {
            assignment = Some(offset + (line.len() - trimmed.len()) + "members".len());
            break;
        }
        offset += line.len();
    }

    let assignment = &workspace[assignment.expect("[workspace] must define members")..];
    let array = &assignment[assignment
        .find('[')
        .expect("workspace members must be an array")
        + 1..];

    let mut members = Vec::new();
    let mut member = None::<String>;
    let mut escaped = false;
    let mut comment = false;
    let mut closed = false;
    for ch in array.chars() {
        if comment {
            if ch == '\n' {
                comment = false;
            }
            continue;
        }
        if let Some(value) = member.as_mut() {
            if escaped {
                value.push(ch);
                escaped = false;
            } else {
                match ch {
                    '\\' => escaped = true,
                    '"' => members.push(member.take().expect("member string is open")),
                    _ => value.push(ch),
                }
            }
            continue;
        }
        match ch {
            '"' => member = Some(String::new()),
            '#' => comment = true,
            ']' => {
                closed = true;
                break;
            }
            _ => {}
        }
    }
    assert!(closed, "workspace members array has no closing bracket");
    assert!(
        member.is_none(),
        "workspace members array has an open string"
    );
    members
}

/// The body of `[section]` in `manifest`, up to the next `[` at column 0. `None` if absent.
fn section<'a>(manifest: &'a str, header: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start = None;
    for line in manifest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == header {
            body_start = Some(offset + line.len());
            break;
        }
        offset += line.len();
    }

    let body = &manifest[body_start?..];
    let mut body_end = body.len();
    let mut offset = 0;
    for line in body.split_inclusive('\n') {
        if line.starts_with('[') {
            body_end = offset;
            break;
        }
        offset += line.len();
    }
    Some(&body[..body_end])
}

fn member_inherits_lints(manifest: &str) -> bool {
    section(manifest, "[lints]").is_some_and(|body| body.contains("workspace = true"))
}

/// Whether one manifest line assigns the `build` key.
fn is_build_script_key(line: &str) -> bool {
    let Some((key, _)) = line.split_once('=') else {
        return false;
    };
    key.trim() == "build"
}

/// Whether a member manifest names a build script. `[package] build = "…"` is the one key that puts
/// a program of walrus's own between Cargo and rustc, and so the one way a `cargo::rustc-check-cfg`
/// directive could register a cfg name or feature value the workspace lint table never saw.
fn declares_build_script(manifest: &str) -> bool {
    let package = section(manifest, "[package]").unwrap_or_default();
    package.lines().any(is_build_script_key)
}

/// How many entries in a lint-table body pin `lint` at `deny`, whatever their indentation.
fn clippy_deny_count(table: &str, lint: &str) -> usize {
    let entry = format!(r#"{lint} = "deny""#);
    table.lines().filter(|line| line.trim() == entry).count()
}

/// The same count for an entry that carries its rationale as a trailing `#` comment, which is how
/// the panic family is spelled. Stripping the comment first keeps the comparison exact: a `contains`
/// loose enough to see past a rationale would also match a lint named *inside* one.
fn clippy_deny_count_ignoring_rationale(table: &str, lint: &str) -> usize {
    let entry = format!(r#"{lint} = "deny""#);
    table
        .lines()
        .map(|line| line.split_once('#').map_or(line, |(code, _)| code))
        .filter(|code| code.trim() == entry)
        .count()
}

/// How many entries pin the lint *group* `group` at `deny` with `priority = -1` — the only spelling
/// a group may take in a table whose named lints sit at the default priority.
fn clippy_group_deny_count(table: &str, group: &str) -> usize {
    let entry = format!(r#"{group} = {{ level = "deny", priority = -1 }}"#);
    table.lines().filter(|line| line.trim() == entry).count()
}

/// How many entries in a lint-table body pin `lint` at `allow` — the deliberate hole in a denied
/// group, which has to be spelled out by name rather than taken by dropping the group itself.
fn clippy_allow_count(table: &str, lint: &str) -> usize {
    let entry = format!(r#"{lint} = "allow""#);
    table.lines().filter(|line| line.trim() == entry).count()
}

/// The integer `key` is set to in a `clippy.toml` body, or `None` when the key is not stated at all
/// — the difference between a threshold walrus chose and one it left at clippy's default.
fn clippy_threshold(cfg: &str, key: &str) -> Option<u64> {
    for line in cfg.lines() {
        if let Some(rest) = line.trim().strip_prefix(key)
            && let Some(value) = rest.trim_start().strip_prefix('=')
        {
            return value.trim().parse().ok();
        }
    }
    None
}

/// The body of a `clippy.toml` list key — everything between its brackets — or `None` when the key
/// is absent at all. A lint that reads its subjects from such a list is only as strong as the list
/// is populated, whatever level the manifest pins it at, so an emptied array (`Some("")`) and an
/// absent key are two different failures and this tells them apart.
fn clippy_list(cfg: &str, key: &str) -> Option<String> {
    let (_, rest) = cfg.split_once(&format!("{key} = ["))?;
    let (body, _) = rest.split_once(']')?;
    Some(body.to_string())
}

#[test]
fn every_member_opts_into_the_workspace_lint_table() {
    let wrapped_members = r#"[workspace]
members = [
    "crates/common",
    # Non-crates members must remain visible to the guard.
    "tests/e2e",
]

[package]
name = "synthetic"
"#;
    assert_eq!(
        workspace_members(wrapped_members),
        ["crates/common", "tests/e2e"]
    );

    let dependency_only = "[dependencies]\ncommon = { workspace = true }\n";
    assert!(!member_inherits_lints(dependency_only));

    let root = repo_root();
    assert!(Path::new(&root).is_absolute());
    let root_manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let members = workspace_members(&root_manifest);
    assert!(
        members.len() >= 6,
        "member list parsed as {members:?} — the parser is broken"
    );

    let mut missing: Vec<String> = Vec::new();
    for member in &members {
        let path = root.join(member).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("member {member} has no manifest at {}: {e}", path.display())
        });
        if !member_inherits_lints(&manifest) {
            missing.push(member.clone());
        }
    }
    assert!(
        missing.is_empty(),
        "these workspace members do not inherit [workspace.lints] — add\n\n    [lints]\n    workspace = true\n\nto: {missing:?}"
    );
}

#[test]
fn the_workspace_lint_table_still_denies_unwrap_and_expect() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert!(rust.contains(r#"warnings = "deny""#));
    assert!(clippy.contains(r#"unwrap_used = "deny""#));
    assert!(clippy.contains(r#"expect_used = "deny""#));
}

/// `clippy::correctness` is the group for code that is outright wrong — `unit_cmp`, `never_loop`,
/// `unused_io_amount`, `iter_next_loop`. Its named entry has no site to go red: every one of those
/// lints also arrives through the `clippy::all` deny above, so dropping it changes no diagnostic
/// today. What it changes is what a regrouping of `clippy::all` — or a member downgrading that one
/// entry — can quietly take away, which is why this test exists at all.
///
/// The `priority = -1` half is not cosmetic. A lint group at or above a named lint's priority
/// overrides that lint, so the plain `correctness = "deny"` spelling would go red under
/// `clippy::lint_groups_priority` — itself a correctness lint — against every named entry below it,
/// all of which sit at the default 0.
#[test]
fn the_workspace_lint_table_still_denies_the_correctness_group() {
    let synthetic = "correctness = \"deny\"\n";
    assert_eq!(clippy_group_deny_count(synthetic, "correctness"), 0);
    assert_eq!(clippy_deny_count(synthetic, "correctness"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_group_deny_count(clippy, "correctness"),
        1,
        "workspace policy must pin the correctness group at deny with priority = -1"
    );
    assert_eq!(
        clippy_deny_count(clippy, "correctness"),
        0,
        "a priority-less `correctness = \"deny\"` collides with the named lints below it"
    );
    assert_eq!(
        clippy_group_deny_count(clippy, "all"),
        1,
        "correctness is pinned alongside clippy::all, never instead of it"
    );
}

/// `clippy::suspicious` is the group next door to correctness: code that compiles and is not
/// provably wrong, but is almost always a mistake — `suspicious_else_formatting`, `suspicious_map`,
/// `suspicious_splitn`, `suspicious_arithmetic_impl`. Its named entry has no site to go red for the
/// same reason correctness's does not: `clippy::all` carries the group, so dropping it changes no
/// diagnostic today — only what a regrouping of `clippy::all` can quietly take away.
///
/// Unlike correctness, this group's reach here is already visible: three of the lints it carries
/// today are pinned by name below, each argued on its own (`await_holding_lock`,
/// `await_holding_refcell_ref`, `await_holding_invalid_type`). Those named entries sit at the
/// default priority, so the group must sit below them — the priority-less spelling would override
/// every one of them and go red under `clippy::lint_groups_priority`.
#[test]
fn the_workspace_lint_table_still_denies_the_suspicious_group() {
    let synthetic = "suspicious = \"deny\"\n";
    assert_eq!(clippy_group_deny_count(synthetic, "suspicious"), 0);
    assert_eq!(clippy_deny_count(synthetic, "suspicious"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_group_deny_count(clippy, "suspicious"),
        1,
        "workspace policy must pin the suspicious group at deny with priority = -1"
    );
    assert_eq!(
        clippy_deny_count(clippy, "suspicious"),
        0,
        "a priority-less `suspicious = \"deny\"` collides with the named lints below it"
    );

    let named_members = [
        "await_holding_lock",
        "await_holding_refcell_ref",
        "await_holding_invalid_type",
    ];
    for lint in named_members {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "{lint} is a suspicious-group member; it must keep the default priority"
        );
    }
}

/// The third of those three names pins a level and nothing else. `await_holding_invalid_type` ships
/// with no type list of its own, so the manifest entry is inert until `clippy.toml`'s
/// `await-holding-invalid-types` names something: emptying that array leaves the manifest untouched
/// and every assertion above green while the deny loses every site it could fire on — the same
/// hollowing-out a raised threshold performs on the complexity and perf groups further down.
///
/// What the array must keep naming is `tokio::sync::watch::Ref`. The lint's two siblings know
/// `std::sync` and `parking_lot` guards and stop there, but a `Ref` IS a read guard on the watch
/// channel's inner lock: retained across an `.await` it blocks every `Sender::send`, which in the
/// transformer is the single epoch poller feeding every apply worker. The apply loop's two borrow sites
/// copy the value out inside the borrowing statement (`*ctx.epoch_rx.borrow()`), and this array is
/// what keeps that a rule rather than a habit.
///
/// Tokio's own `Mutex`/`RwLock` guards stay off the list deliberately. No production site holds
/// one, and the source-database serialization locks in the compose-gated integration tests hold a
/// `tokio::sync::MutexGuard` across the whole test body on purpose — the lint reaches test targets
/// too, so naming those types would fail those tests for the behavior they exist to have.
#[test]
fn the_clippy_config_still_arms_the_await_holding_type_list() {
    let synthetic = concat!(
        "await-holding-invalid-types = [\n",
        "  { path = \"a::B\" },\n",
        "]\n",
        "disallowed-methods = []\n"
    );
    let listed = clippy_list(synthetic, "await-holding-invalid-types").unwrap();
    assert!(listed.contains(r#"path = "a::B""#));
    assert_eq!(clippy_list(synthetic, "disallowed-types"), None);
    assert_eq!(
        clippy_list(synthetic, "disallowed-methods").as_deref(),
        Some(""),
        "an emptied array is the failure this guard exists for; it must not read as absent"
    );

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_deny_count(clippy, "await_holding_invalid_type"),
        1,
        "the array below is enforcement only while the manifest still denies the lint"
    );

    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    let types = clippy_list(&cfg, "await-holding-invalid-types")
        .expect("clippy.toml must state await-holding-invalid-types");
    assert!(
        types.contains(r#"path = "tokio::sync::watch::Ref""#),
        "await_holding_invalid_type has no type to fire on unless the list names the watch read \
         guard; found: {types}"
    );
}

/// `clippy::style` is the third rung: code that is correct and idiomatically misspelled —
/// `len_zero`, `redundant_field_names`, `needless_return`, `single_match`, `question_mark`. Like
/// its two siblings above, the named entry costs no diagnostic today, because `clippy::all` carries
/// the group; unlike them, its reach is already visible outside this table. Three of its lints are
/// pinned by name below on their own merits (`from_over_into`, `ptr_arg`, `missing_safety_doc`) —
/// all at the default priority, so the group must sit under them or `clippy::lint_groups_priority`
/// reports every one — and two sites hold a scoped allow with a reason, which is what a group with
/// live sites looks like.
#[test]
fn the_workspace_lint_table_still_denies_the_style_group() {
    let synthetic = "style = \"deny\"\n";
    assert_eq!(clippy_group_deny_count(synthetic, "style"), 0);
    assert_eq!(clippy_deny_count(synthetic, "style"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_group_deny_count(clippy, "style"),
        1,
        "workspace policy must pin the style group at deny with priority = -1"
    );
    assert_eq!(
        clippy_deny_count(clippy, "style"),
        0,
        "a priority-less `style = \"deny\"` collides with the named lints below it"
    );

    let named_members = ["from_over_into", "ptr_arg", "missing_safety_doc"];
    for lint in named_members {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "{lint} is a style-group member; it must keep the default priority"
        );
    }
}

/// The two style-group suppressions in the tree. Both are deliberate — a `new` that hands back the
/// shared handle rather than `Self`, and a test that reaches for the borrowed operator impl on
/// purpose — and `allow_attributes_without_reason` already forces each to carry a `reason`. What
/// this asserts is the *count*: the group is enforced everywhere else, so a third suppression is a
/// policy decision that has to be made here rather than in a source file.
#[test]
fn the_style_group_carries_exactly_two_scoped_allows() {
    const HEALTH: &str = include_str!("../../extractor/src/health.rs");
    const LSN_TEST: &str = include_str!("../src/lsn_test.rs");

    assert_eq!(HEALTH.matches("clippy::new_ret_no_self").count(), 1);
    assert!(HEALTH.contains("reason = \"intentionally returns the shared handle"));
    assert_eq!(LSN_TEST.matches("clippy::op_ref").count(), 1);
    assert!(LSN_TEST.contains("reason = \"exercise the explicit borrowed Sub impl\""));
}

/// `clippy::complexity` is the fourth group pinned by name, and the first that is not another rung
/// on the correctness → suspicious → style ladder: its lints — `needless_match`, `useless_format`,
/// `unnecessary_cast`, `clone_on_copy`, `filter_next` — flag code that is correct and idiomatic
/// but takes the long way round. Like all three siblings, the entry costs no diagnostic today,
/// because `clippy::all` carries the group.
///
/// The priority half is what differs. Suspicious and style each have members named below at the
/// default priority, so the group demonstrably has to sit under them; complexity has none — and
/// still needs `priority = -1`, because `clippy::lint_groups_priority` compares a group against
/// every named lint in the table rather than only the ones it carries. That is the half a later
/// edit is likeliest to talk itself out of, so assert it directly.
#[test]
fn the_workspace_lint_table_still_denies_the_complexity_group() {
    let synthetic = "complexity = \"deny\"\n";
    assert_eq!(clippy_group_deny_count(synthetic, "complexity"), 0);
    assert_eq!(clippy_deny_count(synthetic, "complexity"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_group_deny_count(clippy, "complexity"),
        1,
        "workspace policy must pin the complexity group at deny with priority = -1"
    );
    assert_eq!(
        clippy_deny_count(clippy, "complexity"),
        0,
        "a priority-less `complexity = \"deny\"` collides with the named lints below it"
    );
}

/// The one complexity-group suppression in the tree. `too_many_arguments` fires above seven
/// parameters, and the fixture it sits on takes one for every raw-row field it seeds, on purpose:
/// the parameter struct that would silence the lint has to be built and destructured at each of its
/// call sites, which is the boilerplate the fixture exists to remove.
/// `allow_attributes_without_reason` already forces the `reason`; what this pins is the count, so
/// that a second complexity suppression is a policy decision taken here rather than in a source
/// file.
#[test]
fn the_complexity_group_carries_exactly_one_scoped_allow() {
    const TRANSFORM: &str = include_str!("../../transformer/tests/transform.rs");

    assert_eq!(TRANSFORM.matches("clippy::too_many_arguments").count(), 1);
    assert!(TRANSFORM.contains("reason = \"test fixture seeds every raw-row field explicitly\""));
}

/// The complexity group's two configurable members. `too_many_arguments` and `type_complexity` read
/// their limits from `clippy.toml` rather than from the lint table, so raising either threshold
/// silences the lint with the group's deny still in place and nothing in the manifest moved. walrus
/// states neither key; the one threshold it does state, `enum-variant-size-threshold`, *lowers* a
/// limit rather than raising one, and belongs to a perf lint besides.
#[test]
fn the_clippy_config_does_not_raise_a_complexity_threshold() {
    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    assert!(
        cfg.contains("enum-variant-size-threshold"),
        "the clippy.toml scan must not be vacuous"
    );
    for key in ["too-many-arguments-threshold", "type-complexity-threshold"] {
        assert!(
            !cfg.lines().any(|line| line.trim_start().starts_with(key)),
            "clippy.toml sets {key}, which can only weaken the denied complexity group"
        );
    }
}

/// `clippy::perf` is the fifth and last group `clippy::all` carries. Where complexity flags code
/// that takes the long way round, perf flags code that takes a fine way round and still asks the
/// machine for work nobody needs — `single_char_pattern`, `unnecessary_to_owned`, `manual_memcpy`,
/// `slow_vector_initialization`, `large_enum_variant`, `result_large_err`. The named entry costs no
/// diagnostic today, exactly as its four siblings do; what it buys is that a regrouping of
/// `clippy::all` cannot take the group away in silence.
///
/// `priority = -1` is required for complexity's reason rather than style's: this group has no
/// member named below it either — the tree's large-value gates are pedantic or nursery lints — and
/// `clippy::lint_groups_priority` measures a group against every named lint in the table, not only
/// the ones it carries.
#[test]
fn the_workspace_lint_table_still_denies_the_perf_group() {
    let synthetic = "perf = \"deny\"\n";
    assert_eq!(clippy_group_deny_count(synthetic, "perf"), 0);
    assert_eq!(clippy_deny_count(synthetic, "perf"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_group_deny_count(clippy, "perf"),
        1,
        "workspace policy must pin the perf group at deny with priority = -1"
    );
    assert_eq!(
        clippy_deny_count(clippy, "perf"),
        0,
        "a priority-less `perf = \"deny\"` collides with the named lints below it"
    );
}

/// The perf group's scoped suppression for the config crate's hermetic `figment::Jail` helper.
#[test]
fn the_perf_group_carries_one_scoped_allow() {
    const JAIL_HELPERS: [&str; 1] = [include_str!("../../walrus-config/tests/loading.rs")];

    for helper in JAIL_HELPERS {
        assert_eq!(helper.matches("clippy::result_large_err").count(), 1);
        assert!(helper.contains("reason = \"figment Jail requires Result<(), figment::Error>"));
    }
}

/// The perf group's four configurable members. `large_enum_variant`, `result_large_err`,
/// `useless_vec` and `large_const_arrays` read their limits from `clippy.toml` rather than from the
/// lint table, so raising one silences the lint with the group's deny still in place and nothing in
/// the manifest moved. walrus states exactly one of the four keys and it *lowers* a limit —
/// `enum-variant-size-threshold = 64`, under a third of clippy's 200-byte default — so the
/// assertion is on the value, not on the key's presence. The other three stay unstated, on the
/// strength of the recorded decision that a threshold move is a separate, noisier call.
#[test]
fn the_clippy_config_does_not_raise_a_perf_threshold() {
    let synthetic = "enum-variant-size-threshold = 64\nlarge-error-threshold=256\n";
    assert_eq!(
        clippy_threshold(synthetic, "enum-variant-size-threshold"),
        Some(64)
    );
    assert_eq!(
        clippy_threshold(synthetic, "large-error-threshold"),
        Some(256)
    );
    assert_eq!(clippy_threshold(synthetic, "too-large-for-stack"), None);

    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    let enum_variant = clippy_threshold(&cfg, "enum-variant-size-threshold")
        .expect("clippy.toml must state enum-variant-size-threshold");
    assert!(
        enum_variant <= 200,
        "enum-variant-size-threshold = {enum_variant} sits above clippy's default, which can only \
         weaken the denied perf group"
    );
    for key in [
        "large-error-threshold",
        "too-large-for-stack",
        "array-size-threshold",
    ] {
        assert_eq!(
            clippy_threshold(&cfg, key),
            None,
            "clippy.toml sets {key}; a perf threshold belongs in this test, not only in the config"
        );
    }
}

/// `clippy::cargo` is the sixth group pinned by name and the only one of the six that
/// `clippy::all` does not carry, so unlike its five predecessors this entry is the *whole*
/// enforcement: drop it and five manifest lints fall back to allow-by-default with no source file
/// and no other manifest line to notice. Three of them have live reach in this tree the moment a
/// wildcard requirement or a `no-`/`with-`-shaped feature name arrives. The other two are owned
/// elsewhere, which is what the allow below the group records rather than hides:
/// `cargo_common_metadata` skips every package that declares itself unpublishable — all seven do, and
/// `publish_policy.rs` guards that key — while `multiple_crate_versions` belongs to `deny.toml`'s
/// `[bans]`, asserted separately below.
///
/// That allow is also what forces `priority = -1` here: it sits at the default 0 like every named
/// entry in the table, so a priority-less group would both override it and go red under
/// `clippy::lint_groups_priority`.
#[test]
fn the_workspace_lint_table_still_denies_the_cargo_group() {
    let synthetic = "cargo = \"deny\"\n  multiple_crate_versions = \"allow\"\n";
    assert_eq!(clippy_group_deny_count(synthetic, "cargo"), 0);
    assert_eq!(clippy_deny_count(synthetic, "cargo"), 1);
    assert_eq!(clippy_allow_count(synthetic, "multiple_crate_versions"), 1);
    assert_eq!(clippy_allow_count(synthetic, "cargo"), 0);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_group_deny_count(clippy, "cargo"),
        1,
        "workspace policy must pin the cargo group at deny with priority = -1"
    );
    assert_eq!(
        clippy_deny_count(clippy, "cargo"),
        0,
        "a priority-less `cargo = \"deny\"` collides with the named lints below it"
    );
    assert_eq!(
        clippy_allow_count(clippy, "cargo"),
        0,
        "the cargo group is denied, never allowed wholesale — its two exceptions are named"
    );
    assert_eq!(
        clippy_allow_count(clippy, "multiple_crate_versions"),
        1,
        "the one cargo-group member walrus does not enforce must be excused by name, not by \
         weakening the group"
    );
    assert_eq!(
        clippy_allow_count(clippy, "cargo_common_metadata"),
        0,
        "cargo_common_metadata is inert while every member sets publish = false; an allow would \
         also cover the member that stops"
    );
}

/// The receiving end of that allow: excusing `multiple_crate_versions` is a hand-off, not a
/// dismissal. `cargo deny check` asks the same question in a compile-free CI job, over the whole
/// lock file rather than one package's dependency graph, and its `skip` list takes a reason per
/// crate — so what this asserts is that the key is still there to receive it. The level is
/// deliberately not asserted: `multiple-versions` stays at "warn" until the tree is
/// de-duplicated and anticipates the tightening to "deny", which must not fail this guard.
#[test]
fn duplicate_crate_versions_are_still_owned_by_the_supply_chain_gate() {
    let synthetic = "[bans]\nskip = []\n";
    assert!(section(synthetic, "[bans]").is_some_and(|bans| !bans.contains("multiple-versions")));

    let deny = std::fs::read_to_string(repo_root().join("deny.toml")).unwrap();
    let bans = section(&deny, "[bans]").expect("deny.toml must define [bans]");
    assert!(
        bans.lines()
            .any(|line| line.trim_start().starts_with("multiple-versions")),
        "deny.toml [bans] must keep stating multiple-versions — the clippy allow above hands the \
         duplicate-crate question to it"
    );
}

/// `clippy::pedantic` is the one group within this table's reach that is deliberately *not* pinned
/// by name, and that absence is itself the policy. Its members are style opinions rather than
/// degrees of wrongness, and enough of them are wrong here that the group spelling plus a column of
/// re-allows would say less than a list of names does: `missing_panics_doc` cannot see five of the
/// eight production panic sites, `module_name_repetitions` describes `error::TransformerError` and every
/// sibling that names its own module, `too_many_lines` prices a decoder arm like a getter.
///
/// So the group entry must stay absent while the named members stay present — two halves of one
/// decision, which is why they are asserted together. The list below is a slice of the pick rather
/// than all of it: it covers every member whose only reason for being denied is the pick itself,
/// plus the two the usual pedantic advice turns *off* — `must_use_candidate` and
/// `missing_errors_doc`, denied here on their own recorded merits. The cast and float lints are
/// pedantic too, and stay out because they are asserted by their dedicated policy checks.
#[test]
fn the_workspace_lint_table_selects_pedantic_lints_by_name_never_as_a_group() {
    let synthetic = "pedantic = { level = \"deny\", priority = -1 }\n";
    assert_eq!(clippy_group_deny_count(synthetic, "pedantic"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");

    assert_eq!(clippy_group_deny_count(clippy, "pedantic"), 0);
    assert_eq!(clippy_deny_count(clippy, "pedantic"), 0);
    assert_eq!(clippy_allow_count(clippy, "pedantic"), 0);
    for line in clippy.lines() {
        assert!(
            !line.trim_start().starts_with("pedantic"),
            "the pedantic group is enabled one lint at a time; a group entry replaces that \
             decision with a column of re-allows"
        );
    }

    for lint in [
        "linkedlist",
        "redundant_closure_for_method_calls",
        "match_wildcard_for_single_variants",
        "manual_let_else",
        "match_bool",
        "must_use_candidate",
        "return_self_not_must_use",
        "ref_option",
        "assigning_clones",
        "trivially_copy_pass_by_ref",
        "non_std_lazy_statics",
        "inline_always",
        "unnecessary_safety_doc",
        "missing_errors_doc",
        "implicit_clone",
        "unnested_or_patterns",
        "used_underscore_binding",
        "string_add_assign",
        "semicolon_if_nothing_returned",
        "wildcard_imports",
    ] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "{lint} is part of the selective pedantic pick; it must stay pinned by name"
        );
    }
}

/// `clippy::nursery` is the second group this table enables one lint at a time, and its absent group
/// entry is the same decision the pedantic one above records, reached for one extra reason. Like
/// `pedantic`, the group is not carried by `clippy::all`, so the entry would be a behaviour change
/// rather than a pin. Unlike `pedantic`, its members are lints clippy itself calls unfinished:
/// heuristics still being tightened, false positives still being fixed, names that graduate out into
/// `style` or `perf`. A column of re-allows under `nursery = { level = "deny", priority = -1 }`
/// would therefore have to be re-argued on every toolchain bump — churn bought with a group entry,
/// which is the failure this test exists to keep out.
///
/// So the two halves are asserted together, exactly as for pedantic: the group entry must stay
/// absent while the named members stay present. Every name below is denied on its own recorded
/// merit — lock scope, the mutable-borrow gate, `const fn` reachability,
/// Send-ness of a spawned future, clone-vs-move, an elidable lifetime and singular
/// generic bounds — and `or_fun_call` joins them as the eager-fallback gate the pick was missing.
/// Its reach is zero today, for the reason the manifest gives, so nothing in the source goes red if
/// that entry is dropped and this is the thing that does.
#[test]
fn the_workspace_lint_table_selects_nursery_lints_by_name_never_as_a_group() {
    let synthetic = "nursery = { level = \"deny\", priority = -1 }\n";
    assert_eq!(clippy_group_deny_count(synthetic, "nursery"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");

    assert_eq!(clippy_group_deny_count(clippy, "nursery"), 0);
    assert_eq!(clippy_deny_count(clippy, "nursery"), 0);
    assert_eq!(clippy_allow_count(clippy, "nursery"), 0);
    for line in clippy.lines() {
        assert!(
            !line.trim_start().starts_with("nursery"),
            "the nursery group is enabled one lint at a time; a group entry replaces that decision \
             with a column of re-allows that every toolchain bump reopens"
        );
    }

    for lint in [
        "significant_drop_in_scrutinee",
        "significant_drop_tightening",
        "needless_pass_by_ref_mut",
        "missing_const_for_fn",
        "future_not_send",
        "redundant_clone",
        "elidable_lifetime_names",
        "type_repetition_in_bounds",
        "trait_duplication_in_bounds",
        "or_fun_call",
    ] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "{lint} is part of the selective nursery pick; it must stay pinned by name"
        );
    }
}

/// The one member of that recommended starter set which is pedantic rather than nursery, so the
/// pick above cannot carry it and the pedantic guard further up does not name it either: it is
/// denied on the nursery review's finding rather than on that pick's own. An `else` block whose
/// sibling `if` always returns, breaks, continues or panics is a level of indentation with nothing
/// behind it — the early exit reads as one only once the tail is unindented past it. Zero sites,
/// like `dbg_macro` and `format_push_string`: every `else` in the tree pairs with a then-branch that
/// produces a value, so no source file goes red if the entry is dropped and this is what does.
#[test]
fn the_workspace_lint_table_still_denies_an_else_after_a_diverging_if() {
    let synthetic = "redundant_else = \"warn\"\n  or_fun_call = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "redundant_else"), 0);
    assert_eq!(clippy_deny_count(synthetic, "or_fun_call"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_deny_count(clippy, "redundant_else"),
        1,
        "workspace policy must pin redundant_else = \"deny\" exactly once"
    );
}

/// The wildcard-import deny rests on an exemption clippy grants rather than on anything the lint
/// table states: `use super::*` inside a `#[cfg(test)]` module and `use x::prelude::*` are skipped
/// while `warn-on-all-wildcard-imports` keeps its default of `false`. 64 `*_test.rs` siblings here
/// take the first spelling and `lsn_test.rs` takes the second, so stating that one key as `true`
/// would put all 65 in scope of a deny from `clippy.toml`, with the manifest untouched.
///
/// That leaves exactly one site, and it is the case a glob is for: a module whose whole body
/// forwards another crate's namespace under this crate's path.
#[test]
fn the_wildcard_import_deny_rests_on_clippys_own_carve_outs() {
    const OIDS: &str = include_str!("../../pg-to-arrow/src/oids.rs");

    assert_eq!(OIDS.matches("clippy::wildcard_imports").count(), 1);
    assert!(OIDS.contains("reason = \"forwarding shim"));

    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    for line in cfg.lines() {
        assert!(
            !line
                .trim_start()
                .starts_with("warn-on-all-wildcard-imports"),
            "clippy.toml sets warn-on-all-wildcard-imports; that widens the deny to every test \
             module's `use super::*`"
        );
    }
}

#[test]
fn the_workspace_lint_table_denies_redundant_method_closures() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy
            .lines()
            .filter(|line| line.trim() == r#"redundant_closure_for_method_calls = "deny""#)
            .count(),
        1,
        "workspace policy must deny redundant method-forwarding closures exactly once"
    );
}

/// `extern` blocks are the one FFI surface walrus does not own: the native engine arrives through
/// `duckdb`'s pinned `libduckdb-sys`, so first-party sources declare none. Edition 2024 already
/// makes the pre-2024 `extern "C" { … }` spelling a hard error — but only for a member on that
/// edition. A crate added with `edition = "2021"` gets nothing but this named deny, which is
/// allow-by-default and so out of reach of `warnings = "deny"`.
#[test]
fn the_workspace_lint_table_still_denies_pre_2024_extern_blocks() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    assert_eq!(
        rust.lines()
            .filter(|line| line.trim() == r#"missing_unsafe_on_extern = "deny""#)
            .count(),
        1,
        "workspace policy must reject un-`unsafe` extern blocks exactly once"
    );
}

/// walrus exports no symbols: zero `#[no_mangle]` / `#[export_name]` / `#[link_section]`
/// attributes and no `crate-type`, so both binaries are ordinary executables with no plugin ABI.
/// Two items claiming one exported symbol is linker-level UB with no diagnostic, so the bare
/// spelling must stay unreachable. Edition 2024 makes it a hard error — but again only for a member
/// on that edition, and the lint that covers an earlier-edition one is allow-by-default, hence
/// beyond `warnings = "deny"`.
#[test]
fn the_workspace_lint_table_still_denies_bare_export_attributes() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    assert_eq!(
        rust.lines()
            .filter(|line| line.trim() == r#"unsafe_attr_outside_unsafe = "deny""#)
            .count(),
        1,
        "workspace policy must reject bare `#[no_mangle]`-family attributes exactly once"
    );
}

/// The two entries above are allow-by-default lints, so `warnings = "deny"` never reaches them and
/// the named deny *is* the enforcement. This one is the other shape: `unexpected_cfgs` has been
/// warn-by-default since Rust 1.80, so the group already escalates it and dropping this line
/// changes no diagnostic today — which is precisely why nothing in the source would go red. What
/// it enforces is that an unknown cfg stays a build failure over both halves of the predicate: the
/// cfg *name*, and the *value* of `feature = "…"`, which Cargo checks against the declaring
/// package's own feature list. Without it a gate misspelled `feature = "sqlx_"` is not an error
/// but a constant false, and its block leaves the build in silence.
///
/// The second half of the guard is the `check-cfg` array this entry deliberately does not carry.
/// walrus declares no custom cfg — every predicate in the tree is built-in or one of the four
/// declared features — so there is nothing to list. But a build script registers cfgs from outside
/// the manifest entirely, and one `cargo::rustc-check-cfg=cfg(feature, values(any()))` line
/// blanket-accepts every feature spelling with this deny still in place and no lint table moved. No
/// member has a build script; if one ever arrives, its directives are what this guard must read
/// instead of its absence.
#[test]
fn the_workspace_lint_table_still_denies_unexpected_cfgs() {
    assert!(declares_build_script("[package]\nbuild = \"gen.rs\"\n"));
    assert!(!declares_build_script("[package]\nname = \"common\"\n"));
    assert!(!declares_build_script("[build-dependencies]\ncc = \"1\"\n"));

    let root = repo_root();
    let root_manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    assert_eq!(
        rust.lines()
            .filter(|line| line.trim() == r#"unexpected_cfgs = "deny""#)
            .count(),
        1,
        "workspace policy must reject unknown cfg names and feature values exactly once"
    );

    let members = workspace_members(&root_manifest);
    assert!(
        members.len() >= 6,
        "member list parsed as {members:?} — the parser is broken"
    );
    for member in &members {
        let dir = root.join(member);
        assert!(
            !dir.join("build.rs").exists(),
            "{member} has a build.rs, which can register cfgs the lint table never declared"
        );
        let manifest = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
        assert!(
            !declares_build_script(&manifest),
            "{member} names a build script in [package]; see the build.rs note above"
        );
    }
}

/// Both directions of the `# Safety` section. The workspace forbid means walrus authors no
/// `unsafe fn`, so the caller-obligation lint is belt-and-braces like its siblings above: it is
/// what still demands the section if a member ever stops inheriting that forbid, and it reaches
/// the tree today only through the `clippy::all` group. Its inverse is the half with live reach —
/// while the forbid holds, every `# Safety` heading this tree could grow sits on a *safe* item,
/// promising callers an obligation the compiler never imposes.
#[test]
fn the_workspace_lint_table_still_polices_safety_doc_sections() {
    let synthetic = "missing_safety_doc = \"allow\"\n  unnecessary_safety_doc = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "missing_safety_doc"), 0);
    assert_eq!(clippy_deny_count(synthetic, "unnecessary_safety_doc"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    for lint in ["missing_safety_doc", "unnecessary_safety_doc"] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "workspace policy must pin {lint} = \"deny\" exactly once"
        );
    }
}

/// The block-side half of that family, and the half no other guard covers: a `// SAFETY:` comment
/// on every `unsafe { … }`, and one auditable operation per block so a single comment never has to
/// justify two. Both are `clippy::restriction` lints, outside the `clippy::all` group denied above,
/// so nothing but these named entries reaches them — and while `unsafe_code = "forbid"` holds they
/// have no site to go red, which is exactly why dropping one is invisible in the source. They are
/// what documents the first unsafe block this tree ever grows, in the member that stopped
/// inheriting the forbid; `scripts/check-unsafe-invariants.sh` guards the forbid itself.
#[test]
fn the_workspace_lint_table_still_demands_documented_unsafe_blocks() {
    let synthetic = "undocumented_unsafe_blocks = \"warn\"\n\
                     multiple_unsafe_ops_per_block = \"deny\"\n";
    assert_eq!(
        clippy_deny_count(synthetic, "undocumented_unsafe_blocks"),
        0
    );
    assert_eq!(
        clippy_deny_count(synthetic, "multiple_unsafe_ops_per_block"),
        1
    );

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    for lint in [
        "undocumented_unsafe_blocks",
        "multiple_unsafe_ops_per_block",
    ] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "workspace policy must pin {lint} = \"deny\" exactly once"
        );
    }
}

/// Diagnostics are `tracing` events, never raw stream writes. That was prose in
/// `crates/common/src/telemetry.rs` before these two lints backed it: `println!` and `eprintln!`
/// compile cleanly under every other entry in the table, so nothing went red when one
/// appeared. Both are `clippy::restriction` lints, outside the `clippy::all` group denied above,
/// so that group entry does not reach them either — only these named denies do.
#[test]
fn the_workspace_lint_table_still_denies_raw_stream_prints() {
    let synthetic = "print_stdout = \"warn\"\n  print_stderr = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "print_stdout"), 0);
    assert_eq!(clippy_deny_count(synthetic, "print_stderr"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    for lint in ["print_stdout", "print_stderr"] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "workspace policy must pin {lint} = \"deny\" exactly once"
        );
    }
}

/// The third member of that family, and the one the two denies above cannot reach. `dbg!` expands
/// to `eprintln!` inside std, but clippy attributes a macro to its outermost call in first-party
/// code — so `print_stderr` is handed `dbg!` and stays quiet, which is why clippy ships a separate
/// lint for it at all. It is `clippy::restriction` like that pair, outside the `clippy::all` group
/// denied above, so nothing but the named entry reaches it; unlike that pair it carries no
/// carve-out, in production or in tests. Zero sites means no source file goes red if the entry is
/// dropped, so this test is the thing that does — along with the one key that could hollow it out
/// from the other side. `allow-dbg-in-tests` would exempt every `#[cfg(test)]` module and every
/// integration target with the deny still in place and nothing in the manifest moved, exactly as a
/// raised perf threshold would; walrus leaves it at clippy's default of false by not stating it.
#[test]
fn the_workspace_lint_table_still_denies_the_debug_macro() {
    let synthetic = "dbg_macro = \"warn\"\n  print_stderr = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "dbg_macro"), 0);
    assert_eq!(clippy_deny_count(synthetic, "print_stderr"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_deny_count(clippy, "dbg_macro"),
        1,
        "workspace policy must pin dbg_macro = \"deny\" exactly once"
    );

    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    assert!(
        cfg.contains("allow-unwrap-in-tests"),
        "the clippy.toml scan must not be vacuous"
    );
    let key = "allow-dbg-in-tests";
    assert!(
        !cfg.lines().any(|line| line.trim_start().starts_with(key)),
        "clippy.toml sets {key}, which exempts every test target from the deny above"
    );
}

/// Multi-file module layout is a convention with nothing behind it: `foo.rs` + `foo/` and
/// `foo/mod.rs` both compile, so a second style can arrive one directory at a time. walrus has
/// exactly one such directory (`crates/extractor/src/pgoutput/`) and it uses `mod.rs`, which means
/// this lint has zero sites — no source file goes red if the entry is dropped, so this test is the
/// thing that does. Its inverse must stay absent for the same reason it was not chosen: enabling
/// `mod_module_files` would ban the very file the tree standardised on.
#[test]
fn the_workspace_lint_table_still_pins_one_module_layout() {
    let synthetic = "self_named_module_files = \"warn\"\n  mod_module_files = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "self_named_module_files"), 0);
    assert_eq!(clippy_deny_count(synthetic, "mod_module_files"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_deny_count(clippy, "self_named_module_files"),
        1,
        "workspace policy must pin self_named_module_files = \"deny\" exactly once"
    );
    assert_eq!(
        clippy_deny_count(clippy, "mod_module_files"),
        0,
        "mod_module_files is the inverse policy — it would ban pgoutput/mod.rs"
    );
}

/// `push_str(&format!(..))` allocates a whole `String` to copy it into a buffer that already
/// exists — the tree uses `write!`, `push_str`, and caller-owned scratch buffers instead of
/// `write!`, `push_str` and scratch a caller owns and reuses. `format_push_string` is what keeps it
/// out, and it is `clippy::restriction`: outside `all`, `perf`, `complexity` and every other group
/// this table denies, so none of those entries reaches it and only the named one does. The tree has
/// zero sites, so no source file goes red if that entry is dropped — this test is the thing that
/// does, exactly as it is for `dbg_macro` above. Its two neighbours need no pin of their own:
/// `format_in_format_args` is a perf lint and `useless_format` a complexity one, so the group
/// entries already carry both, and naming either here would contradict the reach those entries
/// record. No lint covers this rule's remaining shape — a `format!` inside a loop — so the
/// benchmarks in `docs/benchmarks.md` stay its only detector.
#[test]
fn the_workspace_lint_table_still_denies_formatting_into_an_existing_buffer() {
    let synthetic = "format_push_string = \"warn\"\n  print_stderr = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "format_push_string"), 0);
    assert_eq!(clippy_deny_count(synthetic, "print_stderr"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_deny_count(clippy, "format_push_string"),
        1,
        "workspace policy must pin format_push_string = \"deny\" exactly once"
    );
    assert_eq!(
        clippy_deny_count(clippy, "format_in_format_args"),
        0,
        "format_in_format_args is a perf lint; the perf group entry above already carries it"
    );
}

/// The five entries that stand between a recoverable failure and an aborted pod. A bad config
/// value, a short wire frame and a malformed range literal are all *expected* — walrus answers each
/// with a typed `Err` that `main` maps onto a `common::ExitCode`. Nothing in the type system
/// requires that; these denies are the requirement. All five are `restriction`/`pedantic` lints
/// outside every group denied above, so only the named entry reaches each one. Dropping an entry
/// can leave production compiling unchanged, exactly as with `dbg_macro` above. The other way to
/// undo this policy — a scoped suppression inside a production module — is what
/// `crates/common/tests/no_panic_suppression.rs` watches.
///
/// Unlike every other entry in the table, these five carry their rationale on the same line, which
/// is why the count used here has to strip it first.
#[test]
fn the_workspace_lint_table_still_denies_the_panic_family() {
    let synthetic = "panic = \"warn\" # rationale\n  todo = \"deny\" # rationale\n";
    assert_eq!(
        clippy_deny_count(synthetic, "todo"),
        0,
        "the exact-match count cannot see past a trailing rationale — hence the second helper"
    );
    assert_eq!(
        clippy_deny_count_ignoring_rationale(synthetic, "panic"),
        0,
        "a rationale must not rescue an entry that is not pinned at deny"
    );
    assert_eq!(
        clippy_deny_count_ignoring_rationale(synthetic, "todo"),
        1,
        "an entry at deny still counts once with a rationale beside it"
    );

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    for lint in [
        "panic",
        "todo",
        "unimplemented",
        "unreachable",
        "panic_in_result_fn",
    ] {
        assert_eq!(
            clippy_deny_count_ignoring_rationale(clippy, lint),
            1,
            "workspace policy must pin {lint} = \"deny\" exactly once"
        );
    }
}

#[test]
fn the_clippy_carve_out_is_still_scoped_to_tests() {
    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    assert!(cfg.contains("allow-unwrap-in-tests = true"));
    assert!(cfg.contains("allow-expect-in-tests = true"));

    for line in cfg.lines().map(str::trim) {
        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.contains("unwrap") || key.contains("expect") {
            assert!(
                key.ends_with("-in-tests"),
                "clippy.toml key {key:?} widens unwrap/expect beyond tests"
            );
        }
    }
}
