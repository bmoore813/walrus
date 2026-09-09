//! Guard for `api-serde-optional`. The rule asks a library crate to hide serde behind a Cargo
//! feature; walrus rejected that for `common` and kept `sqlx` as its one optional seam. Three
//! things drift silently and no lint covers any of them: the manifest could grow a `serde` feature,
//! the crate docs could stop naming the optional feature a consumer has to opt into, and CI could
//! drop the only build that compiles `common` with `sqlx` **off** — the state workspace feature
//! unification otherwise hides.

const COMMON_MANIFEST: &str = include_str!("../Cargo.toml");
const COMMON_LIB: &str = include_str!("../src/lib.rs");
const CI_WORKFLOW: &str = include_str!("../../../.github/workflows/ci.yml");

/// The body of `[section]` in `manifest`, up to the next table header at column 0.
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

/// The `key = …` line for `key` in a TOML table body, skipping comments. `serde_json` and
/// `humantime-serde` must not answer for `serde`, so the name has to be followed by its `=`.
fn assignment<'a>(body: &'a str, key: &str) -> Option<&'a str> {
    body.lines().map(str::trim).find(|line| {
        if line.starts_with('#') {
            return false;
        }
        let Some(rest) = line.strip_prefix(key) else {
            return false;
        };
        rest.trim_start().starts_with('=')
    })
}

fn serde_policy(manifest: &str) -> Result<(), &'static str> {
    let dependencies = section(manifest, "[dependencies]").ok_or("missing [dependencies]")?;
    let serde = assignment(dependencies, "serde").ok_or("serde is not a direct dependency")?;
    let features = section(manifest, "[features]").unwrap_or_default();

    if serde.contains("optional") {
        Err("serde is optional")
    } else if assignment(features, "serde").is_some() {
        Err("a serde feature is declared")
    } else {
        Ok(())
    }
}

/// The `sqlx`-off compile has to stay a CI contract: it is the only build in which `common`'s
/// optional dependency is absent, so it is the only place a leaked `use sqlx::…` goes red.
fn sqlx_off_build(workflow: &str) -> Result<(), &'static str> {
    let needles = ["cargo", "-p common", "--no-default-features"];
    for line in workflow.lines() {
        if needles.iter().all(|needle| line.contains(needle)) {
            return Ok(());
        }
    }
    Err("no CI build compiles `common` with its optional feature off")
}

/// The rule's feature-documentation half is the part walrus does owe its readers: an opt-in
/// feature the crate root never mentions is one no consumer can discover.
fn documents_the_optional_feature(lib_rs: &str) -> Result<(), &'static str> {
    let docs = lib_rs
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("//!"))
        .collect::<Vec<_>>()
        .join("\n");

    if !docs.contains("# Features") {
        Err("crate docs have no `# Features` section")
    } else if !docs.contains("`sqlx`") {
        Err("crate docs do not name the `sqlx` feature")
    } else {
        Ok(())
    }
}

#[test]
fn serde_stays_a_hard_dependency_of_common() {
    assert_eq!(serde_policy(COMMON_MANIFEST), Ok(()));
}

#[test]
fn the_sqlx_off_build_is_still_a_ci_contract() {
    assert_eq!(sqlx_off_build(CI_WORKFLOW), Ok(()));
}

#[test]
fn the_optional_feature_is_documented_at_the_crate_root() {
    assert_eq!(documents_the_optional_feature(COMMON_LIB), Ok(()));
}

#[test]
fn the_serde_policy_rejects_fabricated_manifests() {
    let cases = [
        ("[dependencies]\nserde = { workspace = true }\n", Ok(())),
        ("[features]\nsqlx = []\n", Err("missing [dependencies]")),
        (
            "[dependencies]\nserde_json = { workspace = true }\n",
            Err("serde is not a direct dependency"),
        ),
        (
            "[dependencies]\n# serde = { workspace = true, optional = true }\n",
            Err("serde is not a direct dependency"),
        ),
        (
            "[dependencies]\nserde = { version = \"1\", optional = true }\n",
            Err("serde is optional"),
        ),
        (
            "[features]\nserde = [\"dep:serde\"]\n[dependencies]\nserde = { path = \".\" }\n",
            Err("a serde feature is declared"),
        ),
    ];

    for (manifest, expected) in cases {
        assert_eq!(serde_policy(manifest), expected, "manifest:\n{manifest}");
    }
}

#[test]
fn the_seam_and_documentation_policies_reject_fabricated_input() {
    assert_eq!(
        sqlx_off_build("run: cargo check -p common --no-default-features"),
        Ok(())
    );
    assert_eq!(
        sqlx_off_build("- run: cargo test --workspace\n"),
        Err("no CI build compiles `common` with its optional feature off")
    );

    assert_eq!(
        documents_the_optional_feature("//! Shared primitives.\npub mod lsn;\n"),
        Err("crate docs have no `# Features` section")
    );
    assert_eq!(
        documents_the_optional_feature("//! # Features\n//!\n//! - `integration`\n"),
        Err("crate docs do not name the `sqlx` feature")
    );
}
