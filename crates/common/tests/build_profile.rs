//! Guards the workspace release-profile decision against silent drift.

use std::path::Path;

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const CI_WORKFLOW: &str = include_str!("../../../.github/workflows/ci.yml");
const DOCKERFILE_EXTRACTOR: &str = include_str!("../../../deploy/docker/Dockerfile.extractor");
const DOCKERFILE_TRANSFORMER: &str = include_str!("../../../deploy/docker/Dockerfile.transformer");
const JUSTFILE: &str = include_str!("../../../justfile");
const BENCH_E2E: &str = include_str!("../../../scripts/bench-e2e.sh");
const PROFILE_BENCH: &str = include_str!("../../../scripts/profile-bench.sh");
const EXTRACTOR_SMOKE: &str = include_str!("../../../scripts/extractor-smoke.sh");
const EXTRACTOR_MAIN: &str = include_str!("../../extractor/src/main.rs");
const TRANSFORMER_MAIN: &str = include_str!("../../transformer/src/main.rs");
// Every surface that can hand a target-CPU or ISA flag to a cargo invocation: the manifest, the
// sole workflow, both shipped Dockerfiles, and the three developer entry points that build a
// walrus binary. The rule's own Cargo-config example is titled "native builds for development", so
// the local recipes are a first-class drift vector. This list is wider than CARGO_BUILD_SURFACES
// because it also covers debug-only `scripts/extractor-smoke.sh`: an ISA floor is not a release-profile
// knob, so a "native" flag on *any* build produces a host-specific binary.
const TARGET_CPU_SURFACES: &[(&str, &str)] = &[
    ("Cargo.toml", WORKSPACE_MANIFEST),
    (".github/workflows/ci.yml", CI_WORKFLOW),
    ("deploy/docker/Dockerfile.extractor", DOCKERFILE_EXTRACTOR),
    (
        "deploy/docker/Dockerfile.transformer",
        DOCKERFILE_TRANSFORMER,
    ),
    ("justfile", JUSTFILE),
    ("scripts/bench-e2e.sh", BENCH_E2E),
    ("scripts/profile-bench.sh", PROFILE_BENCH),
    ("scripts/extractor-smoke.sh", EXTRACTOR_SMOKE),
];
// Every surface that invokes cargo to build a shipped artifact or a benchmark, which is where a
// profile key — a codegen-unit count, a panic strategy, a strip setting — can be reinstated from
// outside the manifest. The manifest is deliberately absent: its rejection prose names those keys,
// and `profile_key_declaration` already parses the tables there.
const CARGO_BUILD_SURFACES: &[(&str, &str)] = &[
    (".github/workflows/ci.yml", CI_WORKFLOW),
    ("deploy/docker/Dockerfile.extractor", DOCKERFILE_EXTRACTOR),
    (
        "deploy/docker/Dockerfile.transformer",
        DOCKERFILE_TRANSFORMER,
    ),
    ("justfile", JUSTFILE),
    ("scripts/bench-e2e.sh", BENCH_E2E),
    ("scripts/profile-bench.sh", PROFILE_BENCH),
];
const PGO_SURFACES: &[(&str, &str)] = &[
    ("Cargo.toml", WORKSPACE_MANIFEST),
    (".github/workflows/ci.yml", CI_WORKFLOW),
    ("deploy/docker/Dockerfile.extractor", DOCKERFILE_EXTRACTOR),
    (
        "deploy/docker/Dockerfile.transformer",
        DOCKERFILE_TRANSFORMER,
    ),
    ("justfile", JUSTFILE),
    ("scripts/bench-e2e.sh", BENCH_E2E),
    ("scripts/profile-bench.sh", PROFILE_BENCH),
];

fn table_body<'a>(manifest: &'a str, header: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start: Option<usize> = None;

    for line in manifest.split_inclusive('\n') {
        let trimmed = line.trim();
        if let Some(start) = body_start {
            if trimmed.starts_with('[') {
                return Some(&manifest[start..offset]);
            }
        } else if trimmed == header {
            body_start = Some(offset + line.len());
        }
        offset += line.len();
    }

    body_start.map(|start| &manifest[start..])
}

fn assignment_value<'a>(body: &'a str, key: &str) -> Option<&'a str> {
    body.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }

        let (name, value) = line.split_once('=')?;
        if name.trim() != key {
            return None;
        }

        Some(value.split('#').next()?.trim())
    })
}

fn release_lto(manifest: &str) -> Result<&str, &'static str> {
    let release = table_body(manifest, "[profile.release]").ok_or("missing [profile.release]")?;
    let value = assignment_value(release, "lto").ok_or("missing lto")?;
    let value = value.trim_matches('"');

    if matches!(value, "false" | "off") {
        Err("lto disabled")
    } else {
        Ok(value)
    }
}

fn profiling_profile_policy(manifest: &str) -> Result<(), &'static str> {
    let body = table_body(manifest, "[profile.profiling]").ok_or("missing profiling profile")?;
    if assignment_value(body, "inherits") != Some("\"release\"") {
        return Err("profiling profile must inherit release");
    }
    if assignment_value(body, "debug") != Some("true") {
        return Err("profiling profile must carry full debug info");
    }
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, _)) = line.split_once('=') else {
            return Err("unparseable profiling profile entry");
        };
        if !matches!(key.trim(), "inherits" | "debug") {
            return Err("profiling profile changes code generation");
        }
    }
    Ok(())
}

/// The `[profile.…]` header that declares `key`, if any. *Every* profile table counts, not only
/// `[profile.release]`: the rules park an override in a `bench`, `production` or
/// `release-with-debug` table just as readily, and `bench` inherits `release` — an override there
/// would quietly de-couple `docs/benchmarks.md`'s numbers from the shipped artifact. Per-package
/// tables (`[profile.release.package.…]`) accept the keys too. Comments are not assignments, so the
/// rationale above the table may keep naming them, and a key that doubles as a lint name — `panic`
/// sits in `[workspace.lints.clippy]` — counts only under a `[profile…]` header.
fn profile_key_declaration<'a>(manifest: &'a str, key: &str) -> Option<&'a str> {
    let mut profile = None;

    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            profile = line.starts_with("[profile").then_some(line);
        } else if let Some(header) = profile
            && assignment_value(line, key).is_some()
        {
            return Some(header);
        }
    }

    None
}

fn codegen_units_policy(manifest: &str) -> Result<(), &'static str> {
    if profile_key_declaration(manifest, "codegen-units").is_some() {
        Err("codegen-units declared")
    } else {
        Ok(())
    }
}

/// Cargo also reads the setting from outside the manifest: `CARGO_PROFILE_<name>_CODEGEN_UNITS` in
/// the environment, `-C codegen-units` in a rustc flag, or a `--config` override on the command
/// line. Each of those reinstates the rejected setting with the manifest guard still green.
fn codegen_units_override_policy(body: &str) -> Result<(), &'static str> {
    for (needle, diagnostic) in [
        ("CODEGEN_UNITS", "a codegen-units env override is set"),
        ("codegen-units", "a codegen-units build flag is set"),
    ] {
        if body.contains(needle) {
            return Err(diagnostic);
        }
    }
    Ok(())
}

/// The two knobs the release-profile rule adds beyond LTO and codegen units. Both are rejected on
/// behaviour rather than build cost: `panic = "abort"` deletes `crates/extractor/src/reload.rs`'s
/// `JoinError::is_panic` arm from the shipped binary, so one panicking exporter task would take the
/// pod down instead of leaving a classified log line and an expiring lease; `strip` removes the
/// symbol table that names the frames in a profile of the release — and, by inheritance, `bench` —
/// binary, which `docs/benchmarks.md` pins as its methodology.
fn release_profile_policy(manifest: &str) -> Result<(), &'static str> {
    for (key, diagnostic) in [
        ("panic", "a panic strategy is declared"),
        ("strip", "symbol stripping is declared"),
    ] {
        if profile_key_declaration(manifest, key).is_some() {
            return Err(diagnostic);
        }
    }

    Ok(())
}

/// The out-of-manifest half, one environment needle and one build-flag needle per key, exactly as
/// `codegen_units_override_policy` has. The trailing `=` is what makes the second pair safe: unlike
/// `codegen-units`, these key names are ordinary English words, so a bare needle would reject any
/// surface whose comments discussed a panic. `-C panic=abort` is caught twice over — here, and by
/// `target_cpu_policy` below, whose sole rustflags exception is the exact Tokio diagnostic cfg.
fn profile_key_override_policy(body: &str) -> Result<(), &'static str> {
    for (needle, diagnostic) in [
        ("_PANIC", "a panic-strategy env override is set"),
        ("_STRIP", "a symbol-stripping env override is set"),
        ("panic=", "a panic-strategy build flag is set"),
        ("strip=", "a symbol-stripping build flag is set"),
    ] {
        if body.contains(needle) {
            return Err(diagnostic);
        }
    }
    Ok(())
}

/// `target-feature` is the fourth spelling and needs its own needle: `cargo rustc --release --
/// -C target-feature=+avx2,+fma` establishes the same ISA floor as a named CPU — the AVX2/FMA
/// instructions the rule's "What Changes" section is about — while mentioning neither `target-cpu`
/// nor either rustflags variable, so the first three needles all miss it.
fn rustflags_value(line: &str) -> Option<&str> {
    let (_, suffix) = line.split_once("RUSTFLAGS")?;
    let suffix = suffix.trim_start();
    let value = suffix
        .strip_prefix('=')
        .or_else(|| suffix.strip_prefix(':'))?;
    let value = value.trim_start();
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted.split_once('"').map(|(value, _)| value);
    }
    value.split_whitespace().next()
}

fn target_cpu_policy(body: &str) -> Result<(), &'static str> {
    if body.contains("target-cpu") {
        return Err("target-cpu is set");
    }
    if body.contains("rustflags") {
        return Err("rustflags is set");
    }
    if body.contains("target-feature") {
        return Err("a target-feature flag is set");
    }
    for line in body.lines().filter(|line| line.contains("RUSTFLAGS")) {
        if rustflags_value(line) != Some("--cfg tokio_unstable") {
            return Err("RUSTFLAGS contains a non-diagnostic flag");
        }
    }
    Ok(())
}

/// The paths named by `[workspace] members`. Cargo discovers configuration from the *invocation's*
/// directory upwards, so a `.cargo/config.toml` dropped inside a member crate injects its
/// `rustflags` into every `cargo build` run from there while the workspace root stays clean.
/// Reading the list from the manifest keeps the check honest when a member is added.
fn workspace_members(manifest: &str) -> Vec<&str> {
    let Some(body) = table_body(manifest, "[workspace]") else {
        return Vec::new();
    };
    let Some(members) = assignment_value(body, "members") else {
        return Vec::new();
    };

    members
        .trim_matches(['[', ']'])
        .split(',')
        .map(|entry| entry.trim().trim_matches('"'))
        .filter(|entry| !entry.is_empty())
        .collect()
}

const fn cargo_config_policy(
    config_toml_exists: bool,
    legacy_config_exists: bool,
) -> Result<(), &'static str> {
    if config_toml_exists {
        Err(".cargo/config.toml exists")
    } else if legacy_config_exists {
        Err(".cargo/config exists")
    } else {
        Ok(())
    }
}

/// The build-surface half of the PGO rejection. `-Cprofile-generate` and `-Cprofile-use` are the
/// instrument and optimize passes; `llvm-profdata` is the merge between them. `llvm-bolt` needs its
/// own needle because the rule's post-link follow-on reorders blocks and functions from a
/// `perf.data` profile *after* the linker has run: a bare `RUN llvm-bolt …` layer carries none of
/// the `-Cprofile-*` flags the first three look for and applies to an ordinary release binary, so
/// nothing above would see it.
fn pgo_policy(body: &str) -> Result<(), &'static str> {
    for (needle, diagnostic) in [
        ("profile-generate", "PGO profile generation is enabled"),
        ("profile-use", "PGO profile use is enabled"),
        ("llvm-profdata", "llvm-profdata is invoked"),
        ("llvm-bolt", "a BOLT post-link step is added"),
    ] {
        if body.contains(needle) {
            return Err(diagnostic);
        }
    }
    Ok(())
}

#[test]
fn workspace_release_profile_keeps_thin_lto() {
    assert_eq!(release_lto(WORKSPACE_MANIFEST), Ok("thin"));
}

#[test]
fn workspace_has_one_release_equivalent_profiling_profile() {
    assert_eq!(profiling_profile_policy(WORKSPACE_MANIFEST), Ok(()));
}

#[test]
fn dhat_output_stays_outside_the_strict_application_config_namespace() {
    for (name, body) in [
        ("scripts/bench-e2e.sh", BENCH_E2E),
        ("crates/extractor/src/main.rs", EXTRACTOR_MAIN),
        ("crates/transformer/src/main.rs", TRANSFORMER_MAIN),
    ] {
        assert!(
            body.contains("DHAT_OUTPUT"),
            "{name} must share the diagnostic output variable"
        );
        assert!(
            !body.contains("WALRUS_DHAT_OUTPUT"),
            "{name} must not feed a diagnostic-only key into strict WALRUS_ config"
        );
    }
}

#[test]
fn profiling_profile_policy_rejects_missing_symbols_or_codegen_drift() {
    let cases = [
        (
            "[profile.release]\nlto = \"thin\"\n",
            Err("missing profiling profile"),
        ),
        (
            "[profile.profiling]\ninherits = \"dev\"\ndebug = true\n",
            Err("profiling profile must inherit release"),
        ),
        (
            "[profile.profiling]\ninherits = \"release\"\ndebug = \"line-tables-only\"\n",
            Err("profiling profile must carry full debug info"),
        ),
        (
            "[profile.profiling]\ninherits = \"release\"\ndebug = true\nopt-level = 2\n",
            Err("profiling profile changes code generation"),
        ),
    ];
    for (manifest, expected) in cases {
        assert_eq!(
            profiling_profile_policy(manifest),
            expected,
            "manifest:\n{manifest}"
        );
    }
}

#[test]
fn lto_policy_rejects_disabled_missing_and_comment_only_values() {
    let cases = [
        (
            "[workspace]\nmembers = []\n",
            Err("missing [profile.release]"),
        ),
        ("[profile.release]\nopt-level = 3\n", Err("missing lto")),
        ("[profile.release]\nlto = false\n", Err("lto disabled")),
        ("[profile.release]\nlto = \"off\"\n", Err("lto disabled")),
        (
            "[profile.release]\n# lto = \"thin\"\ncodegen-units = 16\n",
            Err("missing lto"),
        ),
    ];

    for (manifest, expected) in cases {
        assert_eq!(release_lto(manifest), expected, "manifest:\n{manifest}");
    }
}

#[test]
fn profile_comment_does_not_declare_codegen_units() {
    let release = table_body(WORKSPACE_MANIFEST, "[profile.release]").expect("release profile");
    assert_eq!(assignment_value(release, "codegen-units"), None);
}

#[test]
fn no_profile_table_declares_codegen_units() {
    assert_eq!(
        profile_key_declaration(WORKSPACE_MANIFEST, "codegen-units"),
        None,
        "walrus keeps the default codegen-unit count; see the Cargo.toml release-profile rationale"
    );
}

#[test]
fn no_build_surface_overrides_codegen_units() {
    for (name, body) in CARGO_BUILD_SURFACES {
        assert_eq!(
            codegen_units_override_policy(body),
            Ok(()),
            "{name} must leave codegen-units at the default"
        );
    }
}

#[test]
fn workspace_rejects_codegen_units() {
    assert_eq!(codegen_units_policy(WORKSPACE_MANIFEST), Ok(()));
}

#[test]
fn codegen_units_policy_rejects_fabricated_input() {
    let default = "[profile.release]\nlto = \"thin\"\n";
    let override_value = concat!(
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "codegen-units = 1\n",
    );
    let commented_value = concat!(
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "# codegen-units = 1\n",
    );
    let bench_override = concat!(
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "\n",
        "[profile.bench]\n",
        "inherits = \"release\"\n",
        "codegen-units = 1\n",
    );
    let package_override = concat!(
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "\n",
        "[profile.release.package.duckdb]\n",
        "codegen-units = 1\n",
    );
    let cases = [
        (default, Ok(())),
        (commented_value, Ok(())),
        (override_value, Err("codegen-units declared")),
        (bench_override, Err("codegen-units declared")),
        (package_override, Err("codegen-units declared")),
    ];

    for (manifest, expected) in cases {
        assert_eq!(
            codegen_units_policy(manifest),
            expected,
            "manifest:\n{manifest}"
        );
    }
}

#[test]
fn codegen_units_override_policy_rejects_fabricated_input() {
    let cases = [
        ("RUN cargo build --release", Ok(())),
        (
            "ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1",
            Err("a codegen-units env override is set"),
        ),
        (
            "RUSTFLAGS=\"-C codegen-units=1\" cargo build --release",
            Err("a codegen-units build flag is set"),
        ),
        (
            "cargo build --release --config profile.release.codegen-units=1",
            Err("a codegen-units build flag is set"),
        ),
    ];

    for (body, expected) in cases {
        assert_eq!(
            codegen_units_override_policy(body),
            expected,
            "surface:\n{body}"
        );
    }
}

#[test]
fn no_profile_table_declares_panic_or_strip() {
    assert_eq!(
        release_profile_policy(WORKSPACE_MANIFEST),
        Ok(()),
        "walrus keeps unwinding and its symbol table; see the Cargo.toml release-profile rationale"
    );
}

#[test]
fn release_profile_policy_rejects_fabricated_input() {
    let default = "[profile.release]\nlto = \"thin\"\n";
    // The workspace's own `panic = "deny"` clippy entry is not a profile key.
    let lint_table_panic = concat!(
        "[workspace.lints.clippy]\n",
        "panic = \"deny\"\n",
        "\n",
        "[profile.release]\n",
        "lto = \"thin\"\n",
    );
    let abort_panic = concat!(
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "panic = \"abort\"\n",
    );
    // `bench` inherits `release`, so stripping it alone still costs the benchmark its frames.
    let stripped_bench = concat!(
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "\n",
        "[profile.bench]\n",
        "inherits = \"release\"\n",
        "strip = true\n",
    );
    let cases = [
        (default, Ok(())),
        (lint_table_panic, Ok(())),
        (abort_panic, Err("a panic strategy is declared")),
        (stripped_bench, Err("symbol stripping is declared")),
    ];

    for (manifest, expected) in cases {
        assert_eq!(
            release_profile_policy(manifest),
            expected,
            "manifest:\n{manifest}"
        );
    }
}

#[test]
fn no_build_surface_overrides_panic_or_strip() {
    for (name, body) in CARGO_BUILD_SURFACES {
        assert_eq!(
            profile_key_override_policy(body),
            Ok(()),
            "{name} must build the profile the manifest declares"
        );
    }
}

#[test]
fn profile_key_override_policy_rejects_fabricated_input() {
    let cases = [
        ("RUN cargo build --release", Ok(())),
        // Prose about a panicking task is not an override; only the two shapes below are.
        ("# the exporter panics, the lease expires", Ok(())),
        (
            "ENV CARGO_PROFILE_RELEASE_PANIC=abort",
            Err("a panic-strategy env override is set"),
        ),
        (
            "ENV CARGO_PROFILE_RELEASE_STRIP=true",
            Err("a symbol-stripping env override is set"),
        ),
        (
            "RUSTFLAGS=\"-C panic=abort\" cargo build --release",
            Err("a panic-strategy build flag is set"),
        ),
        (
            "cargo build --release --config profile.bench.strip=true",
            Err("a symbol-stripping build flag is set"),
        ),
    ];

    for (body, expected) in cases {
        assert_eq!(
            profile_key_override_policy(body),
            expected,
            "surface:\n{body}"
        );
    }
}

#[test]
fn workspace_rejects_panic_and_strip_profile_overrides() {
    assert_eq!(release_profile_policy(WORKSPACE_MANIFEST), Ok(()));
}

#[test]
fn no_build_surface_sets_a_target_cpu() {
    for (name, body) in TARGET_CPU_SURFACES {
        assert_eq!(
            target_cpu_policy(body),
            Ok(()),
            "{name} must remain portable"
        );
    }
}

#[test]
fn no_cargo_config_exists() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let members = workspace_members(WORKSPACE_MANIFEST);
    assert!(
        !members.is_empty(),
        "the member list must stay parseable, or this check silently shrinks to the root"
    );

    for dir in std::iter::once(".").chain(members) {
        let path = root.join(dir);
        assert_eq!(
            cargo_config_policy(
                path.join(".cargo/config.toml").exists(),
                path.join(".cargo/config").exists(),
            ),
            Ok(()),
            "{dir}/.cargo must remain absent so builds stay portable"
        );
    }
}

#[test]
fn workspace_members_reads_every_declared_member() {
    let manifest = concat!(
        "[workspace]\n",
        "resolver = \"2\"\n",
        "members = [\"crates/common\", \"tests/e2e\"] # one crate at a time\n",
        "\n",
        "[workspace.package]\n",
        "edition = \"2024\"\n",
    );

    assert_eq!(workspace_members(manifest), ["crates/common", "tests/e2e"]);
    assert_eq!(
        workspace_members("[package]\nname = \"common\"\n"),
        Vec::<&str>::new()
    );
    assert!(workspace_members(WORKSPACE_MANIFEST).contains(&"crates/transformer"));
}

#[test]
fn target_cpu_policy_rejects_fabricated_input() {
    let surface_cases = [
        ("RUN cargo build --release", Ok(())),
        (
            "ENV RUSTFLAGS=\"-C target-cpu=native\"",
            Err("target-cpu is set"),
        ),
        (
            "ENV RUSTFLAGS=-Copt-level=3",
            Err("RUSTFLAGS contains a non-diagnostic flag"),
        ),
        (
            "RUSTFLAGS=\"--cfg tokio_unstable\" cargo clippy --all-features",
            Ok(()),
        ),
        (
            "RUSTFLAGS=\"--cfg tokio_unstable -C target-cpu=native\" cargo build",
            Err("target-cpu is set"),
        ),
        (
            "rustflags = [\"-C\", \"target-feature=+avx2\"]",
            Err("rustflags is set"),
        ),
        // Neither `target-cpu` nor a rustflags variable appears here, so only the fourth needle
        // rejects this ISA floor.
        (
            "cargo rustc --release -p transformer -- -C target-feature=+avx2,+fma",
            Err("a target-feature flag is set"),
        ),
    ];
    for (body, expected) in surface_cases {
        assert_eq!(target_cpu_policy(body), expected, "surface:\n{body}");
    }

    let config_cases = [
        ((false, false), Ok(())),
        ((true, false), Err(".cargo/config.toml exists")),
        ((false, true), Err(".cargo/config exists")),
    ];
    for ((config_toml_exists, legacy_config_exists), expected) in config_cases {
        assert_eq!(
            cargo_config_policy(config_toml_exists, legacy_config_exists),
            expected
        );
    }
}

#[test]
fn no_build_surface_enables_pgo() {
    for (name, body) in PGO_SURFACES {
        assert_eq!(
            pgo_policy(body),
            Ok(()),
            "{name} must remain free of PGO instrumentation"
        );
    }
}

#[test]
fn pgo_policy_rejects_fabricated_input() {
    let cases = [
        ("RUN cargo build --release", Ok(())),
        (
            "ENV RUSTFLAGS=\"-Cprofile-generate=/tmp/pgo\"",
            Err("PGO profile generation is enabled"),
        ),
        (
            "RUSTFLAGS=\"-Cprofile-use=/tmp/pgo.profdata\" cargo build",
            Err("PGO profile use is enabled"),
        ),
        (
            "pgo:\n    llvm-profdata merge -o merged.profdata raw",
            Err("llvm-profdata is invoked"),
        ),
        (
            "RUN llvm-bolt target/release/walrus-transformer -o walrus-transformer.bolt -data=perf.data",
            Err("a BOLT post-link step is added"),
        ),
    ];

    for (body, expected) in cases {
        assert_eq!(pgo_policy(body), expected, "surface:\n{body}");
    }
}
