//! Crate-wide serde policy guards and dependency-behavior evidence.

#[allow(
    unused_imports,
    reason = "crate-root policy tests keep the same parent-import convention as module tests"
)]
use super::*;
use serde::Deserialize;

const SERDE_MODULES: [(&str, &str); 7] = [
    (
        "walrus-config/shared.rs",
        include_str!("../../walrus-config/src/shared.rs"),
    ),
    (
        "walrus-config/extractor.rs",
        include_str!("../../walrus-config/src/extractor.rs"),
    ),
    (
        "walrus-config/transformer.rs",
        include_str!("../../walrus-config/src/transformer.rs"),
    ),
    ("telemetry.rs", include_str!("telemetry.rs")),
    ("extractor_meta.rs", include_str!("extractor_meta.rs")),
    ("pg_shape.rs", include_str!("pg_shape.rs")),
    ("type_descriptor.rs", include_str!("type_descriptor.rs")),
];

fn flatten_violation(name: &str, src: &str) -> Option<String> {
    // Removing whitespace catches standalone and combined attributes regardless of formatting.
    let compact: String = src.chars().filter(|ch| !ch.is_whitespace()).collect();
    let starts_attribute = compact.contains(concat!("serde(", "flatten"));
    let later_argument = compact.contains(concat!(",", "flatten"));
    (starts_attribute || later_argument)
        .then(|| format!("{name}: serde flatten conflicts with the strict unknown-field policy"))
}

#[test]
fn no_serde_flatten_in_common_wire_and_config_modules() {
    for (name, src) in SERDE_MODULES {
        assert_eq!(flatten_violation(name, src), None, "{name}");
    }
}

#[test]
fn no_serde_flatten_source_guard_rejects_a_combined_fixture() {
    let fixture = concat!("#[serde(default,", " flatten)] struct Candidate;");
    assert_eq!(
        flatten_violation("fixture/flatten.rs", fixture).as_deref(),
        Some("fixture/flatten.rs: serde flatten conflicts with the strict unknown-field policy")
    );
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct StrictCommonFixture {
    shared: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct StrictServiceFixture {
    service_only: bool,
    #[serde(flatten)]
    common: StrictCommonFixture,
}

#[test]
fn no_serde_flatten_evidence_fixture_preserves_strict_unknown_keys() {
    let parsed: StrictServiceFixture =
        serde_json::from_str(r#"{"shared":"ok","service_only":true}"#).unwrap();
    assert_eq!(parsed.common.shared, "ok");
    assert!(parsed.service_only);
    assert!(
        serde_json::from_str::<StrictServiceFixture>(
            r#"{"shared":"ok","service_only":true,"nonsense":1}"#
        )
        .is_err()
    );
}

#[test]
fn no_serde_flatten_decision_is_explained_at_the_natural_site() {
    assert!(
        include_str!("../../walrus-config/src/shared.rs")
            .contains("#[serde(deny_unknown_fields, default)]")
    );
}
