//! Every serde-carrying enum in the workspace lives in `common` and has a bare-scalar JSON form.
//! `Op`, `Kind`, and `ReplicaIdentity` use serde's default external enum representation; `Tier` uses
//! numeric `into`/`try_from` conversion. Each expected value comes from an exhaustive match with no
//! `_` arm, making a new variant or payload a compile error before it can change the wire format.
//!
//! Because every variant is a unit variant, all four of serde's tagging strategies would produce a
//! *different* document, and only the default one produces the scalar the persisted contracts use.
//! The source guard at the bottom of the file is what keeps a later attribute from switching it.

use common::{Kind, Op, ReplicaIdentity, Tier};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

/// Serialize, compare against the golden scalar, prove it is a scalar, and round-trip.
fn assert_scalar_wire<T>(value: T, expected: &Value)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + Copy,
{
    let serialized = serde_json::to_value(value);
    assert_eq!(
        serialized.as_ref().ok(),
        Some(expected),
        "wire form drifted for {value:?}; serialization error: {:?}",
        serialized.as_ref().err()
    );
    let Ok(wire) = serialized else {
        return;
    };
    assert!(
        wire.is_string() || wire.is_number(),
        "must stay a JSON scalar; an object or array means a variant gained a payload: {wire}"
    );
    let round_tripped = serde_json::from_value::<T>(wire);
    assert_eq!(
        round_tripped.as_ref().ok(),
        Some(&value),
        "round-trip failed for {value:?}: {:?}",
        round_tripped.as_ref().err()
    );
}

/// Exhaustive: no wildcard arm. Keys are documented in architecture.md section 1.4.
fn op_wire(op: Op) -> Value {
    match op {
        Op::Insert => json!("i"),
        Op::Update => json!("u"),
        Op::Delete => json!("d"),
        Op::Truncate => json!("t"),
    }
}

#[test]
fn op_is_a_single_char_scalar() {
    for op in [Op::Insert, Op::Update, Op::Delete, Op::Truncate] {
        assert_scalar_wire(op, &op_wire(op));
    }
}

fn kind_wire(kind: Kind) -> Value {
    match kind {
        Kind::Snapshot => json!("snapshot"),
        Kind::Stream => json!("stream"),
        Kind::Reload => json!("reload"),
    }
}

#[test]
fn kind_is_a_lowercase_scalar() {
    for kind in [Kind::Snapshot, Kind::Stream, Kind::Reload] {
        assert_scalar_wire(kind, &kind_wire(kind));
    }
}

fn replica_identity_wire(identity: ReplicaIdentity) -> Value {
    match identity {
        ReplicaIdentity::Default => json!("default"),
        ReplicaIdentity::Nothing => json!("nothing"),
        ReplicaIdentity::Full => json!("full"),
        ReplicaIdentity::Index => json!("index"),
    }
}

#[test]
fn replica_identity_is_a_lowercase_scalar() {
    for identity in [
        ReplicaIdentity::Default,
        ReplicaIdentity::Nothing,
        ReplicaIdentity::Full,
        ReplicaIdentity::Index,
    ] {
        assert_scalar_wire(identity, &replica_identity_wire(identity));
    }
}

fn tier_wire(tier: Tier) -> Value {
    match tier {
        Tier::One => json!(1),
        Tier::Two => json!(2),
        Tier::Three => json!(3),
    }
}

#[test]
fn tier_is_an_integer_scalar() {
    for tier in [Tier::One, Tier::Two, Tier::Three] {
        assert_scalar_wire(tier, &tier_wire(tier));
    }
}

/// The serde-bearing modules, scanned as source text. The config crate and telemetry module are
/// operator-facing documents where the next enum would most likely land.
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
    ("telemetry.rs", include_str!("../src/telemetry.rs")),
    (
        "extractor_meta.rs",
        include_str!("../src/extractor_meta.rs"),
    ),
    ("pg_shape.rs", include_str!("../src/pg_shape.rs")),
    (
        "type_descriptor.rs",
        include_str!("../src/type_descriptor.rs"),
    ),
];

/// The three arguments that select a non-default enum representation. Any one of them turns a
/// variant from a bare scalar into an object.
const TAGGING_ARGUMENTS: [&str; 3] = ["tag", "content", "untagged"];

/// The argument text of one `#[serde(…)]`, from just past its `(` to the matching `)`.
///
/// Parenthesis-aware so a nested argument — `bound(deserialize = "…")` is the one serde spells that
/// way — cannot close the list early and hide a tagging argument written after it.
fn serde_arguments(after_open_paren: &str) -> &str {
    let mut depth = 1usize;
    for (index, ch) in after_open_paren.char_indices() {
        if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth -= 1;
            if depth == 0 {
                return &after_open_paren[..index];
            }
        }
    }
    after_open_paren
}

/// The first tagging argument of any `#[serde(…)]` in `source`, or `None`.
///
/// Whitespace is stripped first, so neither argument order nor line breaks can hide one:
/// `#[serde(rename_all = "lowercase", tag = "type")]` and the same attribute split over four lines
/// both compact to one comma-separated list. Only text inside an attribute's parentheses is
/// inspected, so prose about tagging in a doc comment is not a violation.
fn tagging_violation(module: &str, source: &str) -> Option<String> {
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();
    let mut rest = compact.as_str();
    while let Some(open) = rest.find("serde(") {
        let after = &rest[open + "serde(".len()..];
        let arguments = serde_arguments(after);
        for argument in arguments.split(',') {
            for tagging in TAGGING_ARGUMENTS {
                // Either the bare `untagged` flag or a `name="value"` argument — never a longer
                // identifier that merely starts the same way, such as `rename = "tag"`.
                let selected = argument
                    .strip_prefix(tagging)
                    .is_some_and(|tail| tail.is_empty() || tail.starts_with('='));
                if selected {
                    return Some(format!(
                        "{module} introduced enum tagging argument {tagging}, which reshapes the \
                         scalar wire form into an object"
                    ));
                }
            }
        }
        rest = &after[arguments.len()..];
    }
    None
}

#[test]
fn no_enum_tagging_attribute_is_introduced() {
    for (module, source) in SERDE_MODULES {
        assert_eq!(tagging_violation(module, source), None, "{module}");
    }
}

#[test]
fn the_tagging_guard_catches_every_spelling() {
    // The literals are split so walrus source never carries a contiguous tagging attribute, the
    // same care `src/lib_test.rs` takes with its `flatten` fixture.
    let fixtures = [
        concat!("#[serde(", r#"tag = "type")] enum E {}"#),
        concat!("#[serde(rename_all = \"lowercase\", ", r#"tag = "type")]"#),
        concat!("#[serde(\n    ", "tag = \"t\",\n    content = \"c\",\n)]"),
        concat!("#[serde(bound(deserialize = \"T: Copy\"), ", "untagged)]"),
    ];
    for fixture in fixtures {
        assert!(
            tagging_violation("fixture.rs", fixture).is_some(),
            "guard missed {fixture}"
        );
    }
}

#[test]
fn the_tagging_guard_leaves_the_attributes_walrus_actually_uses_alone() {
    let benign = [
        "/// Internally tagged, adjacently tagged and untagged all reshape this into an object.",
        r#"#[serde(rename_all = "lowercase")]"#,
        r#"#[serde(rename = "tag")]"#,
        "#[serde(deny_unknown_fields, default)]",
        r#"#[serde(try_from = "u8", into = "u8")]"#,
    ];
    for source in benign {
        assert_eq!(tagging_violation("fixture.rs", source), None, "{source}");
    }
}
