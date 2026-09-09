use super::*;
use std::num::NonZeroU32;

/// The walrus-extractor.md §2.6 interval descriptor, comment-free.
const DOCS_DESCRIPTOR: &str = r#"{
        "column": "duration",
        "pg_type_oid": 1186,
        "pg_type": "interval",
        "tier": 2,
        "arrow": "Struct/Decomposed",
        "duckdb": "INTERVAL",
        "emit": ["duration_months:INT32", "duration_days:INT32", "duration_micros:INT64"],
        "recombine": "to_months(m)+to_days(d)+to_microseconds(us)",
        "meta": {
            "enum_labels": null,
            "bit_length": null,
            "char_length": null,
            "money_fraction_digits": null
        }
    }"#;

/// A tier-1 scalar descriptor as an older extractor wrote it: no `recombine`, no `meta`. `schema_registry`
/// is never pruned, so rows in this shape stay readable forever.
const LEGACY_SCALAR_DESCRIPTOR: &str = r#"{
        "column": "id",
        "pg_type_oid": 23,
        "pg_type": "int4",
        "tier": 1,
        "arrow": "Int32",
        "duckdb": "INTEGER",
        "emit": ["id:INT32"]
    }"#;

#[test]
fn tier_serializes_as_integer() {
    assert_eq!(serde_json::to_string(&Tier::One).unwrap(), "1");
    assert_eq!(serde_json::to_string(&Tier::Two).unwrap(), "2");
    assert_eq!(serde_json::to_string(&Tier::Three).unwrap(), "3");
    assert_eq!(serde_json::from_str::<Tier>("2").unwrap(), Tier::Two);
    assert!(serde_json::from_str::<Tier>("4").is_err());
    // A quoted string is NOT a valid tier — the contract is a JSON number.
    assert!(serde_json::from_str::<Tier>("\"2\"").is_err());
}

#[test]
fn tier_validation_is_callable_outside_serde() {
    assert_eq!(Tier::try_from(1u8).unwrap(), Tier::One);
    assert_eq!(Tier::try_from(2u8).unwrap(), Tier::Two);
    assert_eq!(Tier::try_from(3u8).unwrap(), Tier::Three);

    // The rejection path is now ordinary Rust — no serde_json round trip needed.
    for bad in [0u8, 4u8, 255u8] {
        let err = Tier::try_from(bad).unwrap_err();
        assert!(
            err.to_string().contains("invalid tier"),
            "message is the operator-grepped one: {err}"
        );
    }

    // And the serialize direction goes through the same conversion.
    assert_eq!(u8::from(Tier::Three), 3);
}

#[test]
fn docs_descriptor_deserializes_every_mapping_field() {
    // Arrange
    let source = DOCS_DESCRIPTOR;

    // Act
    let descriptor: TypeDescriptor = serde_json::from_str(source).unwrap();

    // Assert
    assert_eq!(descriptor.column, "duration");
    assert_eq!(descriptor.pg_type_oid, 1186);
    assert_eq!(descriptor.pg_type, "interval");
    assert_eq!(descriptor.tier, Tier::Two);
    assert_eq!(descriptor.emit.len(), 3);
    assert_eq!(
        descriptor.recombine.as_deref(),
        Some("to_months(m)+to_days(d)+to_microseconds(us)")
    );
    assert_eq!(descriptor.meta, TypeMeta::default()); // all None
}

#[test]
fn docs_descriptor_round_trip_preserves_the_contract_shape() {
    // Arrange
    let expected: serde_json::Value = serde_json::from_str(DOCS_DESCRIPTOR).unwrap();

    // Act
    let descriptor: TypeDescriptor = serde_json::from_str(DOCS_DESCRIPTOR).unwrap();
    let reserialized: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&descriptor).unwrap()).unwrap();

    // Assert
    assert_eq!(reserialized, expected);
    // `tier` is the integer 2, not the string "2".
    assert_eq!(reserialized["tier"], serde_json::json!(2));
}

#[test]
fn tier_one_scalar_descriptor_round_trips() {
    let d = TypeDescriptor {
        column: "id".to_string(),
        pg_type_oid: 23,
        pg_type: "int4".to_string(),
        tier: Tier::One,
        arrow: "Int32".to_string(),
        duckdb: "INTEGER".to_string(),
        emit: vec!["id:INT32".to_string()],
        recombine: None,
        meta: TypeMeta::default(),
    };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
    assert_eq!(v["tier"], serde_json::json!(1));
    assert_eq!(v["recombine"], serde_json::Value::Null);

    let back: TypeDescriptor = serde_json::from_value(v).unwrap();
    assert_eq!(back, d);
}

#[test]
fn descriptor_without_meta_loads_as_no_metadata() {
    let d: TypeDescriptor = serde_json::from_str(LEGACY_SCALAR_DESCRIPTOR).unwrap();

    assert_eq!(d.meta, TypeMeta::default());
    assert_eq!(d.recombine, None);
    assert_eq!(d.tier, Tier::One);

    // The omitted key is a read-side allowance only: writing it back restores the §2.6 shape.
    let v: serde_json::Value = serde_json::to_value(&d).unwrap();
    assert_eq!(v["meta"]["enum_labels"], serde_json::Value::Null);
}

#[test]
fn a_missing_mapping_key_is_still_a_hard_error() {
    // Only `meta` defaults — dropping a mapping key must stay a loud decode failure, never a
    // silently wrong plan.
    for key in [
        "column",
        "pg_type_oid",
        "pg_type",
        "tier",
        "arrow",
        "duckdb",
        "emit",
    ] {
        let mut doc: serde_json::Value = serde_json::from_str(LEGACY_SCALAR_DESCRIPTOR).unwrap();
        doc.as_object_mut().unwrap().remove(key);

        let error = serde_json::from_value::<TypeDescriptor>(doc).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("missing field `{key}`")),
            "missing {key} must remain a hard error: {error}"
        );
    }
}

#[test]
fn type_meta_defaults_every_key() {
    // The all-optional metadata bag: an empty object is `TypeMeta::default()`, so a registry row
    // that predates a metadata key still loads.
    assert_eq!(
        serde_json::from_str::<TypeMeta>("{}").unwrap(),
        TypeMeta::default()
    );
}

#[test]
fn type_meta_carries_enum_labels() {
    let meta = TypeMeta {
        enum_labels: Some(vec![
            "happy".to_string(),
            "meh".to_string(),
            "sad".to_string(),
        ]),
        ..TypeMeta::default()
    };
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    assert_eq!(v["enum_labels"], serde_json::json!(["happy", "meh", "sad"]));
    assert_eq!(v["bit_length"], serde_json::Value::Null);
}

#[test]
fn type_meta_nonzero_lengths_keep_the_json_number_shape() {
    let meta = TypeMeta {
        bit_length: NonZeroU32::new(8),
        char_length: NonZeroU32::new(5),
        ..TypeMeta::default()
    };
    let value = serde_json::to_value(&meta).unwrap();
    assert_eq!(value["bit_length"], serde_json::json!(8));
    assert_eq!(value["char_length"], serde_json::json!(5));
    assert_eq!(serde_json::from_value::<TypeMeta>(value).unwrap(), meta);

    let invalid = serde_json::json!({
        "enum_labels": null,
        "bit_length": 0,
        "char_length": null,
        "money_fraction_digits": null
    });
    assert!(serde_json::from_value::<TypeMeta>(invalid).is_err());
}
