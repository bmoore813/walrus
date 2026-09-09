use super::*;
use std::num::NonZeroU32;

fn col(name: &str, oid: u32, typmod: i32) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    }
}

#[test]
fn interval_descriptor_has_three_emits_and_recombine() {
    let d = describe_column(&col("duration", oids::INTERVAL, -1)).unwrap();
    assert_eq!(d.tier, Tier::Two);
    assert_eq!(
        d.emit,
        vec![
            "duration_months:INT32",
            "duration_days:INT32",
            "duration_micros:INT64",
        ]
    );
    assert_eq!(
        d.recombine.as_deref(),
        Some("to_months(m)+to_days(d)+to_microseconds(us)")
    );
    assert_eq!(d.duckdb, "INTERVAL");
    assert_eq!(d.pg_type, "interval");
}

#[test]
fn enum_descriptor_carries_ordered_labels() {
    let labels = vec!["happy".to_string(), "meh".to_string(), "sad".to_string()];
    let d = describe_column_with_labels(&col("mood", 16400, -1), Some(labels.clone())).unwrap();
    assert_eq!(d.tier, Tier::Three);
    assert_eq!(d.emit, vec!["mood:VARCHAR"]);
    assert_eq!(d.duckdb, "VARCHAR");
    assert_eq!(d.pg_type, "enum");
    assert_eq!(d.meta.enum_labels, Some(labels));
    // The ordered set is the only populated meta field.
    assert_eq!(d.meta.bit_length, None);
    assert_eq!(d.meta.char_length, None);
}

#[test]
fn char_n_descriptor_carries_char_length() {
    // char(5) → bpchar, typmod = 5 + VARHDRSZ(4) = 9.
    let d = describe_column(&col("code", oids::BPCHAR, 9)).unwrap();
    assert_eq!(d.tier, Tier::One);
    assert_eq!(d.meta.char_length, NonZeroU32::new(5));
    assert_eq!(d.emit, vec!["code:VARCHAR"]);

    let invalid = describe_column(&col("code", oids::BPCHAR, 4)).unwrap();
    assert_eq!(invalid.meta.char_length, None);
}

#[test]
fn bit_descriptor_carries_bit_length() {
    // bit(8): atttypmod is the raw bit count (no VARHDRSZ), and it's a Tier-3 VARCHAR carrier.
    let d = describe_column(&col("flags", oids::BIT, 8)).unwrap();
    assert_eq!(d.tier, Tier::Three);
    assert_eq!(d.meta.bit_length, NonZeroU32::new(8));

    let invalid = describe_column(&col("flags", oids::BIT, 0)).unwrap();
    assert_eq!(invalid.meta.bit_length, None);
}

#[test]
fn tier_matches_emit_fields_for_every_supported_oid() {
    // Representative (oid, typmod) across all tiers/shapes. For each, the descriptor's emit[] must
    // equal emit_fields verbatim (they share one dispatch), and the tier must be sensible.
    let samples: &[(u32, i32)] = &[
        (oids::BOOL, -1),
        (oids::INT4, -1),
        (oids::NUMERIC, 655366), // numeric(10,2) → Tier-1
        (oids::NUMERIC, -1),     // unconstrained → Tier-3
        (oids::BPCHAR, 9),       // char(5) → Tier-1
        (oids::TIMESTAMPTZ, -1),
        (oids::INTERVAL, -1),
        (oids::TIMETZ, -1),
        (oids::INT4RANGE, -1),
        (oids::INT4MULTIRANGE, -1),
        (oids::POINT, -1),
        (oids::PATH, -1),
        (oids::BIT, 8),
        (oids::INET, -1),
        (oids::UUID, -1),
        (16400, -1), // enum
    ];
    for &(oid, typmod) in samples {
        let c = col("c", oid, typmod);
        let expected: Vec<String> = schema::emit_fields(&c)
            .unwrap()
            .iter()
            .map(|f| format!("{}:{}", f.name(), arrow_emit_name(f.data_type())))
            .collect();
        let d = describe_column(&c).unwrap();
        assert_eq!(d.emit, expected, "emit drift for oid {oid}");
        assert!(
            !d.emit.is_empty(),
            "supported oid {oid} must emit ≥1 column"
        );
        assert!(matches!(d.tier, Tier::One | Tier::Two | Tier::Three));
    }
}

#[test]
fn unmapped_builtin_oid_is_the_only_describe_failure() {
    // money (790) is a builtin with no mapping, and 790 < FIRST_NORMAL_OID so the enum rule does
    // not claim it either. Every descriptor field routes through `emit_fields`, so this is the one
    // failure `describe_column`'s `# Errors` promises for a `#[non_exhaustive]` enum.
    let err = describe_column(&col("price", 790, -1)).unwrap_err();
    assert!(matches!(
        err,
        Error::NotTier1 {
            oid: 790,
            typmod: -1
        }
    ));
}

#[test]
fn descriptor_json_roundtrips() {
    let d = describe_column(&col("duration", oids::INTERVAL, -1)).unwrap();
    let json = serde_json::to_string(&d).unwrap();
    let back: TypeDescriptor = serde_json::from_str(&json).unwrap();
    assert_eq!(back, d);
    // tier is the integer 2 in the §2.6 JSON shape.
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["tier"], serde_json::json!(2));
}
