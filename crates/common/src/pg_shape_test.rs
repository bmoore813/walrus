use super::*;

fn col(name: &str, type_oid: u32, type_modifier: i32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid,
        type_modifier,
        is_key,
    }
}

#[test]
fn replica_identity_try_from_char() {
    assert_eq!(
        ReplicaIdentity::try_from(b'd').unwrap(),
        ReplicaIdentity::Default
    );
    let nothing: ReplicaIdentity = b'n'.try_into().unwrap();
    assert_eq!(nothing, ReplicaIdentity::Nothing);
    assert_eq!(
        ReplicaIdentity::try_from(b'f').unwrap(),
        ReplicaIdentity::Full
    );
    let index: ReplicaIdentity = b'i'.try_into().unwrap();
    assert_eq!(index, ReplicaIdentity::Index);
    assert!(ReplicaIdentity::try_from(b'x').is_err());
    assert!(ReplicaIdentity::try_from(0).is_err());
}

#[test]
fn replica_identity_parses_the_catalog_cast_not_the_serde_word() {
    // `relreplident::text` is the byte above in its catalog-cast form, so both readers agree.
    for (text, expected) in [
        ("d", ReplicaIdentity::Default),
        ("n", ReplicaIdentity::Nothing),
        ("f", ReplicaIdentity::Full),
        ("i", ReplicaIdentity::Index),
    ] {
        assert_eq!(text.parse::<ReplicaIdentity>().unwrap(), expected);
    }

    // The persisted word form is a different wire form and must not sneak in through this door.
    assert!("default".parse::<ReplicaIdentity>().is_err());
    assert!("".parse::<ReplicaIdentity>().is_err());
    assert!("dd".parse::<ReplicaIdentity>().is_err());
    assert!("x".parse::<ReplicaIdentity>().is_err());
}

#[test]
fn replica_identity_wire_form_is_lowercase_and_accepts_legacy() {
    let cases = [
        (ReplicaIdentity::Default, "\"default\"", "\"Default\""),
        (ReplicaIdentity::Nothing, "\"nothing\"", "\"Nothing\""),
        (ReplicaIdentity::Full, "\"full\"", "\"Full\""),
        (ReplicaIdentity::Index, "\"index\"", "\"Index\""),
    ];

    for (variant, lowercase, legacy) in cases {
        assert_eq!(serde_json::to_string(&variant).unwrap(), lowercase);
        assert_eq!(
            serde_json::from_str::<ReplicaIdentity>(lowercase).unwrap(),
            variant
        );
        assert_eq!(
            serde_json::from_str::<ReplicaIdentity>(legacy).unwrap(),
            variant
        );
    }

    assert!(serde_json::from_str::<ReplicaIdentity>("\"partial\"").is_err());
}

#[test]
fn a_legacy_registry_columns_document_still_hydrates() {
    let legacy = r#"{
        "oid": 42,
        "schema": "public",
        "name": "orders",
        "replica_identity": "Default",
        "columns": [{"name":"id","type_oid":23,"type_modifier":-1,"is_key":true}]
    }"#;

    let relation: PgRelation = serde_json::from_str(legacy).unwrap();
    assert_eq!(relation.replica_identity, ReplicaIdentity::Default);

    let serialized = serde_json::to_value(relation).unwrap();
    assert_eq!(serialized["replica_identity"], "default");
}

#[test]
fn numeric_typmod_decodes_precision_and_scale() {
    // The proto §4 example: atttypmod 655366 → numeric(10, 2).
    let c = col("amount", crate::oids::NUMERIC, 655366, false);
    assert_eq!(c.numeric_precision_scale(), Some((10, 2)));

    // Unconstrained numeric → None (no panic).
    assert_eq!(
        col("n", crate::oids::NUMERIC, -1, false).numeric_precision_scale(),
        None
    );

    // A non-numeric column with a typmod (e.g. varchar) → None.
    assert_eq!(
        col("label", 1043, 259, false).numeric_precision_scale(),
        None
    );
}

#[test]
fn key_columns_preserve_relation_order() {
    // customers has a COMPOSITE PK (region, id); order must be preserved.
    let rel = PgRelation {
        oid: 42,
        schema: "public".to_string(),
        name: "customers".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("region", 25, -1, true),
            col("id", 23, -1, true),
            col("name", 25, -1, false),
        ],
    };
    assert_eq!(rel.to_key_columns(), vec!["region", "id"]);

    // A key column declared after a non-key one still keeps relation order.
    let rel2 = PgRelation {
        columns: vec![
            col("a", 23, -1, false),
            col("b", 23, -1, true),
            col("c", 23, -1, true),
        ],
        ..rel
    };
    assert_eq!(rel2.to_key_columns(), vec!["b", "c"]);
}

#[test]
fn key_columns_have_no_walrus_specific_width_limit() {
    const POSTGRES_MAX_INDEX_KEYS: usize = 32;
    let columns = (1..=POSTGRES_MAX_INDEX_KEYS)
        .map(|index| col(&format!("key_{index:02}"), 23, -1, true))
        .chain(std::iter::once(col("payload", 25, -1, false)))
        .collect();
    let rel = PgRelation {
        oid: 43,
        schema: "public".to_string(),
        name: "wide_keys".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    };

    let keys = rel.to_key_columns();
    assert_eq!(keys.len(), POSTGRES_MAX_INDEX_KEYS);
    assert_eq!(keys.first(), Some(&"key_01"));
    assert_eq!(keys.last(), Some(&"key_32"));
    assert!(!keys.contains(&"payload"));
}

#[test]
fn tuple_value_null_and_unchanged_toast_are_distinct() {
    assert_ne!(TupleValue::Null, TupleValue::UnchangedToast);
    assert_eq!(TupleValue::Null, TupleValue::Null);
    assert_eq!(
        TupleValue::Text("x".to_string()),
        TupleValue::Text("x".to_string())
    );
    // Binary carries bytes zero-copy.
    assert_eq!(
        TupleValue::Binary(Bytes::from_static(b"\x00\x01")),
        TupleValue::Binary(Bytes::from_static(b"\x00\x01"))
    );
    assert_ne!(
        TupleValue::Binary(Bytes::from_static(b"\x00")),
        TupleValue::Null
    );
}

#[test]
fn tuple_value_size_budget() {
    // The const assert in `pg_shape` is the real gate; this exists so a breach prints the number.
    assert!(
        size_of::<TupleValue>() <= super::TUPLE_VALUE_MAX_BYTES,
        "TupleValue grew to {} bytes (budget {}) — see own-move-large",
        size_of::<TupleValue>(),
        super::TUPLE_VALUE_MAX_BYTES,
    );
}
