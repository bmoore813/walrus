use super::*;
use arrow::datatypes::DataType;
use common::{PgColumn, ReplicaIdentity, SchemaVersionNo};
use pg_to_arrow::oids;

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, typmod: i32, is_key: bool| PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key,
    };
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", oids::INT4, -1, true),
            col("amount", oids::NUMERIC, 655366, false), // numeric(10,2)
            col("created_at", oids::TIMESTAMPTZ, -1, false),
            col("note", oids::TEXT, -1, false),
        ],
    }
}

fn table_at(oid: u32, name: &str) -> PgRelation {
    let mut relation = orders();
    relation.oid = oid;
    relation.name = name.to_string();
    relation
}

#[test]
fn caches_arrow_schema_and_descriptors_by_versioned_key() {
    let mut cache = RelationCache::default();
    let entry = cache
        .upsert_from_relation(orders(), SchemaVersionNo(1))
        .unwrap();
    // Tier-1 schema: 4 data cols + the trailing meta col.
    assert_eq!(entry.arrow_schema.fields().len(), 5);
    assert_eq!(entry.arrow_schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(
        entry.arrow_schema.field(4).name(),
        pg_to_arrow::EXTRACTOR_META_COLUMN
    );
    assert_eq!(entry.descriptors.len(), 4);
    // Keyed by (oid, version): a lookup at a different version misses.
    assert!(cache.get(16397, SchemaVersionNo(1)).is_some());
    assert!(cache.get(16397, SchemaVersionNo(2)).is_none());
}

#[test]
fn rejects_a_source_column_that_would_require_identifier_quotes() {
    for name in ["CustomerID", "customer-id", "select"] {
        let mut relation = orders();
        relation.columns[0].name = name.to_string();
        let error = RelationCache::default()
            .upsert_from_relation(relation, SchemaVersionNo(1))
            .unwrap_err();
        assert!(matches!(error, RelationError::ColumnName { .. }), "{error}");
    }
}

#[test]
fn hydrate_round_trips_through_a_registry_row() {
    // Simulate what on_relation persists, then hydrate a fresh cache from it.
    let relation = orders();
    let descriptors = pg_to_arrow::describe_relation(&relation).unwrap();
    let row = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(3),
        descriptors: descriptors.clone(),
        columns: serde_json::to_value(&relation).unwrap(),
    };
    let mut cache = RelationCache::default();
    cache.hydrate(vec![row]).unwrap();
    let entry = cache
        .get(16397, SchemaVersionNo(3))
        .expect("hydrated entry");
    assert_eq!(entry.relation, relation);
    assert_eq!(entry.descriptors, descriptors);
    assert_eq!(entry.arrow_schema.fields().len(), 5);
}

#[test]
fn matching_relation_message_preserves_additive_bootstrap_metadata() {
    let relation = orders();
    let mut columns = serde_json::to_value(&relation).unwrap();
    columns["table_comment"] = serde_json::json!("orders table");
    columns["columns"][0]["comment"] = serde_json::json!("order key");
    let row = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(1),
        descriptors: pg_to_arrow::describe_relation(&relation).unwrap(),
        columns: columns.clone(),
    };
    let mut cache = RelationCache::default();
    cache.hydrate(vec![row]).unwrap();

    let entry = cache
        .upsert_from_relation(relation, SchemaVersionNo(1))
        .unwrap();

    assert_eq!(entry.registry_columns, columns);
}

#[test]
fn internal_tables_are_recognised() {
    assert!(is_internal_table("public", "walrus_heartbeat"));
    assert!(is_internal_table("public", "walrus_ddl_audit"));
    assert!(is_internal_table("public", "walrus_reload_signal"));
    assert!(is_internal_table("public", "walrus_reload_event"));
    assert!(!is_internal_table("public", "orders"));
    assert!(
        !is_internal_table("walrus", "walrus_reload_signal"),
        "schema-scoped"
    );
    assert!(!is_internal_table("walrus", "something_else"));
}

#[test]
fn collects_and_extends_like_a_collection() {
    let first = build_cached(orders(), SchemaVersionNo(1)).unwrap();
    let second = build_cached(orders(), SchemaVersionNo(2)).unwrap();
    let cache: RelationCache = [first, second].into_iter().collect();

    assert_eq!(cache.len(), 2);
    assert!(cache.get(16397, SchemaVersionNo(1)).is_some());
    assert!(cache.get(16397, SchemaVersionNo(2)).is_some());

    let mut grown = RelationCache::default();
    grown.extend([build_cached(orders(), SchemaVersionNo(3)).unwrap()]);
    assert_eq!(grown.len(), 1);
    assert!(grown.get(16397, SchemaVersionNo(3)).is_some());
}

#[test]
fn iterates_by_ref_by_mut_and_by_value() {
    let cache: RelationCache = [build_cached(orders(), SchemaVersionNo(7)).unwrap()]
        .into_iter()
        .collect();

    assert_eq!(cache.iter().count(), 1);
    assert_eq!((&cache).into_iter().count(), 1);

    let mut cache = cache;
    for relation in &mut cache {
        let before = Arc::strong_count(relation);
        let clone = Arc::clone(relation);
        assert_eq!(Arc::strong_count(relation), before + 1);
        drop(clone);
    }

    let versions: Vec<SchemaVersionNo> = cache
        .into_iter()
        .map(|relation| relation.schema_version)
        .collect();
    assert_eq!(versions, vec![SchemaVersionNo(7)]);
}

#[test]
fn hydrate_message_is_unchanged_on_a_malformed_snapshot() {
    let relation = orders();
    let good = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(1),
        descriptors: pg_to_arrow::describe_relation(&relation).unwrap(),
        columns: serde_json::to_value(relation).unwrap(),
    };
    let malformed = serde_json::json!({"not": "a PgRelation"});
    let source = serde_json::from_value::<PgRelation>(malformed.clone()).unwrap_err();
    let bad = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(2),
        descriptors: vec![],
        columns: malformed,
    };
    let mut cache = RelationCache::default();

    let error = cache.hydrate(vec![good, bad]).unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "hydrate from walrus_schema_registry: public.orders: columns snapshot is not a PgRelation: {source}"
        )
    );
    // The rendered sentence is only half of it: serde's own failure stays in the chain, so a
    // reporter can still reach its line/column and category instead of re-parsing the text.
    let cause = std::error::Error::source(&error).expect("hydrate keeps the decode error");
    assert!(
        cause.downcast_ref::<serde_json::Error>().is_some(),
        "the decode failure must stay downcastable"
    );
    assert_eq!(cause.to_string(), source.to_string());
    assert!(
        cache.is_empty(),
        "hydration must not partially update the cache"
    );
}

#[test]
fn latest_for_picks_the_highest_version_per_oid_across_interleaved_tables() {
    let mut cache = RelationCache::default();
    for (oid, name, version) in [
        (16397, "orders", 3),
        (16400, "customers", 1),
        (16397, "orders", 1),
        (16401, "products", 7),
        (16397, "orders", 2),
        (16400, "customers", 5),
    ] {
        cache
            .upsert_from_relation(table_at(oid, name), SchemaVersionNo(version))
            .unwrap();
    }

    assert_eq!(
        cache.latest_for(16397).unwrap().schema_version,
        SchemaVersionNo(3)
    );
    assert_eq!(
        cache.latest_for(16400).unwrap().schema_version,
        SchemaVersionNo(5)
    );
    assert_eq!(
        cache.latest_for(16401).unwrap().schema_version,
        SchemaVersionNo(7)
    );
    assert!(cache.latest_for(9999).is_none());
}

#[test]
fn latest_for_respects_neighbour_and_full_integer_range_edges() {
    let mut cache = RelationCache::default();
    for (oid, name, version) in [
        (42, "lower", i64::MIN),
        (43, "neighbour", i64::MAX),
        (u32::MAX, "max_oid", i64::MIN),
        (u32::MAX, "max_oid", i64::MAX),
    ] {
        cache
            .upsert_from_relation(table_at(oid, name), SchemaVersionNo(version))
            .unwrap();
    }

    assert_eq!(
        cache.latest_for(42).unwrap().schema_version,
        SchemaVersionNo(i64::MIN)
    );
    assert_eq!(
        cache.latest_for(43).unwrap().schema_version,
        SchemaVersionNo(i64::MAX)
    );
    assert_eq!(
        cache.latest_for(u32::MAX).unwrap().schema_version,
        SchemaVersionNo(i64::MAX)
    );
}

#[test]
fn named_iterators_preserve_items_and_follow_key_order() {
    let entries = [
        build_cached(table_at(9, "nine"), SchemaVersionNo(3)).unwrap(),
        build_cached(table_at(2, "two"), SchemaVersionNo(7)).unwrap(),
        build_cached(table_at(9, "nine"), SchemaVersionNo(-1)).unwrap(),
        build_cached(table_at(3, "three"), SchemaVersionNo(0)).unwrap(),
    ];
    let mut cache: RelationCache = entries.into_iter().collect();
    let expected = vec![
        (2, SchemaVersionNo(7)),
        (3, SchemaVersionNo(0)),
        (9, SchemaVersionNo(-1)),
        (9, SchemaVersionNo(3)),
    ];

    // The annotations are the assertion: each iterator is named after the method that builds it,
    // and none of them spell the private `(u32, SchemaVersionNo)` key or the backing `BTreeMap`.
    let values: Iter<'_> = cache.iter();
    assert_eq!(
        values
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );

    let shared: Iter<'_> = (&cache).into_iter();
    assert_eq!(
        shared
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );

    let mutable: IterMut<'_> = (&mut cache).into_iter();
    assert_eq!(
        mutable
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );

    let owned: IntoIter = cache.into_iter();
    assert_eq!(
        owned
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );
}
