use super::{
    VersionSchema, classify_manifest_get_error, classify_manifest_object_error,
    destination_columns_between, manifest_unit_target, pause_began, raw_append_lag_bytes,
    validate_object_fingerprint, validate_reload_manifest_at_f,
};
use crate::duck::{ReloadBuild, ReloadPhase};
use common::{
    EpochNo, Lsn, ManifestId, PgColumn, PgRelation, ReloadId, ReplicaIdentity, SchemaVersionNo,
    Tier, TypeDescriptor, TypeMeta, oids,
};
use std::cell::Cell;
use std::collections::BTreeMap;

fn relation(columns: &[&str]) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, name)| PgColumn {
                name: (*name).to_string(),
                type_oid: if index == 0 { 23 } else { 25 },
                type_modifier: -1,
                is_key: index == 0,
            })
            .collect(),
    }
}

fn range_descriptor(column: &str, emit_prefix: &str) -> TypeDescriptor {
    let emit = vec![
        format!("{emit_prefix}_lower:INT32"),
        format!("{emit_prefix}_upper:INT32"),
        format!("{emit_prefix}_lower_inc:BOOLEAN"),
        format!("{emit_prefix}_upper_inc:BOOLEAN"),
        format!("{emit_prefix}_empty:BOOLEAN"),
    ];
    TypeDescriptor {
        column: column.to_string(),
        pg_type_oid: oids::INT4RANGE,
        pg_type: "int4range".to_string(),
        tier: Tier::Two,
        arrow: "Struct/Decomposed".to_string(),
        duckdb: "IGNORED".to_string(),
        emit,
        recombine: None,
        meta: TypeMeta::default(),
    }
}

#[test]
fn empty_queue_is_zero_lag() {
    assert_eq!(raw_append_lag_bytes(None, Lsn::new(100)), 0);
}

#[test]
fn lag_is_head_minus_frontier() {
    assert_eq!(
        raw_append_lag_bytes(Some(Lsn::new(500)), Lsn::new(200)),
        300
    );
    assert_eq!(raw_append_lag_bytes(Some(Lsn::new(200)), Lsn::new(200)), 0);
}

#[test]
fn frontier_ahead_of_queue_saturates_to_zero() {
    // A just-advanced frontier can momentarily lead a stale MAX read — never underflow.
    assert_eq!(raw_append_lag_bytes(Some(Lsn::new(100)), Lsn::new(300)), 0);
}

#[test]
fn add_drop_lineage_does_not_position_zip_shifted_survivors() {
    let mut schemas = BTreeMap::new();
    for (version, columns) in [
        (1, &["id", "doomed", "tail"][..]),
        (2, &["id", "doomed", "tail", "extra"][..]),
        (3, &["id", "tail", "extra"][..]),
    ] {
        schemas.insert(
            SchemaVersionNo(version),
            VersionSchema::from_registry_parts(relation(columns), &[]).unwrap(),
        );
    }

    assert_eq!(
        destination_columns_between(SchemaVersionNo(1), SchemaVersionNo(3), &schemas).unwrap(),
        ["id", "doomed", "tail", "walrus_extractor_meta"],
        "the dropped column keeps its retained raw destination even though later attnums shift"
    );
    assert_eq!(
        destination_columns_between(SchemaVersionNo(2), SchemaVersionNo(3), &schemas).unwrap(),
        ["id", "doomed", "tail", "extra", "walrus_extractor_meta"],
        "a post-ADD child preserves every historical destination across the later DROP"
    );
}

#[test]
fn tier2_name_substitution_is_rejected_without_stable_column_lineage() {
    let mut old_relation = relation(&["id", "span"]);
    old_relation.columns[1].type_oid = oids::INT4RANGE;
    let mut new_relation = old_relation.clone();
    new_relation.columns[1].name = "time_window".to_string();

    let old = VersionSchema::from_registry_parts(old_relation, &[range_descriptor("span", "span")])
        .unwrap();
    let new = VersionSchema::from_registry_parts(
        new_relation,
        &[range_descriptor("time_window", "time_window")],
    )
    .unwrap();
    let schemas = BTreeMap::from([(SchemaVersionNo(1), old), (SchemaVersionNo(2), new)]);
    assert!(matches!(
        destination_columns_between(SchemaVersionNo(1), SchemaVersionNo(2), &schemas),
        Err(crate::error::TransformerError::ManifestInvariant { message })
            if message.contains("genuine RENAME and same-statement DROP+ADD")
    ));
}

#[test]
fn phase_a_registry_plan_requires_complete_descriptor_identity() {
    let mut rel = relation(&["id", "span"]);
    rel.columns[1].type_oid = oids::INT4RANGE;
    let descriptor = range_descriptor("span", "span");

    assert!(matches!(
        VersionSchema::from_registry_parts(
            rel.clone(),
            &[descriptor.clone(), descriptor.clone()]
        ),
        Err(crate::error::TransformerError::ManifestInvariant { message })
            if message.contains("duplicate descriptors")
    ));

    let mut unknown = descriptor.clone();
    unknown.column = "missing".to_string();
    assert!(matches!(
        VersionSchema::from_registry_parts(rel.clone(), &[unknown]),
        Err(crate::error::TransformerError::ManifestInvariant { message })
            if message.contains("descriptor for unknown column")
    ));

    let mut wrong_oid = descriptor;
    wrong_oid.pg_type_oid = oids::INT4;
    assert!(matches!(
        VersionSchema::from_registry_parts(rel, &[wrong_oid]),
        Err(crate::error::TransformerError::ManifestInvariant { message })
            if message.contains("has type OID")
    ));
}

#[test]
fn streamed_unit_uses_the_final_post_ddl_schema_without_a_post_ddl_row_file() {
    let file = control::ManifestRow {
        id: ManifestId(11),
        epoch: EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        s3_uri: "s3://test/pre-ddl.parquet".to_string(),
        kind: control::ManifestKind::Stream,
        row_count: 1,
        object_size: 128,
        sha256: vec![7; 32],
        lsn_start: Lsn::new(90),
        lsn_end: Lsn::new(100),
        schema_version: SchemaVersionNo(1),
        status: control::ManifestStatus::Ready,
        reload_id: None,
        stream_group_id: Some(control::ManifestGroupId(9)),
        stream_group_ordinal: Some(0),
        stream_commit_ts: Some("2026-01-01T00:00:00Z".to_string()),
        stream_top_xid: Some(42),
        stream_group_expected_files: Some(1),
        stream_group_row_count: Some(1),
        stream_group_final_schema_version: Some(SchemaVersionNo(2)),
    };

    assert_eq!(
        manifest_unit_target(&[&file]).unwrap(),
        SchemaVersionNo(2),
        "DDL after the transaction's last v1 row is still a v2 append barrier"
    );
}

#[test]
fn object_size_or_hash_mismatch_is_typed_corruption() {
    let expected = [7_u8; 32];
    assert!(validate_object_fingerprint("s3://b/k", 10, &expected, 10, &expected).is_ok());
    assert!(matches!(
        validate_object_fingerprint("s3://b/k", 10, &expected, 11, &expected),
        Err(crate::error::TransformerError::ObjectIntegrity { .. })
    ));
    assert!(matches!(
        validate_object_fingerprint("s3://b/k", 10, &expected, 10, &[8_u8; 32]),
        Err(crate::error::TransformerError::ObjectIntegrity { .. })
    ));
}

#[test]
fn missing_durable_object_is_typed_corruption_but_transport_failure_is_not() {
    let missing = object_store::Error::NotFound {
        path: "missing.parquet".to_string(),
        source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
    };
    assert!(matches!(
        classify_manifest_get_error("s3://b/missing.parquet", missing),
        crate::error::TransformerError::ObjectIntegrity { reason, .. }
            if reason == "durable manifest object is missing"
    ));

    let transport = object_store::Error::Generic {
        store: "test",
        source: Box::new(std::io::Error::from(std::io::ErrorKind::TimedOut)),
    };
    assert!(matches!(
        classify_manifest_get_error("s3://b/present.parquet", transport),
        crate::error::TransformerError::ObjectStore { .. }
    ));

    let missing_mid_stream = object_store::Error::NotFound {
        path: "vanished.parquet".to_string(),
        source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
    };
    assert!(matches!(
        classify_manifest_object_error(
            "s3://b/vanished.parquet",
            "stream manifest object",
            missing_mid_stream,
        ),
        crate::error::TransformerError::ObjectIntegrity { .. }
    ));
}

#[test]
fn pause_logs_once_per_pause_and_relatches_on_a_new_reload() {
    let latch = Cell::new(None);
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(7))),
        Some(common::ReloadId(7)),
        "a new pause logs"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(7))),
        None,
        "same pause: silent on later polls"
    );
    assert_eq!(
        pause_began(&latch, None),
        None,
        "lifted: silent, latch cleared"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(8))),
        Some(common::ReloadId(8)),
        "the next reload logs again"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(9))),
        Some(common::ReloadId(9)),
        "a superseding reload logs without an intervening lift"
    );
}

#[test]
fn active_reload_manifest_must_name_the_build_and_be_stamped_exactly_at_f() {
    let f = Lsn::new(100);
    let build = ReloadBuild {
        reload_id: ReloadId(7),
        shadow_table: "orders__reload_7".into(),
        schema_version: SchemaVersionNo(3),
        start_lsn: f,
        final_lsn: Lsn::new(200),
        publication_nonce: uuid::Uuid::from_u128(1),
        raw_appended_lsn: f,
        transformed_lsn: f,
        phase: ReloadPhase::Building,
    };
    let mut file = control::ManifestRow {
        id: ManifestId(11),
        epoch: EpochNo(1),
        source_schema: "public".into(),
        source_table: "orders".into(),
        s3_uri: "s3://test/reload.parquet".into(),
        kind: control::ManifestKind::Reload,
        row_count: 1,
        object_size: 128,
        sha256: vec![7; 32],
        lsn_start: f,
        lsn_end: f,
        schema_version: SchemaVersionNo(3),
        status: control::ManifestStatus::Ready,
        reload_id: Some(build.reload_id),
        stream_group_id: None,
        stream_group_ordinal: None,
        stream_commit_ts: None,
        stream_top_xid: None,
        stream_group_expected_files: None,
        stream_group_row_count: None,
        stream_group_final_schema_version: None,
    };

    validate_reload_manifest_at_f(&file, &build).unwrap();

    file.lsn_end = Lsn::new(201);
    assert!(
        validate_reload_manifest_at_f(&file, &build)
            .unwrap_err()
            .to_string()
            .contains("do not equal active reload"),
        "a malformed baseline above H must fail before the generic H barrier"
    );

    file.lsn_end = f;
    file.reload_id = Some(ReloadId(8));
    assert!(validate_reload_manifest_at_f(&file, &build).is_err());
}
