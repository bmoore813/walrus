use super::*;

fn ddl_audit_rel() -> PgRelation {
    let col = |name: &str| PgColumn {
        name: name.into(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    };
    PgRelation {
        oid: 90_002,
        schema: "public".into(),
        name: "walrus_ddl_audit".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: [
            "id",
            "c_lsn",
            "c_event",
            "c_tag",
            "ts",
            "c_schema",
            "c_table",
            "c_columns",
            "c_dropped",
            "c_rel_oid",
            "c_replica_identity",
            "c_ddl_text",
            "c_table_comment",
        ]
        .into_iter()
        .map(col)
        .collect(),
    }
}

fn tuple(id: i64, tag: &str, columns: &str) -> Vec<TupleValue> {
    vec![
        TupleValue::Text(id.to_string()),
        TupleValue::Text("1/AB".into()),
        TupleValue::Text("ddl_command_end".into()),
        TupleValue::Text(tag.into()),
        TupleValue::Text("2026-07-07T12:00:00Z".into()),
        TupleValue::Text("public".into()),
        TupleValue::Text("orders".into()),
        TupleValue::Text(columns.into()),
        TupleValue::Null,
        TupleValue::Text("42".into()),
        TupleValue::Text("d".into()),
        TupleValue::Text("ALTER TABLE orders ADD COLUMN extra text".into()),
        TupleValue::Text("customer orders".into()),
    ]
}

fn event(id: i64, schema: &str, table: &str, tag: &str) -> DdlEvent {
    DdlEvent {
        source_audit_id: id,
        capture_lsn: Lsn::new(u64::try_from(id).unwrap()),
        c_event: "ddl_command_end".into(),
        c_tag: tag.into(),
        source_schema: schema.into(),
        source_table: table.into(),
        c_rel_oid: Some(42),
        c_replica_identity: Some(ReplicaIdentity::Default),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: Some(format!("{tag} {schema}.{table}")),
        c_table_comment: None,
    }
}

fn registry(table: &str, version: i64) -> control::RegistryRow {
    control::RegistryRow {
        epoch: EpochNo(1),
        source_schema: "public".into(),
        source_table: table.into(),
        schema_version: SchemaVersionNo(version),
        descriptors: Vec::new(),
        columns: serde_json::json!({"name": table, "version": version}),
    }
}

fn tracked_orders() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
    }
}

fn disconnected_pool() -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(100))
        .connect_lazy("postgres://walrus@127.0.0.1:1/unused")
        .expect("a lazy pool parses its DSN without connecting")
}

#[test]
fn ddl_audit_insert_parses_identity_snapshot_and_audit_sql() {
    let ev = DdlEvent::from_tuple(
        &ddl_audit_rel(),
        &tuple(
            17,
            "ALTER TABLE",
            r#"[{"name":"id","type_oid":23,"type_modifier":-1,"is_key":true}]"#,
        ),
    )
    .unwrap();
    assert_eq!(ev.source_audit_id, 17);
    assert_eq!(ev.capture_lsn, "1/AB".parse().unwrap());
    assert_eq!(ev.c_rel_oid, Some(42));
    assert_eq!(ev.c_replica_identity, Some(ReplicaIdentity::Default));
    assert_eq!(ev.c_table_comment.as_deref(), Some("customer orders"));
    assert_eq!(
        ev.c_ddl_text.as_deref(),
        Some("ALTER TABLE orders ADD COLUMN extra text")
    );
}

#[test]
fn malformed_column_snapshot_stays_the_json_class() {
    let err =
        DdlEvent::from_tuple(&ddl_audit_rel(), &tuple(1, "ALTER TABLE", "{not json")).unwrap_err();
    assert!(matches!(&err, DdlError::Json(_)));
    assert!(err.to_string().starts_with("parse ddl_audit json: "));
}

#[test]
fn relation_snapshot_preserves_key_from_catalog_and_previous_shape() {
    let previous = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
    };
    let mut ev = event(1, "public", "orders", "ALTER TABLE");
    ev.c_columns = Some(serde_json::json!([
        {"name":"id", "type_oid":23, "type_modifier":-1},
        {"name":"extra", "type_oid":25, "type_modifier":-1, "is_key":false}
    ]));
    let after = ev.relation_after(Some(&previous)).unwrap().unwrap();
    assert!(
        after.columns[0].is_key,
        "old trigger snapshots inherit key flags"
    );
    assert!(!after.columns[1].is_key);
    assert_eq!(after.columns.len(), 2);
}

#[test]
fn comment_is_metadata_only_and_does_not_advance_projected_version() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let observation = consumer.observe(
        TransactionScope::Ordinary,
        event(1, "public", "orders", "COMMENT"),
        None,
    );
    assert_eq!(observation.structural_version, None);
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(1));
    assert!(
        !consumer.is_provisional("public", "orders", SchemaVersionNo(1)),
        "COMMENT audit state must not commit-gate an unchanged relation snapshot"
    );
    consumer.stage_registry(TransactionScope::Ordinary, registry("orders", 1));
    let prepared = consumer.prepare_ordinary_commit(Lsn::new(200)).unwrap();
    assert!(!prepared.has_structural_ddl());
    assert_eq!(prepared.ddl_rows()[0].c_tag, "COMMENT");
    assert!(
        prepared.registry_rows().is_empty(),
        "COMMENT cannot own a registry row even through the direct staging API"
    );
}

#[test]
fn structural_registry_stays_shape_only_while_ddl_retains_comments() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let mut ddl = event(9, "public", "orders", "ALTER TABLE");
    ddl.c_table_comment = Some("orders table".into());
    ddl.c_columns = Some(serde_json::json!([
        {
            "name": "id",
            "type_oid": 23,
            "type_modifier": -1,
            "is_key": true,
            "comment": "order key"
        }
    ]));
    consumer.observe(TransactionScope::Ordinary, ddl, Some(&tracked_orders()));
    let mut row = registry("orders", 2);
    row.columns = serde_json::to_value(tracked_orders()).unwrap();
    consumer.stage_registry(TransactionScope::Ordinary, row);

    let prepared = consumer.prepare_ordinary_commit(Lsn::new(900)).unwrap();
    let columns = &prepared.registry_rows()[0].columns;
    assert!(columns.get("table_comment").is_none());
    assert!(columns["columns"][0].get("comment").is_none());
    assert_eq!(
        prepared.ddl_rows()[0].c_table_comment.as_deref(),
        Some("orders table")
    );
    assert_eq!(
        prepared.ddl_rows()[0].c_columns.as_ref().unwrap()[0]["comment"],
        "order key"
    );
}

#[test]
fn provisional_version_is_visible_only_inside_its_streamed_transaction() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let owner = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    let neighbour = TransactionScope::Streamed {
        top_xid: 200,
        sub_xid: 200,
    };
    let observation = consumer.observe(owner, event(1, "public", "orders", "ALTER TABLE"), None);
    assert_eq!(observation.structural_version, Some(SchemaVersionNo(2)));
    assert_eq!(
        consumer.version_for(owner, "public", "orders"),
        SchemaVersionNo(2)
    );
    assert_eq!(
        consumer.version_for(neighbour, "public", "orders"),
        SchemaVersionNo(1)
    );
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1)
    );
}

#[test]
fn streamed_prepare_is_read_only_and_durable_replay_finalization_is_idempotent() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    consumer.observe(scope, event(7, "public", "orders", "ALTER TABLE"), None);
    consumer.stage_registry(scope, registry("orders", 2));

    let prepared = consumer.prepare_stream_commit(100, Lsn::new(900)).unwrap();
    assert_eq!(prepared.ddl_rows().len(), 1);
    assert_eq!(prepared.ddl_rows()[0].source_audit_id, 7);
    assert_eq!(prepared.ddl_rows()[0].c_lsn, Lsn::new(900));
    assert_eq!(prepared.registry_rows(), &[registry("orders", 2)]);
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1),
        "a failed/rolled-back publication must leave the DDL provisional"
    );
    assert_eq!(
        consumer.prepare_stream_commit(100, Lsn::new(900)).unwrap(),
        prepared,
        "the same pending transaction can be prepared again after a failed publication"
    );

    consumer.finalize_stream_commit(prepared.clone());
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(2)
    );
    assert!(consumer.pending.is_empty());
    assert!(consumer.pending_registry.is_empty());
    assert_eq!(consumer.processed.get(&7), Some(&prepared.ddl_rows()[0]));

    consumer.finalize_stream_commit(prepared.clone());
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(2),
        "AlreadyPublished replay finalization must be idempotent"
    );
    assert_eq!(consumer.processed.get(&7), Some(&prepared.ddl_rows()[0]));
}

#[test]
fn ordinary_prepare_is_read_only_and_lost_ack_finalization_is_idempotent() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    consumer.observe(
        TransactionScope::Ordinary,
        event(8, "public", "orders", "ALTER TABLE"),
        None,
    );
    consumer.stage_registry(TransactionScope::Ordinary, registry("orders", 2));

    let prepared = consumer.prepare_ordinary_commit(Lsn::new(901)).unwrap();
    assert!(prepared.has_structural_ddl());
    assert_eq!(prepared.ddl_rows().len(), 1);
    assert_eq!(prepared.ddl_rows()[0].source_audit_id, 8);
    assert_eq!(prepared.ddl_rows()[0].c_lsn, Lsn::new(901));
    assert_eq!(prepared.registry_rows(), &[registry("orders", 2)]);
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1),
        "a failed or ambiguously acknowledged publication must remain retryable"
    );
    assert_eq!(
        consumer.prepare_ordinary_commit(Lsn::new(901)).unwrap(),
        prepared,
        "WAL replay must rebuild the exact same ordinary publication payload"
    );

    consumer.finalize_ordinary_commit(prepared.clone());
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(2)
    );
    assert!(consumer.pending.is_empty());
    assert!(consumer.pending_registry.is_empty());
    assert_eq!(consumer.processed.get(&8), Some(&prepared.ddl_rows()[0]));

    consumer.finalize_ordinary_commit(prepared.clone());
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(2),
        "AlreadyPublished ordinary replay finalization must be idempotent"
    );
    assert_eq!(consumer.processed.get(&8), Some(&prepared.ddl_rows()[0]));
}

#[test]
fn ordinary_structural_prepare_requires_publication_even_without_registry() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    consumer.observe(
        TransactionScope::Ordinary,
        event(9, "public", "orders", "ALTER TABLE"),
        None,
    );

    let prepared = consumer.prepare_ordinary_commit(Lsn::new(902)).unwrap();
    assert!(
        prepared.has_structural_ddl(),
        "selection is driven by structural DDL, so control can reject the missing registry"
    );
    assert!(prepared.registry_rows().is_empty());
}

#[test]
fn subtransaction_abort_removes_only_its_schema_version() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let orders_scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    let items_scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 102,
    };
    consumer.observe(
        orders_scope,
        event(1, "public", "orders", "ALTER TABLE"),
        None,
    );
    consumer.observe(
        items_scope,
        event(2, "public", "items", "ALTER TABLE"),
        None,
    );
    consumer.stage_registry(orders_scope, registry("orders", 2));
    consumer.stage_registry(items_scope, registry("items", 2));

    let removed = consumer.on_stream_abort(100, 101);
    assert_eq!(
        removed,
        vec![("public".into(), "orders".into(), SchemaVersionNo(2))]
    );
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(1));
    assert_eq!(consumer.version_of("public", "items"), SchemaVersionNo(2));
    assert_eq!(consumer.pending_registry.len(), 1);
    assert_eq!(consumer.pending_registry[0].row.source_table, "items");
}

#[test]
fn whole_stream_abort_removes_every_provisional_ddl_for_the_top_xid() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    for (id, sub) in [(1, 101), (2, 102)] {
        consumer.observe(
            TransactionScope::Streamed {
                top_xid: 100,
                sub_xid: sub,
            },
            event(id, "public", "orders", "ALTER TABLE"),
            None,
        );
    }
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(3));
    assert_eq!(consumer.on_stream_abort(100, 100).len(), 2);
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(1));
}

#[test]
fn lost_ack_streamed_ddl_rebuilds_exact_pending_state_without_future_version_fallback() {
    let mut consumer = DdlConsumer::new(EpochNo(7));
    let durable_v2 = control::DdlRow {
        id: DdlId(9),
        epoch: EpochNo(7),
        source_audit_id: 55,
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_lsn: Lsn::new(900),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE public.orders".into()),
        c_table_comment: None,
    };
    let durable_future = control::DdlRow {
        id: DdlId(10),
        source_audit_id: 56,
        c_lsn: Lsn::new(1_000),
        schema_version: SchemaVersionNo(3),
        ..durable_v2.clone()
    };
    consumer.hydrate_history(vec![durable_v2.clone(), durable_future]);

    let scope = TransactionScope::Streamed {
        top_xid: 857,
        sub_xid: 858,
    };
    let observation = consumer.observe(scope, event(55, "public", "orders", "ALTER TABLE"), None);
    consumer.stage_registry(
        scope,
        control::RegistryRow {
            epoch: EpochNo(7),
            ..registry("orders", 2)
        },
    );

    assert!(observation.replay);
    assert_eq!(observation.structural_version, Some(SchemaVersionNo(2)));
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(3),
        "startup hydration may already contain a later durable DDL"
    );
    assert_eq!(
        consumer.version_for(scope, "public", "orders"),
        SchemaVersionNo(2),
        "the replaying transaction must bind its historical version, never committed max v3"
    );

    let prepared = consumer
        .prepare_stream_commit(857, durable_v2.c_lsn)
        .unwrap();
    assert!(prepared.has_structural_ddl());
    assert_eq!(prepared.ddl_rows().len(), 1);
    assert_eq!(prepared.ddl_rows()[0].source_audit_id, 55);
    assert_eq!(prepared.ddl_rows()[0].schema_version, SchemaVersionNo(2));
    assert_eq!(prepared.ddl_rows()[0].c_lsn, durable_v2.c_lsn);
    assert_eq!(prepared.ddl_rows()[0].c_tag, durable_v2.c_tag);
    assert_eq!(prepared.registry_rows().len(), 1);
    assert_eq!(
        prepared.registry_rows()[0].schema_version,
        SchemaVersionNo(2)
    );
    assert_eq!(
        consumer
            .prepare_stream_commit(857, durable_v2.c_lsn)
            .unwrap(),
        prepared,
        "a failed retry remains exactly reproducible before durable publication"
    );

    consumer.finalize_stream_commit(prepared);
    assert!(consumer.pending.is_empty());
    assert!(consumer.pending_registry.is_empty());
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(3),
        "finalizing an older lost-ACK replay cannot roll committed history backward"
    );
}

#[test]
fn relation_binding_uses_exact_historical_shape_and_rejects_future_fallback() {
    let mut v1 = tracked_orders();
    let mut v2 = tracked_orders();
    v2.columns.push(PgColumn {
        name: "v2".into(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    });
    let mut v3 = v2.clone();
    v3.columns.push(PgColumn {
        name: "v3".into(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    });
    let mut cache = RelationCache::default();
    for (relation, version) in [
        (v1.clone(), SchemaVersionNo(1)),
        (v2.clone(), SchemaVersionNo(2)),
        (v3.clone(), SchemaVersionNo(3)),
    ] {
        cache.upsert_from_relation(relation, version).unwrap();
    }

    let mut consumer = DdlConsumer::new(EpochNo(7));
    let durable_v2 = control::DdlRow {
        id: DdlId(9),
        epoch: EpochNo(7),
        source_audit_id: 55,
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_lsn: Lsn::new(900),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE public.orders".into()),
        c_table_comment: None,
    };
    let durable_v3 = control::DdlRow {
        id: DdlId(10),
        source_audit_id: 56,
        schema_version: SchemaVersionNo(3),
        c_lsn: Lsn::new(1_000),
        ..durable_v2.clone()
    };
    consumer.hydrate_history(vec![durable_v2, durable_v3]);
    let scope = TransactionScope::Streamed {
        top_xid: 857,
        sub_xid: 858,
    };

    assert_eq!(
        consumer
            .relation_version_for(scope, &v1, Lsn::new(800), &cache)
            .unwrap(),
        SchemaVersionNo(1),
        "a pre-DDL Relation in replay binds by its old shape, not committed max v3"
    );
    consumer.observe(
        scope,
        event(55, "public", "orders", "ALTER TABLE"),
        Some(&v1),
    );
    assert_eq!(
        consumer
            .relation_version_for(scope, &v2, Lsn::new(875), &cache)
            .unwrap(),
        SchemaVersionNo(2)
    );
    assert!(matches!(
        consumer.relation_version_for(scope, &v3, Lsn::new(875), &cache),
        Err(DdlError::RelationVersionBinding {
            scoped_version: Some(SchemaVersionNo(2)),
            ..
        })
    ));

    // Without transaction-local evidence, use the durable DDL commit boundary rather than the
    // hydrated maximum, even when two versions have byte-for-byte identical wire shapes.
    v1.columns = v2.columns.clone();
    cache
        .upsert_from_relation(v1.clone(), SchemaVersionNo(1))
        .unwrap();
    let neighbour = TransactionScope::Streamed {
        top_xid: 999,
        sub_xid: 999,
    };
    assert_eq!(
        consumer
            .relation_version_for(neighbour, &v1, Lsn::new(800), &cache)
            .unwrap(),
        SchemaVersionNo(1)
    );
    assert_eq!(
        consumer
            .relation_version_for(neighbour, &v1, Lsn::new(950), &cache)
            .unwrap(),
        SchemaVersionNo(2)
    );
    assert_eq!(
        consumer
            .relation_version_for(neighbour, &v2, Lsn::new(900), &cache)
            .unwrap(),
        SchemaVersionNo(2),
        "the decode commit frontier binds v2 even when its Relation transport frame had wal_start=0"
    );
}

#[tokio::test]
async fn ordinary_structural_ddl_with_routed_data_fails_before_control_publication() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    consumer.observe(
        TransactionScope::Ordinary,
        event(8, "public", "orders", "ALTER TABLE"),
        None,
    );
    consumer.stage_registry(TransactionScope::Ordinary, registry("orders", 2));

    let err = consumer
        .on_commit(
            &disconnected_pool(),
            77,
            Lsn::new(901),
            UtcTimestamp::now(),
            true,
            0,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        DdlError::MixedOrdinaryDataAndStructuralDdl {
            top_xid: 77,
            commit_lsn,
        } if commit_lsn == Lsn::new(901)
    ));
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1),
        "the rejected source commit must not become visible"
    );
    assert_eq!(
        consumer
            .prepare_ordinary_commit(Lsn::new(901))
            .unwrap()
            .registry_rows(),
        &[registry("orders", 2)],
        "the rejected publication must remain exactly replayable"
    );
}

#[tokio::test]
async fn ordinary_structural_ddl_with_reload_effects_fails_before_control_publication() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    consumer.observe(
        TransactionScope::Ordinary,
        event(8, "public", "orders", "ALTER TABLE"),
        None,
    );
    consumer.stage_registry(TransactionScope::Ordinary, registry("orders", 2));

    let err = consumer
        .on_commit(
            &disconnected_pool(),
            77,
            Lsn::new(901),
            UtcTimestamp::now(),
            false,
            2,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        DdlError::MixedOrdinaryReloadEffectsAndStructuralDdl {
            top_xid: 77,
            commit_lsn,
            committed_reload_effects: 2,
        } if commit_lsn == Lsn::new(901)
    ));
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1)
    );
    assert_eq!(
        consumer
            .prepare_ordinary_commit(Lsn::new(901))
            .unwrap()
            .registry_rows(),
        &[registry("orders", 2)],
        "the rejected source commit must remain exactly replayable"
    );
}

#[tokio::test]
async fn ordinary_structural_publication_failure_keeps_commit_provisional() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    consumer.observe(
        TransactionScope::Ordinary,
        event(8, "public", "orders", "ALTER TABLE"),
        None,
    );
    consumer.stage_registry(TransactionScope::Ordinary, registry("orders", 2));

    let err = consumer
        .on_commit(
            &disconnected_pool(),
            77,
            Lsn::new(901),
            UtcTimestamp::now(),
            false,
            0,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DdlError::Control(_)));
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1)
    );
    let retry = consumer.prepare_ordinary_commit(Lsn::new(901)).unwrap();
    assert_eq!(retry.ddl_rows().len(), 1);
    assert_eq!(retry.registry_rows(), &[registry("orders", 2)]);
}

#[tokio::test]
async fn committed_tracked_table_rename_fails_before_control_persistence() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let previous = tracked_orders();
    let rename = event(1, "public", "orders_v2", "ALTER TABLE");

    let observation = consumer.observe(TransactionScope::Ordinary, rename, Some(&previous));
    assert_eq!(observation.structural_version, Some(SchemaVersionNo(2)));

    let err = consumer
        .on_commit(
            &disconnected_pool(),
            10,
            Lsn::new(10),
            UtcTimestamp::now(),
            false,
            0,
        )
        .await
        .unwrap_err();
    let DdlError::TrackedTableIdentityChange(change) = err else {
        panic!("expected tracked identity error, got {err:?}");
    };
    assert_eq!(change.kind, TrackedTableIdentityChangeKind::Renamed);
    assert_eq!(change.relation_oid, 42);
    assert_eq!(change.previous_schema, "public");
    assert_eq!(change.previous_table, "orders");
    assert_eq!(change.new_schema.as_deref(), Some("public"));
    assert_eq!(change.new_table.as_deref(), Some("orders_v2"));
}

#[test]
fn legacy_null_oid_drop_is_commit_gated_as_a_tracked_identity_change() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let previous = tracked_orders();
    let mut dropped = event(1, "public", "orders", "DROP TABLE");
    dropped.c_event = "sql_drop".into();
    dropped.c_rel_oid = None;

    consumer.observe(TransactionScope::Ordinary, dropped, Some(&previous));
    let error = consumer.prepare_ordinary_commit(Lsn::new(10)).unwrap_err();
    assert!(matches!(
        error,
        DdlError::TrackedTableIdentityChange(TrackedTableIdentityChange {
            kind: TrackedTableIdentityChangeKind::Dropped,
            relation_oid: 42,
            ..
        })
    ));
}

#[test]
fn same_name_new_identity_is_commit_gated_as_recreation() {
    let previous = tracked_orders();
    for tag in ["CREATE TABLE AS", "SELECT INTO"] {
        for incoming_oid in [Some(43), None] {
            let mut consumer = DdlConsumer::new(EpochNo(1));
            let mut recreated = event(1, "public", "orders", tag);
            recreated.c_rel_oid = incoming_oid;

            consumer.observe(TransactionScope::Ordinary, recreated, Some(&previous));
            let error = consumer.prepare_ordinary_commit(Lsn::new(10)).unwrap_err();
            assert!(matches!(
                error,
                DdlError::TrackedTableIdentityChange(TrackedTableIdentityChange {
                    kind: TrackedTableIdentityChangeKind::Recreated,
                    relation_oid: 42,
                    ..
                })
            ));
        }
    }
}

#[test]
fn unresolved_legacy_identity_fails_on_commit_but_stream_abort_discards_it() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    let mut ambiguous = event(7, "archive", "orders", "ALTER TABLE");
    ambiguous.c_rel_oid = None;
    consumer.observe_unresolved_identity(scope, ambiguous);

    let error = consumer
        .prepare_stream_commit(100, Lsn::new(10))
        .unwrap_err();
    assert!(matches!(
        error,
        DdlError::UnresolvedRelationIdentity {
            source_audit_id: 7,
            ref schema,
            ref table,
            ref c_tag,
            relation_oid: None,
        } if schema == "archive" && table == "orders" && c_tag == "ALTER TABLE"
    ));

    assert_eq!(
        consumer.on_stream_abort(100, 101),
        vec![("archive".into(), "orders".into(), SchemaVersionNo(2))]
    );
    let prepared = consumer
        .prepare_stream_commit(100, Lsn::new(20))
        .expect("an aborted unresolved legacy event cannot poison replay");
    assert!(prepared.ddl_rows().is_empty());
    assert!(prepared.registry_rows().is_empty());
}

#[tokio::test]
async fn committed_tracked_table_schema_move_fails_loudly() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let previous = tracked_orders();
    consumer.observe(
        TransactionScope::Ordinary,
        event(1, "archive", "orders", "ALTER TABLE"),
        Some(&previous),
    );

    let err = consumer
        .on_commit(
            &disconnected_pool(),
            10,
            Lsn::new(10),
            UtcTimestamp::now(),
            false,
            0,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        DdlError::TrackedTableIdentityChange(TrackedTableIdentityChange {
            kind: TrackedTableIdentityChangeKind::SchemaMoved,
            ..
        })
    ));
}

#[test]
fn committed_streamed_tracked_table_drop_fails_loudly() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let previous = tracked_orders();
    let scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 100,
    };
    let mut drop_event = event(1, "public", "orders", "DROP SCHEMA");
    drop_event.c_event = "sql_drop".into();
    consumer.observe(scope, drop_event, Some(&previous));

    let err = consumer
        .prepare_stream_commit(100, Lsn::new(10))
        .unwrap_err();
    assert!(matches!(
        err,
        DdlError::TrackedTableIdentityChange(TrackedTableIdentityChange {
            kind: TrackedTableIdentityChangeKind::Dropped,
            new_schema: None,
            new_table: None,
            ..
        })
    ));
}

#[test]
fn aborted_streamed_drop_sentinel_has_no_relation_shape_or_commit_failure() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let previous = tracked_orders();
    let scope = TransactionScope::Streamed {
        top_xid: 200,
        sub_xid: 201,
    };
    let mut drop_event = event(2, "public", "orders", "DROP SCHEMA");
    drop_event.c_event = "sql_drop".into();

    assert!(
        drop_event
            .relation_after(Some(&previous))
            .unwrap()
            .is_none(),
        "a drop sentinel must never reach cache_relation as an empty shape"
    );
    consumer.observe(scope, drop_event, Some(&previous));
    assert_eq!(consumer.on_stream_abort(200, 201).len(), 1);
    let prepared = consumer
        .prepare_stream_commit(200, Lsn::new(20))
        .expect("StreamAbort must discard the provisional drop failure");
    assert!(prepared.ddl_rows().is_empty());
    assert!(prepared.registry_rows().is_empty());
}

#[test]
fn aborted_streamed_identity_change_is_discarded_without_failing() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let previous = tracked_orders();
    let scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    consumer.observe(
        scope,
        event(1, "public", "orders_v2", "ALTER TABLE"),
        Some(&previous),
    );

    let removed = consumer.on_stream_abort(100, 101);
    assert_eq!(
        removed,
        vec![("public".into(), "orders_v2".into(), SchemaVersionNo(2))]
    );
    let prepared = consumer
        .prepare_stream_commit(100, Lsn::new(10))
        .expect("an aborted provisional rename must not fail at StreamCommit");
    assert!(prepared.ddl_rows().is_empty());
    assert!(prepared.registry_rows().is_empty());
}
