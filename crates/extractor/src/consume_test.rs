use super::*;
use crate::batch::SystemClock;
use crate::reload_event::{
    FencePhase, FenceWaiters, PendingReloadEvent, PendingReloadEvents, ReloadEventKind,
    ReloadScope, ReloadTarget,
};
use arrow::array::{Array, Int32Array, StringArray};
use common::{ExtractorMeta, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo, TupleValue};
use object_store::{ObjectStore, PutPayload, path::Path};
use pg_to_arrow::oids;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use uuid::Uuid;

fn orders_relation(with_extra: bool) -> PgRelation {
    let mut columns = vec![
        PgColumn {
            name: "id".into(),
            type_oid: oids::INT4,
            type_modifier: -1,
            is_key: true,
        },
        PgColumn {
            name: "note".into(),
            type_oid: oids::TEXT,
            type_modifier: -1,
            is_key: false,
        },
    ];
    if with_extra {
        columns.push(PgColumn {
            name: "extra".into(),
            type_oid: oids::TEXT,
            type_modifier: -1,
            is_key: false,
        });
    }
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

fn invoices_relation() -> PgRelation {
    let mut relation = orders_relation(false);
    relation.oid = 43;
    relation.name = "invoices".into();
    relation
}

fn wide_key_relation() -> PgRelation {
    let columns = (1..=32)
        .map(|index| PgColumn {
            name: format!("key_{index:02}"),
            type_oid: oids::INT4,
            type_modifier: -1,
            is_key: true,
        })
        .chain(std::iter::once(PgColumn {
            name: "payload".into(),
            type_oid: oids::TEXT,
            type_modifier: -1,
            is_key: false,
        }))
        .collect();
    PgRelation {
        oid: 43,
        schema: "public".into(),
        name: "wide_keys".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

fn wide_key_values(last_key: &str, payload: &str) -> Vec<TupleValue> {
    let mut values = (1..=32)
        .map(|_| TupleValue::Text("1".into()))
        .collect::<Vec<_>>();
    *values.last_mut().unwrap() = TupleValue::Text(last_key.into());
    values.push(TupleValue::Text(payload.into()));
    values
}

#[test]
fn malformed_reload_event_is_a_decode_error() {
    let rel = PgRelation {
        oid: 91,
        schema: "public".into(),
        name: "walrus_reload_event".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: Vec::new(),
    };

    let error = decode_reload_event(&rel, &[], None, None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("parse public.walrus_reload_event tuple"),
        "malformed source control rows must stop decoding: {error:#}"
    );
}

#[test]
fn stream_commit_publication_contains_every_materialised_object_and_real_timestamp() {
    let epoch = EpochNo(7);
    let commit_lsn = Lsn::new(900);
    let commit_ts = "2026-09-02T12:34:56Z".parse::<UtcTimestamp>().unwrap();
    let objects = [
        crate::staging::WrittenObject {
            s3_uri: "s3://walrus/7/public/orders/first.parquet".into(),
            key: Path::from("7/public/orders/first.parquet"),
            source_schema: "public".into(),
            source_table: "orders".into(),
            lsn_start: Lsn::new(100),
            lsn_end: commit_lsn,
            row_count: 3,
            object_size: 128,
            sha256: [1; 32],
            schema_version: SchemaVersionNo(1),
            kind: crate::staging::FileKind::Spill,
        },
        crate::staging::WrittenObject {
            s3_uri: "s3://walrus/7/public/invoices/second.parquet".into(),
            key: Path::from("7/public/invoices/second.parquet"),
            source_schema: "public".into(),
            source_table: "invoices".into(),
            lsn_start: Lsn::new(200),
            lsn_end: commit_lsn,
            row_count: 5,
            object_size: 256,
            sha256: [2; 32],
            schema_version: SchemaVersionNo(2),
            kind: crate::staging::FileKind::Stream,
        },
    ];

    let publication =
        stream_commit_publication(epoch, 857, commit_lsn, commit_ts, &objects, &[], &[]);

    assert_eq!(publication.epoch, epoch);
    assert_eq!(publication.top_xid, 857);
    assert_eq!(publication.commit_lsn, commit_lsn);
    assert_eq!(publication.commit_ts, commit_ts);
    assert!(publication.ddl_rows.is_empty());
    assert!(publication.registry_rows.is_empty());
    assert_eq!(publication.files.len(), objects.len());
    assert_eq!(publication.files[0].s3_uri, objects[0].s3_uri);
    assert_eq!(publication.files[0].kind, control::ManifestKind::Spill);
    assert_eq!(publication.files[1].s3_uri, objects[1].s3_uri);
    assert_eq!(publication.files[1].kind, control::ManifestKind::Stream);
}

#[tokio::test]
async fn seal_covered_ordinary_object_cleanup_removes_the_unreferenced_bytes() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let stager = crate::staging::ParquetStager::new(Arc::clone(&store), "walrus", EpochNo(7));
    let object = crate::staging::WrittenObject {
        s3_uri: "s3://walrus/7/public/orders/replayed.parquet".into(),
        key: Path::from("7/public/orders/replayed.parquet"),
        source_schema: "public".into(),
        source_table: "orders".into(),
        lsn_start: Lsn::new(90),
        lsn_end: Lsn::new(100),
        row_count: 1,
        object_size: 1,
        sha256: [7; 32],
        schema_version: SchemaVersionNo(1),
        kind: crate::staging::FileKind::Stream,
    };
    store
        .put(&object.key, PutPayload::from_static(b"x"))
        .await
        .unwrap();

    delete_seal_covered_object(&stager, &object, Lsn::new(100)).await;

    assert!(matches!(
        store.head(&object.key).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[test]
fn explicit_ordinary_flush_rejects_grouped_and_reload_kinds_before_put() {
    validate_ordinary_flush_kind(crate::staging::FileKind::Stream).unwrap();
    validate_ordinary_flush_kind(crate::staging::FileKind::Snapshot).unwrap();
    assert!(validate_ordinary_flush_kind(crate::staging::FileKind::Spill).is_err());
    assert!(validate_ordinary_flush_kind(crate::staging::FileKind::Reload).is_err());
}

#[test]
fn ddl_tracking_uses_oid_and_only_unique_legacy_name_resolution() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();

    let mut event = crate::ddl::DdlEvent {
        source_audit_id: 1,
        capture_lsn: Lsn::new(100),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_rel_oid: Some(43),
        c_replica_identity: Some(ReplicaIdentity::Default),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: None,
        c_table_comment: None,
    };

    assert!(
        matches!(
            tracked_relation_for_ddl(&cache, &event),
            TrackedDdlResolution::Tracked(relation) if relation.oid == 42
        ),
        "a different OID at the same frozen name must reach commit-gated recreation validation"
    );

    event.c_rel_oid = Some(42);
    assert!(
        matches!(
            tracked_relation_for_ddl(&cache, &event),
            TrackedDdlResolution::Tracked(relation) if relation.oid == 42
        ),
        "the exact frozen relation identity remains tracked"
    );

    event.c_rel_oid = None;
    assert!(
        matches!(
            tracked_relation_for_ddl(&cache, &event),
            TrackedDdlResolution::Tracked(relation) if relation.oid == 42
        ),
        "a legacy null OID resolves only through one unique frozen qualified name"
    );

    event.source_table = "orders_v2".into();
    assert_eq!(
        tracked_relation_for_ddl(&cache, &event),
        TrackedDdlResolution::Unresolved,
        "a null-OID rename cannot be mistaken for unrelated DDL"
    );

    event.c_rel_oid = Some(43);
    assert_eq!(
        tracked_relation_for_ddl(&cache, &event),
        TrackedDdlResolution::Unrelated,
        "a different explicit OID and name proves the table is outside the frozen epoch"
    );
}

#[test]
fn legacy_name_resolution_rejects_multiple_frozen_oids() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut replacement = orders_relation(false);
    replacement.oid = 43;
    cache
        .upsert_from_relation(replacement, SchemaVersionNo(2))
        .unwrap();
    let event = crate::ddl::DdlEvent {
        source_audit_id: 1,
        capture_lsn: Lsn::new(100),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_rel_oid: None,
        c_replica_identity: Some(ReplicaIdentity::Default),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: None,
        c_table_comment: None,
    };

    assert_eq!(
        tracked_relation_for_ddl(&cache, &event),
        TrackedDdlResolution::Unresolved,
        "qualified name is not identity proof when two frozen OIDs share it"
    );
}

fn router() -> BatchRouter<Arc<SystemClock>> {
    BatchRouter::new(
        BatchTriggers {
            max_rows: NonZeroU64::MIN,
            max_bytes: NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        EpochNo(1),
        "test",
    )
}

fn quiet_router() -> BatchRouter<Arc<SystemClock>> {
    BatchRouter::new(
        BatchTriggers {
            max_rows: NonZeroU64::MAX,
            max_bytes: NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        EpochNo(1),
        "test",
    )
}

fn batch_ops(batch: &SealedBatch) -> Vec<Op> {
    let meta = batch
        .record_batch
        .column(batch.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..meta.len())
        .map(|row| {
            serde_json::from_str::<ExtractorMeta>(meta.value(row))
                .unwrap()
                .op
        })
        .collect()
}

#[test]
fn build_names_the_first_missing_field() {
    // The DecodeLoopBuilder compile-fail doctest is the real dropped-setter regression; a runtime
    // test cannot express a compile-time unused-result error.
    let Err(error) = DecodeLoop::<Arc<SystemClock>>::builder().build() else {
        panic!("an empty builder must reject its first missing field");
    };
    assert_eq!(
        error.to_string(),
        "decode loop builder: missing required field `stream`"
    );
}

/// Stands in for a frame future that makes partial progress before completing.
async fn stepwise(progress: &AtomicU32, steps: u32) {
    for _ in 0..steps {
        tokio::time::sleep(Duration::from_millis(10)).await;
        progress.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test(start_paused = true)]
async fn pinned_branch_survives_other_arm_firing() {
    let progress = AtomicU32::new(0);
    let frame = stepwise(&progress, 5);
    tokio::pin!(frame);
    let mut ticker = tokio::time::interval(Duration::from_millis(3));
    let mut interruptions = 0_u32;
    loop {
        tokio::select! {
            () = &mut frame => break,
            _ = ticker.tick() => interruptions += 1,
        }
    }
    assert!(interruptions > 0, "the sibling arm must win at least once");
    assert_eq!(
        progress.load(Ordering::Relaxed),
        5,
        "partial progress must survive"
    );
}

#[tokio::test(start_paused = true)]
async fn recreated_branch_loses_progress() {
    let progress = AtomicU32::new(0);
    let mut ticker = tokio::time::interval(Duration::from_millis(3));
    let mut interruptions = 0_u32;
    for _ in 0..10 {
        tokio::select! {
            () = stepwise(&progress, 5) => break,
            _ = ticker.tick() => interruptions += 1,
        }
    }
    assert_eq!(interruptions, 10, "the sibling arm must keep interrupting");
    assert_eq!(
        progress.load(Ordering::Relaxed),
        0,
        "recreating the future loses progress"
    );
}

#[test]
fn committed_structural_ddl_promotes_the_ordinary_binding() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(true), SchemaVersionNo(2))
        .unwrap();
    let mut router = quiet_router();
    router.bind_relation(42, SchemaVersionNo(1));
    let structural = control::DdlRow {
        id: common::DdlId(1),
        epoch: EpochNo(1),
        source_audit_id: 10,
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_lsn: Lsn::new(900),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE public.orders ADD COLUMN extra text".into()),
        c_table_comment: None,
    };
    let comment = control::DdlRow {
        id: common::DdlId(2),
        source_audit_id: 11,
        c_tag: "COMMENT".into(),
        schema_version: SchemaVersionNo(9),
        c_rel_oid: Some(99),
        ..structural.clone()
    };

    let bindings = ddl_relation_bindings(&[structural.clone(), comment], &cache).unwrap();
    assert_eq!(bindings, vec![(42, SchemaVersionNo(2))]);
    assert_eq!(router.bindings.get(&42), Some(&SchemaVersionNo(1)));
    router.commit_ddl_bindings(&bindings);
    assert_eq!(router.bindings.get(&42), Some(&SchemaVersionNo(2)));
    assert_eq!(
        router.bindings.get(&99),
        None,
        "metadata-only events must not invent a new routing version"
    );

    let legacy = control::DdlRow {
        c_rel_oid: None,
        ..structural
    };
    assert_eq!(
        ddl_relation_bindings(&[legacy], &cache).unwrap(),
        vec![(42, SchemaVersionNo(2))],
        "the unique legacy name resolver reaches the null-OID commit binding path"
    );
}

#[test]
fn explicit_ordinary_binding_never_falls_forward_when_its_cache_entry_is_missing() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(true), SchemaVersionNo(2))
        .unwrap();
    let mut router = quiet_router();
    router.bind_relation(42, SchemaVersionNo(1));

    let error = router.cached_relation(&cache, 42).unwrap_err();
    assert_eq!(
        error.to_string(),
        "bound ordinary change relation version is not cached: oid=42 version=1"
    );
}

#[test]
fn decode_commit_frontier_rejects_regression_without_mutating() {
    let mut frontier = Lsn::new(900);
    advance_processed_through(&mut frontier, Lsn::new(900), Lsn::new(910)).unwrap();
    assert_eq!(
        frontier,
        Lsn::new(910),
        "an exact legacy commit replay repairs the cursor to end_lsn"
    );

    let error = advance_processed_through(&mut frontier, Lsn::new(909), Lsn::new(920)).unwrap_err();
    assert!(error.to_string().contains("source commit LSN regressed"));
    assert_eq!(
        frontier,
        Lsn::new(910),
        "a rejected commit cannot move schema history backward"
    );

    let error = advance_processed_through(&mut frontier, Lsn::new(910), Lsn::new(910)).unwrap_err();
    assert!(error.to_string().contains("end LSN did not follow"));
    assert_eq!(
        frontier,
        Lsn::new(910),
        "an invalid end position cannot mutate the history cursor"
    );
}

#[test]
fn relation_history_is_strictly_before_both_resume_and_ordinary_commit_boundaries() {
    assert_eq!(
        relation_history_cutoff(Lsn::new(900), None),
        Lsn::new(899),
        "streamed replay at a legacy commit_lsn must exclude that commit's future history"
    );
    assert_eq!(
        relation_history_cutoff(Lsn::new(910), None),
        Lsn::new(909),
        "a normal end_lsn cursor retains history committed before the end position"
    );
    assert_eq!(
        relation_history_cutoff(Lsn::new(1_000), Some(Lsn::new(950))),
        Lsn::new(949),
        "ordinary Begin.final_lsn remains the tighter bound"
    );
    assert_eq!(
        relation_history_cutoff(Lsn::ZERO, None),
        Lsn::ZERO,
        "the initial cursor subtraction saturates"
    );
}

#[test]
fn ordinary_ddl_cut_preserves_pre_ddl_rows_and_separates_schema_versions() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut router = quiet_router();
    router.bind_relation(42, SchemaVersionNo(1));

    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 700,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    assert!(
        !router.has_open_ordinary_data(),
        "Begin alone carries no data durability work"
    );
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 42,
                new: vec![TupleValue::Text("1".into()), TupleValue::Text("old".into())],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    assert!(router.has_open_ordinary_data());

    assert!(
        router
            .cut_table(&cache, "public", "orders")
            .unwrap()
            .is_empty(),
        "an open transaction cannot be sealed at its DDL event"
    );
    assert_eq!(router.pending_cuts.len(), 1);
    assert!(
        router.has_open_ordinary_data(),
        "a pre-DDL cut must remain visible to the mixed-transaction guard"
    );

    cache
        .upsert_from_relation(orders_relation(true), SchemaVersionNo(2))
        .unwrap();
    router.bind_relation(42, SchemaVersionNo(2));
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 42,
                new: vec![
                    TupleValue::Text("2".into()),
                    TupleValue::Text("new".into()),
                    TupleValue::Text("v2".into()),
                ],
            },
            Lsn::new(120),
            SchemaVersionNo(2),
        )
        .unwrap();

    let sealed = router
        .commit(
            Lsn::new(900),
            UtcTimestamp::from_pg_micros(0).unwrap(),
            true,
        )
        .unwrap();

    assert_eq!(sealed.len(), 2);
    assert_eq!(
        sealed
            .iter()
            .map(|batch| (batch.schema_version, batch.row_count, batch.lsn_end))
            .collect::<Vec<_>>(),
        vec![
            (SchemaVersionNo(1), 1, Lsn::new(900)),
            (SchemaVersionNo(2), 1, Lsn::new(900)),
        ]
    );
    assert!(router.pending_cuts.is_empty());
    assert!(
        !router.has_open_ordinary_data(),
        "Commit promotion clears the transaction-local data predicate"
    );
}

#[test]
fn ordinary_structural_commit_flushes_prior_prefix_before_replay_stable_children() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    cache
        .upsert_from_relation(invoices_relation(), SchemaVersionNo(1))
        .unwrap();
    let mut router = quiet_router();
    router.bind_relation(42, SchemaVersionNo(1));
    router.bind_relation(43, SchemaVersionNo(1));

    // A prior transaction for a different table remains below every normal flush trigger.
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 600,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 43,
                new: vec![
                    TupleValue::Text("1".into()),
                    TupleValue::Text("prior".into()),
                ],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    assert!(
        router
            .route(
                &cache,
                &Message::Commit {
                    flags: 0,
                    commit_lsn: Lsn::new(200),
                    end_lsn: Lsn::new(201),
                    commit_ts: 0,
                },
                Lsn::new(200),
                SchemaVersionNo(1),
            )
            .unwrap()
            .is_empty()
    );

    // The mixed transaction starts in orders v1. Its DDL cut must return the older committed
    // invoices prefix too, while preserving this xid's pre-DDL orders row as a pending segment.
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(300),
                commit_ts: 0,
                xid: 700,
            },
            Lsn::new(300),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 42,
                new: vec![
                    TupleValue::Text("2".into()),
                    TupleValue::Text("before-ddl".into()),
                ],
            },
            Lsn::new(310),
            SchemaVersionNo(1),
        )
        .unwrap();
    let prefixes = router.cut_table(&cache, "public", "orders").unwrap();
    assert_eq!(
        prefixes
            .iter()
            .map(|batch| (
                batch.table.as_str(),
                batch.schema_version,
                batch.row_count,
                batch.lsn_start,
                batch.lsn_end
            ))
            .collect::<Vec<_>>(),
        vec![(
            "invoices",
            SchemaVersionNo(1),
            1,
            Lsn::new(200),
            Lsn::new(200)
        )],
        "the atomic receipt must never absorb a prior buffered commit"
    );

    cache
        .upsert_from_relation(orders_relation(true), SchemaVersionNo(2))
        .unwrap();
    router.bind_relation(42, SchemaVersionNo(2));
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 42,
                new: vec![
                    TupleValue::Text("3".into()),
                    TupleValue::Text("after-ddl".into()),
                    TupleValue::Text("v2".into()),
                ],
            },
            Lsn::new(320),
            SchemaVersionNo(2),
        )
        .unwrap();

    let commit_lsn = Lsn::new(900);
    let children = router
        .commit(commit_lsn, UtcTimestamp::from_pg_micros(0).unwrap(), true)
        .unwrap();
    assert_eq!(
        children
            .iter()
            .map(|batch| (
                batch.schema_version,
                batch.row_count,
                batch.lsn_start,
                batch.lsn_end
            ))
            .collect::<Vec<_>>(),
        vec![
            (SchemaVersionNo(1), 1, commit_lsn, commit_lsn),
            (SchemaVersionNo(2), 1, commit_lsn, commit_lsn),
        ],
        "pre- and post-DDL tails must be forced into replay-stable receipt children"
    );
    assert!(router.pending_cuts.is_empty());
    assert_eq!(router.undurable_floor(), None);
}

#[test]
fn durable_frontier_waits_for_every_older_or_equal_unsealed_commit() {
    let durable = Some(Lsn::new(900));
    assert_eq!(durable_frontier(durable, None), durable);
    assert_eq!(
        durable_frontier(durable, Some(Lsn::new(901))),
        durable,
        "a later unsealed commit does not block an earlier durable group"
    );
    assert_eq!(
        durable_frontier(durable, Some(Lsn::new(900))),
        None,
        "a sibling at the same commit LSN must be durable before acknowledgement"
    );
    assert_eq!(
        durable_frontier(durable, Some(Lsn::new(800))),
        None,
        "an older unsealed commit fences the slot"
    );
    assert_eq!(durable_frontier(None, Some(Lsn::new(800))), None);
}

#[test]
fn schema_barrier_alone_does_not_advance_data_checkpoint() {
    let resume_lsn = Lsn::new(700);
    let mut checkpoint = crate::checkpoint::DurabilityCheckpoint::new(resume_lsn);

    assert_eq!(
        record_durable_frontier(None, None, &mut checkpoint).unwrap(),
        None,
        "a schema publication receipt is not a durable file/control frontier"
    );
    assert_eq!(checkpoint.confirmed_flush(), resume_lsn);
}

#[tokio::test]
async fn failed_ordinary_schema_publication_never_polls_persistence_or_ack_stage() {
    let persistence_polled = AtomicBool::new(false);

    let result = persist_after_ordinary_schema_publication(
        Err::<(), anyhow::Error>(crate::ddl::DdlError::MissingColumn("publication failed").into()),
        async {
            persistence_polled.store(true, Ordering::Relaxed);
            Ok::<(), anyhow::Error>(())
        },
    )
    .await;

    assert!(result.is_err());
    assert!(
        !persistence_polled.load(Ordering::Relaxed),
        "file persistence and its downstream checkpoint/ACK stage must remain unpolled"
    );
}

#[test]
fn key_changing_update_emits_old_key_delete_then_new_image() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut router = router();
    router.bind_relation(42, SchemaVersionNo(1));
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Update {
                xid: None,
                relation_oid: 42,
                old_kind: Some(crate::pgoutput::OldTupleKind::Key),
                old: Some(vec![TupleValue::Text("1".into()), TupleValue::Null]),
                new: vec![
                    TupleValue::Text("2".into()),
                    TupleValue::Text("moved".into()),
                ],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    assert_eq!(sealed.len(), 1);
    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 2);
    assert_eq!(batch_ops(&batch), vec![Op::Delete, Op::Update]);
    let ids = batch
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!((ids.value(0), ids.value(1)), (1, 2));
}

#[test]
fn update_with_unchanged_key_emits_only_new_image() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut router = router();
    router.bind_relation(42, SchemaVersionNo(1));
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Update {
                xid: None,
                relation_oid: 42,
                old_kind: Some(crate::pgoutput::OldTupleKind::Full),
                old: Some(vec![
                    TupleValue::Text("1".into()),
                    TupleValue::Text("before".into()),
                ]),
                new: vec![
                    TupleValue::Text("1".into()),
                    TupleValue::Text("after".into()),
                ],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 1);
    assert_eq!(batch_ops(&batch), vec![Op::Update]);
}

#[test]
fn wide_key_change_detection_uses_the_final_component() {
    let relation = wide_key_relation();
    let before = wide_key_values("1", "before");
    let payload_only = wide_key_values("1", "after");
    let moved = wide_key_values("2", "after");

    assert!(
        !update_changes_key(&relation, &before, &payload_only).unwrap(),
        "a non-key payload update must not become a key move"
    );
    assert!(
        update_changes_key(&relation, &before, &moved).unwrap(),
        "the 32nd key component must participate in key-change detection"
    );
}

#[test]
fn ordinary_update_normalizes_key_toast_and_records_non_key_toast() {
    let mut relation = orders_relation(false);
    relation.replica_identity = ReplicaIdentity::Index;
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(relation, SchemaVersionNo(1))
        .unwrap();
    let mut router = router();
    router.bind_relation(42, SchemaVersionNo(1));
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Update {
                xid: None,
                relation_oid: 42,
                old_kind: Some(crate::pgoutput::OldTupleKind::Key),
                old: Some(vec![TupleValue::Text("1".into()), TupleValue::Null]),
                new: vec![TupleValue::UnchangedToast, TupleValue::UnchangedToast],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 1, "the resolved key did not move");
    let ids = batch
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1, "key sentinel was replaced from old image");
    let meta = batch
        .record_batch
        .column(batch.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let meta: ExtractorMeta = serde_json::from_str(meta.value(0)).unwrap();
    assert_eq!(meta.unchanged_toast.as_ref(), &["note".to_string()]);
}

#[test]
fn ordinary_truncate_becomes_a_durable_table_boundary_row() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut router = router();
    router.bind_relation(42, SchemaVersionNo(1));
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Truncate {
                xid: None,
                cascade: false,
                restart_identity: false,
                relations: vec![42],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 1);
    assert_eq!(batch_ops(&batch), vec![Op::Truncate]);
    let ids = batch
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(ids.is_null(0), "truncate carries no synthetic key value");
}

#[test]
fn truncate_with_unknown_relation_fails_instead_of_being_ignored() {
    let cache = RelationCache::default();
    let mut router = router();
    let error = router
        .route(
            &cache,
            &Message::Truncate {
                xid: None,
                cascade: false,
                restart_identity: false,
                relations: vec![999],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap_err();
    assert!(error.to_string().contains("oid=999"));
}

#[test]
fn force_flush_table_seals_only_the_target_and_keeps_other_ack_floor() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut invoices = orders_relation(false);
    invoices.oid = 43;
    invoices.name = "invoices".into();
    cache
        .upsert_from_relation(invoices, SchemaVersionNo(1))
        .unwrap();

    let mut router = quiet_router();
    router.bind_relation(42, SchemaVersionNo(1));
    router.bind_relation(43, SchemaVersionNo(1));
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 700,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    for (relation_oid, id) in [(42, "1"), (43, "2")] {
        router
            .route(
                &cache,
                &Message::Insert {
                    xid: None,
                    relation_oid,
                    new: vec![TupleValue::Text(id.into()), TupleValue::Text("row".into())],
                },
                Lsn::new(110),
                SchemaVersionNo(1),
            )
            .unwrap();
    }
    assert!(
        router
            .route(
                &cache,
                &Message::Commit {
                    flags: 0,
                    commit_lsn: Lsn::new(200),
                    end_lsn: Lsn::new(201),
                    commit_ts: 0,
                },
                Lsn::new(200),
                SchemaVersionNo(1),
            )
            .unwrap()
            .is_empty(),
        "both committed tables remain below their ordinary triggers"
    );

    let target = router.force_flush_table("public", "orders").unwrap();
    assert_eq!(target.len(), 1);
    assert_eq!(target[0].table, "orders");
    assert_eq!(target[0].lsn_end, Lsn::new(200));
    assert_eq!(
        router.undurable_floor(),
        Some(Lsn::new(200)),
        "the unrelated committed table still fences confirmed_flush"
    );
    assert!(
        router
            .force_flush_table("public", "orders")
            .unwrap()
            .is_empty(),
        "a repeated fence is idempotent in memory"
    );

    let other = router.force_flush_table("public", "invoices").unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].table, "invoices");
    assert_eq!(router.undurable_floor(), None);
}

#[tokio::test]
async fn committed_end_fence_resolves_only_from_the_persisted_state() {
    let reload_id = common::ReloadId(91);
    let request_id = Uuid::new_v4();
    let waiters = FenceWaiters::default();
    let mut waiter = Box::pin(waiters.subscribe(reload_id, FencePhase::End));
    let mut pending = PendingReloadEvents::default();
    pending.push(PendingReloadEvent {
        event_id: Uuid::new_v4(),
        request_id,
        reload_id: Some(reload_id),
        kind: ReloadEventKind::EndFence,
        scope: ReloadScope::Table,
        source_schema: Some("public".into()),
        source_table: Some("orders".into()),
        targets: Vec::new(),
        schema_version: Some(SchemaVersionNo(1)),
        embedded_lsn: Lsn::new(250),
        xid: None,
        top_xid: None,
    });

    let committed = pending.on_commit(Lsn::new(300));
    assert_eq!(
        waiters.waiter_count(),
        1,
        "transaction commit alone must not resolve an end fence"
    );
    tokio::select! {
        biased;
        result = &mut waiter => panic!("end fence resolved before durability: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    // Production can construct this state only after target PUT + manifest + end-marker commit.
    let requests = PersistedReloadEvents { events: committed }.resolve_fences(&waiters);
    assert!(requests.is_empty());
    let echo = waiter.await.unwrap();
    assert_eq!(echo.commit_lsn, Lsn::new(300));
    assert_eq!(echo.embedded_lsn, Lsn::new(250));
}

#[test]
fn only_failed_attempt_with_exact_target_is_a_stale_fence_noop() {
    let request_id = Uuid::new_v4();
    let version = SchemaVersionNo(3);
    let attempt = FailedReloadAttempt {
        status: control::ReloadStatus::Failed,
        target: ("public", "orders"),
        request_id: Some(request_id),
        schema_version: Some(version),
    };
    let fence = DecodedFenceIdentity {
        target: Some(("public", "orders")),
        request_id,
        schema_version: Some(version),
    };

    assert!(is_matching_failed_fence(attempt, fence));
    assert!(!is_matching_failed_fence(
        FailedReloadAttempt {
            status: control::ReloadStatus::Exporting,
            ..attempt
        },
        fence,
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            target: Some(("public", "invoices")),
            ..fence
        },
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            target: None,
            ..fence
        },
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            request_id: Uuid::new_v4(),
            ..fence
        },
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            schema_version: Some(SchemaVersionNo(4)),
            ..fence
        },
    ));
}

#[test]
fn all_published_fanout_uses_and_dedupes_the_frozen_event_inventory() {
    let targets = vec![
        ReloadTarget {
            schema: "public".into(),
            table: "orders".into(),
        },
        ReloadTarget {
            schema: "sales".into(),
            table: "invoices".into(),
        },
        ReloadTarget {
            schema: "public".into(),
            table: "orders".into(),
        },
    ];

    assert_eq!(
        dedupe_reload_targets(&targets)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![("public", "orders"), ("sales", "invoices")]
    );
}
