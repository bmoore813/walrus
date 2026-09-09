#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the `file_manifest` queue models.
//!
//! Each test runs inside a rolled-back transaction and namespaces its rows by a unique `epoch`, so
//! the tests are isolated from each other and idempotent across runs. Gated behind the
//! `integration` feature (needs the compose control Postgres).
#![cfg(feature = "integration")]

use common::{DdlId, EpochNo, Lsn, SchemaVersionNo, UtcTimestamp};
use control::NewManifestFile;
use control::reload::{self, ExportRangePlan, ExportSnapshot, ReloadFenceIdentity, ReloadFlavor};
use control::{
    DdlRow, NewStreamCommitPublication, PublishManifestOutcome, PublishStreamOutcome, RegistryRow,
    claim_ready, claim_ready_units, complete_schema_barriers, connect, delete_claimed,
    insert_ready, list_manifest_uris, manifest_work_exists, max_ready_lsn_end,
    publish_ready_manifest, publish_stream_commit, run_migrations,
};
use sqlx::{
    Connection,
    postgres::{PgConnection, PgPool},
};
use uuid::Uuid;

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn pool() -> PgPool {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

async fn publishing_reload(
    conn: &mut PgConnection,
    epoch: EpochNo,
    table: &str,
    final_lsn: Lsn,
    publication_nonce: Uuid,
) -> i64 {
    let reload_id = reload::request(&mut *conn, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    let claimed = reload::claim_requested(&mut *conn, epoch, "manifest-test", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease = claimed.exporter_lease("manifest-test").unwrap();
    let start_lsn = Lsn::new(0x100);
    let schema_version = SchemaVersionNo(1);
    let identity = ReloadFenceIdentity {
        request_id: claimed.parent_request_id,
        source_schema: "public",
        source_table: table,
        schema_version,
    };
    reload::record_start_fence(&mut *conn, reload_id, start_lsn, identity)
        .await
        .unwrap();
    reload::begin_export_plan(
        &mut *conn,
        &lease,
        start_lsn,
        schema_version,
        ExportSnapshot {
            identity: "1:2:",
            xmin: 1,
            xmax: 2,
        },
        &[ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }],
    )
    .await
    .unwrap();
    reload::record_export_range(&mut *conn, &lease, 0, 0, 0)
        .await
        .unwrap();
    reload::seal_export(&mut *conn, &lease, start_lsn, schema_version)
        .await
        .unwrap();
    reload::record_end_marker(&mut *conn, reload_id, final_lsn, identity)
        .await
        .unwrap();
    reload::complete_export(&mut *conn, &lease, final_lsn)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE public.walrus_table_reload
         SET status = 'publishing', publication_nonce = $2,
             publisher_owner_pod = 'manifest-test', publisher_fencing_token = 1,
             publishing_at = now()
         WHERE reload_id = $1",
    )
    .bind(reload_id.0)
    .bind(publication_nonce)
    .execute(&mut *conn)
    .await
    .unwrap();
    reload_id.0
}

fn file(epoch: EpochNo, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!(
            "s3://walrus/{epoch}/public/{table}/{lsn_end}-{}.parquet",
            Uuid::new_v4()
        ),
        kind: control::ManifestKind::Stream,
        row_count: 1,
        object_size: 1,
        sha256: vec![0; 32],
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: None,
    }
}

fn data_only_publication(
    epoch: EpochNo,
    table: &str,
    commit_lsn: Lsn,
    top_xid: u32,
) -> NewStreamCommitPublication {
    let mut child = file(epoch, table, &commit_lsn.to_string());
    child.lsn_start = commit_lsn;
    child.lsn_end = commit_lsn;
    NewStreamCommitPublication {
        epoch,
        top_xid,
        commit_lsn,
        commit_ts: "2026-09-02T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        ddl_rows: Vec::new(),
        registry_rows: Vec::new(),
        files: vec![child],
    }
}

#[tokio::test]
async fn ordinary_replay_at_or_below_seal_is_covered_without_recreating_work() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_012);
    let table = "ordinary_seal_replay";
    let seal = Lsn::new(0x500);
    let publication_nonce = Uuid::new_v4();

    let reload_id = publishing_reload(&mut tx, epoch, table, seal, publication_nonce).await;
    sqlx::query("SELECT set_config('walrus.manifest_seal_protocol', '2', true)")
        .execute(&mut *tx)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO public.walrus_manifest_publication_fence
           (epoch, source_schema, source_table, sealed_through_lsn,
            sealed_reload_id, sealed_publication_nonce)
         VALUES ($1, 'public', $2, $3, $4, $5)",
    )
    .bind(epoch.0)
    .bind(table)
    .bind(seal)
    .bind(reload_id)
    .bind(publication_nonce)
    .execute(&mut *tx)
    .await
    .unwrap();

    let before = file(epoch, table, "0/400");
    assert_eq!(
        publish_ready_manifest(&mut tx, &before).await.unwrap(),
        PublishManifestOutcome::CoveredBySeal(seal)
    );
    let exactly = file(epoch, table, "0/500");
    assert_eq!(
        publish_ready_manifest(&mut tx, &exactly).await.unwrap(),
        PublishManifestOutcome::CoveredBySeal(seal)
    );
    let covered_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest
         WHERE epoch=$1 AND source_schema='public' AND source_table=$2",
    )
    .bind(epoch.0)
    .bind(table)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        covered_rows, 0,
        "covered replay must not recreate queue work"
    );

    let after = file(epoch, table, "0/501");
    let PublishManifestOutcome::Published(id) =
        publish_ready_manifest(&mut tx, &after).await.unwrap()
    else {
        panic!("a commit above the seal must publish a fresh manifest");
    };
    let published_uri: String =
        sqlx::query_scalar("SELECT s3_uri FROM public.walrus_file_manifest WHERE id=$1")
            .bind(id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(published_uri, after.s3_uri);

    let mut referenced_uri = file(epoch, table, "0/300");
    referenced_uri.s3_uri.clone_from(&after.s3_uri);
    assert!(matches!(
        publish_ready_manifest(&mut tx, &referenced_uri).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));

    let mut straddling = file(epoch, table, "0/501");
    straddling.lsn_start = Lsn::new(0x400);
    assert!(matches!(
        publish_ready_manifest(&mut tx, &straddling).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));

    let mut malformed = file(epoch, table, "0/300");
    malformed.object_size = 0;
    assert!(matches!(
        publish_ready_manifest(&mut tx, &malformed).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn claim_orders_by_lsn_end_then_id() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_001);

    // Insert with lsn_ends out of order, plus two files that SHARE one lsn_end (0/20).
    let a = insert_ready(&mut *tx, &file(epoch, "t", "0/30"))
        .await
        .unwrap();
    let b = insert_ready(&mut *tx, &file(epoch, "t", "0/10"))
        .await
        .unwrap();
    let c = insert_ready(&mut *tx, &file(epoch, "t", "0/20"))
        .await
        .unwrap();
    let d = insert_ready(&mut *tx, &file(epoch, "t", "0/20"))
        .await
        .unwrap();
    assert!(c < d, "c was inserted before d, so its serial id is lower");

    let claimed = claim_ready(&mut *tx, epoch, "public", "t", 100)
        .await
        .unwrap();
    let order: Vec<common::ManifestId> = claimed.iter().map(|r| r.id).collect();
    // (lsn_end ASC, id ASC): 0/10 (b), then 0/20 (c before d), then 0/30 (a).
    assert_eq!(order, vec![b, c, d, a]);

    // The commit-LSN values round-trip through pg_lsn keeping their ordering.
    assert_eq!(claimed[0].lsn_end, "0/10".parse::<Lsn>().unwrap());
    assert_eq!(claimed[3].lsn_end, "0/30".parse::<Lsn>().unwrap());

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn claim_does_not_skip_equal_lsn_end_snapshot_files() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_002);

    // Many snapshot files sharing one lsn_end (the exported snapshot's consistent_point).
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(
            insert_ready(&mut *tx, &file(epoch, "snap", "0/AAAA"))
                .await
                .unwrap(),
        );
    }

    let claimed = claim_ready(&mut *tx, epoch, "public", "snap", 100)
        .await
        .unwrap();
    // ALL five are claimed (none skipped by an `lsn_end >` filter), in ascending id order.
    assert_eq!(claimed.len(), 5);
    assert_eq!(claimed.iter().map(|r| r.id).collect::<Vec<_>>(), ids);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn delete_claimed_retires_exactly_the_given_ids() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_003);

    let id1 = insert_ready(&mut *tx, &file(epoch, "d", "0/10"))
        .await
        .unwrap();
    let id2 = insert_ready(&mut *tx, &file(epoch, "d", "0/20"))
        .await
        .unwrap();
    let id3 = insert_ready(&mut *tx, &file(epoch, "d", "0/30"))
        .await
        .unwrap();

    let n = delete_claimed(&mut *tx, &[id1, id3]).await.unwrap();
    assert_eq!(n, 2);

    let remaining = claim_ready(&mut *tx, epoch, "public", "d", 100)
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![id2]
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn manifest_uri_inventory_includes_every_status_and_only_the_requested_epoch() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_005);

    let ready = file(epoch, "inventory", "0/10");
    let failed = file(epoch, "inventory", "0/20");
    insert_ready(&mut *tx, &ready).await.unwrap();
    let failed_id = insert_ready(&mut *tx, &failed).await.unwrap();
    sqlx::query("UPDATE public.walrus_file_manifest SET status = 'failed' WHERE id = $1")
        .bind(failed_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    insert_ready(&mut *tx, &file(EpochNo(epoch.0 + 1), "inventory", "0/30"))
        .await
        .unwrap();

    assert_eq!(
        list_manifest_uris(&mut *tx, epoch).await.unwrap(),
        vec![ready.s3_uri, failed.s3_uri],
        "failed manifests remain durable references; other epochs do not"
    );

    tx.rollback().await.unwrap();
}

async fn clear_stream_publication_epoch(pool: &PgPool, epoch: EpochNo) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM public.walrus_file_manifest WHERE epoch = $1 AND (SELECT protocol='2' FROM authorized)")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_manifest_group WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_txn_publication WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_manifest_publication_fence WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_ddl_manifest WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT pg_catalog.set_config(
             'walrus.schema_registry_maintenance', '1-delete', true
           ) AS protocol
         )
         DELETE FROM public.walrus_schema_registry
         WHERE epoch = $1 AND (SELECT protocol = '1-delete' FROM authorized)",
    )
    .bind(epoch.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn streamed_ddl_and_registry_share_manifest_rollback_and_replay_receipt() {
    let pool = pool().await;
    let epoch = EpochNo(900_006);
    let commit_lsn = Lsn::new(0x200);
    clear_stream_publication_epoch(&pool, epoch).await;

    let ddl = DdlRow {
        id: DdlId(0),
        epoch,
        source_audit_id: 70_001,
        source_schema: "public".into(),
        source_table: "atomic_stream".into(),
        c_lsn: commit_lsn,
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([{"name": "id", "type_oid": 23}])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE atomic_stream ADD COLUMN value text".into()),
        c_table_comment: None,
    };
    let registry = RegistryRow {
        epoch,
        source_schema: "public".into(),
        source_table: "atomic_stream".into(),
        schema_version: SchemaVersionNo(2),
        descriptors: Vec::new(),
        columns: ddl.c_columns.clone().unwrap(),
    };
    // The transaction's last data is still v1, then its DDL advances the same table to v2 without
    // emitting a post-DDL row. The group barrier must carry v2 independently of its child files.
    let mut first = file(epoch, "atomic_stream", "0/200");
    first.s3_uri = format!("s3://walrus/{epoch}/public/atomic_stream/duplicate.parquet");
    let second = first.clone();
    let mut publication = NewStreamCommitPublication {
        epoch,
        top_xid: 857,
        commit_lsn,
        commit_ts: "2026-09-02T12:34:56Z".parse::<UtcTimestamp>().unwrap(),
        ddl_rows: vec![ddl],
        registry_rows: vec![registry],
        files: vec![first, second],
    };

    publish_stream_commit(&pool, &publication)
        .await
        .expect_err("the duplicate object URI must roll back the whole control transaction");
    for relation in [
        "stream_txn_publication",
        "stream_manifest_group",
        "ddl_manifest",
        "schema_registry",
    ] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM public.walrus_{relation} WHERE epoch = $1"
        ))
        .bind(epoch.0)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "{relation} leaked out of the failed publication");
    }

    publication.files[1].s3_uri =
        format!("s3://walrus/{epoch}/public/atomic_stream/second.parquet");
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::Published
    );
    for relation in ["stream_manifest_group", "stream_txn_publication"] {
        let mut guarded = pool.begin().await.unwrap();
        let error = sqlx::query(&format!(
            "DELETE FROM public.walrus_{relation} WHERE epoch=$1"
        ))
        .bind(epoch.0)
        .execute(&mut *guarded)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("manifest_publication_receipt_removal")
        );
        guarded.rollback().await.unwrap();
    }
    assert!(matches!(
        sqlx::query(
            "UPDATE public.walrus_stream_txn_publication
             SET commit_ts = commit_ts || '-changed'
             WHERE epoch = $1"
        )
        .bind(epoch.0)
        .execute(&pool)
        .await
        .map_err(control::ControlError::from),
        Err(control::ControlError::CheckViolation { .. })
    ));
    assert!(
        matches!(
            publish_stream_commit(&pool, &publication).await,
            Err(control::ControlError::ManifestInvariant { .. })
        ),
        "an AlreadyPublished outcome must never authorize deletion of a replay URI still referenced by live manifest work"
    );
    let mut replay = publication.clone();
    replay.files[0].s3_uri =
        format!("s3://walrus/{epoch}/public/atomic_stream/replay-first.parquet");
    replay.files[1].s3_uri =
        format!("s3://walrus/{epoch}/public/atomic_stream/replay-second.parquet");
    assert_eq!(
        publish_stream_commit(&pool, &replay).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished,
        "lost-ACK replay with fresh unreferenced object keys must reuse the durable receipt"
    );
    let mut changed_barrier = publication.clone();
    changed_barrier.ddl_rows[0].schema_version = SchemaVersionNo(3);
    changed_barrier.registry_rows[0].schema_version = SchemaVersionNo(3);
    assert!(matches!(
        publish_stream_commit(&pool, &changed_barrier).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));
    let claimed = claim_ready(&pool, epoch, "public", "atomic_stream", 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2, "one touched group remains indivisible");
    assert!(
        claimed
            .iter()
            .all(|child| child.schema_version == SchemaVersionNo(1))
    );
    assert!(
        claimed
            .iter()
            .all(|child| { child.stream_group_final_schema_version == Some(SchemaVersionNo(2)) })
    );
    let durable_final: i64 = sqlx::query_scalar(
        "SELECT final_schema_version FROM public.walrus_stream_manifest_group WHERE epoch = $1",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(durable_final, 2);

    let mut changed_rows = publication.clone();
    changed_rows.files[0].row_count += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed_rows).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));
    let mut changed_ddl = publication.clone();
    changed_ddl.ddl_rows[0].source_audit_id += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed_ddl).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));
    let mut changed_xid = publication.clone();
    changed_xid.top_xid += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed_xid).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));

    let child_ids = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM public.walrus_file_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch.0)
    .fetch_all(&pool)
    .await
    .unwrap();
    let mut retirement = pool.begin().await.unwrap();
    let mut failed_fixture = Connection::begin(&mut *retirement).await.unwrap();
    sqlx::query("UPDATE public.walrus_file_manifest SET status='failed' WHERE id=$1")
        .bind(child_ids[0])
        .execute(&mut *failed_fixture)
        .await
        .unwrap();
    let child_ids = child_ids.into_iter().map(Into::into).collect::<Vec<_>>();
    assert_eq!(
        delete_claimed(&mut *failed_fixture, &child_ids)
            .await
            .unwrap(),
        0,
        "a complete ID list must not retire a group containing a failed child"
    );
    failed_fixture.rollback().await.unwrap();
    assert_eq!(
        delete_claimed(&mut *retirement, &child_ids).await.unwrap(),
        2,
        "the complete ready group retires atomically"
    );
    retirement.commit().await.unwrap();
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished,
        "lost-ACK replay remains exact after child queue rows have been retired"
    );
    assert!(matches!(
        publish_stream_commit(&pool, &changed_rows).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));

    let ddl_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_ddl_manifest WHERE epoch = $1 AND c_lsn = $2",
    )
    .bind(epoch.0)
    .bind(commit_lsn)
    .fetch_one(&pool)
    .await
    .unwrap();
    let registry_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_schema_registry WHERE epoch = $1 AND source_table = $2",
    )
    .bind(epoch.0)
    .bind("atomic_stream")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((ddl_count, registry_count), (1, 1));

    clear_stream_publication_epoch(&pool, epoch).await;
}

#[tokio::test]
async fn streamed_ddl_only_publication_claims_and_retires_a_schema_barrier() {
    let pool = pool().await;
    let epoch = EpochNo(900_007);
    let commit_lsn = Lsn::new(0x300);
    clear_stream_publication_epoch(&pool, epoch).await;

    let columns = serde_json::json!([{"name": "id", "type_oid": 23}]);
    let ddl = DdlRow {
        id: DdlId(0),
        epoch,
        source_audit_id: 70_002,
        source_schema: "public".into(),
        source_table: "schema_only_stream".into(),
        c_lsn: commit_lsn,
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(43),
        c_columns: Some(columns.clone()),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE schema_only_stream ADD COLUMN value text".into()),
        c_table_comment: None,
    };
    let publication = NewStreamCommitPublication {
        epoch,
        top_xid: 858,
        commit_lsn,
        commit_ts: "2026-09-02T12:35:56Z".parse::<UtcTimestamp>().unwrap(),
        ddl_rows: vec![ddl],
        registry_rows: vec![RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "schema_only_stream".into(),
            schema_version: SchemaVersionNo(2),
            descriptors: Vec::new(),
            columns,
        }],
        files: Vec::new(),
    };

    let mut missing_registry = publication.clone();
    missing_registry.registry_rows.clear();
    assert!(matches!(
        publish_stream_commit(&pool, &missing_registry).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::Published
    );
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished
    );
    let (publications, groups, ddl_rows, registry_rows): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
           (SELECT count(*) FROM public.walrus_stream_txn_publication WHERE epoch = $1),
           (SELECT count(*) FROM public.walrus_stream_manifest_group WHERE epoch = $1),
           (SELECT count(*) FROM public.walrus_ddl_manifest WHERE epoch = $1),
           (SELECT count(*) FROM public.walrus_schema_registry WHERE epoch = $1)",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (publications, groups, ddl_rows, registry_rows),
        (1, 1, 1, 1),
        "DDL-only commits retain exact replay history and one durable zero-child transformer barrier"
    );
    assert_eq!(
        max_ready_lsn_end(&pool, epoch, "public", "schema_only_stream")
            .await
            .unwrap(),
        Some(commit_lsn)
    );
    assert!(
        claim_ready(&pool, epoch, "public", "schema_only_stream", 10)
            .await
            .unwrap()
            .is_empty(),
        "the compatibility file-only claim must not fabricate a manifest row"
    );
    let work = claim_ready_units(&pool, epoch, "public", "schema_only_stream", 10)
        .await
        .unwrap();
    let [control::ReadyManifestUnit::SchemaBarrier(barrier)] = work.as_slice() else {
        panic!("expected exactly one typed schema barrier, got {work:?}");
    };
    assert_eq!(barrier.commit_lsn, commit_lsn);
    assert_eq!(barrier.final_schema_version, SchemaVersionNo(2));
    assert_eq!(
        complete_schema_barriers(&pool, std::slice::from_ref(barrier))
            .await
            .unwrap(),
        1
    );
    assert!(
        claim_ready_units(&pool, epoch, "public", "schema_only_stream", 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        max_ready_lsn_end(&pool, epoch, "public", "schema_only_stream")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished,
        "lost-ACK replay remains exact after the zero-child barrier is applied"
    );

    clear_stream_publication_epoch(&pool, epoch).await;
}

#[tokio::test]
async fn streamed_comment_only_publication_does_not_create_a_schema_barrier() {
    let pool = pool().await;
    let epoch = EpochNo(900_008);
    let commit_lsn = Lsn::new(0x400);
    clear_stream_publication_epoch(&pool, epoch).await;
    let publication = NewStreamCommitPublication {
        epoch,
        top_xid: 859,
        commit_lsn,
        commit_ts: "2026-09-02T12:36:56Z".parse::<UtcTimestamp>().unwrap(),
        ddl_rows: vec![DdlRow {
            id: DdlId(0),
            epoch,
            source_audit_id: 70_003,
            source_schema: "public".into(),
            source_table: "comment_only_stream".into(),
            c_lsn: commit_lsn,
            c_event: "ddl_command_end".into(),
            c_tag: "COMMENT".into(),
            schema_version: SchemaVersionNo(1),
            c_rel_oid: Some(44),
            c_columns: None,
            c_dropped: None,
            c_ddl_text: Some("COMMENT ON TABLE comment_only_stream IS 'note'".into()),
            c_table_comment: Some("note".into()),
        }],
        registry_rows: Vec::new(),
        files: Vec::new(),
    };

    let mut comment_with_registry = publication.clone();
    comment_with_registry.registry_rows.push(RegistryRow {
        epoch,
        source_schema: "public".into(),
        source_table: "comment_only_stream".into(),
        schema_version: SchemaVersionNo(1),
        descriptors: Vec::new(),
        columns: serde_json::json!([{"name": "id", "type_oid": 23}]),
    });
    assert!(matches!(
        publish_stream_commit(&pool, &comment_with_registry).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::Published
    );
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished
    );
    let groups: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_stream_manifest_group WHERE epoch = $1",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(groups, 0, "metadata-only DDL is not a structural barrier");

    clear_stream_publication_epoch(&pool, epoch).await;
}

#[tokio::test]
async fn corrupt_or_failed_head_unit_blocks_every_later_claim() {
    let pool = pool().await;

    // A ready positive-size parent remains authoritative even if its only child disappears. It
    // must be reported as corruption at the head, rather than disappearing from the inventory and
    // allowing the later singleton to pass.
    let missing_child_epoch = EpochNo(900_009);
    clear_stream_publication_epoch(&pool, missing_child_epoch).await;
    let missing_child = data_only_publication(
        missing_child_epoch,
        "missing_child_head",
        Lsn::new(0x500),
        860,
    );
    publish_stream_commit(&pool, &missing_child).await.unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol
         )
         DELETE FROM public.walrus_file_manifest
         WHERE epoch = $1 AND (SELECT protocol = '2' FROM authorized)",
    )
    .bind(missing_child_epoch.0)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        max_ready_lsn_end(&pool, missing_child_epoch, "public", "missing_child_head")
            .await
            .unwrap(),
        Some(Lsn::new(0x500)),
        "the ready parent itself is pending work even with no child"
    );
    assert!(
        manifest_work_exists(&pool, missing_child_epoch, "public", "missing_child_head")
            .await
            .unwrap()
    );
    insert_ready(
        &pool,
        &file(missing_child_epoch, "missing_child_head", "0/600"),
    )
    .await
    .unwrap();
    assert!(matches!(
        claim_ready_units(
            &pool,
            missing_child_epoch,
            "public",
            "missing_child_head",
            100
        )
        .await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));
    clear_stream_publication_epoch(&pool, missing_child_epoch).await;

    // Failed singleton rows are queue work, not skippable rows. A later ready file must remain
    // unreachable until integrity recovery resolves the failed head.
    let mut tx = pool.begin().await.unwrap();
    let failed_epoch = EpochNo(900_010);
    let failed = insert_ready(&mut *tx, &file(failed_epoch, "failed_head", "0/700"))
        .await
        .unwrap();
    sqlx::query("UPDATE public.walrus_file_manifest SET status='failed' WHERE id=$1")
        .bind(failed.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    insert_ready(&mut *tx, &file(failed_epoch, "failed_head", "0/800"))
        .await
        .unwrap();
    assert!(matches!(
        claim_ready_units(&mut *tx, failed_epoch, "public", "failed_head", 100).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));
    assert!(
        manifest_work_exists(&mut *tx, failed_epoch, "public", "failed_head")
            .await
            .unwrap(),
        "failed work must block legacy replay-fence migration"
    );
    tx.rollback().await.unwrap();

    // Applied/superseded parents are retained replay receipts, but retaining a child beneath one
    // is a torn retirement. The terminal parent must still enter the ordered inventory and block.
    let terminal_epoch = EpochNo(900_011);
    clear_stream_publication_epoch(&pool, terminal_epoch).await;
    let terminal =
        data_only_publication(terminal_epoch, "terminal_with_child", Lsn::new(0x900), 861);
    publish_stream_commit(&pool, &terminal).await.unwrap();
    sqlx::query(
        "UPDATE public.walrus_stream_manifest_group
         SET status='applied', applied_at=now()
         WHERE epoch=$1",
    )
    .bind(terminal_epoch.0)
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        claim_ready_units(&pool, terminal_epoch, "public", "terminal_with_child", 100).await,
        Err(control::ControlError::ManifestInvariant { .. })
    ));
    assert!(
        manifest_work_exists(&pool, terminal_epoch, "public", "terminal_with_child")
            .await
            .unwrap(),
        "a terminal parent retaining a child is still corrupt work"
    );
    clear_stream_publication_epoch(&pool, terminal_epoch).await;
}
