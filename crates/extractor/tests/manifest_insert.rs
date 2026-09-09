#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Durability step (b) against compose (`#[ignore]` — needs MinIO + control PG). After a durable PUT,
//! a `file_manifest` `ready` row is committed with `lsn_end` = the **commit** LSN. Each test runs in a
//! rolled-back transaction (control DB) under a unique epoch, and cleans up its S3 object.
//!
//!   cargo test -p extractor --test manifest_insert -- --ignored

use common::{
    EpochNo, ExtractorMeta, Kind, Lsn, Op, PgColumn, PgRelation, ReplicaIdentity, TupleValue,
    UtcTimestamp,
};
use control::reload::{self, ExportRangePlan, ExportSnapshot, ReloadFenceIdentity, ReloadFlavor};
use control::{connect, run_migrations};
use extractor::batch::SealedBatch;
use extractor::checkpoint::DurabilityCheckpoint;
use extractor::consume::flush_batch;
use extractor::staging::ParquetStager;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use pg_to_arrow::{BatchBuilder, oids};
use sqlx::postgres::{PgConnection, PgPool};
use std::sync::Arc;

fn minio_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .expect("build MinIO store"),
    )
}

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn control_pool() -> PgPool {
    let pool = connect(&control_url())
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
    publication_nonce: uuid::Uuid,
) -> i64 {
    let reload_id = reload::request(&mut *conn, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    let claimed = reload::claim_requested(&mut *conn, epoch, "manifest-insert-test", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease = claimed.exporter_lease("manifest-insert-test").unwrap();
    let start_lsn: Lsn = "0/100".parse().unwrap();
    let schema_version = common::SchemaVersionNo(1);
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
             publisher_owner_pod = 'manifest-insert-test', publisher_fencing_token = 1,
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

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, typmod: i32| PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    };
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", oids::INT4, -1), col("note", oids::TEXT, -1)],
    }
}

/// Row LSN is deliberately `0/10` — far below the commit LSN passed to `sealed()`.
fn meta() -> ExtractorMeta {
    ExtractorMeta {
        op: Op::Insert,
        lsn: "0/10".parse().unwrap(),
        commit_lsn: "0/10".parse().unwrap(),
        commit_ts: "2026-07-07T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        xid: 1,
        epoch: EpochNo(1),
        batch_id: "b".to_string(),
        schema_version: common::SchemaVersionNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "walrus-extractor-0".to_string(),
        extractor_processed_at: UtcTimestamp::now(),
    }
}

/// A one-row batch whose **commit LSN** (`lsn_end`) is `lsn_end`, distinct from the row LSN (`0/10`).
fn sealed(lsn_end: &str) -> SealedBatch {
    let mut bb = BatchBuilder::new(&orders()).unwrap();
    bb.append_row(
        &[TupleValue::Text("1".into()), TupleValue::Text("hi".into())],
        &meta(),
    )
    .unwrap();
    SealedBatch {
        record_batch: bb.into_record_batch().unwrap(),
        schema: "public".to_string(),
        table: "orders".to_string(),
        schema_version: common::SchemaVersionNo(5),
        lsn_start: "0/A000".parse().unwrap(),
        lsn_end: lsn_end.parse().unwrap(),
        row_count: 1,
    }
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn object_and_manifest_row_both_exist_after_flush() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_250_001);
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", epoch);

    let obj = flush_batch(&stager, &mut *tx, epoch, sealed("0/A100"))
        .await
        .unwrap();

    // The object is durably present in S3.
    assert!(store.head(&obj.key).await.unwrap().size > 0);
    // Exactly one ready row was committed (in this tx).
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(count, 1);

    tx.rollback().await.unwrap();
    let _ = store.delete(&obj.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn manifest_lsn_end_equals_commit_lsn_not_row_lsn() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_250_002);
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", epoch);

    // Commit LSN 0/A100; the batch's rows carry row LSN 0/10.
    let obj = flush_batch(&stager, &mut *tx, epoch, sealed("0/A100"))
        .await
        .unwrap();

    let lsn_text: String = sqlx::query_scalar(
        "SELECT lsn_end::text FROM public.walrus_file_manifest WHERE epoch = $1",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let stored: Lsn = lsn_text.parse().unwrap();
    assert_eq!(stored, obj.lsn_end, "lsn_end is the batch's commit LSN");
    assert_eq!(stored, "0/A100".parse().unwrap());
    assert_ne!(stored, "0/10".parse().unwrap(), "NOT the max row LSN");

    tx.rollback().await.unwrap();
    let _ = store.delete(&obj.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn row_is_ready_kind_stream_and_epoch_stamped() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_250_003);
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", epoch);

    let obj = flush_batch(&stager, &mut *tx, epoch, sealed("0/B200"))
        .await
        .unwrap();

    let (kind, status, schema_version): (String, String, i64) = sqlx::query_as(
        "SELECT kind, status, schema_version FROM public.walrus_file_manifest WHERE epoch = $1",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(kind, "stream");
    assert_eq!(status, "ready");
    assert_eq!(schema_version, 5, "schema_version stamped from the batch");

    tx.rollback().await.unwrap();
    let _ = store.delete(&obj.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn sealed_replay_deletes_object_and_remains_clamped_by_an_open_stream() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_250_004);
    let seal: Lsn = "0/B000".parse().unwrap();
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", epoch);
    let publication_nonce = uuid::Uuid::new_v4();

    let reload_id = publishing_reload(&mut tx, epoch, "orders", seal, publication_nonce).await;
    sqlx::query("SELECT set_config('walrus.manifest_seal_protocol', '2', true)")
        .execute(&mut *tx)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO public.walrus_manifest_publication_fence
           (epoch, source_schema, source_table, sealed_through_lsn,
            sealed_reload_id, sealed_publication_nonce)
         VALUES ($1, 'public', 'orders', $2, $3, $4)",
    )
    .bind(epoch.0)
    .bind(seal)
    .bind(reload_id)
    .bind(publication_nonce)
    .execute(&mut *tx)
    .await
    .unwrap();

    // Model the reachable lost-ACK schedule: another protocol-v2 transaction keeps the global slot
    // feedback behind this ordinary commit even though its object/control disposition is durable.
    let resume: Lsn = "0/100".parse().unwrap();
    let mut checkpoint = DurabilityCheckpoint::new(resume);
    let ceiling = checkpoint.capture_pre_stream_start_ceiling();
    checkpoint.on_stream_start(77, ceiling).unwrap();

    let obj = flush_batch(&stager, &mut tx, epoch, sealed("0/A100"))
        .await
        .unwrap();
    assert!(
        store.head(&obj.key).await.is_err(),
        "CoveredBySeal must best-effort delete the new unreferenced object"
    );
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest
         WHERE epoch=$1 AND source_schema='public' AND source_table='orders'",
    )
    .bind(epoch.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(rows, 0, "covered replay must not recreate manifest work");

    let feedback_end = Lsn::new(obj.lsn_end.as_u64().saturating_add(8));
    checkpoint
        .observe_commit(obj.lsn_end, feedback_end)
        .unwrap();
    checkpoint.on_commit_durable(obj.lsn_end).unwrap();
    assert_eq!(
        checkpoint.confirmed_flush(),
        resume,
        "covered ordinary work is durable, but the unrelated open stream still clamps global ACK"
    );
    assert!(checkpoint.on_stream_end(77).unwrap());
    assert_eq!(
        checkpoint.confirmed_flush(),
        feedback_end,
        "once the stream ends, the covered commit's end_lsn becomes ACK-eligible"
    );

    tx.rollback().await.unwrap();
}
