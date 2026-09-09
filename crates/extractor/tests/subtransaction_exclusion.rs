#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! The flagship correctness test (§1.6, proto §9b) against compose (`#[ignore]` — needs source PG with
//! `logical_decoding_work_mem=64kB` + MinIO + control PG). A rolled-back **savepoint** inside an
//! otherwise-committing streamed transaction is *still streamed*; only `Stream Abort {sub != top}` tells
//! us those rows died. 3000 kept-A + a rolled-back savepoint + 3000 kept-B must yield a `ready` file
//! with **exactly 6000** rows — the rolled-back rows are never present. Off-by-one here is precisely the
//! silent mirror corruption this test exists to catch. The demux logic is unit-tested in
//! `src/stream_txn.rs`.
//!
//!   cargo test -p extractor --test subtransaction_exclusion -- --ignored

use common::{EpochNo, Lsn};
use extractor::batch::{BatchTriggers, SystemClock};
use extractor::consume::on_frame;
use extractor::pgoutput::{Message, StreamCtx};
use extractor::relcache::RelationCache;
use extractor::replication::{ReplicationMessage, ReplicationStream};
use extractor::slot::verify_or_create_slot;
use extractor::staging::ParquetStager;
use extractor::stream_txn::StreamDemux;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

#[path = "support/stream_commit.rs"]
mod stream_commit_support;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn source() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn minio() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

async fn clear_control_epoch(pool: &sqlx::PgPool, epoch: EpochNo) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_delete_protocol','2',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    for table in [
        "file_manifest",
        "stream_manifest_group",
        "stream_txn_publication",
        "manifest_publication_fence",
    ] {
        let statement = format!("DELETE FROM public.walrus_{table} WHERE epoch = $1");
        sqlx::query(&statement)
            .bind(epoch)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn savepoint_rollback_ready_file_has_exactly_6000_rows() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_subtxn";
    let epoch = EpochNo(2_310_001);
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 810000 AND 839999",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let stager = ParquetStager::new(minio(), "walrus", epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    // Start clean: a prior failed run may have left ready rows for this epoch.
    clear_control_epoch(&pool, epoch).await;
    let mut demux = StreamDemux::new(
        BatchTriggers {
            max_rows: std::num::NonZeroU64::new(100_000).unwrap(),
            max_bytes: std::num::NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
        std::num::NonZeroU64::MAX,
    );
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // Delay opening replication until setup is complete so the compose
    // source's five-second `wal_sender_timeout` cannot expire an idle sender.
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // proto §9b: kept-A (3000, top branch) · rolled-back savepoint (3000, streamed then discarded) ·
    // kept-B (3000, new savepoint after the rollback). The whole thing exceeds work_mem → streams.
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO public.orders (id, status) SELECT g, 'A' FROM generate_series(810000, 812999) g;
             SAVEPOINT sp;
             INSERT INTO public.orders (id, status) SELECT g, 'X' FROM generate_series(820000, 822999) g;
             ROLLBACK TO SAVEPOINT sp;
             INSERT INTO public.orders (id, status) SELECT g, 'B' FROM generate_series(830000, 832999) g;
             COMMIT;",
        )
        .await
        .unwrap();

    let mut saw_subabort = false;
    let mut committed = false;
    tokio::time::timeout(Duration::from_secs(60), async {
        while !committed {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, xid } => {
                    cache
                        .upsert_from_relation(relation.clone(), common::SchemaVersionNo(1))
                        .unwrap();
                    if let (Some(sub_xid), Some(top_xid)) = (*xid, demux.current_top()) {
                        demux.bind_relation(
                            top_xid,
                            sub_xid,
                            relation.oid,
                            common::SchemaVersionNo(1),
                        );
                    }
                }
                Message::StreamStart { xid, first_segment } => {
                    demux
                        .on_stream_start(*xid, *first_segment, frame_lsn)
                        .unwrap();
                }
                Message::StreamStop => demux.on_stream_stop().unwrap(),
                m @ (Message::Insert { xid: Some(_), .. }
                | Message::Update { xid: Some(_), .. }
                | Message::Delete { xid: Some(_), .. }) => {
                    demux
                        .on_change(&cache, m, &stager, frame_lsn)
                        .await
                        .unwrap();
                }
                Message::StreamAbort { top_xid, sub_xid } => {
                    if top_xid != sub_xid {
                        saw_subabort = true;
                    }
                    demux
                        .on_stream_abort(*top_xid, *sub_xid, &stager)
                        .await
                        .unwrap();
                }
                Message::StreamCommit {
                    xid,
                    commit_lsn,
                    commit_ts,
                    ..
                } => {
                    let commit_timestamp =
                        common::UtcTimestamp::from_pg_micros(*commit_ts).unwrap();
                    let objs = demux
                        .on_stream_commit(*xid, *commit_lsn, commit_timestamp, &cache, &stager)
                        .await
                        .unwrap();
                    assert_eq!(
                        stream_commit_support::publish(
                            &pool,
                            epoch,
                            *xid,
                            *commit_lsn,
                            commit_timestamp,
                            &objs,
                        )
                        .await
                        .unwrap(),
                        control::PublishStreamOutcome::Published,
                    );
                    committed = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the streamed txn commits within 60s");

    assert!(
        saw_subabort,
        "a Stream Abort {{sub != top}} (rolled-back savepoint) was decoded"
    );

    // THE flagship assertion: exactly 6000 rows in the ready file(s) — never the rolled-back savepoint.
    let total_rows: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(row_count), 0)::bigint FROM public.walrus_file_manifest WHERE epoch = $1 AND status = 'ready'",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        total_rows, 6000,
        "the ready file has EXACTLY 6000 rows (3000 kept-A + 3000 kept-B); the rolled-back savepoint's rows are excluded"
    );
    // Every child is grouped streamed work. A transaction may contain both speculative `spill`
    // objects and commit-time `stream` objects; neither is legal as an ungrouped manifest.
    let invalid_children: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest \
         WHERE epoch = $1 AND (kind NOT IN ('stream', 'spill') OR stream_group_id IS NULL)",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        invalid_children, 0,
        "all committed survivors are grouped stream/spill children"
    );
    let groups: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT expected_files, row_count FROM public.walrus_stream_manifest_group \
         WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'orders' \
           AND status = 'ready'",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        groups.len(),
        1,
        "one source commit publishes one table group"
    );
    let child_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(groups[0], (child_count, 6000));

    // Cleanup.
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM public.walrus_file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&object_store::path::Path::from(key)).await;
        }
    }
    clear_control_epoch(&pool, epoch).await;
    let _ = admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 810000 AND 839999",
            &[],
        )
        .await;
    drop(stream);
    drop_slot(&admin, slot).await;
}
