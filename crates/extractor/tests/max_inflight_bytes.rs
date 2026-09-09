#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Aggregate `max_inflight_bytes` ceiling (§1.3) against compose (`#[ignore]` — needs source PG with
//! `logical_decoding_work_mem=64kB` + MinIO + control PG). A large open transaction under a
//! deliberately **low** ceiling spills its buffer speculatively (bounding memory) while
//! `confirmed_flush_lsn` stays held at the open-txn floor. The meter/shed/hysteresis logic is
//! unit-tested in `src/memory.rs`; the per-sub-xid spill in `src/stream_txn.rs`.
//!
//!   cargo test -p extractor --test max_inflight_bytes -- --ignored

use common::{EpochNo, Lsn};
use extractor::batch::{BatchTriggers, SystemClock};
use extractor::checkpoint::DurabilityCheckpoint;
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
async fn large_txn_low_ceiling_spills_and_stays_bounded() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_inflight";
    let epoch = EpochNo(2_320_001);
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 840000 AND 849999",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let stager = ParquetStager::new(minio(), "walrus", epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    clear_control_epoch(&pool, epoch).await;
    // A deliberately LOW aggregate ceiling (64 KiB) — a large open txn must spill, not buffer it all.
    let ceiling: u64 = 64 * 1024;
    let mut demux = StreamDemux::new(
        BatchTriggers {
            max_rows: std::num::NonZeroU64::new(100_000).unwrap(),
            max_bytes: std::num::NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
        std::num::NonZeroU64::new(ceiling).unwrap(),
    );
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // Open replication only after the control-plane cleanup and demux setup.
    // The compose source has a five-second `wal_sender_timeout`, so an idle
    // stream created before that work can expire before frame consumption.
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // 8000 rows in one txn (~640 KiB of Arrow) ≫ the 64 KiB ceiling → repeated speculative spills.
    admin
        .batch_execute(
            "INSERT INTO public.orders (id, status)
             SELECT g, 'inflight' FROM generate_series(840000, 847999) g",
        )
        .await
        .unwrap();

    let mut committed = false;
    let mut checked_open = false;
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
                    let pre_start_ceiling = checkpoint.capture_pre_stream_start_ceiling();
                    demux
                        .on_stream_start(*xid, *first_segment, frame_lsn)
                        .unwrap();
                    if *first_segment {
                        checkpoint.on_stream_start(*xid, pre_start_ceiling).unwrap();
                    }
                }
                Message::StreamStop => demux.on_stream_stop().unwrap(),
                m @ (Message::Insert { xid: Some(_), .. }
                | Message::Update { xid: Some(_), .. }
                | Message::Delete { xid: Some(_), .. }) => {
                    demux
                        .on_change(&cache, m, &stager, frame_lsn)
                        .await
                        .unwrap();
                    // Mid-open-window: spilling happens, but the slot is NOT advanced (floor holds).
                    if !checked_open && demux.spill_count() > 0 {
                        assert_eq!(
                            checkpoint.confirmed_flush(),
                            resume.start_lsn(),
                            "a speculative spill frees memory but never advances confirmed_flush"
                        );
                        assert!(
                            demux.open_floor().is_some(),
                            "the txn is open → the floor holds"
                        );
                        checked_open = true;
                    }
                }
                Message::StreamCommit {
                    xid,
                    commit_lsn,
                    end_lsn,
                    commit_ts,
                    ..
                } => {
                    checkpoint.observe_commit(*commit_lsn, *end_lsn).unwrap();
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
                    checkpoint.on_stream_end(*xid).unwrap();
                    checkpoint.on_commit_durable(*commit_lsn).unwrap();
                    committed = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the streamed txn commits within 60s");

    assert!(
        checked_open,
        "the low ceiling forced at least one speculative spill mid-window"
    );
    assert!(
        demux.spill_count() > 0,
        "spill_count incremented under the low ceiling"
    );

    // All 8000 rows land as ready files on commit (spilled + in-memory, none lost).
    let total_rows: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(row_count), 0)::bigint FROM public.walrus_file_manifest WHERE epoch = $1 AND status = 'ready'",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        total_rows, 8000,
        "every committed row is durable after Stream Commit"
    );

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
            "DELETE FROM public.orders WHERE id BETWEEN 840000 AND 849999",
            &[],
        )
        .await;
    drop(stream);
    drop_slot(&admin, slot).await;
}
