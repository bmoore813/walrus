#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Large-transaction streaming against compose (`#[ignore]` — needs source PG with
//! `logical_decoding_work_mem=64kB` + MinIO + control PG). An 8000-row txn arrives *before* its commit
//! as interleaved `Stream` blocks: the demux stages speculatively (no manifest), holds
//! `confirmed_flush` at the open txn's begin LSN, and only writes `ready` rows on `Stream Commit`. A
//! whole-txn `Stream Abort` leaves no `ready` row. The demux logic is unit-tested in `src/stream_txn.rs`.
//!
//!   cargo test -p extractor --test streaming_large_txn -- --ignored

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
    // A dropped replication stream can remain active briefly while Postgres notices the closed
    // socket. Terminate any leaked test walsender, then wait for the slot to become droppable so
    // this integration target cannot contaminate the following E2E slot-count assertions.
    let _ = admin
        .execute(
            "SELECT pg_terminate_backend(active_pid)
             FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
            &[&slot],
        )
        .await;
    for _ in 0..50 {
        let active = admin
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .ok()
            .flatten()
            .map(|row| row.get::<_, bool>(0));
        match active {
            None => return,
            Some(false) => {
                let _ = admin
                    .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
                    .await;
                return;
            }
            Some(true) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

async fn ready_count(pool: &sqlx::PgPool, epoch: EpochNo) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE epoch = $1 AND status = 'ready'",
    )
    .bind(epoch)
    .fetch_one(pool)
    .await
    .unwrap()
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

async fn cleanup(pool: &sqlx::PgPool, admin: &tokio_postgres::Client, epoch: EpochNo, slot: &str) {
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM public.walrus_file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&object_store::path::Path::from(key)).await;
        }
    }
    clear_control_epoch(pool, epoch).await;
    let _ = admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 800000 AND 809000",
            &[],
        )
        .await;
    drop_slot(admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn large_txn_single_ready_file_only_after_stream_commit() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_stream";
    let epoch = EpochNo(2_300_001);
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 800000 AND 809000",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let stager = ParquetStager::new(minio(), "walrus", epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let mut demux = StreamDemux::new(
        // Small caps → the open txn spills speculatively (no manifest) many times before commit.
        BatchTriggers {
            max_rows: std::num::NonZeroU64::new(500).unwrap(),
            max_bytes: std::num::NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
        std::num::NonZeroU64::MAX,
    );
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // Delay opening replication until setup is complete so the compose
    // source's five-second `wal_sender_timeout` cannot expire an idle sender.
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // 8000 rows in ONE transaction → exceeds logical_decoding_work_mem=64kB → streams before commit.
    admin
        .batch_execute(
            "INSERT INTO public.orders (id, status)
             SELECT g, 'streamed' FROM generate_series(800000, 807999) g",
        )
        .await
        .unwrap();

    let mut streamed_changes = 0u64;
    let mut mid_checked = false;
    let mut commit_boundary: Option<(Lsn, Lsn)> = None;
    tokio::time::timeout(Duration::from_secs(45), async {
        while commit_boundary.is_none() {
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
                    streamed_changes += 1;
                    // Mid-open-window: NOT committed yet — no ready rows, slot NOT advanced.
                    if !mid_checked && streamed_changes >= 1000 {
                        assert_eq!(
                            ready_count(&pool, epoch).await,
                            0,
                            "no ready row while the txn is open"
                        );
                        assert_eq!(
                            checkpoint.confirmed_flush(),
                            resume.start_lsn(),
                            "confirmed_flush held (not advanced) for the whole open window"
                        );
                        assert!(
                            demux.open_floor().is_some(),
                            "the txn is open → a floor exists"
                        );
                        mid_checked = true;
                    }
                }
                Message::StreamCommit {
                    xid,
                    commit_lsn: clsn,
                    end_lsn,
                    commit_ts,
                    ..
                } => {
                    checkpoint.observe_commit(*clsn, *end_lsn).unwrap();
                    let commit_timestamp =
                        common::UtcTimestamp::from_pg_micros(*commit_ts).unwrap();
                    let objs = demux
                        .on_stream_commit(*xid, *clsn, commit_timestamp, &cache, &stager)
                        .await
                        .unwrap();
                    assert_eq!(
                        stream_commit_support::publish(
                            &pool,
                            epoch,
                            *xid,
                            *clsn,
                            commit_timestamp,
                            &objs,
                        )
                        .await
                        .unwrap(),
                        control::PublishStreamOutcome::Published,
                    );
                    checkpoint.on_stream_end(*xid).unwrap();
                    checkpoint.on_commit_durable(*clsn).unwrap();
                    commit_boundary = Some((*clsn, *end_lsn));
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the streamed txn commits within 45s");

    assert!(
        mid_checked,
        "the txn actually streamed (>=1000 changes before commit)"
    );
    let (clsn, end_lsn) = commit_boundary.unwrap();
    // Only AFTER Stream Commit does one complete group become ready. Both speculative `spill`
    // children and commit-time `stream` children share the authoritative commit LSN.
    let files: Vec<(String, String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT kind, lsn_end::text, stream_group_id, stream_group_ordinal \
         FROM public.walrus_file_manifest WHERE epoch = $1 ORDER BY stream_group_ordinal",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        !files.is_empty(),
        "Stream Commit promoted the speculative files to ready"
    );
    assert!(
        files
            .iter()
            .all(|(kind, _, _, _)| matches!(kind.as_str(), "stream" | "spill")),
        "a streamed commit contains only stream/spill children"
    );
    assert!(
        files
            .iter()
            .all(|(_, end, _, _)| end.parse::<Lsn>().unwrap() == clsn),
        "every ready file's lsn_end is the commit LSN"
    );
    let group_id = files[0].2.expect("stream child has a group");
    assert!(
        files
            .iter()
            .enumerate()
            .all(|(ordinal, (_, _, child_group, child_ordinal))| {
                *child_group == Some(group_id) && *child_ordinal == i64::try_from(ordinal).ok()
            }),
        "all children belong to one complete, ordinally contiguous group"
    );
    let (expected_files, group_rows, group_lsn): (i64, i64, String) = sqlx::query_as(
        "SELECT expected_files, row_count, commit_lsn::text \
         FROM public.walrus_stream_manifest_group WHERE id = $1 AND status = 'ready'",
    )
    .bind(group_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(expected_files, i64::try_from(files.len()).unwrap());
    assert_eq!(group_rows, 8000);
    assert_eq!(group_lsn.parse::<Lsn>().unwrap(), clsn);
    assert_eq!(
        checkpoint.confirmed_flush(),
        end_lsn,
        "manifest ordering stays at commit_lsn while confirmed_flush advances to end_lsn"
    );
    assert!(end_lsn > clsn);

    drop(stream);
    cleanup(&pool, &admin, epoch, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn whole_txn_abort_writes_no_ready_row() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_stream_abort";
    let epoch = EpochNo(2_300_002);
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 800000 AND 809000",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let stager = ParquetStager::new(minio(), "walrus", epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let mut demux = StreamDemux::new(
        BatchTriggers {
            max_rows: std::num::NonZeroU64::new(500).unwrap(),
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

    // A big txn that ROLLS BACK: a live walsender streams the rows (proto §9a) then Stream Abort.
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO public.orders (id, status)
             SELECT g, 'aborted' FROM generate_series(800000, 807999) g;
             ROLLBACK;",
        )
        .await
        .unwrap();
    // A trailing committed change gives the loop a definite stop point after the abort.
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (809000, 'after')",
            &[],
        )
        .await
        .unwrap();

    let mut saw_abort = false;
    let mut done = false;
    tokio::time::timeout(Duration::from_secs(45), async {
        while !done {
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
                    demux
                        .on_stream_abort(*top_xid, *sub_xid, &stager)
                        .await
                        .unwrap();
                    saw_abort = true;
                }
                // The trailing small (non-streamed) commit ends the loop.
                Message::Commit { .. } if saw_abort => done = true,
                _ => {}
            }
        }
    })
    .await
    .expect("the aborted txn streams + aborts within 45s");

    assert!(saw_abort, "a whole-txn Stream Abort was decoded");
    assert_eq!(
        ready_count(&pool, epoch).await,
        0,
        "an aborted streamed txn writes NO ready row"
    );

    drop(stream);
    cleanup(&pool, &admin, epoch, slot).await;
}
