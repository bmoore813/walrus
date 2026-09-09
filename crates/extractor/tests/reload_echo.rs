#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Echo routing against compose (`#[ignore]` — needs source PG + control PG). A manual
//! `public.walrus_reload_signal` INSERT comes back through the slot, resolves a subscribed waiter with
//! the transaction's COMMIT LSN (the chunk watermark `L_i`), passes the embedded-LSN cross-check,
//! and never touches a batcher / Parquet file / manifest row — while a **positive control** (a
//! `public.orders` insert in the same window, under a seal-happy `max_rows = 1`) proves the
//! harness genuinely detects sealing, so the never-batched assertion is falsifiable. The
//! registry/buffer semantics are unit-tested in `src/reload_signal.rs`; this drives the REAL
//! seams (`on_frame`, `on_relation` — whose internal-table refusal is the production defense —
//! `InternalTables`, `PendingSignals`, `BatchRouter`, `DurabilityCheckpoint`) against live WAL.
//!
//!   cargo test -p extractor --test reload_echo -- --ignored

use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use extractor::batch::{BatchTriggers, SealedBatch, SystemClock};
use extractor::checkpoint::DurabilityCheckpoint;
use extractor::consume::{BatchRouter, on_frame, on_relation};
use extractor::heartbeat::InternalTables;
use extractor::pgoutput::{Message, StreamCtx};
use extractor::relcache::RelationCache;
use extractor::reload_signal::{PendingSignal, PendingSignals, WatermarkWaiters};
use extractor::replication::{ReplicationMessage, ReplicationStream};
use extractor::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn admin() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
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

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn signal_insert_resolves_waiter_and_never_reaches_parquet() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_reload_echo";
    let reload_id = ReloadId(990_042);
    let epoch = EpochNo(990_042);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    // Cleanup BEFORE the slot exists so these deletes' WAL never streams.
    admin
        .execute(
            "DELETE FROM public.walrus_reload_signal WHERE reload_id = $1",
            &[&reload_id.0],
        )
        .await
        .unwrap();
    admin
        .execute("DELETE FROM public.orders WHERE id = 990042", &[])
        .await
        .unwrap();
    drop_slot(&admin, slot).await;

    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    for tbl in ["file_manifest", "schema_registry"] {
        let sql = cleanup_sql(tbl);
        sqlx::query(&sql).bind(epoch).execute(&pool).await.unwrap();
    }

    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // The real consume-loop pieces exercised by this test.
    let waiters = Arc::new(WatermarkWaiters::default());
    let mut pending = PendingSignals::default();
    let mut internal = InternalTables::default();
    let mut cache = RelationCache::default();
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: std::num::NonZeroU64::MIN, // seal-happy: ANY routed row seals a batch at its commit
            max_bytes: std::num::NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
    );

    // Subscribe-then-insert: the registry holds the sender BEFORE the row exists. The positive
    // control (a published user-table insert) rides the same window so sealing is observable.
    let rx = waiters.subscribe(reload_id, 1);
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (990042, 'echo-control')",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO public.walrus_reload_signal (reload_id, chunk_no) VALUES ($1, 1)",
            &[&reload_id.0],
        )
        .await
        .unwrap();

    // Mini consume loop mirroring src/consume.rs's routing arms for the messages involved. It
    // tolerates foreign commits (returns only at the commit that CARRIED the signal), so another
    // writer on the shared compose source can't wedge `rx` forever.
    let mut ctx = StreamCtx::default();
    let mut sealed: Vec<SealedBatch> = Vec::new();
    let mut signal_seen = false;
    let mut signal_insert_frame_lsn = Lsn::ZERO;
    let (commit_lsn, end_lsn) = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => {
                    internal.note_relation(relation);
                    // The REAL registration path: its `is_internal_table` refusal is the
                    // production defense that keeps the signal table out of the cache — a
                    // membership regression here surfaces below as a sealed signal batch.
                    on_relation(
                        &mut cache,
                        &pool,
                        epoch,
                        relation.clone(),
                        SchemaVersionNo(1),
                    )
                    .await
                    .unwrap();
                }
                Message::Insert {
                    relation_oid,
                    new,
                    xid,
                } if internal.is_reload_signal(*relation_oid) => {
                    let rel = internal.reload_signal_rel().expect("noted relation");
                    pending.push(PendingSignal::from_tuple(rel, new, *xid, None).expect("parses"));
                    signal_seen = true;
                    signal_insert_frame_lsn = frame_lsn;
                }
                Message::Commit {
                    commit_lsn,
                    end_lsn,
                    ..
                } => {
                    sealed.extend(
                        router
                            .route(&cache, &msg, frame_lsn, SchemaVersionNo(1))
                            .unwrap(),
                    );
                    pending.on_commit(*commit_lsn, &waiters);
                    if signal_seen {
                        return (*commit_lsn, *end_lsn);
                    }
                }
                other => {
                    sealed.extend(
                        router
                            .route(&cache, other, frame_lsn, SchemaVersionNo(1))
                            .unwrap(),
                    );
                }
            }
        }
    })
    .await
    .expect("the signal txn commits within 20s");

    // The waiter resolved with the transaction's COMMIT LSN — not the Insert message's frame LSN
    // (the distinction is the whole point: the watermark is a commit-time property).
    let echo = rx.await.expect("waiter resolved at Commit");
    assert_eq!(echo.commit_lsn, commit_lsn, "the stamp is the commit LSN");
    assert!(
        signal_insert_frame_lsn < echo.commit_lsn,
        "the Insert frame's LSN ({signal_insert_frame_lsn}) precedes the commit LSN ({}) — \
         resolving with the frame LSN would be the classic wrong-stamp bug",
        echo.commit_lsn
    );
    assert!(
        echo.embedded_lsn < echo.commit_lsn,
        "embedded wal_insert_lsn ({}) must precede the commit LSN ({})",
        echo.embedded_lsn,
        echo.commit_lsn
    );
    assert_eq!(waiters.crosscheck_violations(), 0);

    // Positive control first: the harness CAN detect sealing — the orders insert sealed under
    // max_rows=1. Then the load-bearing half: no sealed batch is ever the signal table's.
    assert!(
        sealed
            .iter()
            .any(|b| b.schema == "public" && b.table == "orders"),
        "positive control: the user-table insert must seal a batch (else this test proves nothing)"
    );
    assert!(
        !sealed
            .iter()
            .any(|b| b.schema == "public" && b.table == "walrus_reload_signal"),
        "a signal echo must never reach a batcher"
    );
    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE source_table = 'walrus_reload_signal'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        staged, 0,
        "no Parquet/manifest row for the signal table, ever"
    );

    // Signals need no special retention: once the echo is handled, standby feedback advances
    // confirmed_flush to the transaction's end_lsn like any consumed record, and the server accepts
    // it without leaving the commit record at the resume boundary.
    checkpoint.observe_commit(commit_lsn, end_lsn).unwrap();
    checkpoint.on_commit_durable(commit_lsn).unwrap();
    checkpoint.send(&mut stream, false).await.unwrap();
    let confirmed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = admin
                .query_one(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots
                     WHERE slot_name = $1",
                    &[&slot],
                )
                .await
                .unwrap();
            let lsn: Lsn = row.get::<_, String>(0).parse().unwrap();
            if lsn >= end_lsn {
                return lsn;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("confirmed_flush advances past the signal txn");
    assert!(end_lsn > commit_lsn);
    assert!(confirmed >= end_lsn);

    // Cleanup: slot, source rows, and this epoch's control rows (registry writes from on_relation).
    drop(stream);
    drop_slot(&admin, slot).await;
    admin
        .execute(
            "DELETE FROM public.walrus_reload_signal WHERE reload_id = $1",
            &[&reload_id.0],
        )
        .await
        .unwrap();
    admin
        .execute("DELETE FROM public.orders WHERE id = 990042", &[])
        .await
        .unwrap();
    for tbl in ["file_manifest", "schema_registry"] {
        let sql = cleanup_sql(tbl);
        sqlx::query(&sql).bind(epoch).execute(&pool).await.unwrap();
    }
}

fn cleanup_sql(table: &str) -> String {
    if table == "file_manifest" {
        format!(
            "WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM public.walrus_{table} WHERE epoch = $1 AND (SELECT protocol = '2' FROM authorized)"
        )
    } else if table == "schema_registry" {
        format!(
            "WITH authorized AS MATERIALIZED (SELECT set_config('walrus.schema_registry_maintenance','1-delete',true) AS protocol) DELETE FROM public.walrus_{table} WHERE epoch = $1 AND (SELECT protocol = '1-delete' FROM authorized)"
        )
    } else {
        format!("DELETE FROM public.walrus_{table} WHERE epoch = $1")
    }
}
