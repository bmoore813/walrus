#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! The idle heartbeat + round-trip liveness against compose (`#[ignore]` — needs source PG + control
//! PG). On an idle publication a beat fires only after `idle_after`, its `beat_seq` returns through the
//! stream, and `confirmed_flush_lsn` advances as a result — while the beat is **never** staged to S3
//! nor written to a manifest row. The pure idle/degraded logic is unit-tested in `src/heartbeat.rs`.
//!
//!   cargo test -p extractor --test heartbeat_liveness -- --ignored

use common::Lsn;
use extractor::checkpoint::DurabilityCheckpoint;
use extractor::consume::on_frame;
use extractor::heartbeat::{Heartbeat, HeartbeatConfig, InternalTables};
use extractor::pgoutput::{Message, StreamCtx};
use extractor::replication::{ReplicationMessage, ReplicationStream};
use extractor::slot::verify_or_create_slot;
use std::time::Duration;
use tokio::time::Instant;
use tokio_postgres::NoTls;

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

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

async fn confirmed_flush(admin: &tokio_postgres::Client, slot: &str) -> Lsn {
    let row = admin
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
    let s: Option<String> = row.get(0);
    s.map(|x| x.parse().unwrap()).unwrap_or(Lsn::ZERO)
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn idle_publication_beats_and_advances_confirmed_flush() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_heartbeat";
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();

    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let mut heartbeat = Heartbeat::connect(
        &source_url(),
        "walrus-heartbeat-test".to_string(),
        HeartbeatConfig {
            idle_after: Duration::from_millis(300),
            roundtrip_deadline: Duration::from_secs(5),
        },
    )
    .await
    .unwrap();

    // Active (last_activity == now) → the beat is suppressed.
    let t0 = Instant::now();
    assert!(
        heartbeat.maybe_beat(t0, t0).await.unwrap().is_none(),
        "under activity the beat must not fire"
    );

    // Idle past `idle_after` → exactly one beat fires and returns its new seq.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let seq = heartbeat
        .maybe_beat(Instant::now(), t0)
        .await
        .unwrap()
        .expect("an idle publication beats after idle_after");

    // The beat rides the published public.walrus_heartbeat, so it decodes back through the stream.
    let mut internal = InternalTables::default();
    let mut ctx = StreamCtx::default();
    let mut saw_beat = false;
    let mut hb_commit: Option<(Lsn, Lsn)> = None;
    tokio::time::timeout(Duration::from_secs(15), async {
        while hb_commit.is_none() {
            let frame = stream.next().await.unwrap().unwrap();
            let _frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            if let Some(msg) = on_frame(&mut ctx, frame).unwrap() {
                match &msg {
                    Message::Relation { relation, .. } => internal.note_relation(relation),
                    Message::Update {
                        relation_oid, new, ..
                    } if internal.is_internal(*relation_oid) => {
                        let got = internal
                            .beat_seq_of(new)
                            .expect("beat_seq decodes from the tuple");
                        assert!(
                            got >= seq,
                            "returned seq {got} >= pending {seq} closes the round-trip"
                        );
                        heartbeat.observe_return(got, Instant::now());
                        saw_beat = true;
                    }
                    Message::Commit {
                        commit_lsn,
                        end_lsn,
                        ..
                    } if saw_beat => hb_commit = Some((*commit_lsn, *end_lsn)),
                    _ => {}
                }
            }
        }
    })
    .await
    .expect("the beat returns through the stream within 15s");

    // Idle → no un-durable user data → the beat's commit advances confirmed_flush.
    let (hb, hb_end) = hb_commit.unwrap();
    assert!(hb_end > hb, "Commit end_lsn must follow its commit_lsn");
    checkpoint.observe_commit(hb, hb_end).unwrap();
    checkpoint.on_commit_durable(hb).unwrap();
    checkpoint.send(&mut stream, false).await.unwrap();
    let mut reached = false;
    for _ in 0..40 {
        if confirmed_flush(&admin, slot).await >= hb_end {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        reached,
        "the idle beat advanced confirmed_flush_lsn through its commit record"
    );

    // The heartbeat change is NEVER staged: no manifest row was written for public.walrus_heartbeat.
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE source_table = 'walrus_heartbeat'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        staged, 0,
        "the heartbeat is control-plane, never a staged file"
    );

    // A completed round-trip leaves the extractor un-degraded.
    assert!(
        !heartbeat.is_degraded(Instant::now()),
        "a fresh round-trip is not degraded"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}
