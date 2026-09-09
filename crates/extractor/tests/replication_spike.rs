#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
#![allow(
    clippy::unreachable,
    reason = "transport helper: this arm asserts the task cannot finish normally"
)]
//! `START_REPLICATION` transport spike against the compose source (`#[ignore]` — needs source PG on
//! `trust` auth with `wal_sender_timeout=5s`). After `docker compose up --wait`:
//!
//!   cargo test -p extractor --test replication_spike -- --ignored
//!
//! Each test uses its own slot name and cleans up, so runs are idempotent.

use extractor::replication::{ReplicationMessage, ReplicationStream};
use extractor::slot::verify_or_create_slot;
use std::time::Duration;
use tokio_postgres::NoTls;

const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");

// These tests share source-pg state (the public Walrus protocol tables and slots), so they run serially.
static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}

async fn admin() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&source_url(), NoTls)
        .await
        .expect("connect to source");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn drop_slot(client: &tokio_postgres::Client, slot: &str) {
    // Best-effort: only an inactive, present slot can be dropped.
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

/// Ensure the walrus tables exist, drop any leftover slot, and create a fresh one → resume LSN.
async fn fresh_slot(client: &tokio_postgres::Client, slot: &str) -> common::Lsn {
    client.batch_execute(SOURCE_MIGRATION).await.unwrap();
    drop_slot(client, slot).await;
    verify_or_create_slot(client, slot)
        .await
        .expect("create slot")
        .start_lsn()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG, trust auth)"]
async fn source_write_yields_at_least_one_xlogdata() {
    let slot = "walrus_spike_xlog";
    let _guard = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    let start = fresh_slot(&admin, slot).await;

    let mut stream = ReplicationStream::start(&source_url(), slot, start, "walrus_pub")
        .await
        .expect("START_REPLICATION");

    // Generate WAL on a published table.
    admin
        .execute(
            "UPDATE public.walrus_heartbeat SET beat_seq = beat_seq + 1, ts = now() WHERE id = 1",
            &[],
        )
        .await
        .unwrap();

    let saw_xlog = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match stream.next().await.expect("frame") {
                Some(ReplicationMessage::XLogData { data, .. }) if !data.is_empty() => break true,
                Some(_) => continue, // keepalive — keep reading
                None => break false,
            }
        }
    })
    .await
    .expect("an XLogData frame within 10s");
    assert!(
        saw_xlog,
        "an UPDATE on a published table should yield ≥1 XLogData frame"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG, wal_sender_timeout=5s)"]
async fn connection_survives_past_wal_sender_timeout() {
    let slot = "walrus_spike_survive";
    let _guard = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    let start = fresh_slot(&admin, slot).await;
    let mut stream = ReplicationStream::start(&source_url(), slot, start, "walrus_pub")
        .await
        .unwrap();

    // Drive the idle stream for > wal_sender_timeout (5s). Our unconditional feedback keeps it
    // alive, so the loop never returns → the outer timeout fires. If the walsender severed us,
    // `next()` would error before then.
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match stream.next().await {
                Ok(Some(_)) => {} // XLogData / keepalive — still alive
                Ok(None) => return Err("stream ended early".to_string()),
                Err(e) => return Err(format!("stream errored (walsender severed us?): {e}")),
            }
        }
    })
    .await;

    match result {
        Err(_elapsed) => {} // survived the full 8s (> 5s wal_sender_timeout) — the spike's point
        Ok(Err(msg)) => panic!("{msg}"),
        Ok(Ok(())) => unreachable!(),
    }

    drop(stream);
    drop_slot(&admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG, wal_sender_timeout=5s)"]
async fn keepalive_survives_a_no_read_flush_window() {
    // The keepalive-vs-durability contract (§1.9): while the decode loop is busy in a slow/stalled S3
    // flush it is NOT reading frames, yet the walsender still severs us after `wal_sender_timeout` (5s)
    // unless feedback keeps flowing. `flush_batch_keepalive` pumps `send_received_feedback` on the
    // feedback cadence for exactly this reason. Reproduce it at the transport level: stop reading for
    // > 5s while pumping keepalive, then prove the connection is still live (a fresh write streams
    // through). Before the fix this exercised nothing — the pump lived inside `next()`, so not reading
    // meant no feedback and a `terminating walsender` reconnect.
    let slot = "walrus_spike_flush_keepalive";
    let _guard = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    let start = fresh_slot(&admin, slot).await;
    let mut stream = ReplicationStream::start(&source_url(), slot, start, "walrus_pub")
        .await
        .unwrap();

    // Simulate an 8s flush: never call next(); pump keepalive on the feedback cadence instead.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(stream.feedback_budget()).await;
        stream
            .send_received_feedback(false)
            .await
            .expect("keepalive during the simulated flush");
    }

    // Prove liveness: a fresh published write must still stream through (the walsender did not sever).
    admin
        .execute(
            "UPDATE public.walrus_heartbeat SET beat_seq = beat_seq + 1, ts = now() WHERE id = 1",
            &[],
        )
        .await
        .unwrap();
    let alive = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            match stream.next().await.expect("frame") {
                Some(ReplicationMessage::XLogData { data, .. }) if !data.is_empty() => break true,
                Some(_) => continue, // buffered keepalive — keep reading
                None => break false,
            }
        }
    })
    .await
    .expect("stream still live after an 8s no-read flush window");
    assert!(
        alive,
        "keepalive kept the walsender connected through the flush window"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn reply_requested_keepalive_is_answered_immediately() {
    let slot = "walrus_spike_reply";
    let _guard = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    let start = fresh_slot(&admin, slot).await;
    let mut stream = ReplicationStream::start(&source_url(), slot, start, "walrus_pub")
        .await
        .unwrap();
    // Long interval → our periodic feedback never fires, so the server *demands* a reply.
    stream.set_feedback_interval(Duration::from_secs(60));

    // Postgres prompts a fresh standby for its position with a `reply_requested` keepalive; `next()`
    // answers it immediately (internally). Observe the flag, then that the stream stays alive.
    let saw = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match stream.next().await.expect("frame") {
                Some(ReplicationMessage::Keepalive {
                    reply_requested: true,
                    ..
                }) => break true,
                Some(_) => continue,
                None => break false,
            }
        }
    })
    .await
    .expect("a reply_requested keepalive within 8s");
    assert!(saw, "the server should request a reply at least once");

    // Still alive after answering — read one more frame.
    let alive = tokio::time::timeout(Duration::from_secs(6), stream.next()).await;
    assert!(
        matches!(alive, Ok(Ok(Some(_)))),
        "stream stays alive after answering the reply request: {alive:?}"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}
