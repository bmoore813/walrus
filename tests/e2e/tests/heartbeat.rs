#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end idle-publication heartbeat (`architecture.md` "Idle-publication heartbeat" + §1.9): keep
//! the published tables idle while the rest of the DB churns WAL, and assert the extractor fires a beat
//! **only after** `heartbeat_idle_after` (suppressed under active user-table writes), that the beat's
//! `beat_seq` round-trip is observed (surfacing as `degraded = false` on `/ready`), and that this keeps
//! `restart_lsn` / `confirmed_flush_lsn` advancing so retained WAL stays healthy.
//!
//! **"Unpublished churn" under `FOR ALL TABLES`:** the dev-harness publication is `FOR ALL TABLES`, so
//! there is no unpublished *user* table to churn. The faithful stand-in is `pg_logical_emit_message(false,
//! …)` — a non-transactional logical message. It advances server WAL (retained WAL grows, `restart_lsn`
//! pinned), but our slot's `START_REPLICATION` did not request `messages`, so pgoutput never delivers it
//! and the extractor has nothing to confirm — exactly the "published set idle while the DB is busy" scenario.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

/// Prove the idle heartbeat: suppressed under active writes, fires + round-trips when the published
/// tables go idle, advances `confirmed_flush` (so `restart_lsn` follows and retained WAL stays healthy),
/// and surfaces as a healthy (`degraded = false`, `ready`) round-trip on `/ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn idle_heartbeat_advances_restart_lsn() {
    let h = Harness::start()
        .await
        .expect("bring up extractor + transformer");

    // (A) SUPPRESSION. A prime write starts the idle clock clean; then steady writes faster than
    // `heartbeat_idle_after` (1s in the harness) must keep the beat suppressed — there is always fresh
    // published WAL to confirm.
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (90, 'prime')")
        .await
        .unwrap();
    let beats0 = h.heartbeat_beats();
    for i in 0..8 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({}, 'active')",
            100 + i
        ))
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    assert_eq!(
        h.heartbeat_beats(),
        beats0,
        "no beat fired while user-table writes were active (suppressed)"
    );

    // Settle the active writes durably so `confirmed_flush` is at a known baseline before the idle window
    // — then only the beat can advance it.
    let after_active = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999997, 'settle')")
        .await
        .unwrap();
    h.await_confirmed_flush_past(after_active, Duration::from_secs(30))
        .await
        .expect("active writes become durable");
    let cf0 = h.slot_confirmed_flush().await.unwrap();
    let restart0 = h.slot_restart_lsn().await.unwrap();
    let rt0 = h.heartbeat_roundtrips();

    // (B) IDLE FIRE + ROUND-TRIP + SLOT ADVANCE. Keep published tables idle; churn UNSEEN WAL (advances
    // server WAL / pins restart_lsn, but the extractor never sees it). The idle beat must rescue the slot.
    for _ in 0..40 {
        h.source_exec("SELECT pg_logical_emit_message(false, 'walrus-noise', repeat('x', 8192))")
            .await
            .unwrap();
    }
    h.await_heartbeat_roundtrip(rt0 + 1, Duration::from_secs(20))
        .await
        .expect("an idle heartbeat round-trips through the stream");
    assert!(
        h.heartbeat_beats() > beats0,
        "a beat fired once the published tables went idle"
    );
    let cf1 = h
        .await_confirmed_flush_past(cf0, Duration::from_secs(10))
        .await
        .expect("the idle heartbeat advanced confirmed_flush");
    assert!(
        cf1 > cf0,
        "idle heartbeat advanced confirmed_flush ({cf0} -> {cf1}) — only the beat could, tables were idle"
    );

    // (C) The round-trip surfaces as HEALTHY on /ready — degraded = false, ready (§4.3).
    let (ready, degraded) = h.extractor_ready().await.unwrap();
    assert!(ready, "the extractor stays ready");
    assert!(
        !degraded,
        "a fresh heartbeat round-trip clears the degraded field"
    );

    // (D) restart_lsn follows confirmed_flush, releasing the WAL the churn pinned. restart_lsn advances
    // lazily (at a running-xacts record / checkpoint), so nudge it with a CHECKPOINT, then poll.
    h.source_exec("CHECKPOINT").await.unwrap();
    let start = std::time::Instant::now();
    while h.slot_restart_lsn().await.unwrap() <= restart0 {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "restart_lsn never advanced past {restart0} after the beat (WAL not released)"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let retained = h.slot_retained_bytes().await.unwrap();
    assert!(
        retained < 16 * 1024 * 1024,
        "retained WAL stays bounded after the beat ({retained} bytes)"
    );
}
