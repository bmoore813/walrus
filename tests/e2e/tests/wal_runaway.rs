#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end WAL-runaway chaos (`architecture.md` "WAL-runaway chaos" + §1.9): stall the extractor's S3
//! durability and keep writing to source; the slot's retained WAL rises (the retained-WAL alert
//! condition trips) while the walsender stays connected and `confirmed_flush_lsn` holds — bounded,
//! because we resume before any real cap. Then resume and assert full catch-up with no loss/dupes.
//!
//! **Mechanism note (reconciled with the architecture):** the task's "pause the transformer" cannot retain
//! source WAL — the transformer does not own the slot; the extractor advances `confirmed_flush_lsn` on its OWN S3
//! durability, independent of transformer progress (§1.5/§1.9). So the honest way to pin `restart_lsn` and
//! grow retained WAL is to stall the extractor's S3 flush — here `docker pause walrus-minio-1`. The keepalive
//! fix keeps feedback flowing while the PUT is stalled, so the walsender is never severed.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

/// Pause MinIO (stall S3), drive a sustained write workload, and assert retained WAL rises past the
/// alert threshold while the walsender stays connected and `confirmed_flush` is frozen; then resume and
/// assert full catch-up (mirror == source, no loss/dupes) with the retained WAL released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn wal_runaway_is_bounded_then_catches_up() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");

    // A converged baseline so the mirror exists and the slot is caught up before the stall.
    for i in 0..50 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'seed')"
        ))
        .await
        .unwrap();
    }
    let seeded = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999998, 'baseline')")
        .await
        .unwrap();
    h.await_transformed_past("orders", seeded, Duration::from_secs(120))
        .await
        .expect("seed converges");

    // STALL S3: the extractor can no longer finish a durable flush, so `confirmed_flush` freezes, the slot's
    // `restart_lsn` is pinned, and every committed row from here piles up in retained WAL — the runaway.
    h.stall_s3().await.expect("pause MinIO");

    // Sustained workload while S3 is down. Small (< logical_decoding_work_mem = 64 kB) txns so they take
    // the keepalive-wrapped batched flush path and NOT the streamed-spill path (whose S3 PUTs
    // are not keepalive-covered and would sever the connection under a stall). 25 × 40 rows = 1000 rows.
    for c in 0..25 {
        let start = 1000 + c * 40;
        let end = start + 39;
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status, note) \
             SELECT g, 'runaway', repeat('x', 512) FROM generate_series({start}, {end}) g"
        ))
        .await
        .unwrap();
    }

    // The retained-WAL alert condition trips (retained rises well past the alert threshold) while the
    // walsender stays CONNECTED (keepalive) and `confirmed_flush` stays FROZEN. Bounded: we resume before
    // any real cap — the compose slot has no `max_slot_wal_keep_size`, so it never invalidates (slot loss
    // → total-restart is the path, not this one).
    const ALERT: i64 = 256 * 1024;
    let retained = h
        .await_retained_bytes_over(ALERT, Duration::from_secs(30))
        .await
        .expect("retained WAL rises past the alert threshold (runaway detected)");
    assert!(
        h.is_slot_active().await.unwrap(),
        "walsender stays connected while S3 is stalled (keepalive keeps feedback flowing)"
    );
    // confirmed_flush is FROZEN while durability is stalled: two reads 3s apart are equal (the extractor is
    // blocked on the stalled PUT). A tiny advance from a batch that flushed in the `docker pause` latency
    // window is fine — what matters is it has STOPPED advancing while retained WAL keeps growing.
    let cf_stalled = h.slot_confirmed_flush().await.unwrap();
    let restart_pinned = h.slot_restart_lsn().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        h.slot_confirmed_flush().await.unwrap(),
        cf_stalled,
        "confirmed_flush frozen while durability is stalled — no slot advance without S3"
    );

    // RESUME: unpause MinIO → the stalled flush completes, the extractor drains the backlog, the transformer
    // catches up.
    h.unstall_s3().await.expect("unpause MinIO");
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(240))
        .await
        .expect("pipeline catches up after S3 resumes");
    h.stop_transformer().await.unwrap();

    // Full catch-up: mirror == source, no loss/dupes; and retained WAL was released.
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_current")
        .unwrap();
    assert_eq!(
        n,
        50 + 1000 + 2,
        "seed(50) + runaway(1000) + baseline + sentinel, none lost or duplicated"
    );
    h.assert_mirror_equals_source("orders").await.unwrap();

    // Retained WAL released: `restart_lsn` follows `confirmed_flush` once the backlog is durable. It
    // advances lazily (at a running-xacts record / checkpoint), so nudge it with a CHECKPOINT, then poll
    // until it passes the point the stall pinned it at — proof the runaway WAL was freed.
    h.source_exec("CHECKPOINT").await.unwrap();
    let start = std::time::Instant::now();
    while h.slot_restart_lsn().await.unwrap() <= restart_pinned {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "restart_lsn never advanced past the stalled point {restart_pinned} — WAL not released"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let retained_after = h.slot_retained_bytes().await.unwrap();
    assert!(
        retained_after < retained,
        "retained WAL released after catch-up ({retained} peak -> {retained_after})"
    );
}
