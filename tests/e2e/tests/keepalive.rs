#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end keepalive-vs-durability (`architecture.md` "Keepalive vs durability" + §1.9): stall the S3
//! flush past `wal_sender_timeout` and assert keepalive feedback keeps the walsender **connected** (no
//! `terminating walsender` reconnect churn) while `confirmed_flush_lsn` does **not** advance until the
//! flush is durable — the two distinct LSNs (received/keepalive vs confirmed_flush). A catching-up extractor
//! with a stale round-trip must stay in readiness (`degraded` is a field, never a hard gate — §4.3).
//!
//! This is the end-to-end proof of the keepalive fix: the extractor's S3 PUT runs inline in the
//! decode loop, so without a concurrent keepalive pump a stalled flush would starve the walsender and
//! sever the connection after `wal_sender_timeout` (5s in the harness). The stall is `docker pause`d MinIO
//! (every PUT hangs) while the process keeps running and pumping feedback.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

/// Stall S3 past `wal_sender_timeout`; assert the walsender stays connected (keepalive) and
/// `confirmed_flush` holds while durability waits; a catching-up extractor stays ready; then resume and assert
/// `confirmed_flush` advances and the mirror converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn stalled_flush_keeps_connection_without_advancing() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");

    // A converged baseline so `confirmed_flush` is at a known, settled point before the stall.
    for i in 0..40 {
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

    // STALL S3, then write a batch so the extractor attempts a durable flush and blocks on the PUT. Small
    // non-streaming txns → the keepalive-wrapped batched flush path (not the streamed-spill path). The
    // active writes also suppress the idle beat, so nothing but a durable flush could advance the slot —
    // and the extractor is blocked in that flush, so `confirmed_flush` cannot move.
    h.stall_s3().await.expect("pause MinIO");
    for c in 0..10 {
        let start = 1000 + c * 40;
        let end = start + 39;
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status, note) \
             SELECT g, 'stall', repeat('x', 512) FROM generate_series({start}, {end}) g"
        ))
        .await
        .unwrap();
    }

    // Wait PAST `wal_sender_timeout` (5s). The keepalive fix keeps feedback flowing while the PUT is
    // stalled, so the walsender must NOT sever us.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // (1) Connected past the timeout — no `terminating walsender` reconnect churn.
    assert!(
        h.is_slot_active().await.unwrap(),
        "walsender stays connected past wal_sender_timeout during the stall (keepalive)"
    );
    assert!(
        h.is_extractor_running(),
        "the extractor did not exit — the replication connection was not severed"
    );
    assert!(
        !h.extractor_log_contains("source closed the replication connection"),
        "no reconnect churn in the extractor log"
    );
    // (2) confirmed_flush is FROZEN while the flush is stalled — two reads apart are equal (the extractor is
    // blocked on the stalled PUT; a tiny advance in the `docker pause` latency window is fine — what
    // matters is durability, not keepalive, is what moves it). It advances only when S3 is durable.
    let cf_stalled = h.slot_confirmed_flush().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        h.slot_confirmed_flush().await.unwrap(),
        cf_stalled,
        "confirmed_flush holds while the flush is stalled (advances only on durability)"
    );
    // (3) A catching-up extractor is NOT gated out of readiness by staleness (§4.3): /ready stays 200.
    let (ready, _degraded) = h.extractor_ready().await.unwrap();
    assert!(
        ready,
        "a stalled/catching-up extractor stays ready (degraded is a field, not a readiness gate)"
    );

    // RESUME → the stalled flush completes, confirmed_flush advances, the pipeline catches up.
    h.unstall_s3().await.expect("unpause MinIO");
    let advanced = h
        .await_confirmed_flush_past(cf_stalled, Duration::from_secs(60))
        .await
        .expect("confirmed_flush advances once the flush completes durably");
    assert!(
        advanced > cf_stalled,
        "confirmed_flush advanced after S3 resumed"
    );
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("pipeline catches up after S3 resumes");
    h.stop_transformer().await.unwrap();
    h.assert_mirror_equals_source("orders").await.unwrap();
}
