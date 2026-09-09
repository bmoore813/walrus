#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end large-transaction streaming (`architecture.md` §1.6 / §1.3): a large txn stays
//! memory-bounded (the `max_inflight_bytes` ceiling spills open-txn buffers to S3) and appears
//! ATOMICALLY after commit; an aborted large txn leaks nothing; a late-committing large txn is not
//! skipped and last-writer is decided by commit LSN.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

// The compose source runs with `logical_decoding_work_mem=64kB` and the harness sets a 64 KiB extractor
// ceiling, so a few thousand rows in one txn stream + spill.
const N: i64 = 12_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn large_txn_is_atomic_and_bounded() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    // A big txn held OPEN across separate round-trips (BEGIN, INSERT, …), still uncommitted.
    let mut c = h.source_pool().acquire().await.unwrap();
    sqlx::raw_sql("BEGIN").execute(&mut *c).await.unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status) SELECT g, 'big' FROM generate_series(100000, {}) g",
        100000 + N - 1
    ))
    .execute(&mut *c)
    .await
    .unwrap();
    // A committed NEIGHBOUR flushes the WAL that the open txn has written, forcing the walsender to decode
    // forward and stream the still-open big txn (it exceeds `logical_decoding_work_mem`). Without a
    // committed neighbour the walsender may never advance to an open txn's changes, so this makes the
    // spill deterministic (not a fixed sleep).
    h.source_batch(
        "BEGIN; INSERT INTO public.orders (id, status) VALUES (999998, 'nudge'); COMMIT",
    )
    .await
    .unwrap();
    // Bounded: the extractor spilled while the txn is still OPEN — the observable proof that open-txn memory
    // stayed capped. Polling the spill log keeps it open until the spill is observed.
    let spills = h
        .await_spill(1, Duration::from_secs(60))
        .await
        .expect("the max_inflight_bytes ceiling fires → open-txn buffers spill (bounded memory)");
    assert!(
        spills > 0,
        "open-txn buffers spilled while still open: {spills}"
    );
    sqlx::raw_sql("COMMIT").execute(&mut *c).await.unwrap();

    // Converge robustly: capture the watermark AFTER the big txn commits, then a sentinel that lands LAST;
    // awaiting `transformed_lsn` past it guarantees the big txn (and the neighbour) are fully transformed
    // (a pre-commit watermark could return at a transient quiescent point before the big txn's files land).
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("pipeline converges");
    h.stop_transformer().await.unwrap();

    // Atomic: every row present after commit.
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders WHERE status = 'big'")
        .unwrap();
    assert_eq!(n, N, "the whole txn appears atomically after commit");

    // The slot did not leak WAL after commit + consume.
    let retained = h.slot_retained_bytes().await.unwrap();
    assert!(
        retained < 50_000_000,
        "slot retained bytes bounded after commit+consume: {retained}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn aborted_large_txn_leaves_nothing() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    // A large txn then ROLLBACK — streamed, then aborted: speculative spills dropped, no manifest row.
    h.source_batch(&format!(
        "BEGIN; INSERT INTO public.orders (id, status) \
             SELECT g, 'aborted' FROM generate_series(900000, {}) g; ROLLBACK",
        900000 + N - 1
    ))
    .await
    .unwrap();
    // A sentinel commit AFTER the rollback: awaiting it proves the abort was fully processed.
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(120))
        .await
        .expect("pipeline converges past the sentinel");
    h.stop_transformer().await.unwrap();

    // Zero rows leaked to the raw log or the mirror; the sentinel DID land.
    let raw = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_raw WHERE id BETWEEN 900000 AND 999998",
        )
        .unwrap();
    assert_eq!(raw, 0, "aborted txn leaves nothing in <table>_raw");
    let mirror = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'aborted'",
        )
        .unwrap();
    assert_eq!(mirror, 0, "aborted txn leaves nothing in the mirror");
    let sentinel = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders WHERE id = 999999")
        .unwrap();
    assert_eq!(sentinel, 1, "the post-abort sentinel committed and landed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn late_committing_large_txn_not_skipped() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");

    // The row-LSN-bug regression guard: A's rows must have LOWER row LSNs than B's, but A's COMMIT LSN must
    // be HIGHER. So A INSERTs FIRST (low row LSNs) and stays open; B INSERTs LATER (higher row LSNs) and
    // COMMITs FIRST (lower commit LSN); A COMMITs LAST (higher commit LSN). If the transformer watermarked on
    // max ROW LSN, B (higher row LSN) would advance the watermark past A's low-row-LSN rows and A would be
    // SKIPPED — ordering on COMMIT LSN is what keeps A. Disjoint keys: a shared PK would serialize on
    // Postgres row locks and force commit order == write order, dissolving the very inversion under test.
    let mut ca = h.source_pool().acquire().await.unwrap();
    sqlx::raw_sql("BEGIN").execute(&mut *ca).await.unwrap();
    // A: the big insert FIRST (low row LSNs), and A stays open.
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status) SELECT g, 'A' FROM generate_series(200000, {}) g",
        200000 + N - 1
    ))
    .execute(&mut *ca)
    .await
    .unwrap();
    // B: a clean small txn with HIGHER row LSNs that COMMITs FIRST (lower commit LSN). B's commit also
    // FLUSHES the WAL A has written, forcing the walsender to decode forward and stream the still-open A
    // (which exceeds `logical_decoding_work_mem`) — so A commits as a `Stream Commit`, exercising the exact
    // streaming commit-order path this test guards (a placeholder-commit_lsn spill + a lower-LSN regular
    // neighbour). Without streaming, A would be a plain non-streamed txn and the regression path is vacuous.
    h.source_batch(
        "BEGIN; INSERT INTO public.orders (id, status) \
             SELECT g, 'B' FROM generate_series(300000, 300010) g; COMMIT",
    )
    .await
    .unwrap();
    // Hold A open until it has provably streamed + spilled — so A's commit below is a `Stream Commit`.
    let spills = h.await_spill(1, Duration::from_secs(60)).await.expect(
        "A streams while open (its rows carry the lowest row LSNs), exercising the streamed path",
    );
    assert!(spills > 0, "A streamed + spilled while open: {spills}");
    // A COMMITs LAST (higher commit LSN) — must win over its lower row LSNs.
    sqlx::raw_sql("COMMIT").execute(&mut *ca).await.unwrap();

    // Capture the watermark AFTER every real write, then a single sentinel commit that lands LAST. Awaiting
    // `transformed_lsn` past `before` can only cross once the sentinel (the sole commit above `before`) is
    // transformed — and the transformer transforms in `(commit_lsn, lsn)` order, so A and B (both below
    // `before`) are guaranteed applied by then. Awaiting past a pre-write LSN would race: the pipeline hits
    // transient quiescent points between these txns and could return before B arrives.
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("pipeline converges");
    h.stop_transformer().await.unwrap();

    // The late-committing large txn A is fully applied (not skipped), and B too.
    let a = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders WHERE status = 'A'")
        .unwrap();
    assert_eq!(
        a, N,
        "late-committing large txn A fully applied (not skipped)"
    );
    let b = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders WHERE status = 'B'")
        .unwrap();
    assert_eq!(
        b, 11,
        "early-committing (higher row LSN) txn B fully applied"
    );
}
