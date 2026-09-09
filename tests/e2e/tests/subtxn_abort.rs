#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end streamed sub-transaction abort (`architecture.md` "Streamed sub-transaction abort"):
//! a committed top-level txn with a rolled-back savepoint materialises ONLY the surviving rows in
//! `<table>_raw` — the exact 6000 count is unforgiving so an off-by-one in subxact tracking shows up.
//!
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn rolled_back_savepoint_never_materializes() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let before = h.source_wal_lsn().await.unwrap();

    // One committed txn: 3000 kept (A) → SAVEPOINT → 3000 rolled back → 3000 kept (B). 9000 rows in one
    // txn (with logical_decoding_work_mem=64kB) stream; the rolled-back subxact must be excluded.
    h.source_batch(
        "BEGIN; \
         INSERT INTO public.orders (id, status) SELECT g, 'A' FROM generate_series(1, 3000) g; \
         SAVEPOINT sp; \
         INSERT INTO public.orders (id, status) SELECT g, 'ROLLED' FROM generate_series(3001, 6000) g; \
         ROLLBACK TO SAVEPOINT sp; \
         INSERT INTO public.orders (id, status) SELECT g, 'B' FROM generate_series(6001, 9000) g; \
         COMMIT",
    )
    .await
    .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("pipeline converges");
    h.stop_transformer().await.unwrap();

    // EXACTLY 6000 rows in the raw log — the 3000 A + 3000 B, and none from the rolled-back savepoint.
    let raw = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_raw")
        .unwrap();
    assert_eq!(
        raw, 6000,
        "exactly A+B in <table>_raw, the rolled-back subxact excluded"
    );
    let rolled = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'ROLLED'",
        )
        .unwrap();
    assert_eq!(rolled, 0, "no rolled-back savepoint rows in the mirror");
    let kept = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status IN ('A', 'B')",
        )
        .unwrap();
    assert_eq!(kept, 6000, "both kept subxacts materialise");
}
