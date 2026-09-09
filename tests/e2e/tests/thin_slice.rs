#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! The thin vertical slice (`architecture.md` "Local harness"): one `INSERT` / `UPDATE` / `DELETE` on
//! `orders`, asserted through the full chain — Parquet in MinIO, verbatim `orders_raw`, and the `orders`
//! mirror equal to the current source (the row gone after the `DELETE`).
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn insert_update_delete_reaches_mirror() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");

    // The watermark target: a source LSN taken BEFORE the change, so only the streamed change (not the
    // earlier empty snapshot) can cross it.
    let before = h.source_wal_lsn().await.unwrap();

    // Bracket the three commits by the SOURCE clock (Unix-epoch seconds): every txn's real commit time
    // lands in [ts_lo, ts_hi], so the `commit_ts` stamped into each row's meta must too.
    let ts_lo: f64 = sqlx::query_scalar("SELECT extract(epoch FROM clock_timestamp())::float8")
        .fetch_one(h.source_pool())
        .await
        .unwrap();

    // Drive the source: create → change → remove the same PK.
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (1, 'created')")
        .await
        .unwrap();
    h.source_exec("UPDATE public.orders SET status = 'shipped' WHERE id = 1")
        .await
        .unwrap();
    h.source_exec("DELETE FROM public.orders WHERE id = 1")
        .await
        .unwrap();

    let ts_hi: f64 = sqlx::query_scalar("SELECT extract(epoch FROM clock_timestamp())::float8")
        .fetch_one(h.source_pool())
        .await
        .unwrap();

    // Wait on the watermark, never a fixed sleep: the DELETE (committed after `before`) is in the mirror
    // and the queue is drained.
    h.await_transformed_past("orders", before, Duration::from_secs(90))
        .await
        .expect("pipeline converges past the change");

    // (a) At least one Parquet object under the epoch/schema/table prefix in MinIO.
    let objects = h.s3_list("orders").await.unwrap();
    assert!(!objects.is_empty(), "≥1 Parquet object staged: {objects:?}");

    // Release the transformer's DuckDB write lock before reading the file for the final assertions.
    h.stop_transformer().await.unwrap();

    // (b) `orders_raw` holds the three CDC rows VERBATIM — ops i/u/d in order, meta JSON intact, and the
    // four promoted columns populated.
    let ops = h
        .duckdb_rows(
            "orders",
            "SELECT _walrus_op FROM orders_raw WHERE id = 1 ORDER BY _walrus_lsn",
        )
        .unwrap();
    assert_eq!(ops, vec!["i", "u", "d"], "three verbatim CDC ops in order");

    let metas = h
        .duckdb_rows(
            "orders",
            "SELECT walrus_extractor_meta FROM orders_raw WHERE id = 1",
        )
        .unwrap();
    assert_eq!(metas.len(), 3, "three raw rows");
    assert!(
        metas
            .iter()
            .all(|m| m.contains("\"commit_lsn\"") && m.contains("\"op\"")),
        "meta JSON intact on every row: {metas:?}"
    );

    let promoted = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_raw WHERE id = 1 \
             AND _walrus_commit_lsn <> '' AND _walrus_lsn <> '' \
             AND _walrus_extractor_processed_at <> ''",
        )
        .unwrap();
    assert_eq!(
        promoted, 3,
        "op/commit_lsn/lsn/extractor_processed_at promoted on all rows"
    );

    // The provenance `commit_ts` is the real transaction commit time (proto §4 µs-since-Y2K, converted
    // by `UtcTimestamp::from_pg_micros`), not a decode-time placeholder: every row's parsed `commit_ts`
    // falls inside the source-clock bracket, ±1 s for sub-second rounding / minor skew.
    let commit_ts_in_window = h
        .duckdb_scalar(
            "orders",
            &format!(
                "SELECT count(*) FROM orders_raw WHERE id = 1 \
                 AND epoch(CAST(json_extract_string(walrus_extractor_meta, '$.commit_ts') AS TIMESTAMPTZ)) \
                     BETWEEN {lo} AND {hi}",
                lo = ts_lo - 1.0,
                hi = ts_hi + 1.0,
            ),
        )
        .unwrap();
    assert_eq!(
        commit_ts_in_window, 3,
        "commit_ts on every row is the real commit time, within [{ts_lo}, {ts_hi}] ±1s"
    );

    // (c) The `orders` mirror equals the current source — the row is gone after the DELETE.
    let mirror_pk = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders WHERE id = 1")
        .unwrap();
    assert_eq!(mirror_pk, 0, "mirror has no row for the deleted PK");
}
