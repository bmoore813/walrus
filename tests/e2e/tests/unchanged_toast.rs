#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end unchanged-TOAST carry-forward (`architecture.md` "Intra-batch TOAST carry-forward",
//! transformer §5.6): a big TOASTed value written, then an update that does NOT touch it (`REPLICA IDENTITY
//! DEFAULT` sends the unchanged-TOAST sentinel), must land in the mirror as the OLD big value — never
//! NULL — resolved by the transformer's raw back-scan.
//!
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn unchanged_toast_update_keeps_old_big_value() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    // 100 KB unambiguously exceeds the ~2 KB TOAST threshold; `big` is STORAGE EXTENDED (out-of-line).
    let big = "X".repeat(100_000);
    h.source_exec(&format!(
        "INSERT INTO public.types_matrix (id, big, s) VALUES (1, '{big}', 'a')"
    ))
    .await
    .unwrap();

    // Watermark AFTER the insert: we wait for the UPDATE specifically.
    let before = h.source_wal_lsn().await.unwrap();
    // Update a SMALL column only — `big` is untouched, so the WAL carries the unchanged-TOAST sentinel.
    h.source_exec("UPDATE public.types_matrix SET s = 'b' WHERE id = 1")
        .await
        .unwrap();
    h.await_transformed_past("types_matrix", before, Duration::from_secs(90))
        .await
        .expect("pipeline converges past the update");
    h.stop_transformer().await.unwrap();

    // The mirror keeps the OLD big value (resolved via the raw back-scan), NOT NULL.
    let len = h
        .duckdb_scalar(
            "types_matrix",
            "SELECT COALESCE(length(big), -1) FROM types_matrix WHERE id = 1",
        )
        .unwrap();
    assert_eq!(
        len, 100_000,
        "unchanged-TOAST big carried forward (not NULL/truncated)"
    );
    let all_x = h
        .duckdb_scalar(
            "types_matrix",
            "SELECT (big = repeat('X', 100000))::INTEGER FROM types_matrix WHERE id = 1",
        )
        .unwrap();
    assert_eq!(
        all_x, 1,
        "the carried-forward value is exactly the original big string"
    );
    // And the small column DID update.
    let s = h
        .duckdb_rows("types_matrix", "SELECT s FROM types_matrix WHERE id = 1")
        .unwrap()
        .remove(0);
    assert_eq!(s, "b", "the touched column updated");
}
