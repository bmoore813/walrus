#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect make failed setup and protocol assertions immediate"
)]
//! End-to-end proof that a post-start table reload enters through the published source-WAL event,
//! is fenced in control PG, and atomically replaces the DuckLake table without losing DML committed
//! inside the export window.
//!
//! Run this target alone with one test thread because the harness owns fixed ports, one replication
//! slot, and one DuckLake metadata schema:
//!
//! `cargo test -p e2e --features it --test source_reload_event -- --ignored --test-threads=1`

#![cfg(feature = "it")]

use common::{Lsn, ReloadId};
use control::{ReloadMarkerKind, ReloadStatus};
use e2e::Harness;
use std::time::Duration;
use uuid::Uuid;

// Compose runs with logical_decoding_work_mem=64 KiB and the harness gives the extractor the same
// in-flight ceiling. This is large enough to force protocol-v2 streaming and a speculative spill.
const LONG_TXN_FIRST_ID: i64 = 100_000;
const LONG_TXN_ROWS: i64 = 12_000;

async fn await_baseline(h: &Harness, request_id: Uuid, deadline: Duration) -> (ReloadId, Lsn) {
    let started = tokio::time::Instant::now();
    loop {
        let marker = sqlx::query_as::<_, (i64, String)>(
            "SELECT r.reload_id, m.lsn::text
             FROM public.walrus_table_reload r
             JOIN public.walrus_table_reload_marker m USING (reload_id)
             WHERE r.epoch = $1
               AND r.source_request_id = $2
               AND r.source_schema = 'public'
               AND r.source_table = 'orders'
               AND m.marker_kind = 'baseline'",
        )
        .bind(h.epoch)
        .bind(request_id)
        .fetch_optional(h.control_pool())
        .await
        .unwrap();
        if let Some((reload_id, lsn)) = marker {
            return (reload_id.into(), lsn.parse().unwrap());
        }
        assert!(
            started.elapsed() < deadline,
            "source request {request_id} was not decoded and fenced within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_complete(
    h: &Harness,
    reload_id: ReloadId,
    deadline: Duration,
) -> control::ReloadRow {
    let started = tokio::time::Instant::now();
    loop {
        let row = control::reload::get(h.control_pool(), reload_id)
            .await
            .unwrap()
            .expect("decoded source reload has a control row");
        if row.status == ReloadStatus::Complete {
            return row;
        }
        assert_ne!(
            row.status,
            ReloadStatus::Failed,
            "source-driven reload failed: {:?}",
            row.error
        );
        assert!(
            started.elapsed() < deadline,
            "reload {reload_id} remained {:?} beyond {deadline:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn await_stream_publication(h: &Harness, top_xid: u32, deadline: Duration) -> Lsn {
    let started = tokio::time::Instant::now();
    loop {
        let commit_lsns = sqlx::query_scalar::<_, String>(
            "SELECT commit_lsn::text
             FROM public.walrus_stream_txn_publication
             WHERE epoch = $1 AND top_xid = $2
             ORDER BY id",
        )
        .bind(h.epoch)
        .bind(i64::from(top_xid))
        .fetch_all(h.control_pool())
        .await
        .unwrap();
        assert!(
            commit_lsns.len() <= 1,
            "top xid {top_xid} produced duplicate durable stream publications: {commit_lsns:?}"
        );
        if let Some(commit_lsn) = commit_lsns.first() {
            return commit_lsn.parse().unwrap();
        }
        assert!(
            started.elapsed() < deadline,
            "streamed top xid {top_xid} was not durably published within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn source_wal_request_reconciles_f_h_and_pre_f_open_transaction() {
    let mut h = Harness::start().await.unwrap();

    // Establish a non-empty, fully mirrored steady state before asking for a post-start rebuild.
    // Enough rows ensure the exporter has a real Parquet PUT to block while MinIO is paused.
    h.source_batch(
        "INSERT INTO public.orders (id, status)
         SELECT g, 'before-' || g FROM generate_series(1, 2000) g;",
    )
    .await
    .unwrap();
    let seed_floor = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'steady' WHERE id = 1")
        .await
        .unwrap();
    h.await_transformed_past("orders", seed_floor, Duration::from_secs(120))
        .await
        .unwrap();

    // Transaction A begins and writes before F, but remains open across the entire reload. A
    // committed neighbour flushes its WAL and forces pgoutput to stream A while it is still open.
    // Waiting for the extractor's spill probe (and then for the neighbour to transform) proves every
    // pre-F segment reached the extractor before object storage is deliberately paused below.
    let spill_floor = h.extractor_spill_count();
    let mut long_txn = h.source_pool().acquire().await.unwrap();
    sqlx::raw_sql("BEGIN")
        .execute(&mut *long_txn)
        .await
        .unwrap();
    let top_xid_raw: i64 = sqlx::query_scalar("SELECT txid_current()::bigint")
        .fetch_one(&mut *long_txn)
        .await
        .unwrap();
    let top_xid = u32::try_from(top_xid_raw).expect("test source xid remains below wrap");
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status)
         SELECT g, 'long-after-h'
         FROM generate_series({}, {}) g",
        LONG_TXN_FIRST_ID,
        LONG_TXN_FIRST_ID + LONG_TXN_ROWS - 1
    ))
    .execute(&mut *long_txn)
    .await
    .unwrap();
    let neighbour_floor = h.source_wal_lsn().await.unwrap();
    h.source_batch(
        "BEGIN;
         INSERT INTO public.orders (id, status) VALUES (40001, 'stream-neighbour');
         COMMIT;",
    )
    .await
    .unwrap();
    let spills = h
        .await_spill(spill_floor + 1, Duration::from_secs(60))
        .await
        .expect("the pre-F transaction streams and spills while it remains open");
    assert!(
        spills > spill_floor,
        "the open pre-F transaction added a spill: {spill_floor} -> {spills}"
    );
    h.await_transformed_past("orders", neighbour_floor, Duration::from_secs(120))
        .await
        .expect("the committed neighbour lands after all open-transaction segments are decoded");
    let before_f = h.source_wal_lsn().await.unwrap();

    // With object storage paused, the request and F event still decode into control PG, but the
    // first non-empty baseline chunk cannot become durable. The exporter therefore cannot append H.
    h.stall_s3().await.unwrap();
    let request_id = Uuid::new_v4();
    h.request_table_reload(request_id, "public", "orders")
        .await
        .unwrap();

    let source_request = sqlx::query_as::<_, (String, String, String, String, serde_json::Value)>(
        "SELECT request_id::text, event_kind, source_schema, source_table, targets
         FROM public.walrus_reload_event WHERE event_id = $1",
    )
    .bind(request_id)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert_eq!(source_request.0, request_id.to_string());
    assert_eq!(source_request.1, "request");
    assert_eq!(source_request.2, "public");
    assert_eq!(source_request.3, "orders");
    assert_eq!(source_request.4, serde_json::json!([]));

    let (reload_id, f) = await_baseline(&h, request_id, Duration::from_secs(60)).await;
    assert!(
        before_f < f,
        "the long transaction and its committed neighbour precede F: {before_f} < {f}"
    );
    let row = control::reload::get(h.control_pool(), reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.source_request_id, Some(request_id));
    assert_eq!(row.start_lsn, Some(f));
    assert_eq!(row.status, ReloadStatus::Exporting);
    let end_before_dml: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_table_reload_marker
         WHERE reload_id = $1 AND marker_kind = 'end'",
    )
    .bind(reload_id.0)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        end_before_dml, 0,
        "H cannot exist while the baseline PUT is blocked"
    );

    // These commits are strictly after observed F and strictly before H: MinIO remains paused.
    // Together they exercise overwrite, ghost removal, a replica-key move, and a brand-new row.
    h.source_batch(
        "BEGIN;
         UPDATE public.orders SET status = 'updated-between-f-h' WHERE id = 1;
         DELETE FROM public.orders WHERE id = 2;
         UPDATE public.orders SET id = 20003, status = 'moved-between-f-h' WHERE id = 3;
         INSERT INTO public.orders (id, status) VALUES (30001, 'inserted-between-f-h');
         COMMIT;",
    )
    .await
    .unwrap();
    let dml_upper_bound = h.source_wal_lsn().await.unwrap();
    assert!(f < dml_upper_bound, "the controlled DML committed after F");
    let end_while_blocked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_table_reload_marker
         WHERE reload_id = $1 AND marker_kind = 'end'",
    )
    .bind(reload_id.0)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        end_while_blocked, 0,
        "H stayed blocked until after the controlled DML"
    );

    h.unstall_s3().await.unwrap();
    let completed = await_complete(&h, reload_id, Duration::from_secs(180)).await;
    let markers = control::reload::read_markers(h.control_pool(), reload_id)
        .await
        .unwrap();
    assert_eq!(
        markers.len(),
        2,
        "one durable F marker and one durable H marker"
    );
    assert_eq!(markers[0].kind, ReloadMarkerKind::Baseline);
    assert_eq!(markers[0].lsn, f);
    assert_eq!(markers[1].kind, ReloadMarkerKind::End);
    let h_lsn = markers[1].lsn;
    assert!(
        h_lsn > dml_upper_bound,
        "H {h_lsn} must follow the post-DML WAL position {dml_upper_bound}"
    );
    assert_eq!(completed.start_lsn, Some(f));
    assert_eq!(completed.final_lsn, Some(h_lsn));

    // Completion is observed while A is demonstrably still open: its own session sees all rows,
    // other source sessions see none, and StreamCommit has not atomically published a receipt.
    let visible_inside_a: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.orders
         WHERE id BETWEEN $1 AND $2 AND status = 'long-after-h'",
    )
    .bind(LONG_TXN_FIRST_ID)
    .bind(LONG_TXN_FIRST_ID + LONG_TXN_ROWS - 1)
    .fetch_one(&mut *long_txn)
    .await
    .unwrap();
    assert_eq!(visible_inside_a, LONG_TXN_ROWS);
    let visible_outside_a: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.orders
         WHERE id BETWEEN $1 AND $2 AND status = 'long-after-h'",
    )
    .bind(LONG_TXN_FIRST_ID)
    .bind(LONG_TXN_FIRST_ID + LONG_TXN_ROWS - 1)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert_eq!(
        visible_outside_a, 0,
        "the pre-F transaction remains uncommitted through H and reload completion"
    );
    let publications_before_commit: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_stream_txn_publication
         WHERE epoch = $1 AND top_xid = $2",
    )
    .bind(h.epoch)
    .bind(i64::from(top_xid))
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        publications_before_commit, 0,
        "speculative spills are not visible as a durable stream publication before StreamCommit"
    );

    let source_protocol_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_reload_event
         WHERE request_id = $1
           AND (event_kind = 'request' OR reload_id = $2)",
    )
    .bind(request_id)
    .bind(reload_id.0)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert_eq!(
        source_protocol_rows, 3,
        "the source protocol log retains request plus F/H fence events"
    );

    // A commits only after the persisted H and completed cutover. The protocol-v2 publication
    // receipt carries the authoritative commit LSN; it must therefore sort strictly after H.
    sqlx::raw_sql("COMMIT")
        .execute(&mut *long_txn)
        .await
        .unwrap();
    let long_commit_lsn = await_stream_publication(&h, top_xid, Duration::from_secs(180)).await;
    assert!(
        long_commit_lsn > h_lsn,
        "long transaction commit {long_commit_lsn} must follow reload H {h_lsn}"
    );

    // A final committed sentinel gives the transformer an unambiguous watermark beyond A. Once it is
    // transformed, the complete streamed transaction and every earlier reload-window change must
    // already be present.
    let convergence_floor = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (40002, 'post-long-sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", convergence_floor, Duration::from_secs(180))
        .await
        .expect("transformer converges beyond the post-H streamed transaction");

    // `complete` is written only after the transformer publishes its hidden rebuild. Release its DuckDB
    // lock, then require exact source equality and explicitly check the four in-window mutations.
    h.stop_transformer().await.unwrap();
    h.assert_mirror_equals_source("orders").await.unwrap();
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            &format!(
                "SELECT count(*) FROM orders_current
                 WHERE id BETWEEN {} AND {} AND status = 'long-after-h'",
                LONG_TXN_FIRST_ID,
                LONG_TXN_FIRST_ID + LONG_TXN_ROWS - 1
            )
        )
        .unwrap(),
        LONG_TXN_ROWS,
        "all rows from the pre-F, post-H-committing transaction survive the cutover"
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current WHERE id = 2 OR id = 3"
        )
        .unwrap(),
        0,
        "delete and old side of key move leave no ghosts"
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current
             WHERE (id = 1 AND status = 'updated-between-f-h')
                OR (id = 20003 AND status = 'moved-between-f-h')
                OR (id = 30001 AND status = 'inserted-between-f-h')"
        )
        .unwrap(),
        3,
        "all post-F upserts survive the atomic cutover"
    );
}
