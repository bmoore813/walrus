#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end generation recovery. When the replication slot is **lost** on a successful connection,
//! the extractor performs a destructive total restart: it opens a replacement slot and a reconciled
//! successor epoch. When the slot remains healthy across a process restart, the extractor retains its WAL
//! but still opens a reconciled successor because the process cannot prove that its publication-DDL
//! guard remained continuous while it was offline. Only the destructive path is a `TOTAL-RESTART`.
//! In both cases the running transformer detects the new epoch, exits, and rebuilds every `.duckdb` under
//! the successor (raw re-appended, mirror re-derived), resetting **both** watermarks.
//!
//! The self-heal model is crash-and-restart (§ startup/bootstrap): dropping the slot terminates the
//! extractor's walsender, so the extractor exits; on restart it classifies the slot and total-restarts. The transformer
//! likewise exits on a detected epoch bump and rebuilds at its next bootstrap. The harness plays the
//! orchestrator that restarts the crashed processes.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

/// Drop the slot mid-run → the extractor total-restarts (epoch bump + full reconciliation) and the transformer rebuilds
/// every `.duckdb` under the new generation, resetting both watermarks; the mirror converges to the
/// source (a prior DELETE stays deleted because the mirror is re-derived from the new full dump).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn dropping_the_slot_triggers_epoch_bump_and_full_rebuild() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    assert_eq!(h.current_epoch().await.unwrap(), 1, "starts at epoch 1");

    // Steady state under epoch 1: seed rows + a DELETE, converged into the mirror.
    for i in 0..200 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'gen1')"
        ))
        .await
        .unwrap();
    }
    h.source_exec("DELETE FROM public.orders WHERE id = 100")
        .await
        .unwrap();
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("epoch-1 converges");

    // DROP the slot mid-run → the extractor's walsender is terminated and the extractor exits; the change history
    // since confirmed_flush is now gone. Restart the extractor: on a SUCCESSFUL connection it finds the slot
    // ABSENT → TOTAL-RESTART (bump the epoch, new slot, reconcile every table under epoch 2).
    h.drop_slot().await.expect("drop the replication slot");
    h.restart_extractor()
        .await
        .expect("extractor restarts → total-restart");
    let new_epoch = h
        .await_epoch_past(1, Duration::from_secs(60))
        .await
        .expect("the slot loss bumped the epoch");
    assert_eq!(new_epoch, 2, "a new generation was opened");
    h.refresh_epoch().await.unwrap();
    assert!(
        h.extractor_log_contains("TOTAL-RESTART"),
        "the extractor logged a loud total-restart"
    );
    assert!(
        !h.extractor_log_contains("RECONCILED-SUCCESSOR"),
        "destructive slot replacement must not be reported as retained-slot recovery"
    );
    let predecessor_status: String =
        sqlx::query_scalar("SELECT status FROM public.walrus_replication_state WHERE epoch = $1")
            .bind(1_i64)
            .fetch_one(h.control_pool())
            .await
            .unwrap();
    assert_eq!(
        predecessor_status, "total_restart",
        "slot loss durably arms destructive recovery before replacement"
    );

    // The transformer must observe the generation retirement and exit before its replacement rebuilds every
    // .duckdb under epoch 2. Waiting for the actual exit makes this guard observable instead of masking a
    // broken epoch watcher with a test-driven kill.
    h.await_transformer_exited(Duration::from_secs(60))
        .await
        .expect("epoch-1 transformer exits after destructive generation retirement");
    h.restart_transformer()
        .await
        .expect("transformer restarts → rebuild under epoch 2");

    // Converge under epoch 2, then compare.
    let before2 = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'sentinel2' WHERE id = 999999")
        .await
        .unwrap();
    h.await_transformed_past("orders", before2, Duration::from_secs(240))
        .await
        .expect("epoch-2 converges");
    h.stop_transformer().await.unwrap();

    // Both watermarks reset then advanced under the NEW epoch — the epoch-2 checkpoint is a fresh row that
    // has moved off `0/0` (its predecessor was epoch 1, untouched).
    let cp = control::read_checkpoint(h.control_pool(), 2_i64.into(), "public", "orders")
        .await
        .unwrap()
        .expect("epoch-2 checkpoint exists");
    assert!(
        cp.transformed_lsn > common::Lsn::ZERO && cp.raw_appended_lsn >= cp.transformed_lsn,
        "epoch-2 watermarks reset then advanced consistently"
    );

    // Rebuilt-from-the-new-generation dump == source (0..200 minus id=100, plus the sentinel).
    h.assert_mirror_equals_source("orders").await.unwrap();
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_current")
        .unwrap();
    assert_eq!(n, 200, "199 gen1 rows (id=100 deleted) + sentinel");
    let resurrected = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current WHERE id = 100",
        )
        .unwrap();
    assert_eq!(
        resurrected, 0,
        "the deleted row did not resurrect through the rebuild"
    );
}

/// A retained healthy slot avoids destructive replacement, but every new extractor process opens a fenced,
/// fully reconciled successor because publication-guard continuity cannot span the process boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn retained_slot_restart_opens_a_reconciled_successor_without_total_restart() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    for i in 0..100 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'gen1')"
        ))
        .await
        .unwrap();
    }
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("converges");
    assert_eq!(
        h.current_epoch().await.unwrap(),
        1,
        "epoch 1 before the process restart"
    );
    let retained_floor = h
        .slot_confirmed_flush()
        .await
        .expect("read the healthy slot's durable floor before restart");

    // Terminate the walsender WITHOUT dropping the slot. The replacement process cannot inherit the old
    // session's publication guard, so it retains the slot/WAL floor but opens a fully reconciled epoch 2.
    // This is deliberately distinct from the absent-slot path: no total-restart intent is armed and no
    // source slot is replaced.
    h.terminate_walsender()
        .await
        .expect("bounce the extractor's replication connection");
    h.restart_extractor()
        .await
        .expect("extractor restarts with the healthy slot retained");
    let new_epoch = h
        .await_epoch_past(1, Duration::from_secs(60))
        .await
        .expect("the new process opens a reconciled successor");
    assert_eq!(new_epoch, 2, "one successor generation was opened");
    h.refresh_epoch().await.unwrap();

    let successor = control::read_current_epoch(h.control_pool())
        .await
        .unwrap()
        .expect("successor generation exists");
    assert!(
        successor.created_lsn >= retained_floor,
        "the successor fence must not precede the retained slot floor"
    );
    let predecessor_status: String =
        sqlx::query_scalar("SELECT status FROM public.walrus_replication_state WHERE epoch = $1")
            .bind(1_i64)
            .fetch_one(h.control_pool())
            .await
            .unwrap();
    assert_eq!(
        predecessor_status, "streaming",
        "retaining a healthy slot must not arm destructive total-restart intent"
    );
    assert!(
        h.extractor_log_contains("RECONCILED-SUCCESSOR"),
        "the restart is reported as a retained-slot reconciled successor"
    );
    assert!(
        !h.extractor_log_contains("TOTAL-RESTART"),
        "a healthy retained slot must not be reported as destructive replacement"
    );

    // The epoch-1 transformer must retire itself. Its replacement stays unready until the extractor's complete
    // frozen target set has reconciled, then rebuilds the local DuckLake state under epoch 2.
    h.await_transformer_exited(Duration::from_secs(60))
        .await
        .expect("epoch-1 transformer exits after observing the successor");
    h.restart_transformer()
        .await
        .expect("transformer restarts and rebuilds under epoch 2");
    let reconciled: bool = sqlx::query_scalar(
        "SELECT EXISTS (
           SELECT 1
           FROM public.walrus_table_reload tr
           JOIN public.walrus_replication_state rs
             ON rs.epoch = tr.epoch
            AND rs.bootstrap_request_id = tr.parent_request_id
           WHERE tr.epoch = $1
             AND tr.source_schema = 'public'
             AND tr.source_table = 'orders'
             AND tr.request_scope = 'all_published'
             AND tr.status = 'complete'
         )",
    )
    .bind(new_epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert!(
        reconciled,
        "epoch-2 readiness requires the frozen orders baseline to publish completely"
    );
    assert_eq!(
        control::read_current_epoch(h.control_pool())
            .await
            .unwrap()
            .expect("current generation")
            .status,
        control::ReplicationStatus::Streaming,
        "the successor is promoted only after its full reconciliation"
    );

    // A fresh write now converges after the reconciled epoch-2 baseline.
    let before2 = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'sentinel2' WHERE id = 999999")
        .await
        .unwrap();
    h.await_transformed_past("orders", before2, Duration::from_secs(180))
        .await
        .expect("successor converges");
    h.stop_transformer().await.unwrap();

    assert_eq!(
        h.current_epoch().await.unwrap(),
        2,
        "the retained-slot restart remains on its single reconciled successor"
    );
    h.assert_mirror_equals_source("orders").await.unwrap();
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_current")
        .unwrap();
    assert_eq!(n, 101, "100 rows + sentinel, rebuilt intact");
}

/// A crash after creating a replacement slot but before opening its successor generation leaves a
/// healthy slot alongside a durable `total_restart` intent. The next process must finish the restart,
/// not stream that new slot into the old epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn healthy_slot_with_restart_intent_opens_a_reconciled_successor() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let old_epoch = control::read_current_epoch(h.control_pool())
        .await
        .unwrap()
        .expect("initial generation");
    assert_eq!(old_epoch.epoch.0, 1);

    h.kill_extractor()
        .await
        .expect("crash extractor with slot retained");
    assert!(
        control::mark_total_restart(h.control_pool(), old_epoch.epoch)
            .await
            .unwrap(),
        "model the durable intent written immediately before the source slot mutation"
    );
    h.await_transformer_exited(Duration::from_secs(30))
        .await
        .expect("transformer must stop serving the retired generation before a successor exists");
    h.restart_extractor()
        .await
        .expect("healthy slot plus intent completes total restart");

    let new_epoch = h
        .await_epoch_past(1, Duration::from_secs(60))
        .await
        .expect("restart intent opens a successor generation");
    assert_eq!(new_epoch, 2);
    assert!(h.extractor_log_contains("TOTAL-RESTART"));
    assert!(
        !h.extractor_log_contains("RECONCILED-SUCCESSOR"),
        "a healthy replacement slot with durable restart intent still belongs to the destructive path"
    );
}
