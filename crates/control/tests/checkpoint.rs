#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for `transformer_checkpoint` and `replication_state`.
//!
//! Each test runs inside a rolled-back transaction under a unique `epoch`, so tests are isolated
//! and idempotent across runs. Gated behind the `integration` feature (needs the compose control PG).
#![cfg(feature = "integration")]

use common::{EpochNo, FailureClass, Lsn};
use control::{
    ControlError, ReplicationStatus, advance_raw_appended, advance_transformed, connect,
    ensure_checkpoint, insert_epoch, read_checkpoint, read_current_epoch, run_migrations,
};
use sqlx::postgres::PgPool;

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn pool() -> PgPool {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

fn lsn(s: &str) -> Lsn {
    s.parse().unwrap()
}

#[tokio::test]
async fn ensure_then_advance_raw_then_transformed() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let (e, s, t) = (EpochNo(700_001), "public", "c1");

    // Absent before ensure.
    assert!(read_checkpoint(&mut *tx, e, s, t).await.unwrap().is_none());

    ensure_checkpoint(&mut *tx, e, s, t).await.unwrap();
    let cp = read_checkpoint(&mut *tx, e, s, t).await.unwrap().unwrap();
    assert_eq!(cp.raw_appended_lsn, Lsn::ZERO);
    assert_eq!(cp.transformed_lsn, Lsn::ZERO);

    // Phase A then Phase B, transformed staying at/below raw.
    advance_raw_appended(&mut *tx, e, s, t, lsn("0/100"))
        .await
        .unwrap();
    advance_transformed(&mut *tx, e, s, t, lsn("0/80"))
        .await
        .unwrap();

    let cp = read_checkpoint(&mut *tx, e, s, t).await.unwrap().unwrap();
    assert_eq!(cp.raw_appended_lsn, lsn("0/100"));
    assert_eq!(cp.transformed_lsn, lsn("0/80"));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn check_rejects_transformed_ahead_of_raw() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let (e, s, t) = (EpochNo(700_002), "public", "c2");

    ensure_checkpoint(&mut *tx, e, s, t).await.unwrap();
    advance_raw_appended(&mut *tx, e, s, t, lsn("0/100"))
        .await
        .unwrap();

    // Advancing the mirror PAST the raw log violates the DB CHECK — surfaced as a typed terminal
    // error, never a panic or a raw sqlx error leaking.
    let err = advance_transformed(&mut *tx, e, s, t, lsn("0/200"))
        .await
        .expect_err("transformed ahead of raw must be rejected");
    assert!(
        matches!(err, ControlError::CheckViolation { .. }) && err.is_terminal(),
        "expected a terminal CheckViolation, got {err:?}"
    );
    // Against a real server the chain is what carries the SQLSTATE and constraint name; the
    // one-line verdict above is a summary of it.
    assert!(
        std::error::Error::source(&err).is_some(),
        "the driver error must survive classification, got {err:?}"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn advances_are_idempotent_and_monotonic() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let (e, s, t) = (EpochNo(700_003), "public", "c3");

    ensure_checkpoint(&mut *tx, e, s, t).await.unwrap();
    advance_raw_appended(&mut *tx, e, s, t, lsn("0/100"))
        .await
        .unwrap();
    // Re-advance to the same value — a harmless no-op (idempotent after a crash).
    advance_raw_appended(&mut *tx, e, s, t, lsn("0/100"))
        .await
        .unwrap();
    // Advance to a LOWER value — GREATEST keeps the higher frontier (monotonic, never backward).
    advance_raw_appended(&mut *tx, e, s, t, lsn("0/50"))
        .await
        .unwrap();

    let cp = read_checkpoint(&mut *tx, e, s, t).await.unwrap().unwrap();
    assert_eq!(
        cp.raw_appended_lsn,
        lsn("0/100"),
        "watermark never moves backward"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn read_current_epoch_returns_highest_generation() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    // read within the (rolled-back) txn sees only these two inserts.
    insert_epoch(
        &mut *tx,
        EpochNo(700_010),
        "walrus_slot",
        lsn("0/10"),
        ReplicationStatus::Bootstrapping,
    )
    .await
    .unwrap();
    insert_epoch(
        &mut *tx,
        EpochNo(700_011),
        "walrus_slot",
        lsn("0/20"),
        ReplicationStatus::Streaming,
    )
    .await
    .unwrap();

    let current = read_current_epoch(&mut *tx).await.unwrap().unwrap();
    assert_eq!(
        current.epoch,
        EpochNo(700_011),
        "highest epoch is the current generation"
    );
    assert_eq!(current.status, ReplicationStatus::Streaming);
    assert_eq!(current.created_lsn, lsn("0/20"));
    assert_eq!(
        current.catalog_fence_version, 0,
        "the generic insertion helper cannot manufacture resumable catalog provenance"
    );

    tx.rollback().await.unwrap();
}
