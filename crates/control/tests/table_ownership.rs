//! Control-plane integration coverage for table-lease fencing tokens, renewal, and stale-owner
//! rejection against a live PostgreSQL database.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect are test setup and assertions"
)]
#![cfg(feature = "integration")]

use common::EpochNo;
use control::{acquire_lease, connect, release_lease, renew_lease, run_migrations};

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

#[tokio::test]
async fn every_acquire_mints_a_token_and_stale_capabilities_cannot_renew_or_release() {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_001);

    let first = acquire_lease(&mut *tx, epoch, "public", "orders", "transformer-a", 60)
        .await
        .unwrap()
        .expect("new row is acquirable");
    assert_eq!(first.fencing_token, 1);
    assert!(
        acquire_lease(&mut *tx, epoch, "public", "orders", "transformer-b", 60)
            .await
            .unwrap()
            .is_none(),
        "a different owner cannot take a live lease"
    );

    let second = acquire_lease(&mut *tx, epoch, "public", "orders", "transformer-a", 60)
        .await
        .unwrap()
        .expect("the same owner may explicitly reacquire");
    assert_eq!(
        second.fencing_token,
        first.fencing_token + 1,
        "same-name reacquisition must fence the prior process"
    );
    assert!(
        !renew_lease(
            &mut *tx,
            epoch,
            "public",
            "orders",
            "transformer-a",
            first.fencing_token,
            60,
        )
        .await
        .unwrap(),
        "the old token cannot renew after same-owner reacquisition"
    );
    assert!(
        renew_lease(
            &mut *tx,
            epoch,
            "public",
            "orders",
            "transformer-a",
            second.fencing_token,
            60,
        )
        .await
        .unwrap()
    );

    release_lease(
        &mut *tx,
        epoch,
        "public",
        "orders",
        "transformer-a",
        first.fencing_token,
    )
    .await
    .unwrap();
    assert!(
        acquire_lease(&mut *tx, epoch, "public", "orders", "transformer-b", 60)
            .await
            .unwrap()
            .is_none(),
        "a stale release cannot expire the newer same-owner acquisition"
    );

    release_lease(
        &mut *tx,
        epoch,
        "public",
        "orders",
        "transformer-a",
        second.fencing_token,
    )
    .await
    .unwrap();
    let third = acquire_lease(&mut *tx, epoch, "public", "orders", "transformer-b", 60)
        .await
        .unwrap()
        .expect("the exact release makes the row immediately acquirable");
    assert_eq!(third.fencing_token, second.fencing_token + 1);

    sqlx::query(
        "UPDATE public.walrus_table_ownership
         SET lease_expiry = now() - interval '1 second'
         WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'orders'",
    )
    .bind(epoch)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(
        !renew_lease(
            &mut *tx,
            epoch,
            "public",
            "orders",
            "transformer-b",
            third.fencing_token,
            60,
        )
        .await
        .unwrap(),
        "renewal cannot resurrect an expired lease"
    );
    let fourth = acquire_lease(&mut *tx, epoch, "public", "orders", "transformer-b", 60)
        .await
        .unwrap()
        .expect("an expired holder can explicitly reacquire");
    assert_eq!(fourth.fencing_token, third.fencing_token + 1);

    let short = acquire_lease(&mut *tx, epoch, "public", "long_tx", "transformer-a", 1)
        .await
        .unwrap()
        .expect("short lease acquired");
    sqlx::query("SELECT pg_sleep(1.1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        !renew_lease(
            &mut *tx,
            epoch,
            "public",
            "long_tx",
            "transformer-a",
            short.fencing_token,
            60,
        )
        .await
        .unwrap(),
        "lease liveness must use statement time, not the surrounding transaction's frozen now()"
    );

    tx.rollback().await.unwrap();
}
