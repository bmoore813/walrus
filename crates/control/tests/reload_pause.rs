#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the transformer-pause claim predicate.
//!
//! "Pausing is not claiming" (reload §2): a live reload of either persisted flavor in
//! `requested|exporting|export_complete|publishing` makes generic `claim_ready` return nothing for
//! THAT table while its `ready` rows accumulate; every other table claims normally. A fenced
//! publication-specific claim drains the frozen `[F,H]` set, and only terminal states lift the
//! generic pause. `resync` remains a compatibility spelling for the identical rebuild behavior.
#![cfg(feature = "integration")]

use common::{EpochNo, Lsn, SchemaVersionNo};
use control::reload::{
    self, ExportRangePlan, ExportSnapshot, ReloadFenceIdentity, ReloadFlavor, ReloadScope,
    SourceReloadRequest,
};
use control::{
    ControlError, acquire_lease, claim_ready, connect, delete_claimed, ensure_checkpoint,
    insert_ready, max_ready_lsn_end, run_migrations,
};
use control::{ManifestRow, NewManifestFile};
use sqlx::postgres::{PgConnection, PgPool};
use std::time::Duration;
use uuid::Uuid;

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

fn stream_file(epoch: EpochNo, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{lsn_end}.parquet"),
        kind: control::ManifestKind::Stream,
        row_count: 1,
        object_size: 1,
        sha256: vec![0; 32],
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: None,
    }
}

fn ids(rows: &[ManifestRow]) -> Vec<common::ManifestId> {
    rows.iter().map(|r| r.id).collect()
}

async fn finish_fenced(conn: &mut PgConnection, reload_id: common::ReloadId, h: Lsn) {
    let row = reload::get(&mut *conn, reload_id).await.unwrap().unwrap();
    let schema_version = row.schema_version.unwrap_or(SchemaVersionNo(1));
    let f = row.start_lsn.or(row.first_lsn).unwrap_or(h);
    let holder = row.lease_holder.clone().unwrap();
    let lease = row.exporter_lease(&holder).unwrap();
    let identity = ReloadFenceIdentity {
        request_id: row.source_request_id.or(row.parent_request_id),
        source_schema: &row.source_schema,
        source_table: &row.source_table,
        schema_version,
    };
    reload::record_start_fence(&mut *conn, reload_id, f, identity)
        .await
        .unwrap();
    let snapshot = format!("1:{}:", reload_id.0 + 2);
    reload::begin_export_plan(
        conn,
        &lease,
        f,
        schema_version,
        ExportSnapshot {
            identity: &snapshot,
            xmin: 1,
            xmax: reload_id.0 + 2,
        },
        &[ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }],
    )
    .await
    .unwrap();
    reload::record_export_range(&mut *conn, &lease, 0, 0, 0)
        .await
        .unwrap();
    reload::seal_export(conn, &lease, f, schema_version)
        .await
        .unwrap();
    reload::record_end_marker(&mut *conn, reload_id, h, identity)
        .await
        .unwrap();
    reload::complete_export(&mut *conn, &lease, h)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_rebuild_pauses_claims_for_that_table_only() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_001);

    insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/20"))
        .await
        .unwrap();
    let other = insert_ready(&mut *tx, &stream_file(epoch, "customers", "0/10"))
        .await
        .unwrap();

    // `requested` already pauses (the pause must cover the whole pre-export window)…
    reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "a requested rebuild pauses the table's claims"
    );
    // …and `exporting` keeps the pause.
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty()
    );

    // The OTHER table claims normally the whole time, and the paused table's backlog stays
    // visible (the lag gauge SHOULD grow during a pause by design).
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()),
        vec![other]
    );
    assert_eq!(
        max_ready_lsn_end(&mut *tx, epoch, "public", "orders")
            .await
            .unwrap(),
        Some("0/20".parse().unwrap()),
        "ready rows accumulate; nothing is lost or hidden from the backlog gauge"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn publication_claim_drains_through_h_before_complete_lifts_the_generic_pause() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_002);

    // Backlog inserted OUT of order, so the publication claim must return it in
    // `(lsn_end, id)` order.
    let c = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/30"))
        .await
        .unwrap();
    let a = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    let b = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/20"))
        .await
        .unwrap();
    let after_h = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/200"))
        .await
        .unwrap();

    let orders = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty()
    );

    // `export_complete` keeps the generic path paused. The transformer must first acquire the table
    // fence and enter the publication-specific path.
    let h: Lsn = "0/100".parse().unwrap();
    finish_fenced(&mut tx, orders, h).await;
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "export_complete remains paused on the generic claim path"
    );
    assert_eq!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![orders],
        "export_complete remains an active rebuild"
    );

    let owner = "transformer-a";
    let lease = acquire_lease(&mut *tx, epoch, "public", "orders", owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    let publication = reload::claim_publication(
        &mut *tx,
        epoch,
        "public",
        "orders",
        owner,
        lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "publishing remains paused on the generic claim path"
    );
    assert_eq!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![orders],
        "publishing remains an active rebuild"
    );

    let through_h =
        reload::claim_publication_ready(&mut *tx, &publication, owner, lease.fencing_token, 100)
            .await
            .unwrap();
    assert_eq!(
        ids(&through_h),
        vec![a, b, c],
        "the publication claim drains only [F,H] in unchanged (lsn_end, id) order"
    );
    assert_eq!(delete_claimed(&mut *tx, &ids(&through_h)).await.unwrap(), 3);
    assert!(
        reload::seal_publication_if_drained(&mut tx, &publication, owner, lease.fencing_token,)
            .await
            .unwrap()
    );
    assert!(
        reload::finish_publication(&mut *tx, &publication, owner, lease.fencing_token)
            .await
            .unwrap(),
        "the first finish transitions publishing to complete"
    );
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()),
        vec![after_h],
        "complete lifts the generic pause while preserving rows above H"
    );

    // `failed` equally lifts: a second table walks requested → exporting → failed.
    let f = insert_ready(&mut *tx, &stream_file(epoch, "customers", "0/10"))
        .await
        .unwrap();
    let cust = reload::request(&mut *tx, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()
            .is_empty()
    );
    reload::fail(&mut tx, cust, "demo").await.unwrap();
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()),
        vec![f],
        "a failed reload lifts the pause (its own chunk files were purged; stream rows survive)"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn ungrouped_publish_racing_the_reload_seal_is_observed_before_cutover() {
    let pool = pool().await;
    let epoch_bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8].try_into().unwrap();
    let epoch = EpochNo(i64::from_be_bytes(epoch_bytes) & i64::MAX);
    let table = "ungrouped_seal_race";
    let h: Lsn = "0/100".parse().unwrap();

    let reload_id = reload::request(&pool, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&pool, epoch, "extractor-seal-race", 60, 1)
        .await
        .unwrap();
    let mut setup = pool.acquire().await.unwrap();
    finish_fenced(&mut setup, reload_id, h).await;
    drop(setup);

    let owner = "transformer-seal-race";
    let lease = acquire_lease(&pool, epoch, "public", table, owner, 60)
        .await
        .unwrap()
        .unwrap();
    let fencing_token = lease.fencing_token;
    ensure_checkpoint(&pool, epoch, "public", table)
        .await
        .unwrap();
    let publication =
        reload::claim_publication(&pool, epoch, "public", table, owner, fencing_token)
            .await
            .unwrap()
            .unwrap();

    let mut bypass = pool.begin().await.unwrap();
    let direct_complete =
        sqlx::query("UPDATE public.walrus_table_reload SET status='complete' WHERE reload_id=$1")
            .bind(publication.reload_id.0)
            .execute(&mut *bypass)
            .await
            .unwrap_err();
    assert_eq!(
        direct_complete
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("table_reload_v2_completion_guard")
    );
    bypass.rollback().await.unwrap();

    // The uncommitted INSERT holds the same fence row that sealing needs. Once it commits, the
    // seal's later statement snapshot must see the new manifest and return false.
    let mut publisher_tx = pool.begin().await.unwrap();
    let pending = insert_ready(&mut *publisher_tx, &stream_file(epoch, table, "0/80"))
        .await
        .unwrap();
    let seal_pool = pool.clone();
    let seal_publication = publication.clone();
    let seal_task = tokio::spawn(async move {
        let mut tx = seal_pool.begin().await.unwrap();
        let result =
            reload::seal_publication_if_drained(&mut tx, &seal_publication, owner, fencing_token)
                .await;
        tx.commit().await.unwrap();
        result
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !seal_task.is_finished(),
        "seal must wait for the publisher fence"
    );
    publisher_tx.commit().await.unwrap();
    assert!(!seal_task.await.unwrap().unwrap());

    assert!(matches!(
        reload::finish_publication(&pool, &publication, owner, fencing_token).await,
        Err(ControlError::ReloadTransition { .. })
    ));

    // Even an exact seal written with the explicit protocol tripwire must not let an idempotent
    // seal retry skip the fresh pending-work check.
    let mut forged = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_seal_protocol','2',true)")
        .execute(&mut *forged)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE public.walrus_manifest_publication_fence
         SET sealed_through_lsn=$4, sealed_reload_id=$5,
             sealed_publication_nonce=$6, updated_at=now()
         WHERE epoch=$1 AND source_schema=$2 AND source_table=$3",
    )
    .bind(epoch.0)
    .bind("public")
    .bind(table)
    .bind(h)
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .execute(&mut *forged)
    .await
    .unwrap();
    forged.commit().await.unwrap();
    let mut retry = pool.begin().await.unwrap();
    assert!(matches!(
        reload::seal_publication_if_drained(&mut retry, &publication, owner, fencing_token).await,
        Err(ControlError::ManifestInvariant { .. })
    ));
    retry.rollback().await.unwrap();

    let claimed = reload::claim_publication_ready(&pool, &publication, owner, fencing_token, 10)
        .await
        .unwrap();
    assert_eq!(ids(&claimed), vec![pending]);
    assert_eq!(delete_claimed(&pool, &[pending]).await.unwrap(), 1);

    let mut seal_tx = pool.begin().await.unwrap();
    assert!(
        reload::seal_publication_if_drained(&mut seal_tx, &publication, owner, fencing_token,)
            .await
            .unwrap()
    );
    seal_tx.commit().await.unwrap();

    let mut late = stream_file(epoch, table, "0/90");
    late.s3_uri.push_str("-after-seal");
    assert!(matches!(
        insert_ready(&pool, &late).await,
        Err(ControlError::CheckViolation { .. })
    ));

    // The losing finisher begins while the winner's outer transaction still holds its locks.
    // Once that winner commits, the loser's later READ COMMITTED replay statement must see the
    // completed receipt and return Ok(false), not a transient ReloadTransition.
    let mut winner = pool.begin().await.unwrap();
    assert!(
        reload::finish_publication(&mut winner, &publication, owner, fencing_token)
            .await
            .unwrap()
    );
    let loser_pool = pool.clone();
    let loser_publication = publication.clone();
    let loser = tokio::spawn(async move {
        reload::finish_publication(&loser_pool, &loser_publication, owner, fencing_token).await
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !loser.is_finished(),
        "the losing finish must wait on the winner"
    );
    winner.commit().await.unwrap();
    assert!(
        !loser.await.unwrap().unwrap(),
        "the post-wait fresh snapshot recognizes the exact completed receipt"
    );

    let later: Lsn = "0/120".parse().unwrap();
    control::advance_raw_appended(&pool, epoch, "public", table, later)
        .await
        .unwrap();
    control::advance_transformed(&pool, epoch, "public", table, later)
        .await
        .unwrap();
    assert!(
        !reload::finish_publication(&pool, &publication, owner, fencing_token)
            .await
            .unwrap(),
        "the immutable completed receipt remains idempotent after normal checkpoint progress"
    );
    let mut wrong = publication.clone();
    wrong.final_lsn = "0/101".parse().unwrap();
    assert!(matches!(
        reload::finish_publication(&pool, &wrong, owner, fencing_token).await,
        Err(ControlError::ReloadTransition { .. })
    ));

    let mut cleanup = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_manifest_publication_fence WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_transformer_checkpoint WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_reload WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_ownership WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
}

#[tokio::test]
async fn preexisting_ungrouped_straddler_blocks_the_reload_seal() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_011);
    let table = "ungrouped_preexisting_straddler";
    let h = Lsn::new(0x100);

    let reload_id = reload::request(&mut *tx, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-straddler", 60, 1)
        .await
        .unwrap();
    finish_fenced(&mut tx, reload_id, h).await;

    let mut straddler = stream_file(epoch, table, "0/120");
    straddler.lsn_start = Lsn::new(0x80);
    insert_ready(&mut *tx, &straddler).await.unwrap();

    let owner = "transformer-straddler";
    let lease = acquire_lease(&mut *tx, epoch, "public", table, owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut *tx, epoch, "public", table)
        .await
        .unwrap();
    let publication =
        reload::claim_publication(&mut *tx, epoch, "public", table, owner, lease.fencing_token)
            .await
            .unwrap()
            .unwrap();

    assert!(
        reload::claim_publication_ready(&mut *tx, &publication, owner, lease.fencing_token, 10,)
            .await
            .unwrap()
            .is_empty(),
        "a post-H file is not part of the bounded publication drain"
    );
    assert!(matches!(
        reload::seal_publication_if_drained(&mut tx, &publication, owner, lease.fencing_token,)
            .await,
        Err(ControlError::ManifestInvariant { .. })
    ));

    // The database trigger independently protects the receipt from a caller that knows the
    // transaction-local protocol setting and bypasses the Rust drain check.
    sqlx::query("SELECT set_config('walrus.manifest_seal_protocol','2',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let error = sqlx::query(
        "UPDATE public.walrus_manifest_publication_fence
         SET sealed_through_lsn=$4, sealed_reload_id=$5,
             sealed_publication_nonce=$6, updated_at=now()
         WHERE epoch=$1 AND source_schema=$2 AND source_table=$3",
    )
    .bind(epoch.0)
    .bind("public")
    .bind(table)
    .bind(h)
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("manifest_publication_fence_ungrouped_straddler")
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn publishing_reload_can_be_fenced_and_adopted_after_owner_loss() {
    let pool = pool().await;
    let epoch_bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8].try_into().unwrap();
    let epoch = EpochNo(i64::from_be_bytes(epoch_bytes) & i64::MAX);
    let table = "publication_takeover";
    let h: Lsn = "0/300".parse().unwrap();

    let reload_id = reload::request(&pool, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&pool, epoch, "extractor-takeover", 60, 1)
        .await
        .unwrap();
    let mut setup = pool.acquire().await.unwrap();
    finish_fenced(&mut setup, reload_id, h).await;
    drop(setup);
    ensure_checkpoint(&pool, epoch, "public", table)
        .await
        .unwrap();

    let first_lease = acquire_lease(&pool, epoch, "public", table, "transformer-a", 60)
        .await
        .unwrap()
        .unwrap();
    let first = reload::claim_publication(
        &pool,
        epoch,
        "public",
        table,
        "transformer-a",
        first_lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();

    let mut arbitrary = pool.begin().await.unwrap();
    let rewrite = sqlx::query(
        "UPDATE public.walrus_table_reload
         SET publisher_owner_pod='intruder', publisher_fencing_token=999
         WHERE reload_id=$1",
    )
    .bind(reload_id.0)
    .execute(&mut *arbitrary)
    .await
    .unwrap_err();
    assert_eq!(
        rewrite
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("table_reload_publication_owner_transition")
    );
    arbitrary.rollback().await.unwrap();

    sqlx::query(
        "UPDATE public.walrus_table_ownership
         SET lease_expiry=statement_timestamp()-interval '1 second'
         WHERE epoch=$1 AND source_schema='public' AND source_table=$2",
    )
    .bind(epoch.0)
    .bind(table)
    .execute(&pool)
    .await
    .unwrap();
    let second_lease = acquire_lease(&pool, epoch, "public", table, "transformer-b", 60)
        .await
        .unwrap()
        .unwrap();
    assert!(second_lease.fencing_token > first_lease.fencing_token);
    let adopted = reload::claim_publication(
        &pool,
        epoch,
        "public",
        table,
        "transformer-b",
        second_lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(adopted.publication_nonce, first.publication_nonce);
    assert_eq!(adopted.publisher_owner_pod, "transformer-b");
    assert_eq!(adopted.publisher_fencing_token, second_lease.fencing_token);

    let mut unsealed_failure = pool.begin().await.unwrap();
    assert_eq!(
        sqlx::query("UPDATE public.walrus_table_reload SET status='failed' WHERE reload_id=$1")
            .bind(reload_id.0)
            .execute(&mut *unsealed_failure)
            .await
            .unwrap()
            .rows_affected(),
        1,
        "an unsealed publishing attempt may still fail for integrity recovery"
    );
    unsealed_failure.rollback().await.unwrap();

    let mut seal = pool.begin().await.unwrap();
    assert!(
        reload::seal_publication_if_drained(
            &mut seal,
            &adopted,
            "transformer-b",
            second_lease.fencing_token,
        )
        .await
        .unwrap()
    );
    seal.commit().await.unwrap();

    let mut sealed_failure = pool.begin().await.unwrap();
    let error =
        sqlx::query("UPDATE public.walrus_table_reload SET status='failed' WHERE reload_id=$1")
            .bind(reload_id.0)
            .execute(&mut *sealed_failure)
            .await
            .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("table_reload_status_transition")
    );
    sealed_failure.rollback().await.unwrap();

    assert!(
        reload::finish_publication(&pool, &adopted, "transformer-b", second_lease.fencing_token,)
            .await
            .unwrap()
    );

    let mut cleanup = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_manifest_publication_fence WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_transformer_checkpoint WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_reload WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_ownership WHERE epoch=$1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
}

#[tokio::test]
async fn resync_alias_pauses_in_both_live_states() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_003);

    insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the resync compatibility spelling pauses while requested"
    );
    // Flip to exporting and probe again to cover both live states.
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the resync compatibility spelling remains paused while exporting"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn completed_resync_can_drain_before_a_queued_source_reload_starts() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_004);

    let manifest = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    let current = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();

    // Source-backed rows queue even behind an active legacy resync. Both spellings engage the
    // pause, so the current attempt must complete its fenced publication before the queued one
    // starts.
    let queued = reload::request_from_source(
        &mut *tx,
        &SourceReloadRequest {
            epoch,
            source_request_id: Uuid::from_u128(0x920_004),
            parent_request_id: None,
            scope: ReloadScope::Table,
            source_schema: "public",
            source_table: "orders",
            flavor: ReloadFlavor::Reload,
        },
    )
    .await
    .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the queued reload pauses claims until the current export is ready to cut over"
    );

    finish_fenced(&mut tx, current, "0/20".parse().unwrap()).await;
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "export_complete keeps the generic claim paused"
    );
    assert!(
        reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "the queued reload still waits for the resync's transformer completion"
    );

    let owner = "transformer-a";
    let lease = acquire_lease(&mut *tx, epoch, "public", "orders", owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    let publication = reload::claim_publication(
        &mut *tx,
        epoch,
        "public",
        "orders",
        owner,
        lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    let claimed =
        reload::claim_publication_ready(&mut *tx, &publication, owner, lease.fencing_token, 100)
            .await
            .unwrap();
    assert_eq!(ids(&claimed), vec![manifest]);
    assert_eq!(delete_claimed(&mut *tx, &ids(&claimed)).await.unwrap(), 1);
    assert!(
        reload::seal_publication_if_drained(&mut tx, &publication, owner, lease.fencing_token,)
            .await
            .unwrap()
    );
    reload::finish_publication(&mut *tx, &publication, owner, lease.fencing_token)
        .await
        .unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![queued]
    );

    tx.rollback().await.unwrap();
}
