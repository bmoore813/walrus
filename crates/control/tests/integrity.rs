//! Control-plane integration coverage for integrity failure fencing, reload recovery, and
//! quarantine behavior against a live PostgreSQL database.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test setup and assertions"
)]
#![cfg(feature = "integration")]

use common::{EpochNo, Lsn, SchemaVersionNo, UtcTimestamp};
use control::{
    IntegrityFailure, IntegrityFailureOutcome, IntegrityRecoveryStatus, ManifestKind,
    NewManifestFile, NewStreamCommitPublication, acquire_lease, claim_ready, connect,
    handle_integrity_failure, insert_ready, publish_stream_commit, read_integrity_recovery,
    run_migrations,
};
use sqlx::postgres::PgPool;
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

fn epoch() -> EpochNo {
    let bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8].try_into().unwrap();
    EpochNo(i64::from_be_bytes(bytes) & i64::MAX)
}

fn file(epoch: EpochNo, table: &str, suffix: &str) -> NewManifestFile {
    let commit_lsn = Lsn::new(0x200);
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{suffix}.parquet"),
        kind: ManifestKind::Stream,
        row_count: 1,
        object_size: 64,
        sha256: vec![7; 32],
        lsn_start: Lsn::new(0x100),
        lsn_end: commit_lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: None,
    }
}

async fn own(pool: &PgPool, epoch: EpochNo, table: &str) -> i64 {
    acquire_lease(pool, epoch, "public", table, "transformer-0", 60)
        .await
        .unwrap()
        .unwrap()
        .fencing_token
}

async fn cleanup(pool: &PgPool, epoch: EpochNo) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_integrity_recovery WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM public.walrus_file_manifest WHERE epoch = $1 AND (SELECT protocol='2' FROM authorized)")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_manifest_group WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_txn_publication WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_manifest_publication_fence WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_reload WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_ownership WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn wait_for_xid_waiter(pool: &PgPool, xid: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                   SELECT 1 FROM pg_locks
                   WHERE locktype = 'transactionid' AND transactionid::text = $1
                     AND NOT granted
                 )",
            )
            .bind(xid.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("integrity handler never reached the expected row-lock wait");
}

#[tokio::test]
async fn corrupt_stream_child_fails_the_whole_group_and_schedules_one_reload() {
    let pool = pool().await;
    let epoch = epoch();
    let table = format!("integrity_group_{}", epoch.0);
    let token = own(&pool, epoch, &table).await;
    let publication = NewStreamCommitPublication {
        epoch,
        top_xid: 42,
        commit_lsn: Lsn::new(0x200),
        commit_ts: UtcTimestamp::now(),
        ddl_rows: Vec::new(),
        registry_rows: Vec::new(),
        files: vec![file(epoch, &table, "a"), file(epoch, &table, "b")],
    };
    publish_stream_commit(&pool, &publication).await.unwrap();
    let (manifest_id, group_id): (i64, i64) = sqlx::query_as(
        "SELECT id, stream_group_id FROM public.walrus_file_manifest
         WHERE epoch = $1 ORDER BY stream_group_ordinal LIMIT 1",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();

    let outcome = handle_integrity_failure(
        &pool,
        &IntegrityFailure {
            epoch,
            source_schema: "public",
            source_table: &table,
            manifest_id: common::ManifestId(manifest_id),
            reason: "sha256 mismatch",
            owner_pod: "transformer-0",
            fencing_token: token,
            publication: None,
            max_resnapshots: 1,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        IntegrityFailureOutcome::RecoveryScheduled { attempt: 1, .. }
    ));
    let child_statuses: Vec<String> = sqlx::query_scalar(
        "SELECT status FROM public.walrus_file_manifest
         WHERE stream_group_id = $1 ORDER BY stream_group_ordinal",
    )
    .bind(group_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(child_statuses, vec!["failed", "failed"]);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM public.walrus_stream_manifest_group WHERE id = $1"
        )
        .bind(group_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "failed"
    );
    let recovery = read_integrity_recovery(&pool, epoch, "public", &table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovery.status, IntegrityRecoveryStatus::Retrying);
    assert_eq!(recovery.attempt_count, 1);
    assert_eq!(recovery.failed_group_id.unwrap().0, group_id);
    assert!(
        claim_ready(&pool, epoch, "public", &table, 100)
            .await
            .unwrap()
            .is_empty(),
        "table-level recovery state pauses generic claims"
    );

    cleanup(&pool, epoch).await;
}

#[tokio::test]
async fn zero_budget_quarantines_without_creating_an_export() {
    let pool = pool().await;
    let epoch = epoch();
    let table = format!("integrity_stop_{}", epoch.0);
    let token = own(&pool, epoch, &table).await;
    let manifest_id = insert_ready(&pool, &file(epoch, &table, "single"))
        .await
        .unwrap();

    let outcome = handle_integrity_failure(
        &pool,
        &IntegrityFailure {
            epoch,
            source_schema: "public",
            source_table: &table,
            manifest_id,
            reason: "object size mismatch",
            owner_pod: "transformer-0",
            fencing_token: token,
            publication: None,
            max_resnapshots: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, IntegrityFailureOutcome::Quarantined { attempt: 1 });
    let recovery = read_integrity_recovery(&pool, epoch, "public", &table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovery.status, IntegrityRecoveryStatus::Quarantined);
    assert_eq!(recovery.recovery_reload_id, None);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM public.walrus_table_reload WHERE epoch = $1"
        )
        .bind(epoch.0)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    cleanup(&pool, epoch).await;
}

#[tokio::test]
async fn active_recovery_pins_its_budget_across_mixed_transformer_configs() {
    let pool = pool().await;
    let epoch = epoch();
    let table = format!("integrity_budget_{}", epoch.0);
    let token = own(&pool, epoch, &table).await;
    let first = insert_ready(&pool, &file(epoch, &table, "first"))
        .await
        .unwrap();

    let recovery_reload_id = match handle_integrity_failure(
        &pool,
        &IntegrityFailure {
            epoch,
            source_schema: "public",
            source_table: &table,
            manifest_id: first,
            reason: "first bad object",
            owner_pod: "transformer-0",
            fencing_token: token,
            publication: None,
            max_resnapshots: 1,
        },
    )
    .await
    .unwrap()
    {
        IntegrityFailureOutcome::RecoveryScheduled { reload_id, .. } => reload_id,
        other @ IntegrityFailureOutcome::Quarantined { .. } => {
            panic!("first incident must schedule one replacement, got {other:?}")
        }
    };

    let second = insert_ready(&pool, &file(epoch, &table, "second"))
        .await
        .unwrap();
    assert_eq!(
        handle_integrity_failure(
            &pool,
            &IntegrityFailure {
                epoch,
                source_schema: "public",
                source_table: &table,
                manifest_id: second,
                reason: "replacement input was also bad",
                owner_pod: "transformer-0",
                fencing_token: token,
                publication: None,
                // A newly rolled transformer is more permissive, but it must not enlarge a recovery
                // cycle that the first owner already bounded at one resnapshot.
                max_resnapshots: 10,
            },
        )
        .await
        .unwrap(),
        IntegrityFailureOutcome::Quarantined { attempt: 2 }
    );
    let recovery = read_integrity_recovery(&pool, epoch, "public", &table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovery.status, IntegrityRecoveryStatus::Quarantined);
    assert_eq!(
        recovery.max_attempts, 1,
        "the first failure pins the cycle budget"
    );
    assert_eq!(recovery.recovery_reload_id, Some(recovery_reload_id));
    assert_eq!(
        control::reload::get(&pool, recovery_reload_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        control::ReloadStatus::Failed,
        "the now-forbidden requested replacement must not remain live forever"
    );

    cleanup(&pool, epoch).await;
}

#[tokio::test]
async fn grouped_integrity_fencing_waits_for_the_parent_before_locking_a_child() {
    let pool = pool().await;
    let epoch = epoch();
    let table = format!("integrity_lock_order_{}", epoch.0);
    let token = own(&pool, epoch, &table).await;
    publish_stream_commit(
        &pool,
        &NewStreamCommitPublication {
            epoch,
            top_xid: 77,
            commit_lsn: Lsn::new(0x200),
            commit_ts: UtcTimestamp::now(),
            ddl_rows: Vec::new(),
            registry_rows: Vec::new(),
            files: vec![file(epoch, &table, "lock-a"), file(epoch, &table, "lock-b")],
        },
    )
    .await
    .unwrap();
    let (manifest_id, group_id): (i64, i64) = sqlx::query_as(
        "SELECT id, stream_group_id FROM public.walrus_file_manifest
         WHERE epoch = $1 ORDER BY stream_group_ordinal LIMIT 1",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut parent_holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM public.walrus_stream_manifest_group WHERE id = $1 FOR UPDATE")
        .bind(group_id)
        .fetch_one(&mut *parent_holder)
        .await
        .unwrap();
    let holder_xid = sqlx::query_scalar::<_, i64>("SELECT txid_current()::bigint")
        .fetch_one(&mut *parent_holder)
        .await
        .unwrap();

    let task_pool = pool.clone();
    let task_table = table.clone();
    let integrity = tokio::spawn(async move {
        handle_integrity_failure(
            &task_pool,
            &IntegrityFailure {
                epoch,
                source_schema: "public",
                source_table: &task_table,
                manifest_id: common::ManifestId(manifest_id),
                reason: "lock-order probe",
                owner_pod: "transformer-0",
                fencing_token: token,
                publication: None,
                max_resnapshots: 1,
            },
        )
        .await
    });
    wait_for_xid_waiter(&pool, holder_xid).await;

    let mut child_probe = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM public.walrus_file_manifest WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(manifest_id)
        .fetch_one(&mut *child_probe)
        .await
        .expect("a parent-blocked integrity handler must not already hold the child row");
    child_probe.commit().await.unwrap();
    parent_holder.commit().await.unwrap();

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), integrity)
            .await
            .expect("integrity handler deadlocked after the parent lock was released")
            .unwrap()
            .unwrap(),
        IntegrityFailureOutcome::RecoveryScheduled { attempt: 1, .. }
    ));
    cleanup(&pool, epoch).await;
}

#[tokio::test]
async fn quarantine_fence_locks_requested_reload_before_claim_can_promote_it() {
    let pool = pool().await;
    let epoch = epoch();
    let table = format!("integrity_claim_race_{}", epoch.0);
    let token = own(&pool, epoch, &table).await;
    let first = insert_ready(&pool, &file(epoch, &table, "first-race"))
        .await
        .unwrap();
    let recovery_reload_id = match handle_integrity_failure(
        &pool,
        &IntegrityFailure {
            epoch,
            source_schema: "public",
            source_table: &table,
            manifest_id: first,
            reason: "first race failure",
            owner_pod: "transformer-0",
            fencing_token: token,
            publication: None,
            max_resnapshots: 1,
        },
    )
    .await
    .unwrap()
    {
        IntegrityFailureOutcome::RecoveryScheduled { reload_id, .. } => reload_id,
        other @ IntegrityFailureOutcome::Quarantined { .. } => {
            panic!("first failure did not schedule recovery: {other:?}")
        }
    };
    let second = insert_ready(&pool, &file(epoch, &table, "second-race"))
        .await
        .unwrap();

    let mut recovery_holder = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT epoch FROM public.walrus_table_integrity_recovery
         WHERE epoch = $1 AND source_schema = 'public' AND source_table = $2 FOR UPDATE",
    )
    .bind(epoch.0)
    .bind(&table)
    .fetch_one(&mut *recovery_holder)
    .await
    .unwrap();
    let holder_xid = sqlx::query_scalar::<_, i64>("SELECT txid_current()::bigint")
        .fetch_one(&mut *recovery_holder)
        .await
        .unwrap();

    let task_pool = pool.clone();
    let task_table = table.clone();
    let integrity = tokio::spawn(async move {
        handle_integrity_failure(
            &task_pool,
            &IntegrityFailure {
                epoch,
                source_schema: "public",
                source_table: &task_table,
                manifest_id: second,
                reason: "replacement object was also bad",
                owner_pod: "transformer-0",
                fencing_token: token,
                publication: None,
                max_resnapshots: 1,
            },
        )
        .await
    });
    wait_for_xid_waiter(&pool, holder_xid).await;

    assert!(
        control::reload::claim_requested(&pool, epoch, "racing-extractor", 60, 1)
            .await
            .unwrap()
            .is_empty(),
        "the integrity transaction must lock the requested recovery before waiting on its receipt"
    );
    recovery_holder.commit().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), integrity)
            .await
            .expect("integrity handler did not finish after releasing recovery receipt")
            .unwrap()
            .unwrap(),
        IntegrityFailureOutcome::Quarantined { attempt: 2 }
    );
    assert_eq!(
        control::reload::get(&pool, recovery_reload_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        control::ReloadStatus::Failed
    );
    cleanup(&pool, epoch).await;
}
