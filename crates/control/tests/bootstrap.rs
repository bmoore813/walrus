#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect are test setup and assertions"
)]
//! Compose-gated integration coverage for the bootstrap reconciliation group.
#![cfg(feature = "integration")]

use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use control::reload::{
    self, ExportRangePlan, ExportSnapshot, ExporterLease, ReloadFenceIdentity, ReloadFlavor,
    ReloadScope, SourceReloadRequest,
};
use control::{
    ControlError, ReplicationStatus, acquire_lease, bump_bootstrap_epoch, bump_epoch,
    complete_bootstrap, connect, ensure_checkpoint, mark_total_restart, read_bootstrap_progress,
    read_current_epoch, run_migrations,
};
use sqlx::Connection;
use sqlx::postgres::{PgConnection, PgPool};
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

async fn publish_fenced(
    conn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    epoch: EpochNo,
    table: &str,
    reload_id: ReloadId,
) {
    let owner = "bootstrap-transformer";
    let lease = acquire_lease(&mut **conn, epoch, "public", table, owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut **conn, epoch, "public", table)
        .await
        .unwrap();
    let publication = reload::claim_publication(
        &mut **conn,
        epoch,
        "public",
        table,
        owner,
        lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(publication.reload_id, reload_id);
    assert!(
        reload::seal_publication_if_drained(conn, &publication, owner, lease.fencing_token)
            .await
            .unwrap()
    );
    assert!(
        reload::finish_publication(&mut **conn, &publication, owner, lease.fencing_token)
            .await
            .unwrap()
    );
}

async fn seal_empty_export(
    conn: &mut PgConnection,
    lease: &ExporterLease,
    f: Lsn,
    schema_version: SchemaVersionNo,
) {
    let snapshot = format!("1:{}:", lease.reload_id.0 + 2);
    reload::begin_export_plan(
        conn,
        lease,
        f,
        schema_version,
        ExportSnapshot {
            identity: &snapshot,
            xmin: 1,
            xmax: lease.reload_id.0 + 2,
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
    reload::record_export_range(&mut *conn, lease, 0, 0, 0)
        .await
        .unwrap();
    reload::seal_export(conn, lease, f, schema_version)
        .await
        .unwrap();
}

async fn register_table(conn: &mut PgConnection, epoch: EpochNo, table: &str, schema_version: i64) {
    sqlx::query(
        "INSERT INTO public.walrus_schema_registry
           (epoch, source_schema, source_table, schema_version, descriptors, columns)
         VALUES ($1, 'public', $2, $3, '[]'::jsonb, '[]'::jsonb)",
    )
    .bind(epoch.0)
    .bind(table)
    .bind(schema_version)
    .execute(conn)
    .await
    .unwrap();
}

async fn assert_direct_promotion_rejected(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    epoch: EpochNo,
) {
    let mut savepoint = Connection::begin(&mut **tx).await.unwrap();
    let error = sqlx::query(
        "UPDATE public.walrus_replication_state
         SET status = 'streaming'
         WHERE epoch = $1",
    )
    .bind(epoch.0)
    .execute(&mut *savepoint)
    .await
    .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("replication_state_bootstrap_promotion_guard"),
        "the database must independently attest bootstrap readiness: {error}"
    );
    savepoint.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_epoch_atomically_binds_inventory_and_enforces_its_size() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0001);
    let targets = serde_json::json!([
        {"schema": "public", "table": "customers"},
        {"schema": "public", "table": "orders"}
    ]);
    let before = read_current_epoch(&mut *tx).await.unwrap();
    let expected_current = before.as_ref().map(|state| state.epoch);

    // The migration constraint rejects a count that disagrees with the frozen target array. Use a
    // savepoint because a real CHECK violation aborts its surrounding transaction.
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let err = bump_bootstrap_epoch(
            &mut *savepoint,
            expected_current,
            "walrus-bootstrap-invalid",
            "0/700".parse().unwrap(),
            request_id,
            1,
            &targets,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, ControlError::CheckViolation { .. }),
            "target-count mismatch must be a typed invariant failure: {err:?}"
        );
        savepoint.rollback().await.unwrap();
    }
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap(),
        before,
        "a rejected bootstrap must not leave a partially-created epoch"
    );

    let created_lsn: Lsn = "0/800".parse().unwrap();
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-atomic",
        created_lsn,
        request_id,
        2,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");
    let lingering_protocol: Option<String> = sqlx::query_scalar(
        "SELECT NULLIF(
           pg_catalog.current_setting('walrus.catalog_fence_protocol', true),
           ''
         )",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        lingering_protocol, None,
        "the v1 insertion capability must be cleared before bump_bootstrap_epoch returns"
    );
    register_table(&mut tx, epoch, "customers", 1).await;
    register_table(&mut tx, epoch, "orders", 1).await;

    let current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .expect("the new bootstrap epoch is current");
    assert_eq!(current.epoch, epoch);
    assert_eq!(current.slot_name, "walrus-bootstrap-atomic");
    assert_eq!(current.created_lsn, created_lsn);
    assert_eq!(
        current.catalog_fence_version,
        control::CURRENT_CATALOG_FENCE_VERSION
    );
    assert_eq!(current.status, ReplicationStatus::Bootstrapping);

    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .expect("a bootstrapping epoch exposes its bound group");
    assert_eq!(progress.request_id, request_id);
    assert_eq!(progress.expected_tables, 2);
    assert_eq!(progress.targets, targets);
    assert_eq!(progress.children, 0);
    assert_eq!(progress.complete, 0);
    assert_eq!(progress.failed, 0);
    assert!(!progress.is_ready());

    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "an inventory binding alone cannot promote an epoch"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_epoch_deferred_guard_requires_the_exact_registry_inventory() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let targets = serde_json::json!([{"schema": "public", "table": "orders"}]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);

    let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
    bump_bootstrap_epoch(
        &mut *savepoint,
        expected_current,
        "walrus-bootstrap-registry-missing",
        "0/805".parse().unwrap(),
        Uuid::new_v4(),
        1,
        &targets,
    )
    .await
    .unwrap()
    .unwrap();
    let error = sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("replication_state_catalog_registry_inventory"),
        "a missing registry identity must not attest a v1 generation"
    );
    savepoint.rollback().await.unwrap();

    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-registry-exact",
        "0/806".parse().unwrap(),
        Uuid::new_v4(),
        1,
        &targets,
    )
    .await
    .unwrap()
    .unwrap();
    for schema_version in [1_i64, 2] {
        register_table(&mut tx, epoch, "orders", schema_version).await;
    }
    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *tx)
        .await
        .expect("the exact registry table set satisfies the deferred v1 proof");

    control::upsert_registry(
        &mut *tx,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_owned(),
            source_table: "orders".to_owned(),
            schema_version: SchemaVersionNo(2),
            descriptors: Vec::new(),
            columns: serde_json::json!([]),
        },
    )
    .await
    .expect("an exact registry upsert is a permitted semantic no-op");

    for (case_name, mutation, expected_constraint) in [
        (
            "semantic-update",
            "UPDATE public.walrus_schema_registry SET columns = '[1]'::jsonb \
             WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'orders' \
               AND schema_version = 2",
            "schema_registry_semantics_immutable",
        ),
        (
            "delete",
            "DELETE FROM public.walrus_schema_registry WHERE epoch = $1",
            "schema_registry_removal_guard",
        ),
    ] {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(mutation)
            .bind(epoch.0)
            .execute(&mut *savepoint)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some(expected_constraint),
            "schema-registry mutation case {case_name} must be rejected"
        );
        savepoint.rollback().await.unwrap();
    }

    let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
    let extra_target = sqlx::query(
        "INSERT INTO public.walrus_schema_registry
           (epoch, source_schema, source_table, schema_version, descriptors, columns)
         VALUES ($1, 'public', 'customers', 1, '[]'::jsonb, '[]'::jsonb)",
    )
    .bind(epoch.0)
    .execute(&mut *savepoint)
    .await
    .unwrap_err();
    assert_eq!(
        extra_target
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("schema_registry_catalog_membership"),
        "a later schema version may not expand a fenced epoch's target inventory"
    );
    savepoint.rollback().await.unwrap();

    let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
    let truncate = sqlx::query("TRUNCATE TABLE public.walrus_schema_registry")
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
    assert_eq!(
        truncate
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("schema_registry_removal_guard")
    );
    savepoint.rollback().await.unwrap();

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn total_restart_intent_is_idempotent_and_guarded_by_the_current_epoch() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let predecessor = bump_epoch(
        &mut *tx,
        "walrus-restart-predecessor",
        "0/810".parse().unwrap(),
        ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    let current = bump_epoch(
        &mut *tx,
        "walrus-restart-current",
        "0/820".parse().unwrap(),
        ReplicationStatus::Streaming,
    )
    .await
    .unwrap();

    assert!(
        !mark_total_restart(&mut *tx, predecessor).await.unwrap(),
        "a stale observer must lose the guard instead of arming an old generation"
    );
    let before_restart = read_current_epoch(&mut *tx).await.unwrap().unwrap();
    assert_eq!(before_restart.status, ReplicationStatus::Streaming);
    assert_eq!(before_restart.catalog_fence_version, 0);

    assert!(mark_total_restart(&mut *tx, current).await.unwrap());
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().status,
        ReplicationStatus::TotalRestart
    );
    assert!(
        mark_total_restart(&mut *tx, current).await.unwrap(),
        "a crash recovery must be able to re-arm the same current epoch"
    );

    let empty_targets = serde_json::json!([]);
    let stale = bump_bootstrap_epoch(
        &mut *tx,
        Some(predecessor),
        "walrus-restart-stale",
        "0/830".parse().unwrap(),
        Uuid::from_u128(0xb007_0010),
        0,
        &empty_targets,
    )
    .await
    .unwrap();
    assert_eq!(
        stale, None,
        "a stale observer cannot open a successor to a non-current epoch"
    );

    let successor = bump_bootstrap_epoch(
        &mut *tx,
        Some(current),
        "walrus-restart-successor",
        "0/840".parse().unwrap(),
        Uuid::from_u128(0xb007_0011),
        0,
        &empty_targets,
    )
    .await
    .unwrap()
    .expect("the exact current epoch can open one successor");
    let duplicate = bump_bootstrap_epoch(
        &mut *tx,
        Some(current),
        "walrus-restart-duplicate",
        "0/850".parse().unwrap(),
        Uuid::from_u128(0xb007_0012),
        0,
        &empty_targets,
    )
    .await
    .unwrap();
    assert_eq!(
        duplicate, None,
        "the same observed predecessor cannot be used to create e3 after e2 wins"
    );
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().epoch,
        successor
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_promotes_only_after_every_bound_child_completes() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0002);
    let targets = serde_json::json!([
        {"schema": "public", "table": "customers"},
        {"schema": "public", "table": "orders"}
    ]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-progress",
        "0/900".parse().unwrap(),
        request_id,
        2,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");
    register_table(&mut tx, epoch, "customers", 1).await;
    register_table(&mut tx, epoch, "orders", 1).await;

    let orders = SourceReloadRequest {
        epoch,
        source_request_id: request_id,
        parent_request_id: Some(request_id),
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let customers = SourceReloadRequest {
        source_table: "customers",
        ..orders
    };

    let orders_id = reload::request_from_source(&mut *tx, &orders)
        .await
        .unwrap();
    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((progress.children, progress.complete), (1, 0));
    assert!(!progress.is_ready());
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );

    let customers_id = reload::request_from_source(&mut *tx, &customers)
        .await
        .unwrap();
    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((progress.children, progress.complete), (2, 0));
    assert!(!progress.is_ready());

    let claimed = reload::claim_requested(&mut *tx, epoch, "bootstrap-extractor", 60, 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);

    let first_f: Lsn = "0/A00".parse().unwrap();
    let first_h: Lsn = "0/A80".parse().unwrap();
    let orders_fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, orders_id, first_f, orders_fence)
        .await
        .unwrap();
    let orders_lease = reload::get(&mut *tx, orders_id)
        .await
        .unwrap()
        .unwrap()
        .exporter_lease("bootstrap-extractor")
        .unwrap();
    seal_empty_export(&mut tx, &orders_lease, first_f, SchemaVersionNo(1)).await;
    reload::record_end_marker(&mut *tx, orders_id, first_h, orders_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, &orders_lease, first_h)
        .await
        .unwrap();
    publish_fenced(&mut tx, epoch, "orders", orders_id).await;

    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((progress.children, progress.complete), (2, 1));
    assert!(!progress.is_ready());
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "one completed child must not publish a partial generation"
    );

    let second_f: Lsn = "0/B00".parse().unwrap();
    let second_h: Lsn = "0/B80".parse().unwrap();
    let customers_fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "customers",
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, customers_id, second_f, customers_fence)
        .await
        .unwrap();
    let customers_lease = reload::get(&mut *tx, customers_id)
        .await
        .unwrap()
        .unwrap()
        .exporter_lease("bootstrap-extractor")
        .unwrap();
    seal_empty_export(&mut tx, &customers_lease, second_f, SchemaVersionNo(1)).await;
    reload::record_end_marker(&mut *tx, customers_id, second_h, customers_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, &customers_lease, second_h)
        .await
        .unwrap();
    publish_fenced(&mut tx, epoch, "customers", customers_id).await;

    let ready = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((ready.children, ready.complete, ready.failed), (2, 2, 0));
    assert!(ready.is_ready());

    let removed = sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT pg_catalog.set_config(
             'walrus.schema_registry_maintenance', '1-delete', true
           ) AS protocol
         )
         DELETE FROM public.walrus_schema_registry
         WHERE epoch = $1
           AND source_schema = 'public'
           AND source_table = 'customers'
           AND (SELECT protocol = '1-delete' FROM authorized)",
    )
    .bind(epoch.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert_eq!(removed.rows_affected(), 1);
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "promotion must re-attest the exact registry even after child completion"
    );
    assert_direct_promotion_rejected(&mut tx, epoch).await;
    register_table(&mut tx, epoch, "customers", 1).await;

    assert!(
        !complete_bootstrap(&mut *tx, epoch, Uuid::from_u128(0xdead_beef))
            .await
            .unwrap(),
        "a different source request cannot promote the generation"
    );
    assert!(
        complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );

    let current = read_current_epoch(&mut *tx).await.unwrap().unwrap();
    assert_eq!(current.epoch, epoch);
    assert_eq!(current.status, ReplicationStatus::Streaming);
    assert!(
        read_bootstrap_progress(&mut *tx, epoch)
            .await
            .unwrap()
            .is_none(),
        "a promoted generation no longer reports pending bootstrap work"
    );
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "promotion is an idempotent guarded transition"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_promotion_requires_the_exact_frozen_target_set() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0013);
    let targets = serde_json::json!([{"schema": "public", "table": "customers"}]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-target-identity",
        "0/E00".parse().unwrap(),
        request_id,
        1,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");
    register_table(&mut tx, epoch, "customers", 1).await;

    assert_direct_promotion_rejected(&mut tx, epoch).await;

    // A fabricated child for another table preserves all old count-based checks. It must not be
    // allowed to stand in for the exact table frozen in bootstrap_targets, even after that wrong
    // child reaches its fully attested terminal state.
    let wrong_child = SourceReloadRequest {
        epoch,
        source_request_id: request_id,
        parent_request_id: Some(request_id),
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let wrong_id = reload::request_from_source(&mut *tx, &wrong_child)
        .await
        .unwrap();
    let claimed = reload::claim_requested(&mut *tx, epoch, "wrong-target-extractor", 60, 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let lease = claimed[0].exporter_lease("wrong-target-extractor").unwrap();
    let f: Lsn = "0/E10".parse().unwrap();
    let h: Lsn = "0/E80".parse().unwrap();
    let identity = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, wrong_id, f, identity)
        .await
        .unwrap();
    seal_empty_export(&mut tx, &lease, f, SchemaVersionNo(1)).await;
    reload::record_end_marker(&mut *tx, wrong_id, h, identity)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, &lease, h).await.unwrap();
    publish_fenced(&mut tx, epoch, "orders", wrong_id).await;

    let superficial = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            superficial.children,
            superficial.complete,
            superficial.failed
        ),
        (1, 1, 0),
        "this fixture deliberately satisfies the old count-only predicate"
    );
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "the supported Rust path must reject a same-count child for the wrong target"
    );
    assert_direct_promotion_rejected(&mut tx, epoch).await;

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn an_empty_fenced_bootstrap_can_promote_without_children() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0014);
    let targets = serde_json::json!([]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-empty-inventory",
        "0/F00".parse().unwrap(),
        request_id,
        0,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");
    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *tx)
        .await
        .expect("an empty bootstrap requires and has an empty registry inventory");

    assert!(
        complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "an exact empty inventory is already complete"
    );
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().status,
        ReplicationStatus::Streaming
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_progress_uses_the_latest_ddl_restart_attempt_per_target() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0003);
    let targets = serde_json::json!([{"schema": "public", "table": "orders"}]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-restart",
        "0/C00".parse().unwrap(),
        request_id,
        1,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");
    register_table(&mut tx, epoch, "orders", 1).await;
    let child = SourceReloadRequest {
        epoch,
        source_request_id: request_id,
        parent_request_id: Some(request_id),
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let predecessor_id = reload::request_from_source(&mut *tx, &child).await.unwrap();
    reload::claim_requested(&mut *tx, epoch, "bootstrap-extractor", 60, 1)
        .await
        .unwrap();
    let predecessor = reload::get(&mut *tx, predecessor_id)
        .await
        .unwrap()
        .unwrap();

    let successor_id = reload::restart_for_ddl(&mut tx, &predecessor, SchemaVersionNo(2), 3)
        .await
        .unwrap()
        .expect("the restart budget permits a successor");
    assert!(successor_id > predecessor_id);
    let predecessor = reload::get(&mut *tx, predecessor_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(predecessor.status, reload::ReloadStatus::Failed);
    let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
    assert_eq!(successor.parent_request_id, Some(request_id));
    assert_eq!(successor.scope, ReloadScope::AllPublished);

    let restarted = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (restarted.children, restarted.complete, restarted.failed),
        (1, 0, 0),
        "the failed predecessor is history; only its live successor represents this target"
    );
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );

    let f: Lsn = "0/D00".parse().unwrap();
    let h: Lsn = "0/D80".parse().unwrap();
    let successor_fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: SchemaVersionNo(2),
    };
    reload::record_start_fence(&mut *tx, successor_id, f, successor_fence)
        .await
        .unwrap();
    let successor_lease = reload::get(&mut *tx, successor_id)
        .await
        .unwrap()
        .unwrap()
        .exporter_lease("bootstrap-extractor")
        .unwrap();
    seal_empty_export(&mut tx, &successor_lease, f, SchemaVersionNo(2)).await;
    reload::record_end_marker(&mut *tx, successor_id, h, successor_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, &successor_lease, h)
        .await
        .unwrap();
    publish_fenced(&mut tx, epoch, "orders", successor_id).await;

    let complete = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (complete.children, complete.complete, complete.failed),
        (1, 1, 0)
    );
    assert!(complete.is_ready());
    assert!(
        complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().status,
        ReplicationStatus::Streaming
    );

    tx.rollback().await.unwrap();
}
