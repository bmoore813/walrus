#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for `schema_registry` and `ddl_manifest`.
//!
//! Each test runs inside a rolled-back transaction under a unique `epoch`. Gated behind the
//! `integration` feature (needs the compose control PG).
#![cfg(feature = "integration")]

use common::{DdlId, EpochNo, Lsn, SchemaVersionNo, Tier, TypeDescriptor, TypeMeta};
use control::{
    DdlRow, RegistryRow, connect, insert_ddl, read_all_ddl, read_all_registry,
    read_latest_ddl_snapshot, read_latest_ddl_version_through, read_latest_version,
    read_pending_ddl, read_registry, run_migrations, upsert_registry,
};
use sqlx::{Connection, postgres::PgPool};

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

fn descriptors() -> Vec<TypeDescriptor> {
    vec![
        TypeDescriptor {
            column: "id".to_string(),
            pg_type_oid: 23,
            pg_type: "int4".to_string(),
            tier: Tier::One,
            arrow: "Int32".to_string(),
            duckdb: "INTEGER".to_string(),
            emit: vec!["id:INT32".to_string()],
            recombine: None,
            meta: TypeMeta::default(),
        },
        TypeDescriptor {
            column: "duration".to_string(),
            pg_type_oid: 1186,
            pg_type: "interval".to_string(),
            tier: Tier::Two,
            arrow: "Struct/Decomposed".to_string(),
            duckdb: "INTERVAL".to_string(),
            emit: vec![
                "duration_months:INT32".to_string(),
                "duration_days:INT32".to_string(),
                "duration_micros:INT64".to_string(),
            ],
            recombine: Some("to_months(m)+to_days(d)+to_microseconds(us)".to_string()),
            meta: TypeMeta::default(),
        },
    ]
}

fn registry_row(epoch: EpochNo, version: SchemaVersionNo) -> RegistryRow {
    RegistryRow {
        epoch,
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: version,
        descriptors: descriptors(),
        columns: serde_json::json!([
            {"name": "id", "attnum": 1, "not_null": true},
            {"name": "duration", "attnum": 2, "not_null": false}
        ]),
    }
}

fn ddl(epoch: EpochNo, c_lsn: &str, version: SchemaVersionNo) -> DdlRow {
    DdlRow {
        id: DdlId(0), // ignored on insert
        epoch,
        source_audit_id: version.0,
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        c_lsn: c_lsn.parse().unwrap(),
        c_event: "ddl_command_end".to_string(),
        c_tag: "ALTER TABLE".to_string(),
        schema_version: version,
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE public.orders ADD COLUMN duration interval".into()),
        c_table_comment: Some("orders metadata".into()),
    }
}

#[tokio::test]
async fn registry_round_trips_a_type_descriptor_set() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_001);

    let row = registry_row(epoch, SchemaVersionNo(3));
    upsert_registry(&mut *tx, &row).await.unwrap();

    let read = read_registry(&mut *tx, epoch, "public", "orders", SchemaVersionNo(3))
        .await
        .unwrap()
        .unwrap();
    // Descriptors round-trip byte-for-byte equal through jsonb.
    assert_eq!(read.descriptors, row.descriptors);
    assert_eq!(read.columns, row.columns);
    assert_eq!(read.schema_version, SchemaVersionNo(3));

    // An unknown version reads as None.
    assert!(
        read_registry(&mut *tx, epoch, "public", "orders", SchemaVersionNo(99))
            .await
            .unwrap()
            .is_none()
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn upsert_registry_is_idempotent_per_version() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_002);

    let row = registry_row(epoch, SchemaVersionNo(1));
    upsert_registry(&mut *tx, &row).await.unwrap();
    upsert_registry(&mut *tx, &row).await.unwrap(); // same version again

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_schema_registry WHERE epoch = $1 AND schema_version = $2",
    )
    .bind(epoch)
    .bind(1_i64)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        count, 1,
        "a re-write of the same version must not duplicate"
    );

    // read_latest_version reports the max across versions.
    upsert_registry(&mut *tx, &registry_row(epoch, SchemaVersionNo(5)))
        .await
        .unwrap();
    assert_eq!(
        read_all_registry(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.schema_version)
            .collect::<Vec<_>>(),
        vec![SchemaVersionNo(1), SchemaVersionNo(5)],
        "restart hydration retains every historical relation shape"
    );
    let latest = read_latest_version(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    assert_eq!(latest, Some(SchemaVersionNo(5)));
    // and None for an unknown table.
    assert_eq!(
        read_latest_version(&mut *tx, epoch, "public", "no_such")
            .await
            .unwrap(),
        None
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn ddl_row_round_trips_with_commit_lsn() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_003);

    let id = insert_ddl(&mut *tx, &ddl(epoch, "0/500", SchemaVersionNo(5)))
        .await
        .unwrap();
    assert!(id > DdlId(0));

    let pending = read_pending_ddl(
        &mut *tx,
        epoch,
        "public",
        "orders",
        "0/100".parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].c_lsn, "0/500".parse::<Lsn>().unwrap());
    assert_eq!(pending[0].c_tag, "ALTER TABLE");
    assert_eq!(pending[0].c_event, "ddl_command_end");
    assert_eq!(pending[0].schema_version, SchemaVersionNo(5));
    assert_eq!(pending[0].source_audit_id, 5);
    assert_eq!(
        pending[0].c_table_comment.as_deref(),
        Some("orders metadata")
    );
    assert_eq!(
        pending[0].c_ddl_text.as_deref(),
        Some("ALTER TABLE public.orders ADD COLUMN duration interval")
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_audit_identity_makes_ddl_replay_idempotent() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_005);
    let original = ddl(epoch, "0/500", SchemaVersionNo(2));

    let first_id = insert_ddl(&mut *tx, &original).await.unwrap();
    let second_id = insert_ddl(&mut *tx, &original).await.unwrap();
    assert_eq!(second_id, first_id, "WAL replay must reuse the history row");

    let mut changed = original.clone();
    changed.c_tag = "CREATE TABLE".into();
    assert!(matches!(
        insert_ddl(&mut *tx, &changed).await,
        Err(control::ControlError::ImmutableHistoryConflict {
            entity: "ddl_manifest",
            ..
        })
    ));

    let history = read_all_ddl(&mut *tx, epoch).await.unwrap();
    assert_eq!(
        history,
        vec![DdlRow {
            id: first_id,
            ..original
        }]
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_ddl_manifest WHERE epoch = $1 AND source_audit_id = $2",
    )
    .bind(epoch)
    .bind(2_i64)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(count, 1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn ddl_history_is_immutable_and_removal_requires_explicit_maintenance() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_007);
    let original = ddl(epoch, "0/700", SchemaVersionNo(7));
    let id = insert_ddl(&mut *tx, &original).await.unwrap();

    let exact_no_op = sqlx::query(
        "UPDATE public.walrus_ddl_manifest
         SET source_audit_id = source_audit_id
         WHERE id = $1",
    )
    .bind(id.0)
    .execute(&mut *tx)
    .await
    .expect("the replay upsert's exact no-op UPDATE shape must remain valid");
    assert_eq!(exact_no_op.rows_affected(), 1);

    for (case_name, mutation, expected_constraint) in [
        (
            "semantic-update",
            "UPDATE public.walrus_ddl_manifest SET c_tag = 'CREATE TABLE' WHERE id = $1",
            "ddl_manifest_semantics_immutable",
        ),
        (
            "creation-timestamp-update",
            "UPDATE public.walrus_ddl_manifest SET created_at = created_at + interval '1 second' WHERE id = $1",
            "ddl_manifest_semantics_immutable",
        ),
        (
            "delete",
            "DELETE FROM public.walrus_ddl_manifest WHERE id = $1",
            "ddl_manifest_removal_guard",
        ),
    ] {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(mutation)
            .bind(id.0)
            .execute(&mut *savepoint)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some(expected_constraint),
            "DDL history mutation case {case_name} must be rejected"
        );
        savepoint.rollback().await.unwrap();
    }

    let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
    let truncate = sqlx::query("TRUNCATE TABLE public.walrus_ddl_manifest")
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
    assert_eq!(
        truncate
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("ddl_manifest_removal_guard")
    );
    savepoint.rollback().await.unwrap();

    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance', '2-delete', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query("DELETE FROM public.walrus_ddl_manifest WHERE id = $1")
            .bind(id.0)
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected(),
        1,
        "explicit maintenance may remove a selected DDL history row"
    );

    insert_ddl(&mut *tx, &ddl(epoch, "0/710", SchemaVersionNo(8)))
        .await
        .unwrap();
    sqlx::query("TRUNCATE TABLE public.walrus_ddl_manifest")
        .execute(&mut *tx)
        .await
        .expect("explicit maintenance may truncate DDL history");
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM public.walrus_ddl_manifest")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(remaining, 0);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn registry_replay_cannot_rewrite_an_existing_version() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_006);
    let original = registry_row(epoch, SchemaVersionNo(2));
    upsert_registry(&mut *tx, &original).await.unwrap();

    let mut changed = original.clone();
    changed.columns = serde_json::json!([{"name": "different"}]);
    assert!(matches!(
        upsert_registry(&mut *tx, &changed).await,
        Err(control::ControlError::ImmutableHistoryConflict {
            entity: "schema_registry",
            ..
        })
    ));
    assert_eq!(
        read_registry(&mut *tx, epoch, "public", "orders", SchemaVersionNo(2))
            .await
            .unwrap(),
        Some(original),
        "the conflicting replay must leave durable history unchanged"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn read_pending_ddl_orders_by_c_lsn() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(800_004);

    // Insert out of LSN order.
    let v3_id = insert_ddl(&mut *tx, &ddl(epoch, "0/300", SchemaVersionNo(3)))
        .await
        .unwrap();
    insert_ddl(&mut *tx, &ddl(epoch, "0/100", SchemaVersionNo(1)))
        .await
        .unwrap();
    let v2_id = insert_ddl(&mut *tx, &ddl(epoch, "0/200", SchemaVersionNo(2)))
        .await
        .unwrap();

    let all = read_pending_ddl(&mut *tx, epoch, "public", "orders", "0/0".parse().unwrap())
        .await
        .unwrap();
    let lsns: Vec<Lsn> = all.iter().map(|r| r.c_lsn).collect();
    assert_eq!(
        lsns,
        vec![
            "0/100".parse().unwrap(),
            "0/200".parse().unwrap(),
            "0/300".parse().unwrap()
        ]
    );

    // after_lsn is a strict lower bound: only c_lsn > 0/150 (i.e. 0/200, 0/300).
    let after = read_pending_ddl(
        &mut *tx,
        epoch,
        "public",
        "orders",
        "0/150".parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].c_lsn, "0/200".parse::<Lsn>().unwrap());

    let latest_v2 = read_latest_ddl_snapshot(
        &mut *tx,
        epoch,
        "public",
        "orders",
        DdlId(0),
        SchemaVersionNo(2),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(latest_v2.id, v2_id);
    let latest_v3 = read_latest_ddl_snapshot(
        &mut *tx,
        epoch,
        "public",
        "orders",
        v2_id,
        SchemaVersionNo(3),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        latest_v3.id, v3_id,
        "the LSN watermark must find a later-LSN row even when its id was inserted first"
    );

    assert_eq!(
        read_latest_ddl_version_through(
            &mut *tx,
            epoch,
            "public",
            "orders",
            "0/250".parse().unwrap(),
        )
        .await
        .unwrap(),
        Some(SchemaVersionNo(2)),
        "post-boundary DDL must not invalidate an earlier reload fence"
    );
    assert_eq!(
        read_latest_ddl_version_through(
            &mut *tx,
            epoch,
            "public",
            "orders",
            "0/50".parse().unwrap(),
        )
        .await
        .unwrap(),
        None
    );

    tx.rollback().await.unwrap();
}
