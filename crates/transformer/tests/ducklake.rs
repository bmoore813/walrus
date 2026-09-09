#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test setup stays compact"
)]

//! DuckLake compatibility contract against compose.
//!
//! `cargo test -p transformer --test ducklake -- --ignored --test-threads=1`

use common::{EpochNo, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
use tokio_util::sync::CancellationToken;
use transformer::compaction::{full_rebuild, prune_raw_ducklake};
use transformer::config::DuckLakeConfig;
use transformer::duck::{S3Access, TableDb, maintain_catalog};
use transformer::transform::{TransformSql, apply_transform_ducklake};

fn catalog_url() -> String {
    std::env::var("WALRUS_DUCKLAKE_CATALOG_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_ducklake".to_string()
    })
}

fn config() -> DuckLakeConfig {
    DuckLakeConfig {
        catalog_url: catalog_url().into(),
        data_path: "s3://walrus/ducklake/tests/".to_string(),
        install_extensions: true,
        ..DuckLakeConfig::default()
    }
}

fn s3() -> S3Access {
    S3Access {
        endpoint: "localhost:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".into(),
        use_ssl: false,
    }
}

fn relation() -> PgRelation {
    PgRelation {
        oid: 7_700_001,
        schema: "ducklake_contract".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "status".to_string(),
                type_oid: 25,
                type_modifier: -1,
                is_key: false,
            },
        ],
    }
}

#[test]
#[ignore = "requires docker compose control-pg + MinIO"]
fn shared_catalog_supports_walrus_transform_and_read_view() {
    let rel = relation();
    let db = TableDb::open_ducklake(&config(), EpochNo(7_700_001), &rel.schema, &rel.name, &s3())
        .unwrap();
    db.wipe_generation(&rel.name).unwrap();
    db.ensure_tables(&rel, SchemaVersionNo(1)).unwrap();
    db.set_built_epoch(EpochNo(7_700_001)).unwrap();

    db.conn()
        .execute_batch(
            r#"
            INSERT INTO orders_raw VALUES
              (1, 'old', '{}', 'i', '0000000000000010', '0000000000000011', 'now'),
              (1, 'new', '{}', 'u', '0000000000000020', '0000000000000021', 'now'),
              (2, 'gone', '{}', 'i', '0000000000000010', '0000000000000012', 'now'),
              (2, NULL,   '{}', 'd', '0000000000000020', '0000000000000022', 'now');
            "#,
        )
        .unwrap();

    let transform = TransformSql::from_relation(&rel).unwrap();
    db.in_txn("DuckLake contract transform", |conn| {
        apply_transform_ducklake(conn, &transform, common::Lsn::ZERO)
    })
    .unwrap();
    // Replay the same boundary: applied-LSN guards make it a no-op without key constraints.
    db.in_txn("DuckLake contract replay", |conn| {
        apply_transform_ducklake(conn, &transform, common::Lsn::ZERO)
    })
    .unwrap();

    let rows: Vec<(i32, String)> = db
        .conn()
        .prepare("SELECT id, status FROM walrus.ducklake_contract.orders_current ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, "new".to_string())]);

    let primary_keys: i64 = db
        .conn()
        .query_row(
            "SELECT count(*) FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        primary_keys, 0,
        "DuckLake objects do not use unsupported keys"
    );

    full_rebuild(&db, &transform, &CancellationToken::new()).unwrap();
    prune_raw_ducklake(db.conn(), &transform, common::Lsn::from(0x18)).unwrap();
    db.maintain_files(&rel.name).unwrap();
    drop(db);

    // Keep the catalog-wide lifecycle contract covered as well as the per-table SQL. With the
    // default seven-day thresholds this verifies procedure compatibility without expiring this
    // test's just-created snapshot.
    maintain_catalog(&config(), &s3()).unwrap();
}
