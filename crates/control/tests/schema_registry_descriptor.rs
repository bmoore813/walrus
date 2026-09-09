#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose round-trip: a *generated* `pg-to-arrow` `TypeDescriptor` set persists to
//! `schema_registry` and reads back equal. This exercises the extractor↔transformer seam end to end and proves
//! `control` may dev-depend on `pg-to-arrow` with **no DAG cycle** (pg-to-arrow → common only).
//!
//! Unlike the `integration`-feature test files, this one is `#[ignore]`-gated (it dev-deps
//! pg-to-arrow, so it compiles in the default build; it just needs the control PG to *run*). After
//! `docker compose up --wait`:
//!
//! ```text
//! cargo test -p control -- --ignored schema_registry_roundtrips_a_type_descriptor
//! ```

use common::{PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
use control::{RegistryRow, connect, read_registry, run_migrations, upsert_registry};
use pg_to_arrow::describe_relation;

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn col(name: &str, oid: u32, typmod: i32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key,
    }
}

#[tokio::test]
#[ignore = "requires `docker compose up --wait` (control PG)"]
async fn schema_registry_roundtrips_a_type_descriptor() {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");

    // A relation spanning all three tiers: int4 (Tier-1), interval (Tier-2 decomposition), and
    // char(5) (Tier-1 carrying char_length metadata).
    let rel = PgRelation {
        oid: 42_000,
        schema: "public".to_string(),
        name: "widgets".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", 23, -1, true),      // int4
            col("span", 1186, -1, false), // interval → 3 emitted columns + recombine
            col("code", 1042, 9, false),  // char(5): bpchar, typmod = 5 + VARHDRSZ(4)
        ],
    };
    let descriptors = describe_relation(&rel).unwrap();
    assert_eq!(descriptors.len(), 3);

    let row = RegistryRow {
        epoch: common::EpochNo(2_170_017),
        source_schema: "public".to_string(),
        source_table: "widgets".to_string(),
        schema_version: SchemaVersionNo(1),
        descriptors: descriptors.clone(),
        columns: serde_json::json!([]),
    };

    // Isolate in a rolled-back transaction (matches the other control integration tests).
    let mut tx = pool.begin().await.unwrap();
    upsert_registry(&mut *tx, &row).await.unwrap();
    let back = read_registry(&mut *tx, row.epoch, "public", "widgets", SchemaVersionNo(1))
        .await
        .unwrap()
        .expect("registry row present after upsert");

    // The generated descriptors survive the jsonb round-trip byte-for-byte.
    assert_eq!(back.descriptors, descriptors);
    assert_eq!(back, row);
    // Spot-check the interval descriptor made it through intact.
    let interval = &back.descriptors[1];
    assert_eq!(interval.emit.len(), 3);
    assert_eq!(
        interval.recombine.as_deref(),
        Some("to_months(m)+to_days(d)+to_microseconds(us)")
    );

    tx.rollback().await.unwrap();
}
