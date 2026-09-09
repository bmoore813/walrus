#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Relation cache ↔ `schema_registry` round-trip against the compose control PG (`#[ignore]`). After
//! `docker compose up --wait`:
//!
//!   cargo test -p extractor --test relation_cache -- --ignored
//!
//! Each test runs inside a rolled-back transaction under a unique epoch, so it is isolated.

use arrow::datatypes::DataType;
use common::{EpochNo, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
use control::{connect, read_all_latest_registry, read_registry, run_migrations};
use extractor::consume::on_relation;
use extractor::relcache::RelationCache;
use pg_to_arrow::{EXTRACTOR_META_COLUMN, oids};
use sqlx::postgres::PgPool;

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn pool() -> PgPool {
    let pool = connect(&control_url())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

fn col(name: &str, oid: u32, typmod: i32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key,
    }
}

/// The `orders` shape from the unit test (id, amount, created_at, note).
fn orders_relation(oid: u32) -> PgRelation {
    PgRelation {
        oid,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", oids::INT4, -1, true),
            col("amount", oids::NUMERIC, 655366, false), // numeric(10,2)
            col("created_at", oids::TIMESTAMPTZ, -1, false),
            col("note", oids::TEXT, -1, false),
        ],
    }
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn first_relation_writes_a_schema_registry_row() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_220_001);
    let mut cache = RelationCache::default();

    on_relation(
        &mut cache,
        &mut *tx,
        epoch,
        orders_relation(50001),
        SchemaVersionNo(1),
    )
    .await
    .unwrap();
    assert!(
        cache.get(50001, SchemaVersionNo(1)).is_some(),
        "cached under (oid, version)"
    );

    // A repeat at the same schema_version is idempotent — no duplicate row.
    on_relation(
        &mut cache,
        &mut *tx,
        epoch,
        orders_relation(50001),
        SchemaVersionNo(1),
    )
    .await
    .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_schema_registry WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(count, 1, "repeat at same version must not duplicate");

    let row = read_registry(&mut *tx, epoch, "public", "orders", SchemaVersionNo(1))
        .await
        .unwrap()
        .expect("registry row present");
    assert_eq!(row.descriptors.len(), 4, "one descriptor per source column");

    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (grouped with the compose tests)"]
async fn cached_arrow_schema_matches_expected_orders_shape() {
    // Pure (no DB): the cached Arrow schema exactly matches the unit test.
    let mut cache = RelationCache::default();
    let entry = cache
        .upsert_from_relation(orders_relation(50002), SchemaVersionNo(1))
        .unwrap();
    let f = entry.arrow_schema.fields();
    assert_eq!(f.len(), 5); // 4 data + walrus_extractor_meta
    assert_eq!(f[0].name(), "id");
    assert_eq!(f[0].data_type(), &DataType::Int32);
    assert_eq!(f[1].data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(f[3].data_type(), &DataType::Utf8);
    assert_eq!(f[4].name(), EXTRACTOR_META_COLUMN);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn hydrate_reconstructs_cache_from_registry() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_220_002);

    // Persist via on_relation, then hydrate a FRESH cache from what was written.
    let mut writer = RelationCache::default();
    on_relation(
        &mut writer,
        &mut *tx,
        epoch,
        orders_relation(50003),
        SchemaVersionNo(2),
    )
    .await
    .unwrap();

    let rows = read_all_latest_registry(&mut *tx, epoch).await.unwrap();
    assert_eq!(rows.len(), 1);

    let mut fresh = RelationCache::default();
    fresh.hydrate(rows).unwrap();
    let entry = fresh
        .get(50003, SchemaVersionNo(2))
        .expect("hydrated entry");
    assert_eq!(entry.relation.name, "orders");
    assert_eq!(entry.descriptors.len(), 4);
    assert_eq!(entry.arrow_schema.fields().len(), 5);

    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn internal_tables_are_never_registered() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(2_220_003);
    let mut cache = RelationCache::default();

    let heartbeat = PgRelation {
        oid: 60001,
        schema: "public".to_string(),
        name: "walrus_heartbeat".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", oids::INT4, -1, true)],
    };
    on_relation(&mut cache, &mut *tx, epoch, heartbeat, SchemaVersionNo(1))
        .await
        .unwrap();

    assert!(
        cache.get(60001, SchemaVersionNo(1)).is_none(),
        "internal table not cached"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_schema_registry WHERE epoch = $1")
            .bind(epoch)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(count, 0, "internal table never registered");

    tx.rollback().await.unwrap();
}
