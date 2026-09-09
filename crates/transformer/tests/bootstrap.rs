#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Transformer bootstrap against compose (`#[ignore]` — needs control PG + MinIO). Bootstrap acquires the
//! ownership lease and catalog advisory-lock fence, attaches DuckLake with both `<table>` and
//! `<table>_raw`, loads the watermarks, and verifies S3 read. A second live-lease instance exits
//! terminal; an expired lease plus dropped catalog session is reclaimed.
//!
//!   cargo test -p transformer --test bootstrap -- --ignored

use common::{EpochNo, FailureClass, PgColumn, PgRelation, ReplicaIdentity};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::time::Duration;
use transformer::bootstrap::bootstrap;
use transformer::config::DuckLakeConfig;
use transformer::config::TransformerConfig;
use transformer::duck::S3Access;
use transformer::error::TransformerError;
use transformer::health::TransformerState;

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn catalog_url() -> String {
    std::env::var("WALRUS_DUCKLAKE_CATALOG_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_ducklake".to_string()
    })
}

/// The concrete MinIO client, matching what `main::build_store` hands `bootstrap`; `&store()`
/// coerces to the `&dyn ObjectStore` parameter at the call site, exactly as production does.
fn store() -> AmazonS3 {
    AmazonS3Builder::new()
        .with_bucket_name("walrus")
        .with_region("us-east-1")
        .with_endpoint("http://localhost:9000")
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_allow_http(true)
        .build()
        .unwrap()
}

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

fn cfg(pod: &str, ttl: Duration) -> TransformerConfig {
    TransformerConfig {
        common: transformer::config::CommonConfig {
            control_db_url: control_url().into(),
            object_store: transformer::config::ObjectStoreConfig {
                bucket: "walrus".into(),
                endpoint: Some("http://localhost:9000".into()),
                region: "us-east-1".into(),
            },
            ..transformer::config::CommonConfig::default()
        },
        transformer: transformer::config::TransformerSettings {
            instance: pod.into(),
            ducklake: DuckLakeConfig {
                catalog_url: catalog_url().into(),
                data_path: "s3://walrus/ducklake/tests/".to_string(),
                install_extensions: true,
                ..DuckLakeConfig::default()
            },
            lease_ttl: ttl,
            ..transformer::config::TransformerSettings::default()
        },
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

/// Seed a fresh epoch as the current one + register `orders`, cleaning any prior control state.
async fn seed(pool: &sqlx::PgPool, epoch: EpochNo) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.schema_registry_maintenance', '1-delete', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.replication_state_maintenance', '1-delete', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    for tbl in [
        "table_ownership",
        "transformer_checkpoint",
        "schema_registry",
        "replication_state",
    ] {
        let sql = format!("DELETE FROM public.walrus_{tbl} WHERE epoch = $1");
        sqlx::query(&sql)
            .bind(epoch.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    control::insert_epoch(
        pool,
        epoch,
        "walrus_slot",
        "0/0".parse().unwrap(),
        control::ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    let rel = orders();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: rel.schema.clone(),
            source_table: rel.name.clone(),
            schema_version: common::SchemaVersionNo(1),
            descriptors: Vec::new(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn next_epoch(pool: &sqlx::PgPool) -> EpochNo {
    let epoch: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(epoch), 0) + 1 FROM public.walrus_replication_state",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    EpochNo(epoch)
}

fn table_exists(db: &transformer::duck::TableDb, name: &str) -> bool {
    let conn = db.conn();
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
            |r| r.get(0),
        )
        .unwrap();
    n > 0
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn bootstrap_creates_duckdb_with_both_tables_and_takes_the_lease() {
    let _g = LOCK.lock().await;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let epoch = next_epoch(&pool).await;
    seed(&pool, epoch).await;
    let cfg = cfg("transformer-a", Duration::from_secs(30));
    let state = TransformerState::new();

    let owned = bootstrap(&cfg, &pool, &store(), &s3(), &state)
        .await
        .unwrap();
    assert_eq!(owned.tables.len(), 1, "owns the one registered table");
    let orders = &owned.tables[0];
    assert!(table_exists(&orders.db, "orders"), "mirror table exists");
    assert!(
        table_exists(&orders.db, "orders_raw"),
        "CDC log table exists"
    );
    assert!(
        !state.is_ready(),
        "bootstrap does not itself mark ready (main does)"
    );
    assert!(
        state.is_live(),
        "bootstrap stamped one poll → /healthz green"
    );

    // The lease is held by us.
    let owner: String = sqlx::query_scalar(
        "SELECT owner_pod FROM public.walrus_table_ownership WHERE epoch=$1 AND source_table='orders'",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner, "transformer-a");
    assert!(
        orders.db.conn().execute_batch("SELECT 1").is_ok(),
        "DuckLake connection is attached read-write"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn second_instance_with_live_lease_exits_terminal() {
    let _g = LOCK.lock().await;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let epoch = next_epoch(&pool).await;
    seed(&pool, epoch).await;
    let state = TransformerState::new();

    // Instance A takes the lease (live, 30s), catalog fence, and DuckLake connection.
    let _owned_a = bootstrap(
        &cfg("transformer-a", Duration::from_secs(30)),
        &pool,
        &store(),
        &s3(),
        &state,
    )
    .await
    .unwrap();

    // Instance B, while A's lease is live, must fail terminal with LeaseContended.
    let res = bootstrap(
        &cfg("transformer-b", Duration::from_secs(30)),
        &pool,
        &store(),
        &s3(),
        &TransformerState::new(),
    )
    .await;
    let err = res.expect_err("a live lease must be terminal");
    assert!(
        matches!(err, TransformerError::LeaseContended { .. }),
        "a live lease is terminal: {err:?}"
    );
    assert_eq!(err.exit_code(), common::ExitCode::LeaseContended);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn catalog_fence_blocks_a_successor_even_if_the_control_lease_is_released() {
    let _g = LOCK.lock().await;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let epoch = next_epoch(&pool).await;
    seed(&pool, epoch).await;

    let owned_a = bootstrap(
        &cfg("transformer-a", Duration::from_secs(30)),
        &pool,
        &store(),
        &s3(),
        &TransformerState::new(),
    )
    .await
    .unwrap();
    control::release_lease(
        &pool,
        epoch,
        "public",
        "orders",
        "transformer-a",
        owned_a.tables[0].fencing_token,
    )
    .await
    .unwrap();

    let error = bootstrap(
        &cfg("transformer-b", Duration::from_secs(30)),
        &pool,
        &store(),
        &s3(),
        &TransformerState::new(),
    )
    .await
    .expect_err("the independent catalog fence must still reject transformer-b");
    assert!(matches!(error, TransformerError::LeaseContended { .. }));

    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner_pod FROM public.walrus_table_ownership \
         WHERE epoch=$1 AND source_schema='public' AND source_table='orders' \
           AND lease_expiry > now()",
    )
    .bind(epoch.0)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(
        owner, None,
        "failed second-fence acquisition releases transformer-b's lease"
    );
    drop(owned_a);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn expired_lease_and_dropped_catalog_fence_are_reclaimed() {
    let _g = LOCK.lock().await;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let epoch = next_epoch(&pool).await;
    seed(&pool, epoch).await;
    let state = TransformerState::new();

    // Instance A takes a SHORT-TTL lease then "dies": dropping the BootstrapResult closes the
    // DuckDB connection and its dedicated catalog session, releasing every advisory lock.
    {
        let owned_a = bootstrap(
            &cfg("transformer-dead", Duration::from_millis(500)),
            &pool,
            &store(),
            &s3(),
            &state,
        )
        .await
        .unwrap();
        assert_eq!(owned_a.tables[0].fencing_token, 1);
    } // owned_a dropped → catalog fence released; the control lease row remains but will expire.

    tokio::time::sleep(Duration::from_millis(900)).await; // lease expires

    // Instance B reclaims the expired lease and catalog fence. Token bumps to 2.
    let owned_b = bootstrap(
        &cfg("transformer-b", Duration::from_secs(30)),
        &pool,
        &store(),
        &s3(),
        &TransformerState::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        owned_b.tables[0].fencing_token, 2,
        "reclaim by a new owner bumps the fencing token"
    );
    assert!(table_exists(&owned_b.tables[0].db, "orders"));
}
