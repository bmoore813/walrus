#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]

//! Phase B against compose (`#[ignore]` — needs control PG + MinIO). Append a seeded Parquet with
//! intra-batch PK churn, then transform: the mirror `<table>` ends equal to the **current** source
//! (one row per PK, latest values, deletes absent), `transformed_lsn` advances to `max(commit_lsn)`
//! (never past `raw_appended_lsn`), and re-running the transform is byte-identical.
//!
//!   cargo test -p transformer --test phase_b -- --ignored

mod support;

use common::{EpochNo, Kind, Lsn, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
use std::time::Duration;
use transformer::duck::{S3Access, TableDb};
use transformer::health::TransformerState;
use transformer::phase_a::{TableCtx, run_phase_a};
use transformer::phase_b::run_phase_b;

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn s3() -> S3Access {
    S3Access {
        endpoint: "localhost:9000".into(),
        region: "us-east-1".into(),
        access_key_id: "minioadmin".into(),
        secret_access_key: "minioadmin".into(),
        use_ssl: false,
    }
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

/// A scratch directory for one test's `.duckdb` file. The returned guard deletes it on drop — even
/// when an assertion panics, which a trailing `remove_dir_all` would skip.
fn tmpdir(name: &str) -> tempfile::TempDir {
    let prefix = format!("walrus-transformer-pb-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

fn meta(epoch: EpochNo, batch_id: &str, op: &str, l: u64) -> String {
    let lsn = Lsn::new(l).to_string();
    serde_json::to_string(&support::extractor_meta(
        epoch,
        batch_id,
        SchemaVersionNo(1),
        "public",
        "orders",
        Kind::Stream,
        op,
        "0/64",
        &lsn,
    ))
    .unwrap()
}

/// Fixture with intra-batch churn: key 1 (i→i final), key 2 (lone i), key 3 (i→d, deleted).
fn write_fixture(epoch: EpochNo) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
        a.region,
        a.endpoint,
        a.access_key_id,
        a.secret_access_key.expose()
    ))
    .unwrap();
    w.execute_batch(
        "CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR);",
    )
    .unwrap();
    let batch_id = format!("phase-b-{epoch}");
    for (id, status, op, l) in [
        (1, "v1", "i", 1u64),
        (1, "final1", "i", 2),
        (2, "keep2", "i", 3),
        (3, "temp3", "i", 4),
        (3, "temp3", "d", 5),
    ] {
        w.execute(
            "INSERT INTO fixture VALUES (?, ?, ?)",
            duckdb::params![id, status, meta(epoch, &batch_id, op, l)],
        )
        .unwrap();
    }
    let uri = format!("s3://walrus/{epoch}/public/orders/fixture-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn setup(epoch: EpochNo) -> (TableCtx, tempfile::TempDir) {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    support::cleanup_epoch(&pool, epoch).await;
    control::insert_epoch(
        &pool,
        epoch,
        "walrus_slot",
        "0/0".parse().unwrap(),
        control::ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap();
    let uri = write_fixture(epoch);
    let (object_size, sha256) = support::fingerprint(&uri).await;
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri,
            kind: control::ManifestKind::Stream,
            row_count: 5,
            object_size,
            sha256,
            lsn_start: "0/1".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: common::SchemaVersionNo(1),
            reload_id: None,
        },
    )
    .await
    .unwrap();
    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.configure_s3(&s3()).unwrap();
    let (owner_pod, fencing_token) = support::acquire_table(&pool, epoch, "public", "orders").await;
    let ctx = TableCtx {
        pool,
        epoch,
        epoch_rx: transformer::epoch::fixed_epoch_watch(epoch),
        owner_pod,
        fencing_token,
        store: support::store(),
        staging_bucket: "walrus".into(),
        schema: "public".into(),
        table: "orders".into(),
        series: "public.orders".into(),
        rel: orders(),
        db,
        state: TransformerState::new(),
        max_files: std::num::NonZeroI64::new(100).unwrap(),
        max_integrity_resnapshots: 1,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    };
    (ctx, dir)
}

fn mirror(ctx: &TableCtx) -> Vec<(i64, String)> {
    let mut stmt = ctx
        .db
        .conn()
        .prepare("SELECT id, status FROM orders ORDER BY id")
        .unwrap();
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    rows.map(Result::unwrap).collect()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn mirror_equals_current_source_after_transform() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_400_001);
    let (ctx, _dir) = setup(epoch).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror(&ctx),
        vec![(1, "final1".to_string()), (2, "keep2".to_string())],
        "mirror = current source: key 1 latest insert, key 2 kept, key 3 (i→d) absent"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn transformed_lsn_advances_to_max_applied_commit_lsn() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_400_002);
    let (ctx, _dir) = setup(epoch).await;

    run_phase_a(&ctx).await.unwrap();
    let applied = run_phase_b(&ctx).await.unwrap();
    assert_eq!(
        applied,
        Some("0/64".parse().unwrap()),
        "advanced to max(commit_lsn)"
    );

    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cp.transformed_lsn, "0/64".parse::<Lsn>().unwrap());
    assert!(
        cp.transformed_lsn <= cp.raw_appended_lsn,
        "the CHECK invariant holds"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn re_running_phase_b_is_idempotent() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_400_003);
    let (ctx, _dir) = setup(epoch).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    let first = mirror(&ctx);

    // Reset the watermark so the WHOLE tail is re-transformed — the LWW dedup must pick the same winners.
    sqlx::query(
        "UPDATE public.walrus_transformer_checkpoint SET transformed_lsn = '0/0' WHERE epoch = $1",
    )
    .bind(epoch)
    .execute(&ctx.pool)
    .await
    .unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(
        mirror(&ctx),
        first,
        "re-transforming the same tail is byte-identical"
    );
}
