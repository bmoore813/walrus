#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]

//! Phase A against compose (`#[ignore]` — needs control PG + MinIO). A seeded `ready` Parquet is
//! claimed and appended **verbatim** to `<table>_raw` (meta intact + op/commit_lsn/lsn/extractor_processed_at
//! promoted), the watermark advances and the queue row is deleted in one control txn, and a replay of
//! the same file appends **zero** rows. The fixture Parquet is written by DuckDB itself.
//!
//!   cargo test -p transformer --test phase_a -- --ignored

mod support;

use common::{EpochNo, Kind, Lsn, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
use std::time::Duration;
use transformer::config::DuckLakeConfig;
use transformer::duck::{S3Access, TableDb};
use transformer::health::TransformerState;
use transformer::phase_a::{TableCtx, run_phase_a};

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

fn ducklake() -> DuckLakeConfig {
    DuckLakeConfig {
        catalog_url: catalog_url().into(),
        data_path: "s3://walrus/ducklake/tests/".to_string(),
        install_extensions: true,
        ..DuckLakeConfig::default()
    }
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
    let prefix = format!("walrus-transformer-pa-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

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
    let batch_id = format!("phase-a-{epoch}");
    for (id, status) in [(1, "a"), (2, "b")] {
        let meta = serde_json::to_string(&support::extractor_meta(
            epoch,
            &batch_id,
            SchemaVersionNo(1),
            "public",
            "orders",
            Kind::Stream,
            "i",
            "0/64",
            "0/64",
        ))
        .unwrap();
        w.execute(
            "INSERT INTO fixture VALUES (?, ?, ?)",
            duckdb::params![id, status, meta],
        )
        .unwrap();
    }
    let uri = format!("s3://walrus/{epoch}/public/orders/fixture-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn seed_manifest(pool: &sqlx::PgPool, epoch: EpochNo, uri: &str) {
    let (object_size, sha256) = support::fingerprint(uri).await;
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri.into(),
            kind: control::ManifestKind::Stream,
            row_count: 2,
            object_size,
            sha256,
            lsn_start: "0/64".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: common::SchemaVersionNo(1),
            reload_id: None,
        },
    )
    .await
    .unwrap();
}

/// Fresh control state + an owned `TableCtx` (DuckDB in a temp dir).
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
    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open_ducklake(&ducklake(), epoch, "public", "orders", &s3()).unwrap();
    db.wipe_generation("orders").unwrap();
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

fn raw_count(ctx: &TableCtx) -> i64 {
    ctx.db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn appends_rows_verbatim_with_promoted_columns_and_meta_intact() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_200_001);
    let uri = write_fixture(epoch);
    let (ctx, _dir) = setup(epoch).await;
    seed_manifest(&ctx.pool, epoch, &uri).await;

    let lsn = run_phase_a(&ctx).await.unwrap();
    assert_eq!(lsn, Some("0/64".parse().unwrap()));
    assert_eq!(raw_count(&ctx), 2, "both rows appended verbatim");

    let (op, meta, promoted_lsn): (String, String, String) = ctx
        .db
        .conn()
        .query_row(
            "SELECT _walrus_op, walrus_extractor_meta, _walrus_lsn FROM orders_raw WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(op, "i", "wire op promoted from the meta");
    assert!(
        meta.contains("\"op\":\"i\""),
        "walrus_extractor_meta kept intact"
    );
    assert_eq!(
        promoted_lsn, "0000000000000064",
        "lsn promoted (sortable 16-hex)"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn advances_raw_watermark_and_deletes_the_claimed_manifest_rows() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_200_002);
    let uri = write_fixture(epoch);
    let (ctx, _dir) = setup(epoch).await;
    seed_manifest(&ctx.pool, epoch, &uri).await;

    run_phase_a(&ctx).await.unwrap();

    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cp.raw_appended_lsn,
        "0/64".parse::<Lsn>().unwrap(),
        "watermark = max(lsn_end)"
    );
    let remaining = control::claim_ready(&ctx.pool, epoch, "public", "orders", 100)
        .await
        .unwrap();
    assert!(
        remaining.is_empty(),
        "claimed manifest rows deleted in the same control txn"
    );
    let mirror: i64 = ctx
        .db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mirror, 0, "Phase A never writes the mirror");
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn re_running_the_same_file_appends_zero_rows() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_200_003);
    let uri = write_fixture(epoch);
    let (ctx, _dir) = setup(epoch).await;

    seed_manifest(&ctx.pool, epoch, &uri).await;
    run_phase_a(&ctx).await.unwrap();
    assert_eq!(raw_count(&ctx), 2);

    seed_manifest(&ctx.pool, epoch, &uri).await;
    run_phase_a(&ctx).await.unwrap();
    assert_eq!(
        raw_count(&ctx),
        2,
        "the immutable object URI's ingest marker makes the second manifest a no-op"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn pause_withholds_claims_and_lifts_on_failed() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_200_004);
    let uri = write_fixture(epoch);
    let (ctx, _dir) = setup(epoch).await;
    seed_manifest(&ctx.pool, epoch, &uri).await;
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch)
    .execute(&ctx.pool)
    .await
    .unwrap();

    // A live reload: Phase A must treat the table as PAUSED, not idle.
    let reload_id = control::reload::request(
        &ctx.pool,
        epoch,
        "public",
        "orders",
        control::reload::ReloadFlavor::Reload,
    )
    .await
    .unwrap();

    // Paused: nothing claimed or appended, the backlog stays ready, the frontier is frozen (no
    // rewind, no CHECK violation — it simply does not move), and the once-per-pause latch holds
    // exactly this reload_id.
    assert_eq!(run_phase_a(&ctx).await.unwrap(), None);
    assert_eq!(raw_count(&ctx), 0, "nothing appended while paused");
    assert!(
        control::max_ready_lsn_end(&ctx.pool, epoch, "public", "orders")
            .await
            .unwrap()
            .is_some(),
        "the ready backlog accumulates (the lag gauge SHOULD grow by design)"
    );
    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cp.raw_appended_lsn, Lsn::ZERO, "frontier frozen at W");
    assert_eq!(
        ctx.pause_logged.get(),
        Some(reload_id),
        "the pause is latched (logged once)"
    );

    // A second poll changes nothing — same latch value means no re-log.
    assert_eq!(run_phase_a(&ctx).await.unwrap(), None);
    assert_eq!(ctx.pause_logged.get(), Some(reload_id));

    // `failed` lifts the pause: the backlog drains and the latch clears.
    control::reload::claim_requested(&ctx.pool, epoch, "extractor-t", 60, 10)
        .await
        .unwrap();
    let mut conn = ctx.pool.acquire().await.unwrap();
    control::reload::fail(&mut conn, reload_id, "demo")
        .await
        .unwrap();
    drop(conn);
    let lsn = run_phase_a(&ctx).await.unwrap();
    assert_eq!(lsn, Some("0/64".parse().unwrap()));
    assert_eq!(
        raw_count(&ctx),
        2,
        "the paused backlog drained after the lift"
    );
    assert_eq!(
        ctx.pause_logged.get(),
        None,
        "the latch clears when claiming resumes"
    );

    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch)
    .execute(&ctx.pool)
    .await
    .unwrap();
}
