#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]

//! Snapshot/stream boundary through the transform (transformer §7, architecture §1.7) — compose (`#[ignore]`).
//! The transformer has **no special snapshot mode**: `kind='snapshot'` files append into `<table>_raw` like
//! any `ready` file, and the transform collapses the overlap by `(commit_lsn, lsn)`. Two edges proven
//! here: (1) an overlapping stream change out-ranks the snapshot row, and (2) equal-`lsn_end` snapshot
//! files **split across transformer batches** are all applied — none skipped by the watermark. A `lsn_end`-only
//! watermark filter (`lsn_end > raw_appended_lsn`) would drop the equal-`lsn_end` snapshot files; the
//! claim is `ORDER BY lsn_end, id` + queue-delete, and Phase B's `>=` bound + the per-PK guard close the
//! boundary key.
//!
//!   cargo test -p transformer --test snapshot_boundary -- --ignored

mod support;

use common::{EpochNo, Kind, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
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
    let prefix = format!("walrus-transformer-snap-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

/// Write a single-row (id, status, walrus_extractor_meta) Parquet fixture to S3.
fn write_row(
    epoch: EpochNo,
    tag: &str,
    id: i64,
    status: &str,
    kind: Kind,
    op: &str,
    lsn: &str,
) -> String {
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
    let batch_id = format!("snapshot-boundary-{tag}-{epoch}");
    let meta = serde_json::to_string(&support::extractor_meta(
        epoch,
        &batch_id,
        SchemaVersionNo(1),
        "public",
        "orders",
        kind,
        op,
        lsn,
        lsn,
    ))
    .unwrap();
    w.execute(
        "INSERT INTO fixture VALUES (?, ?, ?)",
        duckdb::params![id, status, meta],
    )
    .unwrap();
    let uri = format!("s3://walrus/{epoch}/public/orders/{tag}-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn insert_file(pool: &sqlx::PgPool, epoch: EpochNo, uri: String, kind: &str, lsn_end: &str) {
    let (object_size, sha256) = support::fingerprint(&uri).await;
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri,
            kind: kind.parse::<control::ManifestKind>().unwrap(),
            row_count: 1,
            object_size,
            sha256,
            lsn_start: lsn_end.parse().unwrap(),
            lsn_end: lsn_end.parse().unwrap(),
            schema_version: common::SchemaVersionNo(1),
            reload_id: None,
        },
    )
    .await
    .unwrap();
}

async fn setup(epoch: EpochNo, max_files: i64) -> (TableCtx, tempfile::TempDir) {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    support::cleanup_epoch(&pool, epoch).await;
    control::insert_epoch(
        &pool,
        epoch,
        "walrus_slot",
        "0/64".parse().unwrap(), // consistent_point
        control::ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
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
        max_files: std::num::NonZeroI64::new(max_files).unwrap(),
        max_integrity_resnapshots: 1,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    };
    (ctx, dir)
}

fn mirror(ctx: &TableCtx) -> Vec<(i64, String)> {
    let conn = ctx.db.conn();
    let mut stmt = conn
        .prepare("SELECT id, status FROM orders ORDER BY id")
        .unwrap();
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    rows.map(Result::unwrap).collect()
}

/// A snapshot load then an OVERLAPPING stream change on the same PK → the mirror ends at the stream
/// value (`commit_lsn > consistent_point` out-ranks the snapshot row). One code path, zero dupes.
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn snapshot_then_overlapping_stream_yields_stream_value() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_105_001);
    let (ctx, _dir) = setup(epoch, 100).await;

    // Snapshot file (commit_lsn = consistent_point 0x64) then an overlapping stream update (0xC8).
    let snap = write_row(epoch, "snap", 1, "snap", Kind::Snapshot, "i", "0/64");
    insert_file(&ctx.pool, epoch, snap, "snapshot", "0/64").await;
    let stream = write_row(epoch, "stream", 1, "streamed", Kind::Stream, "u", "0/C8");
    insert_file(&ctx.pool, epoch, stream, "stream", "0/C8").await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror(&ctx),
        vec![(1, "streamed".to_string())],
        "the overlapping stream change wins; zero loss, zero dupes"
    );
}

/// Two equal-`lsn_end` snapshot files, one per transformer batch (`max_files=1`), must BOTH land — none
/// skipped by the watermark. The second file's `commit_lsn == transformed_lsn` after the first cycle;
/// only Phase B's `>=` bound + the per-PK guard apply it.
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn equal_lsn_end_snapshot_files_split_across_batches_all_applied() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_105_002);
    let (ctx, _dir) = setup(epoch, 1).await; // max_files=1 forces the split across batches

    // Two snapshot files at the SAME lsn_end (= consistent_point 0/64), distinct keys.
    let f1 = write_row(epoch, "snapA", 1, "A", Kind::Snapshot, "i", "0/64");
    insert_file(&ctx.pool, epoch, f1, "snapshot", "0/64").await;
    let f2 = write_row(epoch, "snapB", 2, "B", Kind::Snapshot, "i", "0/64");
    insert_file(&ctx.pool, epoch, f2, "snapshot", "0/64").await;

    // Cycle 1: claims + applies file 1 (key A), transformed_lsn reaches the consistent_point.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    // Cycle 2: claims + applies file 2 (key B) — commit_lsn == transformed_lsn; the `>=` bound keeps it.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror(&ctx),
        vec![(1, "A".to_string()), (2, "B".to_string())],
        "BOTH equal-lsn_end snapshot files applied — none skipped by the watermark"
    );
}
