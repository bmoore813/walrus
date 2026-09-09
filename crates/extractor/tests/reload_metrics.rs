#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Reload observability against compose (`#[ignore]` — source + control PG + MinIO). Proves the
//! reload metrics move during a reload: chunk/row counters and the F/H echo-wait histogram tick as
//! an export runs; a missing decoder makes F establishment observably time out; the active gauge
//! rises to 1 while an exporter is in flight and returns to 0 when it ends; and the cross-check
//! violation counter stays 0 on a healthy run. Named registration is covered by
//! `metrics_scrape.rs`; this is the movement proof.
//!
//!   cargo test -p extractor --test reload_metrics -- --ignored --test-threads=1

use extractor::reload::{ReloadController, ReloadControllerConfig};
use extractor::reload_event::FenceWaiters;
use extractor::reload_export::{ChunkExportConfig, ChunkExporter};
use extractor::staging::ParquetStager;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[path = "support/reload_fence.rs"]
mod reload_fence_support;

use common::EpochNo;
use control::reload::{self, ReloadFlavor, ReloadStatus};

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");
const TABLE: &str = "_walrus_met_orders";

#[track_caller]
fn assert_approx_eq(got: f64, want: f64) {
    const EPSILON: f64 = 1e-9;
    assert!(
        (got - want).abs() < EPSILON,
        "{got} != {want} (absolute tolerance {EPSILON})"
    );
}

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn admin() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn minio(epoch: EpochNo) -> ParquetStager {
    ParquetStager::new(
        Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("walrus")
                .with_region("us-east-1")
                .with_endpoint("http://localhost:9000")
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .with_allow_http(true)
                .build()
                .unwrap(),
        ),
        "walrus",
        epoch,
    )
}

async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: EpochNo, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    let rel = extractor::source_catalog::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: common::SchemaVersionNo(1),
            descriptors: pg_to_arrow::describe_relation(&rel).unwrap(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn scrub(pool: &sqlx::PgPool, epoch: EpochNo) {
    for tbl in ["file_manifest", "table_reload", "schema_registry"] {
        let sql = cleanup_sql(tbl);
        sqlx::query(&sql).bind(epoch).execute(pool).await.unwrap();
    }
}

fn cleanup_sql(table: &str) -> String {
    if table == "file_manifest" {
        format!(
            "WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM public.walrus_{table} WHERE epoch = $1 AND (SELECT protocol = '2' FROM authorized)"
        )
    } else if table == "table_reload" {
        format!(
            "WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol) DELETE FROM public.walrus_{table} WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)"
        )
    } else if table == "schema_registry" {
        format!(
            "WITH authorized AS MATERIALIZED (SELECT set_config('walrus.schema_registry_maintenance','1-delete',true) AS protocol) DELETE FROM public.walrus_{table} WHERE epoch = $1 AND (SELECT protocol = '1-delete' FROM authorized)"
        )
    } else {
        format!("DELETE FROM public.walrus_{table} WHERE epoch = $1")
    }
}

fn export_cfg(epoch: EpochNo, chunk_rows: u64, echo_timeout: Duration) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows: std::num::NonZeroU64::new(chunk_rows).unwrap(),
        router_batch_bytes: std::num::NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        worker_admission: extractor::reload_export::ReloadWorkerAdmission::new(
            std::num::NonZeroUsize::new(4).unwrap(),
        ),
        // Metrics assertions count files exactly; parallel worker-tail behavior is covered by the
        // reload export/recovery suites rather than folded into this focused counter test.
        workers_per_table: std::num::NonZeroUsize::new(1).unwrap(),
        echo_timeout,
        instance: "walrus-extractor-test".to_string(),
        epoch,
        publication_name: "walrus_pub".to_string(),
    }
}

async fn request_and_claim(pool: &sqlx::PgPool, epoch: EpochNo) -> control::ReloadRow {
    reload::request(pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(pool, epoch, "walrus-extractor-test", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

/// Sum a metric across all label sets (unlabelled or per-{table}/{flavor}); the current value for
/// a gauge, the total for a counter, the `_count` for a histogram (pass the `_count` name).
fn metric_sum(name: &str) -> f64 {
    let mut total = 0.0;
    for line in common::metrics::render().lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name)
            && (rest.starts_with(' ') || rest.starts_with('{'))
            && let Some(v) = rest.split_whitespace().last()
        {
            total += v.parse::<f64>().unwrap_or(0.0);
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn chunk_export_moves_chunk_row_and_echo_metrics() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = EpochNo(700_001);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 5).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_met_export",
        pool.clone(),
        Arc::clone(&waiters),
        None,
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let resolver = resolver.handle;

    let chunks_before = metric_sum(common::metrics::names::RELOAD_CHUNKS_TOTAL);
    let rows_before = metric_sum(common::metrics::names::RELOAD_ROWS_EXPORTED_TOTAL);
    let echo_before = metric_sum("walrus_reload_echo_wait_seconds_count");
    let crosscheck_before = metric_sum(common::metrics::names::RELOAD_CROSSCHECK_VIOLATIONS);

    // One worker exports 5 rows at chunk_rows=2 ⇒ 3 files (2+2+1), bracketed by F/H echoes.
    let req = request_and_claim(&pool, epoch).await;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        minio(epoch),
        export_cfg(epoch, 2, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    exporter.run(false).await.unwrap();

    assert_approx_eq(
        metric_sum(common::metrics::names::RELOAD_CHUNKS_TOTAL) - chunks_before,
        3.0,
    );
    assert_approx_eq(
        metric_sum(common::metrics::names::RELOAD_ROWS_EXPORTED_TOTAL) - rows_before,
        5.0,
    );
    assert!(
        metric_sum("walrus_reload_echo_wait_seconds_count") - echo_before >= 2.0,
        "the echo-wait histogram observed the start and end fences"
    );
    assert_approx_eq(
        metric_sum(common::metrics::names::RELOAD_CROSSCHECK_VIOLATIONS) - crosscheck_before,
        0.0,
    );

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn start_fence_timeout_is_observable_and_resumable() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = EpochNo(700_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    // NO resolver: establishing F times out before any read/file and leaves the leased row
    // resumable. The controller owns terminal classification; a raw exporter reports the error.
    let waiters = Arc::new(FenceWaiters::default());
    let req = request_and_claim(&pool, epoch).await;
    let err = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters,
        minio(epoch),
        export_cfg(epoch, 1000, Duration::from_millis(300)),
        &req,
    )
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("decode echo timed out"));
    assert_eq!(
        reload::get(&pool, req.reload_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ReloadStatus::Exporting
    );

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn active_gauge_rises_and_returns_to_zero() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = EpochNo(700_003);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    // The controller spawns an exporter that PARKS on the echo await (no resolver) — long enough to
    // observe active=1; cancelling the token ends it and drops the gauge back to 0.
    let token = CancellationToken::new();
    let waiters = Arc::new(FenceWaiters::default());
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        waiters,
        minio(epoch),
        ReloadControllerConfig {
            poll_interval: Duration::from_millis(200),
            max_concurrent_reloads: std::num::NonZeroUsize::new(2).unwrap(),
            workers_per_table: std::num::NonZeroUsize::new(4).unwrap(),
            router_batch_bytes: std::num::NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            worker_admission: extractor::reload_export::ReloadWorkerAdmission::new(
                std::num::NonZeroUsize::new(8).unwrap(),
            ),
            lease_ttl: Duration::from_secs(6),
            instance: "walrus-extractor-test".to_string(),
            publication_name: "walrus_pub".to_string(),
            epoch,
            chunk_rows: std::num::NonZeroU64::new(1000).unwrap(),
            echo_timeout: Duration::from_secs(3600), // park forever, no resolver
            reload_max_restarts: 3,
        },
        token.clone(),
    );
    reload::request(&pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();

    // active{flavor="reload"} rises to 1 within a few poll cadences.
    let rose = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metric_sum(common::metrics::names::RELOAD_ACTIVE) >= 1.0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(rose.is_ok(), "reload_active never reached 1");

    // Cancel: the parked exporter ends (Cancelled) and decrements the gauge back to 0. The gauge
    // is a balanced sum of ±1.0 steps seeded at 0.0, so every value it can hold is exact and the
    // `== 0.0` below is a deliberate bit-exact drain check — a tolerance would swallow an
    // unbalanced decrement, which is exactly the bug this waits to rule out.
    token.cancel();
    handle.await.unwrap();
    let fell = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metric_sum(common::metrics::names::RELOAD_ACTIVE) == 0.0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(fell.is_ok(), "reload_active never returned to 0");

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
