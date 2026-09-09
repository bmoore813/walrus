#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Restart-on-DDL against compose (`#[ignore]` — needs source PG + control PG + MinIO). A schema
//! change queued while the consistent dump snapshot is open lands before the end fence and
//! invalidates the attempt: the exporter returns `SchemaChanged`, and the controller
//! fails-and-reissues in one transaction — the old row turns `failed`, its worker files are purged,
//! and a successor `exporting` at `restart_count+1` starts with zero file progress. The successor
//! then re-exports a fresh shared snapshot at the NEW schema.
//! Past `reload_max_restarts` the reload fails outright with the cap named and no successor. Both
//! paths bump their metric.
//!
//! Every attempt is single-schema by construction (H9), so the transformer never reconciles a version
//! change inside a rebuild — only in the stream, where that logic already runs. The pure staleness
//! and cap predicates are unit-tested in `src/reload_export.rs` / `src/reload.rs`; this is the
//! end-to-end transaction and metric proof.
//!
//!   cargo test -p extractor --test reload_ddl -- --ignored --test-threads=1

use bytes::Bytes;
use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use extractor::reload::{RestartDecision, handle_ddl_restart};
use extractor::reload_event::FenceWaiters;
use extractor::reload_export::{ChunkExportConfig, ChunkExporter, RunOutcome};
use extractor::staging::ParquetStager;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[path = "support/reload_fence.rs"]
mod reload_fence_support;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");
const TABLE: &str = "_walrus_ddl_orders";

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

fn raw_store() -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("walrus")
        .with_region("us-east-1")
        .with_endpoint("http://localhost:9000")
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_allow_http(true)
        .build()
        .unwrap()
}

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(raw_store())
}

fn delayed_store(delay: Duration) -> Arc<dyn ObjectStore> {
    Arc::new(object_store::throttle::ThrottledStore::new(
        raw_store(),
        object_store::throttle::ThrottleConfig {
            wait_put_per_call: delay,
            ..object_store::throttle::ThrottleConfig::default()
        },
    ))
}

/// Seed the target table (a plain 2-column table) with `n` rows and register its shape at v1.
async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: EpochNo, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    register(admin, pool, epoch, SchemaVersionNo(1)).await;
}

/// (Re)register the table's CURRENT source shape at `version` — the extractor's decode loop does this on
/// a Relation message; here the test does it directly to simulate DDL bumping the structural version.
async fn register(
    admin: &tokio_postgres::Client,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    version: SchemaVersionNo,
) {
    let rel = extractor::source_catalog::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: version,
            descriptors: pg_to_arrow::describe_relation(&rel).unwrap(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

/// Queue an AccessExclusive schema change behind the export snapshot's AccessShare lock. Once the
/// exporter drains and commits that snapshot, PostgreSQL grants this older waiter before the H
/// fence; the task then publishes the simulated decoded registry bump.
async fn queue_priority_ddl(
    observer: &tokio_postgres::Client,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> tokio::task::JoinHandle<()> {
    let pool = pool.clone();
    let task = tokio::spawn(async move {
        let ddl = admin().await;
        ddl.batch_execute(&format!(
            "ALTER TABLE public.{TABLE} ADD COLUMN priority int"
        ))
        .await
        .unwrap();
        register(&ddl, &pool, epoch, SchemaVersionNo(2)).await;
    });

    let relation = format!("public.{TABLE}");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let queued: bool = observer
                .query_one(
                    "SELECT EXISTS (
                       SELECT 1 FROM pg_locks
                       WHERE relation = to_regclass($1)
                         AND mode = 'AccessExclusiveLock'
                         AND NOT granted
                     )",
                    &[&relation],
                )
                .await
                .unwrap()
                .get(0);
            if queued {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("schema DDL queues behind the consistent dump snapshot");
    task
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

fn export_cfg(epoch: EpochNo, chunk_rows: u64) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows: std::num::NonZeroU64::new(chunk_rows).unwrap(),
        router_batch_bytes: std::num::NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        worker_admission: extractor::reload_export::ReloadWorkerAdmission::new(
            std::num::NonZeroUsize::new(4).unwrap(),
        ),
        workers_per_table: std::num::NonZeroUsize::new(4).unwrap(),
        echo_timeout: Duration::from_secs(20),
        instance: "walrus-extractor-test".to_string(),
        epoch,
        publication_name: "walrus_pub".to_string(),
    }
}

async fn request_and_claim(pool: &sqlx::PgPool, epoch: EpochNo) -> control::ReloadRow {
    control::reload::request(
        pool,
        epoch,
        "public",
        TABLE,
        control::reload::ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    control::reload::claim_requested(pool, epoch, "walrus-extractor-test", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn reload_rows(pool: &sqlx::PgPool, epoch: EpochNo) -> Vec<control::ReloadRow> {
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT reload_id FROM public.walrus_table_reload
         WHERE epoch = $1 AND source_table = $2 ORDER BY reload_id",
    )
    .bind(epoch)
    .bind(TABLE)
    .fetch_all(pool)
    .await
    .unwrap();
    let mut out = Vec::new();
    for id in ids {
        out.push(
            control::reload::get(pool, ReloadId(id))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    out
}

async fn manifest_count(pool: &sqlx::PgPool, epoch: EpochNo, reload_id: ReloadId) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE epoch = $1 AND reload_id = $2",
    )
    .bind(epoch)
    .bind(reload_id.0)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn wait_for_file_progress(pool: &sqlx::PgPool, reload_id: ReloadId) -> control::ReloadRow {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = control::reload::get(pool, reload_id)
                .await
                .unwrap()
                .expect("reload remains present while its exporter runs");
            if row.chunk_no > 0 {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("parallel reload records its first completed file")
}

/// The (uri, schema_version) of a reload's chunk files, in claim order.
async fn reload_files(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    reload_id: ReloadId,
) -> Vec<(String, SchemaVersionNo)> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT s3_uri, schema_version FROM public.walrus_file_manifest
         WHERE epoch = $1 AND reload_id = $2 ORDER BY lsn_end, id",
    )
    .bind(epoch)
    .bind(reload_id.0)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(uri, version)| (uri, SchemaVersionNo(version)))
        .collect()
}

/// The Arrow column names of a chunk Parquet file — proves which shape it was exported at.
async fn chunk_columns(uri: &str) -> Vec<String> {
    let key = uri.strip_prefix("s3://walrus/").unwrap();
    let bytes: Bytes = store()
        .get(&Path::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// A counter's total from the Prometheus exposition (0 if absent) — summed across all label sets,
/// so it works for both unlabelled (`walrus_reload_restart_cap_exhausted_total`) and per-table
/// (`walrus_reload_restarts_total{table="…"}`) counters.
fn counter_value(name: &str) -> f64 {
    let mut total = 0.0;
    for line in common::metrics::render().lines() {
        if line.starts_with('#') {
            continue;
        }
        // The metric name must end exactly here — the next char is a space (no labels) or `{`.
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
async fn mid_export_ddl_restarts_fresh_attempt_at_new_schema() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = EpochNo(680_001);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 1000).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_ddl_restart",
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

    // Run the shared v1 snapshot in the background. The first durable worker file proves COPY is
    // underway; delayed remote writes keep the coordinator's ACCESS SHARE lock open while the DDL
    // queues for ACCESS EXCLUSIVE. It will land after S commits and before H wins a new lock.
    let req = request_and_claim(&pool, epoch).await;
    let old_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(delayed_store(Duration::from_millis(50)), "walrus", epoch),
        export_cfg(epoch, 50),
        &req,
    )
    .await
    .unwrap();
    let export = tokio::spawn(async move { exporter.run(false).await });
    let progress = wait_for_file_progress(&pool, old_id).await;
    assert_eq!(progress.cursor_pk, None);
    assert!(!export.is_finished(), "DDL is queued while S remains open");
    let ddl = queue_priority_ddl(&admin, &pool, epoch).await;

    // The old snapshot stays internally consistent. Its queued DDL wins before H, whose live-shape
    // check returns SchemaChanged instead of publishing the obsolete baseline.
    let restarts_before = counter_value(common::metrics::names::RELOAD_RESTARTS_TOTAL);
    let outcome = export.await.unwrap().unwrap();
    ddl.await.unwrap();
    assert_eq!(
        outcome,
        RunOutcome::SchemaChanged {
            new_version: SchemaVersionNo(2)
        }
    );

    // The controller fails-and-reissues in one transaction.
    let old_after_export = control::reload::get(&pool, old_id).await.unwrap().unwrap();
    let decision = handle_ddl_restart(&pool, &old_after_export, SchemaVersionNo(2), 3)
        .await
        .unwrap();
    let new_id = match decision {
        RestartDecision::Restarted(id) => id,
        RestartDecision::Capped => panic!("cap of 3 must not be reached on the first DDL"),
    };
    assert_approx_eq(
        counter_value(common::metrics::names::RELOAD_RESTARTS_TOTAL) - restarts_before,
        1.0,
    );

    // Old: failed + superseded reason + zero files. New: exporting, restart_count 1, fresh.
    let old = control::reload::get(&pool, old_id).await.unwrap().unwrap();
    assert_eq!(old.status, control::reload::ReloadStatus::Failed);
    assert!(
        old.error
            .as_deref()
            .unwrap_or_default()
            .contains("superseded"),
        "the old attempt names the supersession: {:?}",
        old.error
    );
    assert_eq!(
        manifest_count(&pool, epoch, old_id).await,
        0,
        "the old attempt's worker files are purged (fail()'s coupling)"
    );
    let new = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(new.status, control::reload::ReloadStatus::Exporting);
    assert_eq!(new.restart_count, 1);
    assert_eq!(
        new.chunk_no, 0,
        "successor starts with zero completed files"
    );
    assert_eq!(new.cursor_pk, None);
    assert_eq!(
        new.schema_version, None,
        "re-freezes at the new version before COPY"
    );
    assert_eq!(
        new.lease_holder.as_deref(),
        Some("walrus-extractor-test"),
        "lease carried"
    );

    // The successor re-exports from zero at the NEW schema and drains.
    let mut resumed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 50),
        &new,
    )
    .await
    .unwrap();
    assert!(matches!(
        resumed.run(false).await.unwrap(),
        RunOutcome::Drained { .. }
    ));

    let done = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(
        done.schema_version,
        Some(SchemaVersionNo(2)),
        "the attempt froze at v2"
    );
    let files = reload_files(&pool, epoch, new_id).await;
    assert!(
        (20..=23).contains(&files.len()),
        "1000 rows / 50 per object plus at most three worker tails: {} files",
        files.len()
    );
    assert!(
        files.iter().all(|f| f.1 == SchemaVersionNo(2)),
        "every successor file stamped v2"
    );
    let cols = chunk_columns(&files[0].0).await;
    assert!(
        cols.iter().any(|c| c == "priority"),
        "the new column is present in the successor's worker files: {cols:?}"
    );

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn restart_cap_exhaustion_fails_loudly() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = EpochNo(680_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 1000).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_ddl_cap",
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

    let req = request_and_claim(&pool, epoch).await;
    let old_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(delayed_store(Duration::from_millis(50)), "walrus", epoch),
        export_cfg(epoch, 50),
        &req,
    )
    .await
    .unwrap();
    let export = tokio::spawn(async move { exporter.run(false).await });
    let progress = wait_for_file_progress(&pool, old_id).await;
    assert_eq!(progress.cursor_pk, None);
    assert!(!export.is_finished(), "DDL is queued while S remains open");
    let ddl = queue_priority_ddl(&admin, &pool, epoch).await;
    assert_eq!(
        export.await.unwrap().unwrap(),
        RunOutcome::SchemaChanged {
            new_version: SchemaVersionNo(2)
        }
    );
    ddl.await.unwrap();

    // Cap 0: the first mid-export DDL fails the reload outright — no successor.
    let cap_before = counter_value(common::metrics::names::RELOAD_RESTART_CAP_EXHAUSTED_TOTAL);
    let old_after_export = control::reload::get(&pool, old_id).await.unwrap().unwrap();
    let decision = handle_ddl_restart(&pool, &old_after_export, SchemaVersionNo(2), 0)
        .await
        .unwrap();
    assert!(
        matches!(decision, RestartDecision::Capped),
        "cap 0 caps the first DDL"
    );
    assert_approx_eq(
        counter_value(common::metrics::names::RELOAD_RESTART_CAP_EXHAUSTED_TOTAL) - cap_before,
        1.0,
    );

    let rows = reload_rows(&pool, epoch).await;
    assert_eq!(rows.len(), 1, "no successor row was written");
    assert_eq!(rows[0].reload_id, old_id);
    assert_eq!(rows[0].status, control::reload::ReloadStatus::Failed);
    let reason = rows[0].error.clone().unwrap_or_default();
    assert!(
        reason.contains("restart cap 0 exhausted"),
        "the failure names the cap: {reason}"
    );
    assert_eq!(
        manifest_count(&pool, epoch, old_id).await,
        0,
        "the failed reload's chunk files are purged"
    );

    // Sanity: the frozen F recorded before COPY is a real LSN (the attempt did run).
    let _f: Lsn = rows[0].first_lsn.expect("the parallel attempt froze F");

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
