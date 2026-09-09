#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Completion & crash recovery against compose (`#[ignore]` — needs source PG + control PG +
//! MinIO). Four proofs cover reload H7/H10:
//!
//! - A "crashed" export (exporter dropped mid-flight) is ADOPTED from control-pg — not WAL
//!   redelivery — then its lost-snapshot predecessor is failed/purged and a fresh fenced successor
//!   re-exports from zero completed files. The transformer (simulated by advancing the checkpoint) flips
//!   `complete` once `transformed_lsn ≥ H`.
//! - A crash after durable H but before `complete_export(H)` adopts the exact F/H boundary and
//!   never scans a source row committed after H into an F-stamped baseline chunk.
//! - `complete` waits for `transformed_lsn ≥ H` both ways: it holds at `export_complete` while the
//!   mirror is behind H, and flips once it catches up. The TRANSFORMER owns the flip.
//! - Adoption is lease-aware: a live FOREIGN lease is never stolen; an EXPIRED one is taken.
//!
//!   cargo test -p extractor --test reload_recovery -- --ignored --test-threads=1

use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use extractor::reload::{RestartDecision, handle_lost_snapshot_restart};
use extractor::reload_event::FenceWaiters;
use extractor::reload_export::{ChunkExportConfig, ChunkExporter, RunOutcome};
use extractor::staging::ParquetStager;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[path = "support/reload_fence.rs"]
mod reload_fence_support;

use control::reload::{ReloadFlavor, ReloadStatus};

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");
const TABLE: &str = "_walrus_rec_orders";
const HOLDER: &str = "walrus-extractor-test";

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

fn store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(raw_store())
}

fn delayed_store(delay: Duration) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(object_store::throttle::ThrottledStore::new(
        raw_store(),
        object_store::throttle::ThrottleConfig {
            wait_put_per_call: delay,
            ..object_store::throttle::ThrottleConfig::default()
        },
    ))
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
            schema_version: SchemaVersionNo(1),
            descriptors: pg_to_arrow::describe_relation(&rel).unwrap(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn scrub(pool: &sqlx::PgPool, epoch: EpochNo) {
    for tbl in [
        "file_manifest",
        "table_reload",
        "schema_registry",
        "transformer_checkpoint",
    ] {
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
        instance: HOLDER.to_string(),
        epoch,
        publication_name: "walrus_pub".to_string(),
    }
}

async fn request_and_claim(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    holder: &str,
) -> control::ReloadRow {
    control::reload::request(pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();
    control::reload::claim_requested(pool, epoch, holder, 3600, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn reload_file_count(pool: &sqlx::PgPool, epoch: EpochNo, reload_id: ReloadId) -> i64 {
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

/// Simulate the transformer reaching a watermark: seed the checkpoint and advance both frontiers (the
/// `raw >= transformed` CHECK needs raw first). Never applies a file — this exercises the
/// completion PREDICATE, not the mirror content (that is the transformer's own suites).
async fn set_transformed(pool: &sqlx::PgPool, epoch: EpochNo, lsn: Lsn) {
    control::ensure_checkpoint(pool, epoch, "public", TABLE)
        .await
        .unwrap();
    control::advance_raw_appended(pool, epoch, "public", TABLE, lsn)
        .await
        .unwrap();
    control::advance_transformed(pool, epoch, "public", TABLE, lsn)
        .await
        .unwrap();
}

async fn status(pool: &sqlx::PgPool, reload_id: ReloadId) -> ReloadStatus {
    control::reload::get(pool, reload_id)
        .await
        .unwrap()
        .unwrap()
        .status
}

async fn wait_for_adoption(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    holder: &str,
) -> control::ReloadRow {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut adopted = control::reload::adopt_resumable(pool, epoch, holder, 60, 5, true)
                .await
                .unwrap();
            match adopted.len() {
                0 => tokio::time::sleep(Duration::from_millis(10)).await,
                count => {
                    assert_eq!(count, 1, "expected exactly one test-owned reload");
                    return adopted.pop().unwrap();
                }
            }
        }
    })
    .await
    .expect("aborted export workers release their control-row locks for adoption")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn kill_mid_export_starts_fresh_successor_and_reaches_export_complete() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_001);
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
        "walrus_rec_resume",
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

    // requested → exporting, at least one independently durable worker file, then "crash": abort
    // the task while the delayed remote stager keeps the shared snapshot genuinely in flight.
    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::Exporting);
    let mut crashed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(delayed_store(Duration::from_millis(50)), "walrus", epoch),
        export_cfg(epoch, 50),
        &req,
    )
    .await
    .unwrap();
    let crashed = tokio::spawn(async move { crashed.run(false).await });
    let progress = wait_for_file_progress(&pool, reload_id).await;
    assert!(
        !crashed.is_finished(),
        "the simulated crash is genuinely mid-export"
    );
    crashed.abort();
    assert!(crashed.await.unwrap_err().is_cancelled());
    let mid = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert!(mid.chunk_no >= progress.chunk_no && mid.chunk_no > 0);
    assert_eq!(
        mid.cursor_pk, None,
        "physical file progress has no PK cursor"
    );

    // "Restart": adoption cannot resurrect the connection-local snapshot, so durable file
    // progress is evidence to purge rather than a safe resume point.
    // Dropping the parent aborts its JoinSet children, but Tokio cancellation is asynchronous. A
    // child may briefly retain the reload-row lock, which the production SKIP LOCKED scan is
    // supposed to skip until the controller's next tick. Poll that same scan rather than turning
    // the intentional transient skip into a flaky assertion.
    let row = wait_for_adoption(&pool, epoch, HOLDER).await;
    assert_eq!(row.reload_id, reload_id);
    assert!(
        row.chunk_no >= mid.chunk_no && row.chunk_no > 0,
        "adoption observes every file durable before the crash task stopped"
    );
    assert_eq!(
        reload_file_count(&pool, epoch, reload_id).await,
        row.chunk_no,
        "the completed-file counter and manifest advance atomically"
    );
    assert_eq!(row.cursor_pk, None);

    let mut adopted_exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 2),
        &row,
    )
    .await
    .unwrap();
    assert_eq!(
        adopted_exporter.run(true).await.unwrap(),
        RunOutcome::SnapshotLost,
        "an adopted attempt without H never issues another source-table SELECT"
    );
    let new_id = match handle_lost_snapshot_restart(&pool, &row, 5).await.unwrap() {
        RestartDecision::Restarted(id) => id,
        RestartDecision::Capped => panic!("restart cap must have room for the first recovery"),
    };
    let failed = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ReloadStatus::Failed);
    assert_eq!(
        reload_file_count(&pool, epoch, reload_id).await,
        0,
        "the lost-snapshot predecessor's files are purged"
    );

    let successor = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(successor.chunk_no, 0);
    assert_eq!(successor.cursor_pk, None);
    assert_eq!(successor.start_lsn, None);
    assert_eq!(successor.schema_version, None);
    let mut restarted = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 50),
        &successor,
    )
    .await
    .unwrap();
    let h = match restarted.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected drain, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("a fresh successor owns its source snapshot"),
    };

    // The extractor's last act applies to the fresh successor, never the snapshot-lost predecessor.
    let successor_lease = successor.exporter_lease(HOLDER).unwrap();
    control::reload::complete_export(&pool, &successor_lease, h)
        .await
        .unwrap();
    let done = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(done.status, ReloadStatus::ExportComplete);
    assert_eq!(done.final_lsn, Some(h), "H recorded");
    let successor_files = reload_file_count(&pool, epoch, new_id).await;
    assert!(
        (20..=23).contains(&successor_files),
        "1000 rows / 50 plus at most three parallel worker tails: {successor_files} files"
    );
    assert_eq!(done.chunk_no, successor_files);
    assert!(
        matches!((done.start_lsn, row.start_lsn), (Some(new_f), Some(old_f)) if new_f > old_f),
        "the successor establishes a fresh F instead of reusing the lost snapshot's fence"
    );

    // The TRANSFORMER flips complete only once transformed_lsn ≥ H. Behind H: it holds.
    set_transformed(&pool, epoch, "0/1".parse().unwrap()).await;
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty(),
        "behind H, the reload stays export_complete"
    );
    assert_eq!(status(&pool, new_id).await, ReloadStatus::ExportComplete);

    // Even at H, the retired checkpoint-only API cannot bypass protocol-v2 publication. The real
    // transformer must build and publish the hidden Duck generation before it can flip complete.
    set_transformed(&pool, epoch, h).await;
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(status(&pool, new_id).await, ReloadStatus::ExportComplete);

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
async fn crash_after_h_before_complete_does_not_export_post_h_rows() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_004);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_rec_after_h",
        pool.clone(),
        Arc::clone(&waiters),
        Some(TABLE),
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let mut watched_commit = resolver.watched_commit;
    let resolver = resolver.handle;

    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    let mut crashed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 10),
        &req,
    )
    .await
    .unwrap();
    let h = match crashed.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected drain, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("a freshly claimed exporter owns its snapshot"),
    };
    let before = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.status, ReloadStatus::Exporting);
    let files_before = reload_file_count(&pool, epoch, reload_id).await;
    assert!(files_before > 0);
    assert_eq!(before.chunk_no, files_before);
    assert_eq!(
        control::reload::read_markers(&pool, reload_id)
            .await
            .unwrap()
            .last()
            .map(|marker| marker.lsn),
        Some(h),
        "H is durable before the simulated crash"
    );
    drop(crashed); // crash after durable H, before complete_export(H)

    // This commit is strictly after H. A buggy adopter that starts another source snapshot would
    // export id=4 as an F-stamped baseline row and contaminate the already fenced H rebuild.
    admin
        .execute(
            &format!("INSERT INTO public.{TABLE} (id, val) VALUES (4, 'after-h')"),
            &[],
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), watched_commit.changed())
        .await
        .expect("post-H table commit reaches the resolver")
        .expect("resolver remains live");
    let post_h_commit =
        (*watched_commit.borrow_and_update()).expect("the watched insert has a commit LSN");
    assert!(
        post_h_commit > h,
        "test mutation must commit strictly after H"
    );

    let mut adopted = control::reload::adopt_resumable(&pool, epoch, HOLDER, 60, 5, true)
        .await
        .unwrap();
    assert_eq!(adopted.len(), 1);
    let row = adopted.pop().unwrap();
    assert_eq!(row.reload_id, reload_id);
    let mut recovered = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 10),
        &row,
    )
    .await
    .unwrap();
    let recovered_h = match recovered.run(true).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected H recovery, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("durable H must recover before snapshot ownership"),
    };
    assert_eq!(recovered_h, h, "the adopter finishes from the original H");

    let after = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.chunk_no, before.chunk_no,
        "no post-H COPY or file commit"
    );
    assert_eq!(
        after.cursor_pk, None,
        "parallel reloads never create a resume cursor"
    );
    assert_eq!(
        reload_file_count(&pool, epoch, reload_id).await,
        files_before,
        "the post-H row was not copied into an F-stamped reload file"
    );
    let recovered_lease = after.exporter_lease(HOLDER).unwrap();
    control::reload::complete_export(&pool, &recovered_lease, recovered_h)
        .await
        .unwrap();
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::ExportComplete);

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
async fn legacy_checkpoint_completion_cannot_bypass_v2_publication() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_rec_wait",
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

    // A clean full export to export_complete(H).
    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 10),
        &req,
    )
    .await
    .unwrap();
    let h = match exporter.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected drain, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("a freshly claimed exporter owns its snapshot"),
    };
    let lease = req.exporter_lease(HOLDER).unwrap();
    control::reload::complete_export(&pool, &lease, h)
        .await
        .unwrap();

    // Frozen transformer (transformed_lsn = 0): complete does NOT fire.
    control::ensure_checkpoint(&pool, epoch, "public", TABLE)
        .await
        .unwrap();
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::ExportComplete);

    // Transformer catches up to H: generation>0 still cannot complete through the legacy shortcut.
    set_transformed(&pool, epoch, h).await;
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::ExportComplete);
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty(),
        "checkpoint-only completion remains a stable no-op for protocol-v2 attempts"
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
#[ignore = "requires docker compose up --wait (control PG)"]
async fn adoption_respects_live_leases_but_takes_expired_ones() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_003);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;

    // A reload held by ANOTHER live instance (fresh 1h lease).
    let req = request_and_claim(&pool, epoch, "walrus-extractor-other").await;
    let reload_id = req.reload_id;

    // We must not steal a live foreign lease.
    assert!(
        control::reload::adopt_resumable(&pool, epoch, "walrus-extractor-me", 60, 5, true)
            .await
            .unwrap()
            .is_empty(),
        "a live foreign lease is left alone"
    );
    assert_eq!(
        control::reload::get(&pool, reload_id)
            .await
            .unwrap()
            .unwrap()
            .lease_holder
            .as_deref(),
        Some("walrus-extractor-other"),
        "foreign holder untouched"
    );

    // Expire it (a dead instance): now it is adoptable, and the guarded UPDATE re-acquires it.
    sqlx::query("UPDATE public.walrus_table_reload SET lease_expiry = now() - interval '1 hour' WHERE reload_id = $1")
        .bind(reload_id.0)
        .execute(&pool)
        .await
        .unwrap();
    let adopted =
        control::reload::adopt_resumable(&pool, epoch, "walrus-extractor-me", 60, 5, true)
            .await
            .unwrap();
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].reload_id, reload_id);
    assert_eq!(
        control::reload::get(&pool, reload_id)
            .await
            .unwrap()
            .unwrap()
            .lease_holder
            .as_deref(),
        Some("walrus-extractor-me"),
        "the expired lease was re-acquired by the adopter"
    );

    scrub(&pool, epoch).await;
}
