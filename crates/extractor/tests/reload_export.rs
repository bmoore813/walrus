#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Parallel export engine against compose (`#[ignore]` — needs source PG + control PG + MinIO).
//! The remote files' union is the table exactly, every row is stamped `commit_lsn = lsn = F`, a
//! crashed shared snapshot is purged and replaced by a fresh fenced successor, a protocol-v2
//! transaction opened before F and committed after H remains a later WAL event, and a missing decoder
//! cannot let an exporter query before F is durable. Per-range tails may add files beyond
//! `ceil(total_rows/chunk_rows)`; `chunk_no` is the durable completed-file count, never a keyset cursor.
//! The SQL/stamp shapes are unit-tested in `src/reload_export.rs`.
//!
//! Each test spins a mini fence resolver: a real slot commit-gates `reload_event`, durably records
//! baseline `F` / terminal `H`, and only then resolves the shared `FenceWaiters`.
//!
//!   cargo test -p extractor --test reload_export -- --ignored --test-threads=1

use bytes::Bytes;
use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use extractor::reload::{RestartDecision, handle_lost_snapshot_restart};
use extractor::reload_event::FenceWaiters;
use extractor::reload_export::{ChunkExportConfig, ChunkExporter, RunOutcome};
use extractor::staging::ParquetStager;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeSet;
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
const TABLE: &str = "_walrus_re_orders";

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}

/// Give new source sessions deliberately incompatible text defaults. The exporter must override
/// these transaction-locally before any `::text` COPY projection reaches pg-to-arrow.
fn hostile_text_source_url() -> String {
    let base = source_url();
    let separator = if base.contains('?') { '&' } else { '?' };
    format!(
        "{base}{separator}options=-c%20DateStyle%3DSQL%2CDMY%20-c%20IntervalStyle%3Diso_8601\
         %20-c%20bytea_output%3Descape%20-c%20extra_float_digits%3D0\
         %20-c%20TimeZone%3DAmerica%2FNew_York"
    )
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn admin() -> tokio_postgres::Client {
    source_client().await
}

async fn source_client() -> tokio_postgres::Client {
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

/// Keep enough remote completions in flight to make the crash/mid-export observation deterministic.
fn delayed_store(delay: Duration) -> Arc<dyn ObjectStore> {
    Arc::new(object_store::throttle::ThrottledStore::new(
        raw_store(),
        object_store::throttle::ThrottleConfig {
            wait_put_per_call: delay,
            ..object_store::throttle::ThrottleConfig::default()
        },
    ))
}

/// Seed the target table with `n` rows and register its shape at the test epoch.
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

async fn seed_guc_sensitive_types(
    admin: &tokio_postgres::Client,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (
                 id int PRIMARY KEY,
                 d date NOT NULL,
                 ts timestamp NOT NULL,
                 tstz timestamptz NOT NULL,
                 span interval NOT NULL,
                 payload bytea NOT NULL,
                 measurement double precision NOT NULL
             );
             INSERT INTO public.{TABLE} VALUES (
                 1,
                 DATE '2024-02-03',
                 TIMESTAMP '2024-02-03 04:05:06.123456',
                 TIMESTAMPTZ '2024-02-03 04:05:06.123456+05:30',
                 INTERVAL '1 year 2 mons 3 days 04:05:06.5',
                 decode('00ff5c', 'hex'),
                 1.2345678901234567
             );"
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

/// Control-side hygiene for a test epoch (safe to run before and after).
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
        workers_per_table: std::num::NonZeroUsize::new(4).unwrap(),
        echo_timeout,
        instance: "walrus-extractor-test".to_string(),
        epoch,
        publication_name: "walrus_pub".to_string(),
    }
}

/// Claim the single requested reload and return its row.
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
    let mut claimed = control::reload::claim_requested(pool, epoch, "walrus-extractor-test", 60, 1)
        .await
        .unwrap();
    claimed.pop().unwrap()
}

/// Manifest rows for the test table's reload files, in claim order.
async fn reload_manifest_rows(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> Vec<(String, i64, String, ReloadId)> {
    let rows = sqlx::query_as::<_, (String, i64, String, i64)>(
        "SELECT s3_uri, row_count, lsn_end::text, reload_id
         FROM public.walrus_file_manifest
         WHERE epoch = $1 AND source_table = $2 AND kind = 'reload'
         ORDER BY lsn_end, id",
    )
    .bind(epoch)
    .bind(TABLE)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(uri, count, lsn, reload_id)| (uri, count, lsn, ReloadId(reload_id)))
        .collect()
}

/// Durable receipts for every physical range in one immutable snapshot plan.
async fn reload_export_range_receipts(
    pool: &sqlx::PgPool,
    reload_id: ReloadId,
) -> Vec<(i64, String, Option<i64>, Option<i64>)> {
    sqlx::query_as(
        "SELECT range_no, status, file_count, row_count
         FROM public.walrus_table_reload_export_range
         WHERE reload_id = $1
         ORDER BY range_no",
    )
    .bind(reload_id.0)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Check file accounting at the same range boundary where the exporter records crash-safe
/// completion. These fixtures are far below the byte/Parquet-metadata limits, so each range emits
/// exactly `ceil(range_rows/chunk_rows)` objects (zero for an empty range).
fn assert_completed_range_receipts(
    receipts: &[(i64, String, Option<i64>, Option<i64>)],
    manifest_count: usize,
    expected_rows: i64,
    chunk_rows: i64,
) {
    assert!(
        !receipts.is_empty(),
        "the snapshot has a durable range plan"
    );
    assert!(
        chunk_rows > 0,
        "the configured object row limit is positive"
    );
    let mut receipt_files = 0i64;
    let mut receipt_rows = 0i64;
    for (expected_range_no, (range_no, status, file_count, row_count)) in
        receipts.iter().enumerate()
    {
        assert_eq!(
            *range_no,
            i64::try_from(expected_range_no).unwrap(),
            "range receipts retain their contiguous plan ordinals"
        );
        assert_eq!(status, "complete", "every planned range is sealed");
        let file_count = file_count.expect("a completed range records its file count");
        let row_count = row_count.expect("a completed range records its row count");
        let expected_file_count = row_count / chunk_rows + i64::from(row_count % chunk_rows != 0);
        assert_eq!(
            file_count, expected_file_count,
            "one independently receipted range has only row-limit objects plus its tail"
        );
        receipt_files += file_count;
        receipt_rows += row_count;
    }
    assert_eq!(
        receipt_files,
        i64::try_from(manifest_count).unwrap(),
        "range receipts account for every reload manifest"
    );
    assert_eq!(
        receipt_rows, expected_rows,
        "range receipts account for the complete snapshot"
    );
}

/// Wait until at least one independently completed worker object is durable in control-pg.
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

/// Read a reload chunk file back: (ids, every-row (commit_lsn, lsn) from the meta JSON).
async fn read_chunk_file(uri: &str) -> (Vec<i32>, Vec<(String, String)>) {
    let key = uri.strip_prefix("s3://walrus/").unwrap();
    let bytes: Bytes = store()
        .get(&Path::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    let mut ids = Vec::new();
    let mut metas = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .clone();
        let meta_col = batch
            .column_by_name(pg_to_arrow::EXTRACTOR_META_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .clone();
        // Two columns walked in lockstep, so `zip` carries the pairing instead of a row index. The
        // `Option`s are the arrays' null slots: unwrapping asserts what the extractor guarantees (an
        // `id` and a stamp on every exported row), which `.value(i)` would have read past silently.
        for (id, meta) in id_col.iter().zip(meta_col.iter()) {
            ids.push(id.unwrap());
            let meta: serde_json::Value = serde_json::from_str(meta.unwrap()).unwrap();
            metas.push((
                meta["commit_lsn"].as_str().unwrap().to_string(),
                meta["lsn"].as_str().unwrap().to_string(),
            ));
        }
    }
    (ids, metas)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn chunks_cover_the_table_exactly_with_shared_baseline_stamp() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_001);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 2500).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_re_cover",
        pool.clone(),
        Arc::clone(&waiters),
        Some(TABLE),
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let mut watch_rx = resolver.watched_commit;
    let mut streamed_watch_rx = resolver.watched_stream_commit;
    let resolver = resolver.handle;

    // Open a transaction before F, make it large enough to force protocol-v2 segments, and leave it
    // uncommitted through the entire export. Its rows are absent from PostgreSQL's exported snapshot;
    // logical decoding must retain the open transaction and surface it only at its later commit LSN.
    let long_tx = source_client().await;
    long_tx
        .batch_execute(&format!(
            "BEGIN;
             SET LOCAL logical_decoding_work_mem = '64kB';
             INSERT INTO public.{TABLE} (id, val)
             SELECT g, repeat('long-open-', 32) || g
               FROM generate_series(2502, 10501) g;"
        ))
        .await
        .unwrap();

    let req = request_and_claim(&pool, epoch).await;
    let reload_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(delayed_store(Duration::from_millis(50)), "walrus", epoch),
        export_cfg(epoch, 250, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();

    // Run the whole parallel snapshot in the background. A durable file proves at least one COPY
    // worker has made progress; the delayed object store and remaining files keep S open while the
    // overlapping WAL transaction commits and is decoded on the single replication stream. Its
    // update/delete/insert mix also proves every range imported the same S: the baseline must still
    // contain deleted id=2 and must not contain newly inserted id=2501.
    let export = tokio::spawn(async move { exporter.run(false).await });
    let progress = wait_for_file_progress(&pool, reload_id).await;
    assert!(
        !export.is_finished(),
        "the overlap transaction lands before the snapshot drains"
    );
    assert_eq!(
        progress.cursor_pk, None,
        "parallel progress is not a PK cursor"
    );
    admin
        .batch_execute(&format!(
            "BEGIN;
             UPDATE public.{TABLE} SET val = 'overlap' WHERE id = 1;
             DELETE FROM public.{TABLE} WHERE id = 2;
             INSERT INTO public.{TABLE} (id, val) VALUES (2501, 'after-snapshot');
             COMMIT;"
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), watch_rx.changed())
        .await
        .expect("the stream keeps decoding while the export is mid-flight")
        .expect("the resolver holds the sender until this test cancels it");
    // Copy the LSN out inside the borrowing statement: a `watch::Ref` is a read guard on the
    // channel, never held across an await (`clippy.toml`'s `await-holding-invalid-types`).
    let overlap_commit =
        (*watch_rx.borrow_and_update()).expect("a change is always a published overlap commit");
    let baseline_f = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap()
        .start_lsn
        .unwrap();
    assert!(
        overlap_commit > baseline_f,
        "the mid-export stream event ({overlap_commit}) outranks baseline F ({baseline_f}) — \
         it wins the transformer's dedup for that PK"
    );

    let final_h = match export.await.unwrap().unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("unexpected schema change to {new_version}")
        }
        RunOutcome::SnapshotLost => panic!("a freshly claimed exporter owns its snapshot"),
    };
    assert!(
        final_h > overlap_commit,
        "terminal H ({final_h}) follows the overlapping WAL commit ({overlap_commit})"
    );
    long_tx.batch_execute("COMMIT").await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), streamed_watch_rx.changed())
        .await
        .expect("the pre-F large transaction eventually reaches StreamCommit")
        .expect("the resolver holds the streamed watcher until cancellation");
    let long_commit = (*streamed_watch_rx.borrow_and_update())
        .expect("a protocol-v2 watched transaction has a commit LSN");
    assert!(
        long_commit > final_h,
        "a transaction begun before F but committed after H must remain a later WAL event: {long_commit} > {final_h}"
    );
    let markers = control::reload::read_markers(&pool, reload_id)
        .await
        .unwrap();
    assert_eq!(
        markers
            .iter()
            .map(|marker| (marker.kind, marker.lsn))
            .collect::<Vec<_>>(),
        vec![
            (control::ReloadMarkerKind::Baseline, baseline_f),
            (control::ReloadMarkerKind::End, final_h),
        ],
        "the decoder persisted F then H before resolving each waiter"
    );

    // Each physical range is independently receipted, so every non-empty range may contribute a
    // partial tail. Validate the persisted range accounting instead of assuming one tail per worker.
    let files = reload_manifest_rows(&pool, epoch).await;
    let receipts = reload_export_range_receipts(&pool, reload_id).await;
    assert_completed_range_receipts(&receipts, files.len(), 2500, 250);
    assert_eq!(files.iter().map(|file| file.1).sum::<i64>(), 2500);
    assert!(files.iter().all(|file| (1..=250).contains(&file.1)));
    let ends: Vec<Lsn> = files.iter().map(|f| f.2.parse().unwrap()).collect();
    assert!(
        ends.iter().all(|end| *end == baseline_f),
        "all chunks use F"
    );
    assert!(files.iter().all(|f| f.3 == reload_id));

    // Durable bookkeeping is the completed-file count plus one immutable F; physical ranges have
    // no resumable logical cursor because the shared PostgreSQL snapshot is connection-local.
    let row = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(usize::try_from(row.chunk_no).unwrap(), files.len());
    assert_eq!(row.cursor_pk, None);
    assert_eq!(row.start_lsn, Some(baseline_f), "start_lsn is baseline F");
    assert_eq!(
        row.first_lsn,
        Some(baseline_f),
        "legacy first_lsn aliases F"
    );
    assert_eq!(
        row.status,
        control::reload::ReloadStatus::Exporting,
        "the controller records export_complete after the exporter drains"
    );

    // Read every worker file back: the union is the table exactly, and EVERY row's meta carries
    // commit_lsn = lsn = shared F, which is every file's lsn_end.
    let mut all_ids = Vec::new();
    // `ends` was parsed out of `files` above, so zipping pairs each file with its own stamp — the
    // row index that carried the pairing before was only ever a way to reach back into `ends`.
    for ((uri, _, _lsn_end, _), end) in files.iter().zip(&ends) {
        let (ids, metas) = read_chunk_file(uri).await;
        for (commit_lsn, lsn) in &metas {
            assert_eq!(commit_lsn, lsn, "stamped commit_lsn = lsn");
            assert_eq!(
                commit_lsn.parse::<Lsn>().unwrap(),
                *end,
                "stamp == the file's lsn_end"
            );
        }
        all_ids.extend(ids);
    }
    let unique: BTreeSet<i32> = all_ids.iter().copied().collect();
    assert_eq!(all_ids.len(), 2500, "no duplicates across chunks");
    assert_eq!(unique.len(), 2500, "no misses");
    assert_eq!(*unique.first().unwrap(), 1);
    assert_eq!(*unique.last().unwrap(), 2500);
    assert!(unique.contains(&2), "all workers retain S-visible deletes");
    assert!(
        !unique.contains(&2501),
        "all workers exclude rows committed after S"
    );
    assert!(
        !unique.contains(&2502) && !unique.contains(&10501),
        "all workers exclude the long transaction that was open in S"
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
async fn adopted_file_progress_is_purged_and_successor_reexports_one_snapshot() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 2500).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_re_resume",
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
    let reload_id = req.reload_id;
    let stager = ParquetStager::new(store(), "walrus", epoch);

    // Start the complete shared snapshot, wait for one durable worker file, then abort the owner to
    // model process death while other ranges and multipart objects are still in flight.
    let mut first = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetStager::new(delayed_store(Duration::from_millis(50)), "walrus", epoch),
        export_cfg(epoch, 250, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    let crashed = tokio::spawn(async move { first.run(false).await });
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
    assert_eq!(mid.cursor_pk, None);
    let baseline_f = mid.start_lsn.expect("F persisted before the first file");

    // Adoption cannot resurrect the connection-local repeatable-read snapshot, so it returns a
    // control-flow outcome without another source-table SELECT.
    let mut adopted = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        stager.clone(),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &mid,
    )
    .await
    .unwrap();
    assert_eq!(adopted.run(true).await.unwrap(), RunOutcome::SnapshotLost);
    let successor_id = match handle_lost_snapshot_restart(&pool, &mid, 3).await.unwrap() {
        RestartDecision::Restarted(id) => id,
        RestartDecision::Capped => panic!("first lost-snapshot restart must be under the cap"),
    };
    assert_eq!(
        reload_manifest_rows(&pool, epoch).await.len(),
        0,
        "the predecessor's files are purged with its terminal transition"
    );

    // A lease-expired predecessor that wakes late cannot commit another file after the atomic
    // terminal+successor transaction. Its duplicate object is an orphan; no manifest gap forms.
    let mut stale = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        stager.clone(),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    let stale_err = stale.run(false).await.unwrap_err();
    assert!(
        format!("{stale_err:#}").contains("illegal reload transition"),
        "the failed predecessor rejects a late file-progress commit: {stale_err:#}"
    );
    // The rejected file leaves this diagnostic exporter's repeatable-read transaction open so
    // production `run()` could roll it back. Release it before the final fixture DROP.
    drop(stale);

    let successor = control::reload::get(&pool, successor_id)
        .await
        .unwrap()
        .unwrap();
    let mut restarted = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        stager,
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &successor,
    )
    .await
    .unwrap();
    assert!(matches!(
        restarted.run(false).await.unwrap(),
        RunOutcome::Drained { .. }
    ));

    let files = reload_manifest_rows(&pool, epoch).await;
    let receipts = reload_export_range_receipts(&pool, successor_id).await;
    assert_completed_range_receipts(&receipts, files.len(), 2500, 1000);
    assert!(files.iter().all(|file| file.3 == successor_id));
    assert_eq!(files.iter().map(|file| file.1).sum::<i64>(), 2500);
    assert!(files.iter().all(|file| (1..=1000).contains(&file.1)));
    let mut total = 0usize;
    let mut unique = BTreeSet::new();
    for (uri, row_count, _, _) in &files {
        let (ids, _) = read_chunk_file(uri).await;
        assert_eq!(
            i64::try_from(ids.len()).unwrap(),
            *row_count,
            "manifest row count matches the Parquet object"
        );
        total += ids.len();
        unique.extend(ids);
    }
    assert_eq!(total, 2500, "one consistent successor baseline");
    assert_eq!(unique.len(), 2500, "no gap in the restarted snapshot");

    let done = control::reload::get(&pool, successor_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(usize::try_from(done.chunk_no).unwrap(), files.len());
    assert!(
        matches!(done.start_lsn, Some(new_f) if new_f > baseline_f),
        "the successor must establish a fresh F"
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
async fn start_fence_timeout_leaves_the_reload_resumable() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_003);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 10).await;

    // NO fence resolver runs. Establishing F fails before any table SELECT or file write; the row
    // remains exporting so lease adoption can retry the same deterministic source event safely.
    let waiters = Arc::new(FenceWaiters::default());
    let req = request_and_claim(&pool, epoch).await;
    let err = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters,
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 1000, Duration::from_secs(2)),
        &req,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("decode echo timed out"),
        "got: {err:#}"
    );

    let row = control::reload::get(&pool, req.reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, control::reload::ReloadStatus::Exporting);
    assert_eq!(row.start_lsn, None, "no decoded F was persisted");
    assert_eq!(reload_manifest_rows(&pool, epoch).await.len(), 0);

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn reload_overrides_hostile_postgres_text_output_defaults() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_004);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed_guc_sensitive_types(&admin, &pool, epoch).await;

    let hostile_url = hostile_text_source_url();
    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        hostile_url.clone(),
        "walrus_re_gucs",
        pool.clone(),
        Arc::clone(&waiters),
        Some(TABLE),
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let resolver = resolver.handle;

    let req = request_and_claim(&pool, epoch).await;
    let mut exporter = ChunkExporter::connect(
        &hostile_url,
        pool.clone(),
        waiters,
        ParquetStager::new(store(), "walrus", epoch),
        export_cfg(epoch, 100, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    assert!(matches!(
        exporter.run(false).await.unwrap(),
        RunOutcome::Drained { .. }
    ));
    let files = reload_manifest_rows(&pool, epoch).await;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].1, 1);

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
