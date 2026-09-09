#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    reason = "integration test — unwrap/expect fine in setup + helpers; the concurrency \
              high-water mark is reported, not asserted, so stderr is its destination"
)]
//! End-to-end: N-table reloads at scale on ONE slot (reload §2/§5). Three tables are
//! seeded and streamed by the real extractor+transformer, then reloaded concurrently with explicit settings
//! for the table cap, per-table COPY workers, and remote-object row count. The load-bearing
//! assertions: neither the table nor derived SQL-worker cap is breached, exactly **one** replication
//! slot remains on the source, every reload completes, and every mirror equals the source.
//!
//!   cargo test -p e2e --features it -- --ignored n_table_reloads

#![cfg(feature = "it")]

use e2e::{Harness, ReloadExtractionConfig};
use std::time::Duration;
use uuid::Uuid;

// Harness-owned fixtures created before bootstrap so the transformer owns them.
const TABLES: [&str; 3] = ["rl1", "rl2", "rl3"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn n_table_reloads_respect_the_cap_on_one_slot() {
    let mut h = Harness::start_with_reload_extraction(ReloadExtractionConfig {
        max_concurrent_reloads: 2,
        reload_workers_per_table: 4,
        reload_chunk_rows: 250,
    })
    .await
    .unwrap();

    // Seed the harness-owned tables (truncated at start) and let the pipeline mirror each. 5k rows
    // is a middle ground: big enough that the reloads take real work (so the cap semaphore genuinely
    // gates the third), small enough that all three apply + complete comfortably in the deadline.
    for t in TABLES {
        h.source_batch(&format!(
            "INSERT INTO public.{t} SELECT g, 'v' || g FROM generate_series(1, 5000) g;"
        ))
        .await
        .unwrap();
    }
    let before = h.source_wal_lsn().await.unwrap();
    // A tiny post-seed write per table gives the transformer a watermark to converge past.
    for t in TABLES {
        h.source_exec(&format!(
            "UPDATE public.{t} SET status = 'seeded' WHERE id = 1"
        ))
        .await
        .unwrap();
    }
    for t in TABLES {
        h.await_transformed_past(t, before, Duration::from_secs(90))
            .await
            .unwrap();
    }

    // Request source-WAL reloads on all three at once. The extractor's table cap is 2 and each table may
    // own at most 4 COPY pipelines. Own the pool
    // (Arc-backed) so it doesn't borrow `h` across the `stop_transformer()` (`&mut h`) before the diff.
    let epoch = h.epoch;
    let pool = h.control_pool().clone();
    let mut reload_ids = Vec::new();
    for t in TABLES {
        let request_id = Uuid::new_v4();
        h.request_table_reload(request_id, "public", t)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let id = loop {
            let id: Option<i64> = sqlx::query_scalar(
                "SELECT reload_id FROM public.walrus_table_reload \
                 WHERE epoch = $1 AND source_request_id = $2 AND source_table = $3",
            )
            .bind(epoch)
            .bind(request_id)
            .bind(t)
            .fetch_optional(&pool)
            .await
            .unwrap();
            if let Some(id) = id {
                break common::ReloadId::from(id);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "source reload request for {t} was not decoded"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        reload_ids.push(id);
    }
    let owned_reload_ids: Vec<i64> = reload_ids.iter().map(|id| id.0).collect();
    let expected_complete = i64::try_from(owned_reload_ids.len()).unwrap();

    // (No concurrent source churn here — the reloads' own signal/echo traffic exercises the one
    // slot, and a static source keeps the final mirror==source comparison free of catch-up races.
    // The "other tables keep streaming during a reload" no-stall promise is proven in
    // `reload_quarantine.rs`.)

    // Poll to completion, sampling the two invariants on a tight cadence: the cap is never breached
    // (≤ 2 exporting at every sample) and exactly one slot exists throughout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    let mut max_exporting = 0i64;
    loop {
        let exporting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM public.walrus_table_reload WHERE epoch = $1 AND status = 'exporting'",
        )
        .bind(epoch)
        .fetch_one(&pool)
        .await
        .unwrap();
        max_exporting = max_exporting.max(exporting);
        assert!(
            exporting <= 2,
            "cap breached: {exporting} exporting at once"
        );

        let reload_connections: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT activity.pid)
             FROM pg_stat_activity activity
             JOIN pg_locks lock ON lock.pid = activity.pid
             JOIN pg_class relation ON relation.oid = lock.relation
             JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
             WHERE activity.backend_type = 'client backend'
               AND lock.granted AND lock.mode = 'AccessShareLock'
               AND namespace.nspname = 'public'
               AND relation.relname IN ('rl1', 'rl2', 'rl3')",
        )
        .fetch_one(h.source_pool())
        .await
        .unwrap();
        assert!(
            reload_connections <= 8,
            "derived table×worker cap breached: {reload_connections} reload SQL connections"
        );

        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
        assert_eq!(slots, 1, "exactly one slot throughout (got {slots})");
        let walsenders: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_replication")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
        assert_eq!(walsenders, 1, "exactly one walsender throughout");

        // Bootstrap also creates terminal reload rows in this epoch. Count only the attempts this
        // test requested; otherwise the global completed total can exceed three before these finish.
        let complete: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM public.walrus_table_reload \
             WHERE epoch = $1 AND reload_id = ANY($2) AND status = 'complete'",
        )
        .bind(epoch)
        .bind(&owned_reload_ids)
        .fetch_one(&pool)
        .await
        .unwrap();
        if complete == expected_complete {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reloads did not all complete in time ({complete}/{expected_complete} complete)"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // All three reached `complete`, none ever exceeded the cap of 2 concurrent exporters (the
    // load-bearing invariant, asserted at every sample above). `max_exporting` is logged, not
    // asserted `>= 2`: a fast reload can slip through `exporting` between samples, so observing 2
    // concurrent is best-effort — the never-breached bound plus the unit test
    // `cap_of_two_holds_and_the_stream_keeps_flowing` are the real concurrency proof.
    eprintln!("max concurrently-exporting observed: {max_exporting}");
    for id in reload_ids {
        let row = control::reload::get(&pool, id).await.unwrap().unwrap();
        assert_eq!(row.status, control::reload::ReloadStatus::Complete);
    }

    // Every mirror equals its source, row for row. Stop the transformer first — it holds an exclusive
    // lock on each `.duckdb`, so the diff helper opens the files read-only only once it's down.
    h.stop_transformer().await.unwrap();
    for t in TABLES {
        h.assert_mirror_equals_source(t).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn one_worker_honors_the_configured_record_chunk_size() {
    let mut h = Harness::start_with_reload_extraction_and_source_seed(
        ReloadExtractionConfig {
            max_concurrent_reloads: 1,
            reload_workers_per_table: 1,
            reload_chunk_rows: 37,
        },
        Some(
            "INSERT INTO public.rl1 \
             SELECT g, 'chunk-' || g FROM generate_series(1, 1001) g;",
        ),
    )
    .await
    .unwrap();
    let floor = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.rl1 SET status = 'ready' WHERE id = 1")
        .await
        .unwrap();
    h.await_transformed_past("rl1", floor, Duration::from_secs(90))
        .await
        .unwrap();
    let initial: (i64, String, i64) = sqlx::query_as(
        "SELECT reload_id, status, chunk_no FROM public.walrus_table_reload \
         WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'rl1' \
         ORDER BY reload_id DESC LIMIT 1",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        initial.1, "complete",
        "non-empty first startup reached cutover"
    );
    let initial_ranges = completed_export_ranges(&h, initial.0).await;
    assert_range_chunking(&initial_ranges, 37, 1001, initial.2);

    // Hold the transformer so exported manifests remain inspectable instead of being claimed/deleted.
    // This lets the test assert the record-count contract on every remote object, not merely the
    // final file counter.
    h.stop_transformer().await.unwrap();
    let request_id = Uuid::new_v4();
    h.request_table_reload(request_id, "public", "rl1")
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    let completed = loop {
        let row = sqlx::query_as::<_, (i64, String, i64)>(
            "SELECT reload_id, status, chunk_no FROM public.walrus_table_reload \
             WHERE epoch = $1 AND source_request_id = $2",
        )
        .bind(h.epoch)
        .bind(request_id)
        .fetch_optional(h.control_pool())
        .await
        .unwrap();
        if let Some((reload_id, status, chunk_no)) = row {
            assert_ne!(status, "failed", "reload {reload_id} failed");
            if status == "export_complete" {
                break (reload_id, chunk_no);
            }
        }
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
        assert_eq!(slots, 1);
        let walsenders: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_replication")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
        assert_eq!(walsenders, 1);
        let reload_connections: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT activity.pid)
             FROM pg_stat_activity activity
             JOIN pg_locks lock ON lock.pid = activity.pid
             JOIN pg_class relation ON relation.oid = lock.relation
             JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
             WHERE activity.backend_type = 'client backend'
               AND lock.granted AND lock.mode = 'AccessShareLock'
               AND namespace.nspname = 'public' AND relation.relname = 'rl1'",
        )
        .fetch_one(h.source_pool())
        .await
        .unwrap();
        assert!(
            reload_connections <= 1,
            "one-worker mode opened {reload_connections} reload SQL connections"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "single-worker reload did not complete"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let completed_ranges = completed_export_ranges(&h, completed.0).await;
    let mut expected_tail_rows = assert_range_chunking(&completed_ranges, 37, 1001, completed.1);
    let object_rows: Vec<i64> = sqlx::query_scalar(
        "SELECT row_count FROM public.walrus_file_manifest \
         WHERE reload_id = $1 AND kind = 'reload' ORDER BY id",
    )
    .bind(completed.0)
    .fetch_all(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        i64::try_from(object_rows.len()).unwrap(),
        completed.1,
        "every recorded reload object has one manifest"
    );
    assert_eq!(object_rows.iter().sum::<i64>(), 1001);
    assert!(
        object_rows.iter().all(|rows| (1..=37).contains(rows)),
        "37 is a strict per-object row maximum: {object_rows:?}"
    );
    let mut actual_tail_rows: Vec<i64> = object_rows
        .iter()
        .copied()
        .filter(|rows| *rows < 37)
        .collect();
    expected_tail_rows.sort_unstable();
    actual_tail_rows.sort_unstable();
    assert_eq!(
        actual_tail_rows, expected_tail_rows,
        "each independently receipted physical range has exactly one final partial object"
    );

    h.restart_transformer().await.unwrap();
    let cutover_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status: String = sqlx::query_scalar(
            "SELECT status FROM public.walrus_table_reload WHERE reload_id = $1",
        )
        .bind(completed.0)
        .fetch_one(h.control_pool())
        .await
        .unwrap();
        if status == "complete" {
            break;
        }
        assert_ne!(status, "failed");
        assert!(tokio::time::Instant::now() < cutover_deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    h.stop_transformer().await.unwrap();
    h.assert_mirror_equals_source("rl1").await.unwrap();
}

/// A process crash loses every PostgreSQL exported snapshot, even though some baseline objects may
/// already be durable. A replacement process must therefore epoch-isolate the abandoned attempt,
/// open a newly fenced all-table reconciliation, and converge without creating a second replication
/// slot or walsender.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn extractor_crash_mid_reload_starts_a_correct_successor_on_one_slot() {
    let mut h = Harness::start_with_reload_extraction(ReloadExtractionConfig {
        max_concurrent_reloads: 1,
        reload_workers_per_table: 2,
        // Many small objects give the test a deterministic, observable window after the first
        // durable chunk but before the snapshot export reaches H.
        reload_chunk_rows: 50,
    })
    .await
    .expect("bring up extractor + transformer");
    let old_epoch = h.epoch;

    // Do not race the source-backed request against the empty bootstrap reconciliation for rl1.
    let bootstrap_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM public.walrus_table_reload \
             WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'rl1' \
             ORDER BY reload_id DESC LIMIT 1",
        )
        .bind(h.epoch)
        .fetch_optional(h.control_pool())
        .await
        .unwrap();
        if status.as_deref() == Some("complete") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < bootstrap_deadline,
            "rl1 bootstrap reload did not complete"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // One transaction keeps setup quick; 400 remote objects keep the later COPY observably in
    // flight. The request is committed after these rows, so its decoded reload sees all of them.
    h.source_batch(
        "INSERT INTO public.rl1 \
         SELECT g, repeat('before-crash-', 8) || g FROM generate_series(1, 20000) g;",
    )
    .await
    .unwrap();
    let request_id = Uuid::new_v4();
    h.request_table_reload(request_id, "public", "rl1")
        .await
        .unwrap();

    // Kill only after control PG proves at least one completed object is durable while the attempt
    // still owns its connection-local snapshot. This is not a sleep-based approximation.
    // A cold CI runner can spend several minutes on the first Parquet encode/upload while the
    // other reload-scale processes are still warming their caches. Keep this scale-only crash
    // probe bounded, but leave enough room to observe the first durable object before SIGKILL.
    let progress_deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut last_observed = None;
    let predecessor_id = loop {
        let row: Option<(i64, String, i64)> = sqlx::query_as(
            "SELECT reload_id, status, chunk_no FROM public.walrus_table_reload \
             WHERE epoch = $1 AND source_request_id = $2 \
             ORDER BY reload_id DESC LIMIT 1",
        )
        .bind(h.epoch)
        .bind(request_id)
        .fetch_optional(h.control_pool())
        .await
        .unwrap();
        if let Some((reload_id, status, chunk_no)) = row {
            last_observed = Some((reload_id, status.clone(), chunk_no));
            assert_ne!(status, "failed", "reload {reload_id} failed before crash");
            if status == "exporting" && chunk_no > 0 {
                break reload_id;
            }
        }
        assert_one_slot_and_at_most_one_walsender(&h).await;
        assert!(
            tokio::time::Instant::now() < progress_deadline,
            "reload did not expose an in-flight durable chunk within 300s; last observed row: \
             {last_observed:?}; extractor log tail:\n{}",
            h.extractor_log_tail(40)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_one_slot_and_at_most_one_walsender(&h).await;
    h.kill_extractor()
        .await
        .expect("SIGKILL extractor during reload");
    assert_one_slot_and_at_most_one_walsender(&h).await;

    // Changes committed while the extractor is dead must survive in retained WAL and be included by
    // the successor's fresh snapshot/replay boundary, never resurrecting the abandoned baseline.
    h.source_batch(
        "UPDATE public.rl1 SET status = 'changed-while-down' WHERE id % 97 = 0; \
         INSERT INTO public.rl1 VALUES (20001, 'inserted-while-down');",
    )
    .await
    .unwrap();
    h.restart_extractor()
        .await
        .expect("extractor restarts into a newly fenced generation");
    let new_epoch = h
        .await_epoch_past(old_epoch, Duration::from_secs(60))
        .await
        .expect("replacement extractor opens a successor generation");
    assert_eq!(new_epoch, old_epoch + 1);
    h.refresh_epoch().await.unwrap();

    // The epoch-1 transformer deliberately retires as soon as it observes the bump. It therefore cannot
    // publish an epoch-2 reload: `complete` is a transformer-owned transition, while `export_complete`
    // is the extractor's durable hand-off boundary. Wait for that hand-off before starting the successor
    // transformer. Waiting for `complete` here would deadlock the test against the process it has not yet
    // started.
    let recovery_deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    let mut last_observed = None;
    let successor_id = loop {
        let row: Option<(i64, String)> = sqlx::query_as(
            "SELECT tr.reload_id, tr.status \
             FROM public.walrus_table_reload tr \
             JOIN public.walrus_replication_state rs \
               ON rs.epoch = tr.epoch \
              AND rs.bootstrap_request_id = tr.parent_request_id \
             WHERE tr.epoch = $1 \
               AND tr.source_schema = 'public' \
               AND tr.source_table = 'rl1' \
               AND tr.request_scope = 'all_published' \
             ORDER BY tr.reload_id DESC LIMIT 1",
        )
        .bind(h.epoch)
        .fetch_optional(h.control_pool())
        .await
        .unwrap();
        if let Some((reload_id, status)) = row {
            last_observed = Some((reload_id, status.clone()));
            if matches!(status.as_str(), "export_complete" | "complete") {
                break reload_id;
            }
            assert!(
                status != "failed",
                "successor bootstrap reload {reload_id} failed"
            );
        }
        assert_one_slot_and_at_most_one_walsender(&h).await;
        assert!(
            tokio::time::Instant::now() < recovery_deadline,
            "successor-generation bootstrap export did not reach its durable transformer hand-off \
             after the process crash; last observed row: {last_observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(successor_id > predecessor_id);
    let successor_files: i64 =
        sqlx::query_scalar("SELECT chunk_no FROM public.walrus_table_reload WHERE reload_id = $1")
            .bind(successor_id)
            .fetch_one(h.control_pool())
            .await
            .unwrap();
    let successor_ranges = completed_export_ranges(&h, successor_id).await;
    assert_range_chunking(&successor_ranges, 50, 20_001, successor_files);
    assert_one_slot_and_at_most_one_walsender(&h).await;

    // The old transformer is fenced out by the epoch bump. Reap that expected exit, then start its
    // replacement after the full successor snapshot is durable. Readiness proves that replacement
    // published every frozen bootstrap reload, including this one.
    h.await_transformer_exited(Duration::from_secs(60))
        .await
        .expect("epoch-1 transformer exits after observing the successor");
    // This fixture deliberately produces 400 tiny reload objects. A healthy CI transformer consumes
    // them in several bounded claim cycles, so give this scale-only reconciliation more time than
    // the harness's ordinary 90-second startup contract.
    h.restart_transformer_with_deadline(Duration::from_secs(300))
        .await
        .expect("transformer rebuilds under the successor generation");
    let successor_status: String =
        sqlx::query_scalar("SELECT status FROM public.walrus_table_reload WHERE reload_id = $1")
            .bind(successor_id)
            .fetch_one(h.control_pool())
            .await
            .unwrap();
    assert_eq!(
        successor_status, "complete",
        "successor transformer readiness requires the rl1 bootstrap publication to complete"
    );

    // Cross a post-snapshot WAL watermark before comparing.
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.rl1 SET status = 'after-restart' WHERE id = 20001")
        .await
        .unwrap();
    h.await_transformed_past("rl1", before, Duration::from_secs(240))
        .await
        .expect("successor generation converges after the process crash");
    h.stop_transformer().await.unwrap();
    h.assert_mirror_equals_source("rl1").await.unwrap();
}

async fn assert_one_slot_and_at_most_one_walsender(h: &Harness) {
    let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots")
        .fetch_one(h.source_pool())
        .await
        .unwrap();
    assert_eq!(slots, 1, "reload recovery must retain exactly one slot");
    let walsenders: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_replication")
        .fetch_one(h.source_pool())
        .await
        .unwrap();
    assert!(
        walsenders <= 1,
        "reload recovery opened {walsenders} walsenders"
    );
}

async fn completed_export_ranges(h: &Harness, reload_id: i64) -> Vec<(i64, i64, i64)> {
    let ranges: Vec<(i64, String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT range_no, status, file_count, row_count \
         FROM public.walrus_table_reload_export_range \
         WHERE reload_id = $1 ORDER BY range_no",
    )
    .bind(reload_id)
    .fetch_all(h.control_pool())
    .await
    .unwrap();
    assert!(!ranges.is_empty(), "reload {reload_id} has no range plan");
    ranges
        .into_iter()
        .enumerate()
        .map(
            |(expected_range_no, (range_no, status, file_count, row_count))| {
                assert_eq!(
                    range_no,
                    i64::try_from(expected_range_no).unwrap(),
                    "reload {reload_id} range plan is not contiguous"
                );
                assert_eq!(
                    status, "complete",
                    "reload {reload_id} range {range_no} is not complete"
                );
                (
                    range_no,
                    file_count.expect("complete range has a file count"),
                    row_count.expect("complete range has a row count"),
                )
            },
        )
        .collect()
}

/// A reload object never crosses a physical range boundary: sealing the range's final object before
/// recording its durable receipt is what makes the range plan auditable after a crash. Consequently,
/// the configured row count is a strict object maximum and each non-divisible range has its own
/// partial tail; it is not a global or per-worker chunking promise.
fn assert_range_chunking(
    ranges: &[(i64, i64, i64)],
    chunk_rows: i64,
    expected_rows: i64,
    expected_files: i64,
) -> Vec<i64> {
    assert!(chunk_rows > 0);
    let mut total_files = 0_i64;
    let mut total_rows = 0_i64;
    let mut tail_rows = Vec::new();
    for &(range_no, file_count, row_count) in ranges {
        assert!(row_count >= 0, "range {range_no} has a negative row count");
        let remainder = row_count % chunk_rows;
        let range_files = row_count / chunk_rows + i64::from(remainder != 0);
        assert_eq!(
            file_count, range_files,
            "range {range_no} did not use the configured {chunk_rows}-row object maximum"
        );
        if remainder != 0 {
            tail_rows.push(remainder);
        }
        total_files += file_count;
        total_rows += row_count;
    }
    assert_eq!(
        total_rows, expected_rows,
        "range receipts lost or added rows"
    );
    assert_eq!(
        total_files, expected_files,
        "range receipts disagree with the reload's durable file count"
    );
    tail_rows
}
