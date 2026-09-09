#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
#![allow(
    clippy::panic,
    reason = "polling helper: panic reports the requested reload state and id on timeout"
)]
//! Reload controller pickup against compose (`#[ignore]` — needs source + control PG). A
//! `requested` row flips to `exporting` within one poll cadence with a live, observably-advancing
//! lease; doomed requests (unpublished / keyless) fail fast with operator-readable reasons while a
//! `resync` of a keyed table is accepted; the `max_concurrent_reloads` cap holds under
//! three requests while the replication
//! stream keeps flowing. The scheduling/lease-cancel semantics are unit-tested in
//! `src/reload.rs`; each test runs its own controller against its own epoch, so tests never
//! claim each other's rows.
//!
//!   cargo test -p extractor --test reload_pickup -- --ignored

use common::{EpochNo, ReloadId, SchemaVersionNo};
use control::reload::{self, ReloadFlavor, ReloadStatus};
use extractor::consume::on_frame;
use extractor::pgoutput::{Message, StreamCtx};
use extractor::reload::{ReloadController, ReloadControllerConfig};
use extractor::reload_event::{self, FenceWaiters};
use extractor::replication::ReplicationStream;
use extractor::slot::verify_or_create_slot;
use extractor::staging::ParquetStager;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");

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

async fn pool_for(epoch: EpochNo) -> sqlx::PgPool {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    // Leftover non-terminal rows from a crashed prior run would trip the one_live index.
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch)
    .execute(&pool)
    .await
    .unwrap();
    pool
}

fn controller_cfg(epoch: EpochNo, cap: NonZeroUsize) -> ReloadControllerConfig {
    ReloadControllerConfig {
        poll_interval: Duration::from_millis(200), // fast cadence for the test
        max_concurrent_reloads: cap,
        workers_per_table: NonZeroUsize::new(4).unwrap(),
        router_batch_bytes: std::num::NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        worker_admission: extractor::reload_export::ReloadWorkerAdmission::new(
            NonZeroUsize::new(8).unwrap(),
        ),
        lease_ttl: Duration::from_secs(6), // renewal at 2s — observable within one test
        instance: "walrus-extractor-test".to_string(),
        publication_name: "walrus_pub".to_string(),
        epoch,
        chunk_rows: std::num::NonZeroU64::new(1000).unwrap(),
        // No decode loop resolves echoes in these tests, so exporters PARK on the echo await —
        // exactly the observable-scheduling role the stub used to play. The echo/export
        // behaviour itself is reload_export.rs's suite.
        echo_timeout: Duration::from_secs(3600),
        reload_max_restarts: 3,
    }
}

fn minio(epoch: EpochNo) -> ParquetStager {
    ParquetStager::new(
        std::sync::Arc::new(
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

/// The exporter reads the reload's schema_version from the registry — seed one per target table
/// (in production the streaming extractor registers every published table long before a reload).
async fn seed_registry(
    admin: &tokio_postgres::Client,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    tables: &[&str],
) {
    for table in tables {
        let rel = extractor::source_catalog::describe_source_relation(admin, "public", table)
            .await
            .unwrap();
        let row = control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: table.to_string(),
            schema_version: SchemaVersionNo(1),
            descriptors: pg_to_arrow::describe_relation(&rel).unwrap(),
            columns: serde_json::to_value(&rel).unwrap(),
        };
        control::upsert_registry(pool, &row).await.unwrap();
    }
}

async fn status_of(pool: &sqlx::PgPool, reload_id: ReloadId) -> (ReloadStatus, Option<String>) {
    let row = reload::get(pool, reload_id).await.unwrap().unwrap();
    (row.status, row.error)
}

fn metric_sum(name: &str) -> f64 {
    common::metrics::render()
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.strip_prefix(name))
        .filter(|rest| rest.starts_with(' ') || rest.starts_with('{'))
        .filter_map(|rest| rest.split_whitespace().last())
        .filter_map(|value| value.parse::<f64>().ok())
        .sum()
}

/// Poll until the row reaches `want` (the controller's cadence is 200ms; give it a few).
async fn await_status(pool: &sqlx::PgPool, reload_id: ReloadId, want: ReloadStatus) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if status_of(pool, reload_id).await.0 == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("reload {reload_id} never reached {want:?}"));
}

/// The production source request is idempotent by UUID and preserves the exact table payload.
/// Reusing the UUID for a different target fails instead of silently changing history.
#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn source_request_api_is_idempotent_and_preserves_table_shape() {
    let _g = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let request_id = uuid::Uuid::from_u128(0x8ba4_774b_8ae5_44ac_842e_b99f_7420_764d);

    reload_event::request_table(&admin, request_id, "public", "orders")
        .await
        .unwrap();
    reload_event::request_table(&admin, request_id, "public", "orders")
        .await
        .unwrap();

    let request_id_text = request_id.to_string();
    let count: i64 = admin
        .query_one(
            "SELECT count(*) FROM public.walrus_reload_event WHERE event_id = $1::text::uuid",
            &[&request_id_text],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "an idempotent retry appends no duplicate event");

    let row = admin
        .query_one(
            "SELECT request_id::text, event_kind, scope, source_schema, source_table,
                    targets::text, reload_id IS NULL, schema_version IS NULL
             FROM public.walrus_reload_event WHERE event_id = $1::text::uuid",
            &[&request_id_text],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), request_id_text);
    assert_eq!(row.get::<_, String>(1), "request");
    assert_eq!(row.get::<_, String>(2), "table");
    assert_eq!(row.get::<_, Option<String>>(3).as_deref(), Some("public"));
    assert_eq!(row.get::<_, Option<String>>(4).as_deref(), Some("orders"));
    assert_eq!(row.get::<_, String>(5), "[]");
    assert!(
        row.get::<_, bool>(6),
        "a request has no control reload id yet"
    );
    assert!(
        row.get::<_, bool>(7),
        "a request freezes schema at F, not here"
    );

    let conflict = reload_event::request_table(&admin, request_id, "public", "customers")
        .await
        .expect_err("the same UUID cannot be rebound to another table");
    assert!(
        conflict.to_string().contains("different request data"),
        "UUID payload conflict stays explicit: {conflict:#}"
    );
}

async fn lease_expiry_epoch(pool: &sqlx::PgPool, reload_id: ReloadId) -> f64 {
    sqlx::query_scalar::<_, f64>(
        "SELECT extract(epoch FROM lease_expiry)::float8
         FROM public.walrus_table_reload WHERE reload_id = $1",
    )
    .bind(reload_id.0)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn pickup_flips_to_exporting_with_a_live_advancing_lease() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(640_001);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = pool_for(epoch).await;
    seed_registry(&admin, &pool, epoch, &["orders"]).await;

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(FenceWaiters::default()),
        minio(epoch),
        controller_cfg(epoch, NonZeroUsize::new(2).unwrap()),
        token.clone(),
    );

    // Legacy direct-control requests remain claimable for rolling compatibility.
    let id = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    await_status(&pool, id, ReloadStatus::Exporting).await;

    let row = reload::get(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.lease_holder.as_deref(), Some("walrus-extractor-test"));
    let now_epoch: f64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::float8")
        .fetch_one(&pool)
        .await
        .unwrap();
    let exp1 = lease_expiry_epoch(&pool, id).await;
    assert!(exp1 > now_epoch, "the lease is live");

    // The exporter parks on its echo await while its lease renews at TTL/3 (2s here): expiry
    // must advance. Poll rather than sleep-once — a loaded runner can delay a renewal tick.
    let exp2 = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let e = lease_expiry_epoch(&pool, id).await;
            if e > exp1 {
                return e;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("lease_expiry observably advances while the exporter runs");
    assert!(exp2 > exp1);

    token.cancel();
    handle.await.unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch)
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn preflight_failures_land_in_failed_with_reasons() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(640_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    // A published-but-keyless table: the dev publication is FOR ALL TABLES, so existence ⇒
    // membership; what it lacks is a PK.
    admin
        .batch_execute(
            "DROP TABLE IF EXISTS public._walrus_rl_keyless;
             CREATE TABLE public._walrus_rl_keyless (x int)",
        )
        .await
        .unwrap();
    let pool = pool_for(epoch).await;

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(FenceWaiters::default()),
        minio(epoch),
        controller_cfg(epoch, NonZeroUsize::new(2).unwrap()),
        token.clone(),
    );

    // (a) Not in the publication (a table that doesn't exist is by definition unpublished).
    let ghost = reload::request(&pool, epoch, "public", "ghost_table", ReloadFlavor::Reload)
        .await
        .unwrap();
    // (b) Published but keyless.
    let keyless = reload::request(
        &pool,
        epoch,
        "public",
        "_walrus_rl_keyless",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    // (c) Resync of a published, keyed table is accepted: it
    // passes preflight like a `reload` and reaches `exporting` (here it parks on the echo await,
    // no resolver runs, exactly like the accepted-request case above).
    let resync = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();

    await_status(&pool, ghost, ReloadStatus::Failed).await;
    await_status(&pool, keyless, ReloadStatus::Failed).await;
    await_status(&pool, resync, ReloadStatus::Exporting).await;

    let (_, err) = status_of(&pool, ghost).await;
    assert!(
        err.as_deref()
            .unwrap()
            .contains("is not in the publication"),
        "ghost: {err:?}"
    );
    let (_, err) = status_of(&pool, keyless).await;
    assert!(
        err.as_deref().unwrap().contains("has no primary key"),
        "keyless: {err:?}"
    );

    token.cancel();
    handle.await.unwrap();
    admin
        .batch_execute("DROP TABLE IF EXISTS public._walrus_rl_keyless")
        .await
        .unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch)
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn cap_of_two_holds_and_the_stream_keeps_flowing() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = EpochNo(640_003);
    let slot = "walrus_reload_pickup";
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    admin
        .batch_execute(
            "DELETE FROM public.orders WHERE id = 640001;
             DELETE FROM public.customers WHERE region = 'rl-seed' AND id = 640002;
             DELETE FROM public.customers WHERE region = 'rl' AND id = 640003;
             DELETE FROM public.items WHERE id = 640003;
             INSERT INTO public.orders (id, status) VALUES (640001, 'reload-cap');
             INSERT INTO public.customers (region, id, name)
                 VALUES ('rl-seed', 640002, 'reload-cap');
             INSERT INTO public.items (id, label, qty) VALUES (640003, 'reload-cap', 1);",
        )
        .await
        .unwrap();
    let pool = pool_for(epoch).await;
    seed_registry(&admin, &pool, epoch, &["orders", "customers", "items"]).await;

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(FenceWaiters::default()),
        minio(epoch),
        controller_cfg(epoch, NonZeroUsize::new(2).unwrap()),
        token.clone(),
    );

    // Three valid requests under a cap of two. The third either waits in `requested`, or starts
    // only after one of the first exporter tasks has legitimately released its permit.
    let a = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    let b = reload::request(&pool, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();
    let c = reload::request(&pool, epoch, "public", "items", ReloadFlavor::Reload)
        .await
        .unwrap();

    await_status(&pool, a, ReloadStatus::Exporting).await;
    await_status(&pool, b, ReloadStatus::Exporting).await;

    // Sample across several poll cadences. Status rows are not the cap: an exporter that hits an
    // infra error deliberately leaves its row `exporting` for lease-based adoption. The active
    // gauge wraps the actual semaphore-held task and therefore measures the load-bearing limit.
    for _ in 0..8 {
        let active = metric_sum(common::metrics::names::RELOAD_ACTIVE);
        assert!(active <= 2.0, "cap breached: {active} active exporters");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    if status_of(&pool, c).await.0 == ReloadStatus::Requested {
        // Free a permit through the shipped lost-lease path. Model a legitimate successor claim:
        // changing the holder must also advance the fencing generation, exactly as adoption does.
        // If an earlier exporter already released a permit naturally, `c` is exporting and this
        // branch is unnecessary.
        sqlx::query(
            "UPDATE public.walrus_table_reload
             SET lease_holder = 'lease-thief',
                 exporter_generation = exporter_generation + 1
             WHERE reload_id = $1",
        )
        .bind(a.0)
        .execute(&pool)
        .await
        .unwrap();
        await_status(&pool, c, ReloadStatus::Exporting).await;
    } else {
        assert_eq!(
            status_of(&pool, c).await.0,
            ReloadStatus::Exporting,
            "the third is either queued or started after a permit was released"
        );
    }
    let active = metric_sum(common::metrics::names::RELOAD_ACTIVE);
    assert!(
        (1.0..=2.0).contains(&active),
        "the no-stall probe needs one or two active exporters, got {active}"
    );

    // Open the no-stall probe while the controller is actively holding two exporter permits. A
    // user change written NOW must decode promptly: controller work runs on its own connections,
    // off the decode path. Starting here also keeps this deliberately manual stream from idling
    // past the compose source's five-second `wal_sender_timeout` before its first `next()` call.
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    admin
        .execute(
            "INSERT INTO public.customers (region, id, name) VALUES ('rl', 640003, 'no-stall')",
            &[],
        )
        .await
        .unwrap();
    // Match ONLY the customers insert by its relation OID — the exporters' own reload_signal
    // inserts also decode as Inserts and must not satisfy the no-stall probe.
    let mut ctx = StreamCtx::default();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut customers_oid: Option<u32> = None;
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            match on_frame(&mut ctx, frame).unwrap() {
                Some(Message::Relation { relation, .. }) if relation.name == "customers" => {
                    customers_oid = Some(relation.oid);
                }
                Some(Message::Insert { relation_oid, .. })
                    if customers_oid == Some(relation_oid) =>
                {
                    return;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the USER insert decodes while the controller holds two exports");

    token.cancel();
    handle.await.unwrap();
    drop(stream);
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
    admin
        .batch_execute(
            "DELETE FROM public.orders WHERE id = 640001;
             DELETE FROM public.customers
                 WHERE (region = 'rl-seed' AND id = 640002)
                    OR (region = 'rl' AND id = 640003);
             DELETE FROM public.items WHERE id = 640003;",
        )
        .await
        .unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch)
    .execute(&pool)
    .await
    .unwrap();
}
