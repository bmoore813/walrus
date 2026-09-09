//! The walrus end-to-end harness brings up **both binaries** — `walrus-extractor` and `walrus-transformer` —
//! as child processes against a compose stack (source PG, control/catalog PG, and MinIO), drives the
//! *source* database, and lets a test assert the full two-hop contract:
//! Parquet in MinIO → verbatim `<table>_raw` → the `<table>` mirror equals the current source.
//! Live scenarios are `#[ignore]` and gated behind `--features it`, so a plain `cargo test
//! --workspace` runs only hermetic verifier tests and never needs Docker.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    clippy::must_use_candidate,
    clippy::disallowed_methods,
    reason = "compose-gated e2e harness, not a published API; unwrap/expect are test setup and \
              anyhow failure is the test failure; synchronous child-log creation and log scrapes \
              observe out-of-process services, not walrus runtime I/O"
)]

use anyhow::{Context, Result};
use common::sql::SqlStrExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

mod parity;
pub use parity::{CompareField, Projection, ScenarioStep, TableExpectation, TableId, TableParity};

const DEFAULT_SOURCE_URL: &str = "postgres://postgres:postgres@localhost:5432/walrus";
const DEFAULT_CONTROL_URL: &str = "postgres://postgres:postgres@localhost:5433/walrus_control";
const DEFAULT_CATALOG_URL: &str = "postgres://postgres:postgres@localhost:5433/walrus_ducklake";
const DUCKLAKE_SCHEMA: &str = "walrus_e2e";
const DUCKLAKE_DATA: &str = "s3://walrus/ducklake/e2e/";
const DEFAULT_S3_ENDPOINT: &str = "http://localhost:9000";
const BUCKET: &str = "walrus";
const SLOT: &str = "walrus_e2e_slot";
/// The standard PostgreSQL build limit for columns participating in one index.
pub const WIDE_PRIMARY_KEY_COLUMNS: usize = 32;
/// Most E2Es intentionally keep this tiny so their large open transactions exercise WAL spilling.
const DEFAULT_E2E_MAX_INFLIGHT_BYTES: u64 = 64 * 1024;
/// Reload-scale E2Es need room for their explicitly configured parallel COPY routes. This is the
/// existing process-wide memory safety ceiling, not a fourth reload extraction control.
const PARALLEL_RELOAD_E2E_MAX_INFLIGHT_BYTES: u64 = 512 * 1024 * 1024;
const TABLE_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x4f02_efc2_39b3_4d9d_a860_22af_7291_8cc8);
/// The MinIO container name (`<compose project>-<service>-1`) — `docker pause`d to stall the extractor's S3
/// durability in the WAL-runaway / keepalive chaos tests.
const DEFAULT_MINIO_CONTAINER: &str = "walrus-minio-1";

/// Connection and process-listener settings for an E2E run. The defaults preserve the historical
/// `just up` workflow; the self-contained acceptance runner supplies isolated, dynamically mapped
/// endpoints through the `WALRUS_E2E_*` variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessConfig {
    /// PostgreSQL URL for the logical-replication source.
    pub source_url: String,
    /// PostgreSQL URL for durable Walrus control state.
    pub control_url: String,
    /// PostgreSQL URL for the DuckLake catalog.
    pub catalog_url: String,
    /// S3-compatible endpoint used for staged objects and DuckLake data.
    pub s3_endpoint: String,
    /// Socket address where the extractor exposes health probes.
    pub extractor_health_addr: String,
    /// Socket address where the transformer exposes health probes.
    pub transformer_health_addr: String,
    /// Compose container name used by MinIO chaos scenarios.
    pub minio_container: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            source_url: env_or("WALRUS_E2E_SOURCE_URL", DEFAULT_SOURCE_URL),
            control_url: env_or("WALRUS_E2E_CONTROL_URL", DEFAULT_CONTROL_URL),
            catalog_url: env_or("WALRUS_E2E_CATALOG_URL", DEFAULT_CATALOG_URL),
            s3_endpoint: env_or("WALRUS_E2E_S3_ENDPOINT", DEFAULT_S3_ENDPOINT),
            extractor_health_addr: env_or("WALRUS_E2E_EXTRACTOR_HEALTH_ADDR", "127.0.0.1:8130"),
            transformer_health_addr: env_or("WALRUS_E2E_TRANSFORMER_HEALTH_ADDR", "127.0.0.1:8131"),
            minio_container: env_or("WALRUS_E2E_MINIO_CONTAINER", DEFAULT_MINIO_CONTAINER),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Keep harness-only `WALRUS_E2E_*` settings out of the real binaries. Their configuration parser
/// deliberately rejects unknown `WALRUS_*` keys, while the test process needs these keys to select
/// its isolated backing services.
fn remove_harness_env(command: &mut Command) {
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("WALRUS_E2E_") {
            command.env_remove(key);
        }
    }
}

fn wide_primary_key_table_ddl() -> String {
    let columns = (1..=WIDE_PRIMARY_KEY_COLUMNS)
        .map(|index| format!("key_{index:02} integer NOT NULL"))
        .collect::<Vec<_>>()
        .join(", ");
    let keys = (1..=WIDE_PRIMARY_KEY_COLUMNS)
        .map(|index| format!("key_{index:02}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "DROP TABLE IF EXISTS public.wide_keys; \
         CREATE TABLE public.wide_keys ({columns}, payload text NOT NULL, PRIMARY KEY ({keys}));"
    )
}

/// A running walrus stack: the compose services (assumed up) plus a live `extractor` and `transformer` spawned
/// as child processes. `Drop` kills both — a leaked extractor holds the replication slot and blocks the next
/// run's bootstrap.
#[derive(Debug)]
pub struct Harness {
    extractor: Child,
    transformer: Child,
    source: sqlx::PgPool,
    control: sqlx::PgPool,
    runtime_dir: PathBuf,
    /// The extractor's captured stdout+stderr (its `tracing` log) — scraped for spill events.
    extractor_log: PathBuf,
    /// The `target/<profile>/` dir the binaries live in — kept so a crashed child can be respawned.
    bins: PathBuf,
    /// User-facing reload extraction limits supplied to every extractor process, including restarts.
    reload_extraction: ReloadExtractionConfig,
    /// Process-wide memory ceiling supplied to every extractor process, including restarts.
    max_inflight_bytes: u64,
    config: HarnessConfig,
    /// The epoch the extractor established (always 1 after the clean reset).
    pub epoch: i64,
}

/// The three independent extraction controls exposed by the extractor. Tests carry these explicitly so
/// a restart cannot silently fall back to different table, worker, or object-size limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReloadExtractionConfig {
    /// Maximum tables whose snapshot exports may overlap.
    pub max_concurrent_reloads: u64,
    /// Maximum COPY streams sharing one table's exported snapshot.
    pub reload_workers_per_table: u64,
    /// Records written to each completed remote object (apart from worker tails).
    pub reload_chunk_rows: u64,
}

impl Default for ReloadExtractionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_reloads: 2,
            reload_workers_per_table: 4,
            reload_chunk_rows: 10_000,
        }
    }
}

impl Harness {
    /// Reset control + source to a clean slate, then bring up both binaries and block until each reports
    /// `/ready`. Fails fast if either bootstrap errors.
    ///
    /// # Errors
    ///
    /// Returns an error if database reset or migration, source setup, child-process launch, or a
    /// readiness deadline fails. The backing databases may already have been reset when an error
    /// is returned.
    pub async fn start() -> Result<Self> {
        Self::start_inner(
            ReloadExtractionConfig::default(),
            None,
            DEFAULT_E2E_MAX_INFLIGHT_BYTES,
        )
        .await
    }

    /// Start the stack with explicit values for all three user-facing reload extraction controls.
    ///
    /// # Errors
    ///
    /// Returns an error for zero extraction controls, failed database setup or migration, a child
    /// launch failure, or a readiness timeout. Setup may have partially reset the test databases.
    pub async fn start_with_reload_extraction(
        reload_extraction: ReloadExtractionConfig,
    ) -> Result<Self> {
        Self::start_inner(
            reload_extraction,
            None,
            PARALLEL_RELOAD_E2E_MAX_INFLIGHT_BYTES,
        )
        .await
    }

    /// Start with rows committed before the slot and first reconciliation attempt exist. This is
    /// the black-box path for proving that non-empty first startup uses the same extraction engine
    /// and limits as a later repair.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid extraction controls, failed database or seed SQL, child launch,
    /// or readiness timeout. Seed SQL and earlier setup statements are not rolled back as a unit.
    pub async fn start_with_reload_extraction_and_source_seed(
        reload_extraction: ReloadExtractionConfig,
        source_seed: Option<&str>,
    ) -> Result<Self> {
        Self::start_inner(
            reload_extraction,
            source_seed,
            PARALLEL_RELOAD_E2E_MAX_INFLIGHT_BYTES,
        )
        .await
    }

    /// Start a clean stack with scenario-owned source setup applied before the replication slot is
    /// created. Tables created here participate in the initial consistent bootstrap snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when reset, migration, `source_setup`, child launch, or readiness fails.
    /// Statements committed before the failure remain in the disposable E2E databases.
    pub async fn start_scenario(source_setup: &str) -> Result<Self> {
        Self::start_inner(
            ReloadExtractionConfig::default(),
            Some(source_setup),
            DEFAULT_E2E_MAX_INFLIGHT_BYTES,
        )
        .await
    }

    async fn start_inner(
        reload_extraction: ReloadExtractionConfig,
        source_seed: Option<&str>,
        max_inflight_bytes: u64,
    ) -> Result<Self> {
        let config = HarnessConfig::default();
        anyhow::ensure!(
            reload_extraction.max_concurrent_reloads > 0
                && reload_extraction.reload_workers_per_table > 0
                && reload_extraction.reload_chunk_rows > 0,
            "reload extraction controls must all be non-zero"
        );
        let control = control::connect(&config.control_url)
            .await
            .context("connect control PG")?;
        // Fully reset control so `read_current_epoch` (MAX) yields a fresh epoch 1 — a leftover higher
        // epoch from another test would otherwise be resumed with no slot/registry behind it.
        sqlx::raw_sql(
            "DROP TABLE IF EXISTS
               public.walrus_table_integrity_recovery,
               public.walrus_stream_manifest_group,
               public.walrus_stream_txn_publication,
               public.walrus_manifest_publication_fence,
               public.walrus_table_reload_export_range,
               public.walrus_table_reload_marker,
               public.walrus_file_manifest,
               public.walrus_table_reload,
               public.walrus_table_ownership,
               public.walrus_transformer_checkpoint,
               public.walrus_ddl_manifest,
               public.walrus_schema_registry,
               public.walrus_replication_state
             CASCADE;
             DO $walrus_reset$
             DECLARE owned_function record;
             BEGIN
               FOR owned_function IN
                 SELECT proc.oid::regprocedure AS identity
                 FROM pg_catalog.pg_proc proc
                 JOIN pg_catalog.pg_namespace namespace ON namespace.oid = proc.pronamespace
                 WHERE namespace.nspname = 'public' AND proc.proname LIKE 'walrus_%'
               LOOP
                 EXECUTE format('DROP FUNCTION %s CASCADE', owned_function.identity);
               END LOOP;
             END
             $walrus_reset$;
             DROP TABLE IF EXISTS _sqlx_migrations;",
        )
        .execute(&control)
        .await
        .context("reset public control tables")?;
        control::run_migrations(&control)
            .await
            .context("control migrations")?;

        let source = control::connect(&config.source_url)
            .await
            .context("connect source PG")?;
        // Idempotent source-side setup (public.walrus_heartbeat / ddl_audit + DDL triggers), a clean `orders`,
        // and a dropped leftover slot so the extractor creates its own fresh one.
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0001_publication.sql"
        ))
        .execute(&source)
        .await
        .context("source 0001")?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0002_ddl_triggers.sql"
        ))
        .execute(&source)
        .await
        .context("source 0002")?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0003_reload_signal.sql"
        ))
        .execute(&source)
        .await
        .context("source 0003")?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0004_reload_event.sql"
        ))
        .execute(&source)
        .await
        .context("source 0004")?;
        // The wide fidelity table — one column per mapped type family + a TOAST-able `big`. It
        // must exist BEFORE the extractor bootstraps so the extractor registers it and the transformer owns it.
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS public.types_matrix ( \
                 id  int PRIMARY KEY, n numeric(10,4), j jsonb, u uuid, ts timestamptz, \
                 b bytea, iv interval, rng int4range, big text, s text); \
             ALTER TABLE public.types_matrix ALTER COLUMN big SET STORAGE EXTENDED;",
        )
        .execute(&source)
        .await
        .context("create types_matrix")?;
        // Boundary-width composite-key fixture. Recreate it so a prior interrupted run cannot leave
        // behind a different shape; all setup WAL precedes the fresh replication slot below.
        sqlx::raw_sql(&wide_primary_key_table_ddl())
            .execute(&source)
            .await
            .context("create wide composite-key fixture")?;
        // The single-table-reload fixtures. Like `types_matrix`, they must exist BEFORE
        // the extractor bootstraps so the extractor registers them and the transformer OWNS them (the transformer only
        // picks up tables at bootstrap — a table created after start is never owned). `q_target.n`
        // is the column the quarantine e2e narrows to int2; `rl1..3` are the scale/others tables.
        // Idle for every other e2e test (streamed, no asserts).
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS public.q_target (id int PRIMARY KEY, status text, n int); \
             CREATE TABLE IF NOT EXISTS public.rl1 (id int PRIMARY KEY, status text); \
             CREATE TABLE IF NOT EXISTS public.rl2 (id int PRIMARY KEY, status text); \
             CREATE TABLE IF NOT EXISTS public.rl3 (id int PRIMARY KEY, status text);",
        )
        .execute(&source)
        .await
        .context("create reload fixtures")?;
        // DDL transaction tests add these columns to `orders`. Normalize the persistent compose
        // source before dropping the old slot, so every test bootstraps from the same v1 shape and
        // none of this cleanup WAL belongs to the fresh epoch.
        sqlx::raw_sql(
            "ALTER TABLE public.orders \
                 DROP COLUMN IF EXISTS ddl_txn_extra, \
                 DROP COLUMN IF EXISTS ddl_stream_extra, \
                 DROP COLUMN IF EXISTS ddl_abort_extra, \
                 DROP COLUMN IF EXISTS ddl_savepoint_extra;",
        )
        .execute(&source)
        .await
        .context("normalize orders DDL test columns")?;
        // The quarantine recovery test narrows this fixture after correcting its out-of-range row.
        // Restore the persistent compose source before dropping the old slot so repeated local runs
        // always bootstrap the intended v1 INTEGER shape.
        sqlx::raw_sql("ALTER TABLE public.q_target ALTER COLUMN n TYPE INTEGER USING n::INTEGER;")
            .execute(&source)
            .await
            .context("normalize q_target quarantine-test type")?;
        sqlx::raw_sql(
            "TRUNCATE public.orders; TRUNCATE public.types_matrix; \
             TRUNCATE public.wide_keys; \
             TRUNCATE public.q_target; TRUNCATE public.rl1; TRUNCATE public.rl2; TRUNCATE public.rl3;",
        )
        .execute(&source)
        .await
        .context("reset source tables")?;
        sqlx::query(
            "SELECT pg_drop_replication_slot(slot_name) \
             FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .execute(&source)
        .await
        .context("reset source slot")?;
        if let Some(source_seed) = source_seed {
            sqlx::raw_sql(source_seed)
                .execute(&source)
                .await
                .context("seed source before first reconciliation")?;
        }

        let bins = target_dir()?;
        if env_or("WALRUS_E2E_SKIP_BUILD", "0") == "1" {
            for binary in ["walrus-extractor", "walrus-transformer"] {
                anyhow::ensure!(
                    bins.join(binary).is_file(),
                    "WALRUS_E2E_SKIP_BUILD=1 but {} is missing",
                    bins.join(binary).display()
                );
            }
        } else {
            build_bins(&bins).await?;
        }
        let catalog = control::connect(&config.catalog_url)
            .await
            .context("connect DuckLake catalog PG")?;
        sqlx::raw_sql("DROP SCHEMA IF EXISTS walrus_e2e CASCADE")
            .execute(&catalog)
            .await
            .context("reset DuckLake e2e metadata schema")?;
        migrate_ducklake(&bins, &config).await?;

        let runtime_dir = std::env::temp_dir().join(format!("walrus-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&runtime_dir);
        std::fs::create_dir_all(&runtime_dir)?;
        let extractor_log = runtime_dir.join("extractor.log");

        let extractor = spawn_extractor(
            &bins,
            &extractor_log,
            reload_extraction,
            max_inflight_bytes,
            &config,
        )?;
        wait_ready(&config.extractor_health_addr, Duration::from_secs(45))
            .await
            .context("extractor /ready")?;
        let transformer = spawn_transformer(&bins, &config)?;
        wait_ready(&config.transformer_health_addr, Duration::from_secs(90))
            .await
            .context("transformer /ready")?;

        Ok(Harness {
            extractor,
            transformer,
            source,
            control,
            runtime_dir,
            extractor_log,
            bins,
            reload_extraction,
            max_inflight_bytes,
            config,
            epoch: 1,
        })
    }

    /// The source Postgres pool — for tests that need multiple concurrent sessions (overlapping txns).
    pub const fn source_pool(&self) -> &sqlx::PgPool {
        &self.source
    }

    /// The control Postgres pool — for tests that read checkpoints / the manifest directly.
    pub const fn control_pool(&self) -> &sqlx::PgPool {
        &self.control
    }

    /// How many speculative spills the extractor has logged so far — an observable test probe showing
    /// that the `max_inflight_bytes` ceiling fired and open-txn memory stayed bounded.
    pub fn extractor_spill_count(&self) -> usize {
        std::fs::read_to_string(&self.extractor_log)
            .map(|s| s.matches("spilled open-txn buffer").count())
            .unwrap_or(0)
    }

    /// Poll [`Harness::extractor_spill_count`] until it reaches `min`, or the deadline elapses. Call this while
    /// the producing txn is STILL OPEN: holding the txn open is what lets the walsender read past
    /// `logical_decoding_work_mem` and stream it (a fast `BEGIN;…;COMMIT` can commit before it is decoded,
    /// so it decodes as a complete, non-streamed txn and never spills). Deterministic, not a fixed sleep.
    ///
    /// # Errors
    ///
    /// Returns an error if the observed spill count does not reach `min` before `deadline`.
    pub async fn await_spill(&self, min: usize, deadline: std::time::Duration) -> Result<usize> {
        let start = tokio::time::Instant::now();
        loop {
            let n = self.extractor_spill_count();
            if n >= min {
                return Ok(n);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("extractor spilled {n} < {min} within {deadline:?}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// The WAL bytes the replication slot is retaining (`restart_lsn` .. current) — bounded once a txn
    /// commits and is consumed.
    ///
    /// # Errors
    ///
    /// Returns an error when the source catalog query fails.
    pub async fn slot_retained_bytes(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .fetch_optional(&self.source)
        .await?
        .unwrap_or(0))
    }

    /// Run a SINGLE SQL statement on the SOURCE database; returns rows affected.
    ///
    /// # Errors
    ///
    /// Returns the source database error if `sql` cannot be executed. The statement may have taken
    /// effect before a connection-level error is observed.
    pub async fn source_exec(&self, sql: &str) -> Result<u64> {
        Ok(sqlx::query(sql)
            .execute(&self.source)
            .await?
            .rows_affected())
    }

    /// Run a MULTI-statement SQL batch on the SOURCE (simple query protocol) — e.g. `BEGIN; …; COMMIT`
    /// with savepoints, which the extended protocol of [`Harness::source_exec`] rejects.
    ///
    /// # Errors
    ///
    /// Returns the source database error if any statement fails. Effects committed by earlier
    /// statements in `sql` are not undone unless the batch itself supplies a transaction.
    pub async fn source_batch(&self, sql: &str) -> Result<()> {
        sqlx::raw_sql(sql).execute(&self.source).await?;
        Ok(())
    }

    /// Append a table-reload request to source Postgres through the production source-event API.
    ///
    /// The separate `tokio-postgres` connection is intentional: the request must enter through
    /// [`extractor::reload_event::request_table`], not through a test-only insert into control PG.
    ///
    /// # Errors
    ///
    /// Returns an error when the source connection, request commit, or connection driver fails.
    pub async fn request_table_reload(
        &self,
        request_id: uuid::Uuid,
        source_schema: &str,
        source_table: &str,
    ) -> Result<()> {
        let (client, connection) =
            tokio_postgres::connect(&self.config.source_url, tokio_postgres::NoTls)
                .await
                .context("connect source PG for reload request")?;
        let driver = tokio::spawn(connection);
        let requested = extractor::reload_event::request_table(
            &client,
            request_id,
            source_schema,
            source_table,
        )
        .await;
        drop(client);
        driver
            .await
            .context("join source reload-request connection driver")?
            .context("drive source reload-request connection")?;
        requested.context("append source table-reload request")
    }

    /// List S3 object keys under `<epoch>/<schema>/<table>/`.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 client cannot be configured or the prefix listing fails.
    pub async fn s3_list(&self, table: &str) -> Result<Vec<String>> {
        use object_store::{ObjectStore, aws::AmazonS3Builder};
        let store = AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_endpoint(&self.config.s3_endpoint)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()?;
        // The Parquet objects sit one delimiter level under `<epoch>/public/<table>/`, so a single
        // `list_with_delimiter` returns them directly — no streaming.
        let prefix = object_store::path::Path::from(format!("{}/public/{}", self.epoch, table));
        let res = store.list_with_delimiter(Some(&prefix)).await?;
        Ok(res
            .objects
            .into_iter()
            .map(|o| o.location.to_string())
            .collect())
    }

    /// The source's current WAL insert position — captured **before** a change as the watermark target
    /// the transformer's `transformed_lsn` must later cross.
    ///
    /// # Errors
    ///
    /// Returns an error when the source query fails or PostgreSQL returns an invalid LSN string.
    pub async fn source_wal_lsn(&self) -> Result<common::Lsn> {
        let s: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.source)
            .await?;
        s.parse().context("parse pg_current_wal_lsn")
    }

    /// Poll `transformer_checkpoint.transformed_lsn` for `table` until it passes `target` (every streamed
    /// change committed before `target` is now in the mirror) AND the queue is drained. Watermark-based,
    /// never a fixed sleep. `target` is a source LSN taken before the change, so only the streamed change
    /// — not the earlier snapshot — can cross it.
    ///
    /// # Errors
    ///
    /// Returns an error on a control-plane query failure or if the checkpoint and queue do not
    /// converge before `deadline`.
    pub async fn await_transformed_past(
        &self,
        table: &str,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<()> {
        self.await_table_transformed_past("public", table, target, deadline)
            .await
    }

    /// Schema-qualified form of [`Harness::await_transformed_past`] used by reusable acceptance
    /// scenarios. The queue and checkpoint predicates include both identity components.
    ///
    /// # Errors
    ///
    /// Returns an error on a control-plane query failure or when the table does not reach the
    /// target with a drained queue before `deadline`.
    pub async fn await_table_transformed_past(
        &self,
        schema: &str,
        table: &str,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            let pending: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM public.walrus_file_manifest \
                 WHERE epoch = $1 AND source_schema = $2 AND source_table = $3",
            )
            .bind(self.epoch)
            .bind(schema)
            .bind(table)
            .fetch_one(&self.control)
            .await?;
            let cp =
                control::read_checkpoint(&self.control, self.epoch.into(), schema, table).await?;
            if let Some(cp) = cp.as_ref()
                && pending == 0
                && cp.transformed_lsn > target
                && cp.transformed_lsn == cp.raw_appended_lsn
            {
                return Ok(());
            }
            if start.elapsed() > deadline {
                let mut extractor_tail = std::fs::read_to_string(&self.extractor_log)
                    .unwrap_or_else(|error| format!("<cannot read extractor log: {error}>"))
                    .lines()
                    .rev()
                    .take(20)
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                extractor_tail.reverse();
                anyhow::bail!(
                    "transformed_lsn for {schema}.{table} never passed {target} within {deadline:?}; \
                     pending={pending}, checkpoint={cp:?}; extractor log tail:\n{}",
                    extractor_tail.join("\n")
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Stop the transformer so its `.duckdb` file lock is released, then query the file. DuckDB is single-writer;
    /// reading after the transformer exits avoids fighting the lock.
    ///
    /// # Errors
    ///
    /// This best-effort helper currently returns `Ok(())` even if the child was already gone or
    /// could not be reaped; the `Result` is retained for harness API consistency.
    pub async fn stop_transformer(&mut self) -> Result<()> {
        let _ = self.transformer.start_kill();
        let _ = self.transformer.wait().await;
        Ok(())
    }

    /// **SIGKILL** the extractor — the *ungraceful* crash path, not the SIGTERM graceful drain.
    /// `tokio::process::Child::start_kill` sends `SIGKILL` (signal 9); `wait` reaps the zombie so the
    /// process is gone (and its walsender connection torn down) before we respawn.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system refuses to signal the extractor. A successfully
    /// signalled process remains killed even if later cleanup is interrupted.
    pub async fn kill_extractor(&mut self) -> Result<()> {
        self.extractor.start_kill().context("SIGKILL extractor")?;
        let _ = self.extractor.wait().await;
        Ok(())
    }

    /// Respawn the extractor fresh and block until `/ready`. After a `SIGKILL` the source still marks the
    /// replication slot **active** until it notices the dropped connection, and the replacement extractor
    /// issues `START_REPLICATION` with no retry — so wait for the slot to go inactive first (what a real
    /// orchestrator's backoff-restart achieves), then reap the old process so its health listener is
    /// definitely released before spawning. A healthy retained slot supplies the durable WAL floor for
    /// the new process's reconciled successor generation.
    ///
    /// # Errors
    ///
    /// Returns an error if slot inactivity times out, the old child cannot be reaped, the replacement
    /// cannot be launched, or it misses its readiness deadline. The old process may already be gone.
    pub async fn restart_extractor(&mut self) -> Result<()> {
        self.await_slot_inactive(Duration::from_secs(30)).await?;
        match tokio::time::timeout(Duration::from_secs(10), self.extractor.wait()).await {
            Ok(status) => {
                status.context("reap old extractor before restart")?;
            }
            Err(_) => {
                self.extractor
                    .start_kill()
                    .context("SIGKILL stale extractor before restart")?;
                self.extractor
                    .wait()
                    .await
                    .context("reap stale extractor before restart")?;
            }
        }
        self.extractor = spawn_extractor(
            &self.bins,
            &self.extractor_log,
            self.reload_extraction,
            self.max_inflight_bytes,
            &self.config,
        )?;
        wait_ready(&self.config.extractor_health_addr, Duration::from_secs(45))
            .await
            .context("extractor /ready after restart")
    }

    /// **SIGKILL** the transformer — ungraceful and distinct from the SIGTERM drain. Process
    /// death closes its DuckLake connections/catalog session and leaves the control lease row in place.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system refuses to signal the transformer. The durable
    /// control lease is intentionally not removed by this operation.
    pub async fn kill_transformer(&mut self) -> Result<()> {
        self.transformer
            .start_kill()
            .context("SIGKILL transformer")?;
        let _ = self.transformer.wait().await;
        Ok(())
    }

    /// Respawn the transformer fresh and block until `/ready`. It reuses `WALRUS_INSTANCE=e2e-transformer`, so
    /// `acquire_lease` sees the lease as **already ours** and reclaims it immediately (no TTL wait); the
    /// PostgreSQL drops the old session's advisory locks on `SIGKILL`. Resume is from the two
    /// persisted watermarks.
    ///
    /// # Errors
    ///
    /// Returns an error if the replacement process cannot be spawned or does not become ready
    /// within the standard deadline.
    pub async fn restart_transformer(&mut self) -> Result<()> {
        self.restart_transformer_with_deadline(Duration::from_secs(90))
            .await
    }

    /// Respawn the transformer with a caller-selected readiness deadline. Scale tests use this without
    /// weakening the ordinary 90-second startup bound for every other scenario.
    ///
    /// # Errors
    ///
    /// Returns an error if spawning fails or `/ready` does not succeed before `deadline`. A spawned
    /// child may still be running when the readiness error is returned.
    pub async fn restart_transformer_with_deadline(&mut self, deadline: Duration) -> Result<()> {
        self.transformer = spawn_transformer(&self.bins, &self.config)?;
        let Err(source) = wait_ready(&self.config.transformer_health_addr, deadline).await else {
            return Ok(());
        };
        let child_status = match self.transformer.try_wait() {
            Ok(None) => "running".to_string(),
            Ok(Some(status)) => format!("exited ({status})"),
            Err(error) => format!("unknown ({error})"),
        };
        let ready_probe =
            match http_get(&health_url(&self.config.transformer_health_addr, "/ready")).await {
                Ok((ok, body)) => format!("http_200={ok}, body={body}"),
                Err(error) => format!("failed: {error:#}"),
            };
        Err(source).with_context(|| {
            format!(
                "transformer /ready after restart; child={child_status}; final /ready probe: {ready_probe}"
            )
        })
    }

    /// Whether the transformer child is still running (`try_wait() == Ok(None)`). A lossy-cast QUARANTINE
    /// makes a table worker return `Err`, which cancels the token and drains the whole transformer — so
    /// this flips to `false`. The recovery test waits on it before requesting a reload.
    pub fn is_transformer_running(&mut self) -> bool {
        matches!(self.transformer.try_wait(), Ok(None))
    }

    /// Await the transformer child's FULL exit after quarantine — `wait()` reaps the
    /// process, so by the time this returns the transformer has released every table's lease and catalog
    /// lock. A plain `try_wait()` poll can report "exited" while the OS is still tearing the process
    /// down, which would let a `restart_transformer` race the old transformer's catalog session.
    ///
    /// # Errors
    ///
    /// Returns an error if the child cannot be waited on or does not exit before `deadline`.
    pub async fn await_transformer_exited(
        &mut self,
        deadline: Duration,
    ) -> Result<std::process::ExitStatus> {
        tokio::time::timeout(deadline, self.transformer.wait())
            .await
            .context("transformer did not exit (quarantine) in time")?
            .context("waiting on transformer exit")
    }

    /// Poll the source until the replication slot is `active = false` (the dead walsender cleaned up), so a
    /// fresh extractor can `START_REPLICATION` without hitting "replication slot is active".
    ///
    /// # Errors
    ///
    /// Returns an error if the source catalog query fails or the slot stays active through
    /// `deadline`.
    pub async fn await_slot_inactive(&self, deadline: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let active: Option<bool> =
                sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                    .bind(SLOT)
                    .fetch_optional(&self.source)
                    .await?;
            if active != Some(true) {
                return Ok(());
            }
            if start.elapsed() > deadline {
                anyhow::bail!("replication slot {SLOT} still active within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Poll `transformer_checkpoint.raw_appended_lsn` for `table` until it passes `target` — i.e. Phase A has
    /// appended the batch to `<table>_raw`, even if Phase B has not yet MERGEd it (the mid-MERGE window,
    /// where `transformed_lsn < raw_appended_lsn`).  uses this to crash the transformer *after append,
    /// before/ during merge*.
    ///
    /// # Errors
    ///
    /// Returns an error on a checkpoint query failure or if `raw_appended_lsn` does not pass
    /// `target` before `deadline`.
    pub async fn await_raw_appended_past(
        &self,
        table: &str,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            if let Some(cp) =
                control::read_checkpoint(&self.control, self.epoch.into(), "public", table).await?
                && cp.raw_appended_lsn > target
            {
                return Ok(());
            }
            if start.elapsed() > deadline {
                anyhow::bail!(
                    "raw_appended_lsn for {table} never passed {target} within {deadline:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Assert the transformer's DuckLake mirror `<table>_current` equals the current source
    /// `public.<table>` **row-by-row** (id + status), the effectively-once convergence check.
    ///
    /// # Errors
    ///
    /// Returns an error if either database cannot be queried or if row count, identity, or status
    /// differs. This assertion is read-only.
    pub async fn assert_mirror_equals_source(&self, table: &str) -> Result<()> {
        let src: Vec<(i32, Option<String>)> = sqlx::query_as(&format!(
            "SELECT id, status FROM public.{table} ORDER BY id"
        ))
        .fetch_all(&self.source)
        .await
        .context("read source rows")?;
        let mirror = self.duckdb_pairs(
            table,
            &format!("SELECT id, status FROM {table}_current ORDER BY id"),
        )?;
        anyhow::ensure!(
            src.len() == mirror.len(),
            "row count mismatch: source has {} rows, mirror has {}",
            src.len(),
            mirror.len()
        );
        for (s, m) in src.iter().zip(mirror.iter()) {
            anyhow::ensure!(s == m, "mirror row {m:?} != source row {s:?}");
        }
        Ok(())
    }

    /// Read `(id, status)` pairs through a direct read-only DuckLake attachment.
    fn duckdb_pairs(&self, table: &str, sql: &str) -> Result<Vec<(i32, Option<String>)>> {
        let conn = ducklake_reader(&self.config, table)?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, i32>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Attach DuckLake read-only and collect the first column of each row as a string.
    ///
    /// # Errors
    ///
    /// Returns an error if the DuckLake attachment, statement preparation, query, or row decoding
    /// fails.
    pub fn duckdb_rows(&self, table: &str, sql: &str) -> Result<Vec<String>> {
        let conn = ducklake_reader(&self.config, table)?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// A single integer scalar from a direct read-only DuckLake attachment.
    ///
    /// # Errors
    ///
    /// Returns an error if the DuckLake attachment or query fails, or the first column is not an
    /// `i64`.
    pub fn duckdb_scalar(&self, table: &str, sql: &str) -> Result<i64> {
        let conn = ducklake_reader(&self.config, table)?;
        Ok(conn.query_row(sql, [], |r| r.get(0))?)
    }

    // ---- slot-liveness chaos (S3 stall, slot status, heartbeat, health) ----------------

    /// Stall the extractor's durability by pausing MinIO (`docker pause`) — every S3 PUT then hangs, so the
    /// extractor cannot finish a durable flush: `confirmed_flush_lsn` freezes and the slot's `restart_lsn`
    /// is pinned, so retained WAL grows (the WAL-runaway). Pausing the **transformer** would NOT do this —
    /// it doesn't own the slot; the extractor advances `confirmed_flush` on its OWN S3 durability (§1.5/§1.9),
    /// so stalling S3 is the only thing that retains source WAL. The keepalive fix keeps the
    /// walsender connected throughout.
    ///
    /// # Errors
    ///
    /// Returns an error if Docker cannot pause the configured MinIO container. A successful pause
    /// persists until [`Harness::unstall_s3`] or harness cleanup runs.
    pub async fn stall_s3(&self) -> Result<()> {
        docker(&["pause", &self.config.minio_container]).await
    }

    /// Resume S3 (`docker unpause` MinIO) — the stalled PUT completes and the extractor drains the backlog.
    ///
    /// # Errors
    ///
    /// Returns an error if Docker cannot unpause the configured MinIO container.
    pub async fn unstall_s3(&self) -> Result<()> {
        docker(&["unpause", &self.config.minio_container]).await
    }

    /// The slot's `confirmed_flush_lsn` — the durable, slot-advancing LSN (moves only after S3 + manifest
    /// durability, or an idle heartbeat commit; never on a stalled flush).
    ///
    /// # Errors
    ///
    /// Returns an error if the source query fails, the replication slot is absent, or its LSN text
    /// is malformed.
    pub async fn slot_confirmed_flush(&self) -> Result<common::Lsn> {
        let s: Option<String> = sqlx::query_scalar(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .fetch_optional(&self.source)
        .await?;
        s.context("replication slot not found")?
            .parse()
            .context("parse confirmed_flush_lsn")
    }

    /// The slot's `restart_lsn` — the oldest WAL the slot still needs. Follows `confirmed_flush` once a
    /// beat or durable flush advances the latter; a stuck `restart_lsn` is what retained WAL measures.
    ///
    /// # Errors
    ///
    /// Returns an error if the source query fails, the replication slot is absent, or its LSN text
    /// is malformed.
    pub async fn slot_restart_lsn(&self) -> Result<common::Lsn> {
        let s: Option<String> = sqlx::query_scalar(
            "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .fetch_optional(&self.source)
        .await?;
        s.context("replication slot not found")?
            .parse()
            .context("parse restart_lsn")
    }

    /// Whether a walsender is attached to the slot (`active = true`) — proof the connection is live. A
    /// severed walsender (e.g. `wal_sender_timeout` with no keepalive) flips this to `false`.
    ///
    /// # Errors
    ///
    /// Returns an error when the source catalog query fails.
    pub async fn is_slot_active(&self) -> Result<bool> {
        let active: Option<bool> =
            sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                .bind(SLOT)
                .fetch_optional(&self.source)
                .await?;
        Ok(active == Some(true))
    }

    /// Poll `slot_retained_bytes` until it exceeds `threshold` (the retained-WAL alert condition trips),
    /// or the deadline elapses. Watermark-based, not a fixed sleep.
    ///
    /// # Errors
    ///
    /// Returns an error if a retained-byte query fails or the threshold is not exceeded before
    /// `deadline`.
    pub async fn await_retained_bytes_over(
        &self,
        threshold: i64,
        deadline: Duration,
    ) -> Result<i64> {
        let start = Instant::now();
        loop {
            let n = self.slot_retained_bytes().await?;
            if n > threshold {
                return Ok(n);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("retained WAL {n} never exceeded {threshold} within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Poll the slot's `confirmed_flush_lsn` until it passes `target`, or the deadline elapses.
    ///
    /// # Errors
    ///
    /// Returns an error if the slot cannot be read or its confirmed flush position does not pass
    /// `target` before `deadline`.
    pub async fn await_confirmed_flush_past(
        &self,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<common::Lsn> {
        let start = Instant::now();
        loop {
            let cf = self.slot_confirmed_flush().await?;
            if cf > target {
                return Ok(cf);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("confirmed_flush {cf} never passed {target} within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// How many times the extractor logged `needle` (log-scrape, like [`Harness::extractor_spill_count`]).
    fn grep_extractor_log(&self, needle: &str) -> usize {
        std::fs::read_to_string(&self.extractor_log)
            .map(|s| s.matches(needle).count())
            .unwrap_or(0)
    }

    /// How many idle heartbeats the extractor has FIRED (idle publication → wrote `public.walrus_heartbeat`).
    pub fn heartbeat_beats(&self) -> usize {
        self.grep_extractor_log("fired idle heartbeat")
    }

    /// How many heartbeat round-trips the extractor has OBSERVED (a `beat_seq` returned through the stream —
    /// the slot-consume liveness signal that feeds the `/ready` `degraded` field).
    pub fn heartbeat_roundtrips(&self) -> usize {
        self.grep_extractor_log("heartbeat round-trip observed")
    }

    /// Whether the extractor log contains `needle` — e.g. a reconnect/sever error the keepalive path must
    /// prevent (`"source closed the replication connection"`).
    pub fn extractor_log_contains(&self, needle: &str) -> bool {
        self.grep_extractor_log(needle) > 0
    }

    /// Return the newest `max_lines` from the extractor log in chronological order. Long-running E2Es
    /// use this in failure messages so a child-process exit reports its classified cause instead
    /// of timing out later on the control-plane symptom.
    pub fn extractor_log_tail(&self, max_lines: usize) -> String {
        let mut tail = std::fs::read_to_string(&self.extractor_log)
            .unwrap_or_else(|error| format!("<cannot read extractor log: {error}>"))
            .lines()
            .rev()
            .take(max_lines)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        tail.reverse();
        tail.join("\n")
    }

    /// Poll [`Harness::heartbeat_roundtrips`] until it reaches `min`, or the deadline elapses.
    ///
    /// # Errors
    ///
    /// Returns an error if the observed heartbeat count does not reach `min` before `deadline`.
    pub async fn await_heartbeat_roundtrip(&self, min: usize, deadline: Duration) -> Result<usize> {
        let start = Instant::now();
        loop {
            let n = self.heartbeat_roundtrips();
            if n >= min {
                return Ok(n);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("heartbeat round-trips {n} < {min} within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// GET the extractor's `/ready`, returning `(ready, degraded)`. Per `walrus-extractor.md` §4.3, `degraded`
    /// is a FIELD, never a readiness gate — a catching-up extractor is `degraded` yet still `ready` (HTTP 200).
    /// `ready` is the HTTP-200 status (equals the body's `ready`); `degraded` is the body's field.
    ///
    /// # Errors
    ///
    /// Returns an error if the readiness endpoint cannot be reached or its response is not valid
    /// JSON.
    pub async fn extractor_ready(&self) -> Result<(bool, bool)> {
        let (ok, body) =
            http_get(&health_url(&self.config.extractor_health_addr, "/ready")).await?;
        let v: serde_json::Value =
            serde_json::from_str(body.trim()).context("parse /ready JSON body")?;
        Ok((ok, v["degraded"].as_bool().unwrap_or(false)))
    }

    /// Whether the extractor child is still running (has not exited) — proof the walsender did not sever it
    /// (a severed replication connection makes the extractor's `next()` error and the process exit).
    pub fn is_extractor_running(&mut self) -> bool {
        matches!(self.extractor.try_wait(), Ok(None))
    }

    // ---- successor generations and destructive slot recovery ----------------------------

    /// The current (highest) epoch in `replication_state`, or 1 if none yet. A new extractor process opens
    /// a reconciled successor even when it retains a healthy slot; slot loss additionally makes that
    /// successor a destructive total restart.
    ///
    /// # Errors
    ///
    /// Returns an error when the control-plane epoch query fails.
    pub async fn current_epoch(&self) -> Result<i64> {
        Ok(control::read_current_epoch(&self.control)
            .await?
            .map(|s| i64::from(s.epoch))
            .unwrap_or(1))
    }

    /// Re-read the current epoch into `self.epoch` after a successor opens, so epoch-namespaced reads
    /// (`s3_list`, `await_transformed_past`, …) target the NEW generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the control-plane epoch query fails. `self.epoch` is changed only after
    /// a successful read.
    pub async fn refresh_epoch(&mut self) -> Result<i64> {
        self.epoch = self.current_epoch().await?;
        Ok(self.epoch)
    }

    /// Poll `current_epoch` until it exceeds `from`, or the deadline elapses.
    ///
    /// # Errors
    ///
    /// Returns an error if an epoch query fails or the epoch does not advance before `deadline`.
    pub async fn await_epoch_past(&self, from: i64, deadline: Duration) -> Result<i64> {
        let start = Instant::now();
        loop {
            let e = self.current_epoch().await?;
            if e > from {
                return Ok(e);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("epoch never advanced past {from} within {deadline:?} (still {e})");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// DROP the source replication slot — the total-restart trigger: its WAL history is gone, so on
    /// restart the extractor classifies the slot `Absent` and bumps the epoch. A slot cannot be dropped while a
    /// walsender is attached, so terminate any attached one and wait for inactivity first.
    ///
    /// # Errors
    ///
    /// Returns an error if terminating the walsender, waiting for inactivity, or dropping the slot
    /// fails. The walsender may already be terminated if a later step fails.
    pub async fn drop_slot(&self) -> Result<()> {
        sqlx::raw_sql(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL;"
        ))
        .execute(&self.source)
        .await?;
        self.await_slot_inactive(Duration::from_secs(30)).await?;
        sqlx::raw_sql(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') \
             FROM pg_replication_slots WHERE slot_name = '{SLOT}';"
        ))
        .execute(&self.source)
        .await?;
        Ok(())
    }

    /// Terminate the extractor's walsender backend WITHOUT dropping the slot. The slot survives, so a new
    /// extractor process retains its WAL floor and opens a reconciled successor without destructive slot
    /// replacement or a `total_restart` intent.
    ///
    /// # Errors
    ///
    /// Returns an error if the source cannot execute the backend-termination query.
    pub async fn terminate_walsender(&self) -> Result<()> {
        sqlx::raw_sql(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL;"
        ))
        .execute(&self.source)
        .await?;
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Best-effort kill both bins — a leaked extractor pins the slot and blocks the next bootstrap.
        let _ = self.extractor.start_kill();
        let _ = self.transformer.start_kill();
        // Undo an S3 stall a panicking test may have left in place, or the next run's extractor bootstrap
        // hangs on a paused MinIO.
        // Quiet: `unpause` errors harmlessly when the container is not paused (the common case).
        let _ = std::process::Command::new("docker")
            .args(["unpause", &self.config.minio_container])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::fs::remove_dir_all(&self.runtime_dir);
    }
}

/// The `target/<profile>/` directory holding the sibling binaries (next to this test binary).
fn target_dir() -> Result<PathBuf> {
    // .../target/<profile>/deps/<thisbin> → up two = target/<profile>/
    // Deliberately not an `expect`: an unresolvable `current_exe` is an environment failure (the test
    // binary was moved or unlinked mid-run), not a violated invariant of ours, so it joins the
    // harness's anyhow chain like every other setup step instead of panicking without a cause.
    let mut p = std::env::current_exe().context("resolve current_exe for target/<profile>")?;
    p.pop(); // deps
    p.pop(); // <profile>
    Ok(p)
}

async fn build_bins(_target: &std::path::Path) -> Result<()> {
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            "extractor",
            "--bin",
            "walrus-extractor",
            "-p",
            "transformer",
            "--bin",
            "walrus-transformer",
        ])
        .status()
        .await
        .context("cargo build bins")?;
    anyhow::ensure!(status.success(), "cargo build of the bins failed");
    Ok(())
}

async fn migrate_ducklake(bins: &std::path::Path, config: &HarnessConfig) -> Result<()> {
    let mut command = Command::new(bins.join("walrus-transformer"));
    command
        .arg("--migrate-ducklake-catalog")
        .env(
            "WALRUS_CONFIG",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../walrus.yaml"),
        )
        .env("WALRUS_CONTROL_DB_URL", &config.control_url)
        .env("WALRUS_OBJECT_STORE__BUCKET", BUCKET)
        .env("WALRUS_OBJECT_STORE__ENDPOINT", &config.s3_endpoint)
        .env("WALRUS_OBJECT_STORE__REGION", "us-east-1")
        .env("WALRUS_INSTANCE", "e2e-transformer-0")
        .env("WALRUS_DUCKLAKE__CATALOG_URL", &config.catalog_url)
        .env("WALRUS_DUCKLAKE__METADATA_SCHEMA", DUCKLAKE_SCHEMA)
        .env("WALRUS_DUCKLAKE__DATA_PATH", DUCKLAKE_DATA)
        .env("WALRUS_DUCKLAKE__INSTALL_EXTENSIONS", "true")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin");
    remove_harness_env(&mut command);
    let status = command
        .status()
        .await
        .context("run DuckLake catalog migration")?;
    anyhow::ensure!(status.success(), "DuckLake catalog migration failed");
    Ok(())
}

fn ducklake_reader(config: &HarnessConfig, table: &str) -> Result<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory().context("open DuckLake reader")?;
    let s3_endpoint = config
        .s3_endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let use_ssl = config.s3_endpoint.starts_with("https://");
    conn.execute_batch(&format!(
        r#"
        INSTALL json; INSTALL httpfs; INSTALL aws; INSTALL postgres; INSTALL ducklake;
        LOAD json; LOAD httpfs; LOAD aws; LOAD postgres; LOAD ducklake;
        CREATE OR REPLACE SECRET e2e_s3 (
            TYPE s3, PROVIDER config, KEY_ID 'minioadmin', SECRET 'minioadmin',
            REGION 'us-east-1', ENDPOINT {s3_endpoint}, URL_STYLE 'path', USE_SSL {use_ssl}
        );
        CREATE OR REPLACE SECRET e2e_catalog (
            TYPE postgres,
            URI {catalog_url}
        );
        ATTACH 'ducklake:postgres:' AS walrus (
            META_SECRET 'e2e_catalog', METADATA_SCHEMA 'walrus_e2e',
            META_SCHEMA 'walrus_e2e', CREATE_IF_NOT_EXISTS false, READ_ONLY
        );
        "#,
        s3_endpoint = s3_endpoint.to_quoted_literal(),
        catalog_url = config.catalog_url.to_quoted_literal(),
    ))
    .context("attach DuckLake reader")?;
    let key = format!("public\0{table}");
    let schema = format!(
        "_walrus_{}",
        uuid::Uuid::new_v5(&TABLE_NAMESPACE, key.as_bytes()).simple()
    );
    conn.execute_batch(&format!("USE walrus.{schema};"))
        .with_context(|| format!("select DuckLake namespace for public.{table}"))?;
    Ok(conn)
}

fn spawn_extractor(
    bins: &std::path::Path,
    log: &std::path::Path,
    reload_extraction: ReloadExtractionConfig,
    max_inflight_bytes: u64,
    config: &HarnessConfig,
) -> Result<Child> {
    // The extractor's `tracing` fmt layer writes to STDOUT (its spill/durability events live there); config
    // errors + panics go to STDERR. Capture BOTH into `extractor.log` (two handles onto one file) so
    // [`Harness::extractor_spill_count`] can scrape the spill events AND a startup failure is still visible.
    let stdout = std::fs::File::create(log).context("create extractor log")?;
    let stderr = stdout.try_clone().context("clone extractor log handle")?;
    let mut command = Command::new(bins.join("walrus-extractor"));
    command
        .stdout(std::process::Stdio::from(stdout))
        .env(
            "WALRUS_CONFIG",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../walrus.yaml"),
        )
        .env("WALRUS_SOURCE_DB_URL", &config.source_url)
        .env("WALRUS_CONTROL_DB_URL", &config.control_url)
        .env("WALRUS_OBJECT_STORE__BUCKET", BUCKET)
        .env("WALRUS_OBJECT_STORE__ENDPOINT", &config.s3_endpoint)
        .env("WALRUS_OBJECT_STORE__REGION", "us-east-1")
        .env("WALRUS_INSTANCE", "e2e-extractor")
        .env("WALRUS_SLOT_NAME", SLOT)
        .env("WALRUS_PUBLICATION_NAME", "walrus_pub")
        .env("WALRUS_MANAGE_PUBLICATION", "false")
        .env("WALRUS_MAX_FILL", "1s")
        .env("WALRUS_MAX_ROWS", "100000")
        // Ordinary E2Es use a 64-KiB aggregate ceiling so a few thousand rows in one open txn spill.
        // Explicit reload-scale E2Es use an ample ceiling so their configured COPY routes can overlap.
        // `max_bytes` (per-batch) must stay ≤ the aggregate ceiling.
        .env("WALRUS_MAX_BYTES", "32768")
        .env("WALRUS_MAX_INFLIGHT_BYTES", max_inflight_bytes.to_string())
        .env(
            "WALRUS_MAX_CONCURRENT_RELOADS",
            reload_extraction.max_concurrent_reloads.to_string(),
        )
        .env(
            "WALRUS_RELOAD_WORKERS_PER_TABLE",
            reload_extraction.reload_workers_per_table.to_string(),
        )
        .env(
            "WALRUS_RELOAD_CHUNK_ROWS",
            reload_extraction.reload_chunk_rows.to_string(),
        )
        .env("WALRUS_HEARTBEAT_IDLE_AFTER", "1s")
        .env("WALRUS_STARTUP_DEADLINE", "30s")
        .env("WALRUS_HEALTH_ADDR", &config.extractor_health_addr)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .stderr(std::process::Stdio::from(stderr))
        .kill_on_drop(true);
    remove_harness_env(&mut command);
    command.spawn().context("spawn walrus-extractor")
}

fn spawn_transformer(bins: &std::path::Path, config: &HarnessConfig) -> Result<Child> {
    // Inherit the test's stdout/stderr (the transformer's `tracing` log) so a bootstrap failure is
    // visible — cargo test surfaces a failed test's captured output, which is how a CI failure is
    // diagnosed. (An earlier `transformer.log` file-redirect hid the reason from the CI job log.)
    let mut command = Command::new(bins.join("walrus-transformer"));
    command
        .env(
            "WALRUS_CONFIG",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../walrus.yaml"),
        )
        .env("WALRUS_CONTROL_DB_URL", &config.control_url)
        .env("WALRUS_OBJECT_STORE__BUCKET", BUCKET)
        .env("WALRUS_OBJECT_STORE__ENDPOINT", &config.s3_endpoint)
        .env("WALRUS_OBJECT_STORE__REGION", "us-east-1")
        .env("WALRUS_INSTANCE", "e2e-transformer-0")
        .env("WALRUS_DUCKLAKE__CATALOG_URL", &config.catalog_url)
        .env("WALRUS_DUCKLAKE__METADATA_SCHEMA", DUCKLAKE_SCHEMA)
        .env("WALRUS_DUCKLAKE__DATA_PATH", DUCKLAKE_DATA)
        .env("WALRUS_DUCKLAKE__INSTALL_EXTENSIONS", "true")
        .env("WALRUS_POLL_INTERVAL", "1s")
        // Generous for a cold CI runner (a dev-profile binary bootstrapping 6 tables).
        .env("WALRUS_STARTUP_DEADLINE", "90s")
        .env("WALRUS_HEALTH_ADDR", &config.transformer_health_addr)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .kill_on_drop(true);
    remove_harness_env(&mut command);
    command.spawn().context("spawn walrus-transformer")
}

/// Poll a `/ready` endpoint until it answers 200 or the deadline elapses.
async fn wait_ready(base: &str, deadline: Duration) -> Result<()> {
    let socket = base
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/');
    let url = health_url(base, "/ready");
    let start = Instant::now();
    loop {
        if let Ok(conn) = tokio::net::TcpStream::connect(socket).await {
            drop(conn);
            // Minimal HTTP GET; treat "200" in the status line as ready.
            if http_get_ok(&url).await {
                return Ok(());
            }
        }
        if start.elapsed() > deadline {
            anyhow::bail!("{url} never became ready within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn health_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.starts_with("http://") || base.starts_with("https://") {
        format!("{base}{path}")
    } else {
        format!("http://{base}{path}")
    }
}

/// A dependency-free HTTP GET returning true iff the status line is `200`.
async fn http_get_ok(url: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.trim_start_matches("http://");
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let Ok(mut stream) = tokio::net::TcpStream::connect(authority).await else {
        return false;
    };
    let req = format!("GET {path} HTTP/1.0\r\nHost: {authority}\r\n\r\n");
    if stream.write_all(req.as_bytes()).await.is_err() {
        return false;
    }
    let mut buf = Vec::new();
    if stream.read_to_end(&mut buf).await.is_err() {
        return false;
    }
    String::from_utf8_lossy(&buf).starts_with("HTTP/1.0 200")
        || String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 200")
}

/// A dependency-free HTTP GET returning `(status_is_200, body)` — used to read the `/ready` JSON body
/// (`{ready, degraded}`), which [`http_get_ok`] discards.
async fn http_get(url: &str) -> Result<(bool, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.trim_start_matches("http://");
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .context("connect for GET")?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: {authority}\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .context("write GET")?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .context("read GET response")?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let ok = text.starts_with("HTTP/1.0 200") || text.starts_with("HTTP/1.1 200");
    // The body follows the blank line after the headers.
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok((ok, body))
}

/// Run a `docker` subcommand (e.g. `pause`/`unpause` the MinIO container) and require success.
async fn docker(args: &[&str]) -> Result<()> {
    let status = Command::new("docker")
        .args(args)
        .status()
        .await
        .with_context(|| format!("run `docker {}`", args.join(" ")))?;
    anyhow::ensure!(status.success(), "`docker {}` failed", args.join(" "));
    Ok(())
}
