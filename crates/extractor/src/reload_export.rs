//! The streaming, parallel reload export engine (reload H1/H2, §5 step 3).
//!
//! Each attempt first appends a start event and waits for its decoded commit `F`. One ordinary SQL
//! connection then exports a read-only `REPEATABLE READ` snapshot `S`; up to the configured number
//! of ordinary SQL workers import that exact snapshot and drain disjoint CTID page ranges. These
//! workers are not replication connections and never create replication slots. Rows flow from
//! PostgreSQL binary `COPY` into small Arrow batches and directly into multipart Parquet uploads,
//! with the remote write awaited before `COPY` is polled again.
//!
//! After every remote object and its manifest are durable, the coordinator commits the snapshot and
//! appends an end event `H`; decode force-flushes this table's WAL and persists the end marker before
//! resolving the exporter. A PostgreSQL snapshot is connection-local: an adopted attempt without
//! durable H is superseded by a fresh attempt/F rather than trying to resume physical ranges.
//!
//! **Why the early stamp converges (the whole proof):** one consistent snapshot `S`, with
//! `F < S < H`, supplies a complete row anchor. WAL changes in `(F, H]` outrank the F-stamped
//! baseline, including old-key deletes/new-key upserts and unchanged-TOAST updates. Transactions
//! after `H` stay queued for the normal live path after the atomic shadow cutover. TRUNCATE is a
//! table-level WAL boundary, so the overlay cannot leave ghosts.
//!
//! **Object-vs-manifest ordering:** each remote object's manifest insert and the attempt's completed
//! object count share ONE control-pg transaction. A crash between object durability and that commit
//! re-exports rows — duplicates the transformer algebra removes. Recording progress first would create a
//! gap nothing could heal. Duplicates are safe; gaps are not.

use crate::reload_event::{
    FenceEmission, FencePhase, FenceSpec, FenceTarget, FenceTimeouts, FenceWaiters,
};
use crate::staging::ParquetStager;
use anyhow::Context;
use common::sql::{SqlIdent, SqlStrExt};
use common::{
    EpochNo, ExtractorMeta, Kind, Lsn, Op, PgRelation, Redacted, ReloadId, SchemaVersionNo,
    TupleValue, UtcTimestamp,
};
use futures_util::StreamExt as _;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_postgres::NoTls;
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::types::Type;
use tracing::Instrument as _;
use uuid::Uuid;

/// Bound how long one queued schema-fence lock may wait behind structural DDL before it retries.
const FENCE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
/// Canonicalize every Postgres type-output function that feeds the Arrow text parsers. Transaction
/// scope prevents reloads from changing the connection's unrelated fence behavior.
const CANONICAL_COPY_GUCS_SQL: &str = "SET LOCAL DateStyle = 'ISO, YMD'; \
    SET LOCAL IntervalStyle = 'postgres'; \
    SET LOCAL bytea_output = 'hex'; \
    SET LOCAL extra_float_digits = 3; \
    SET LOCAL TimeZone = 'UTC';";

fn coordinator_snapshot_setup_sql() -> String {
    format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; \
         {CANONICAL_COPY_GUCS_SQL} \
         SET LOCAL row_security = off; \
         SET LOCAL statement_timeout = 0; \
         SET LOCAL idle_in_transaction_session_timeout = 0; \
         SET LOCAL lock_timeout = '5s'"
    )
}

/// How a whole [`ChunkExporter::run`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The table drained at this attempt's frozen schema — the export is done. `final_lsn` is `H`:
    /// the explicit end-event commit, `>=` the shared baseline `F`. The controller flips
    /// `export_complete(H)`; the transformer then flips
    /// `complete` once `transformed_lsn >= H`.
    Drained {
        /// Final source WAL fence covered by the export.
        final_lsn: Lsn,
    },
    /// DDL bumped the table's structural `schema_version` past the frozen one between chunks: this
    /// attempt is invalid and the controller must restart it at `new_version` (reload H9).
    SchemaChanged {
        /// Registry version observed after the frozen export version.
        new_version: SchemaVersionNo,
    },
    /// This exporter was adopted after losing ownership of its connection-local source snapshot.
    /// Even an attempt with zero completed files cannot safely be reused: the expired prior exporter may
    /// still emit H from an older snapshot. The controller atomically fails/purges this attempt and
    /// creates a fresh lease-carrying successor with a fresh F.
    SnapshotLost,
}

/// A structural change discovered while constructing an exporter or establishing its initial
/// fence. This is a control-flow outcome for the controller's bounded DDL restart loop, not a
/// terminal exporter failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("source shape changed before reload export started (new schema version {new_version})")]
pub struct ConnectSchemaChanged {
    /// Registry version that supersedes the attempt's frozen version.
    pub new_version: SchemaVersionNo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndFenceOutcome {
    Durable(Lsn),
    SchemaChanged(SchemaVersionNo),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotExportOutcome {
    Drained { rows: u64 },
    SchemaChanged { new_version: SchemaVersionNo },
}

/// Has the table's structural `schema_version` moved past the reload's `frozen` version? Returns
/// the new version if so, else `None`. Deliberately compares the REGISTRY's version — which bumps
/// only on structural DDL (a decoded Relation message), so metadata-only DDL (`COMMENT
/// ON`) never trips it — and never restarts backwards (`latest < frozen` is a stale read). Pure so
/// the restart trigger unit-tests without a database.
fn version_changed(
    frozen: SchemaVersionNo,
    latest: Option<SchemaVersionNo>,
) -> Option<SchemaVersionNo> {
    match latest {
        Some(v) if v > frozen => Some(v),
        _ => None,
    }
}

/// Everything the exporter needs beyond the reload row itself.
#[derive(Clone, Debug)]
pub struct ChunkExportConfig {
    /// Maximum rows per completed remote reload object. Non-zero, or no object could complete.
    pub chunk_rows: NonZeroU64,
    /// Maximum estimated Arrow bytes retained by one worker before it must flush. This is derived
    /// from the process-wide in-flight ceiling and the configured table/worker product; it is not
    /// a fourth operator-facing reload control.
    pub router_batch_bytes: NonZeroU64,
    /// Process-wide admission for memory-heavy COPY -> Parquet pipelines. Workers still establish
    /// and import their ordinary SQL snapshots together, but only this many may allocate Arrow and
    /// multipart buffers at once across every table reload.
    pub worker_admission: ReloadWorkerAdmission,
    /// Maximum ordinary SQL COPY workers used for this table, including the snapshot coordinator.
    pub workers_per_table: NonZeroUsize,
    /// How long a start/end fence waits to return through the decode loop.
    pub echo_timeout: Duration,
    /// This pod's identity, written as the reload's `lease_holder`.
    pub instance: String,
    /// The generation the exported chunks are stamped with.
    pub epoch: EpochNo,
    /// The logical publication whose complete target coverage both fences revalidate.
    pub publication_name: String,
}

/// Shared admission gate for the memory-heavy portion of reload workers.
///
/// This is deliberately an internal policy object rather than an operator-facing fourth reload
/// knob. The permit count is derived from `max_inflight_bytes` and the configured table/worker
/// ceiling; callers clone the same value into every exporter.
#[derive(Clone, Debug)]
pub struct ReloadWorkerAdmission {
    permits: Arc<Semaphore>,
    process_budget: Arc<crate::memory::ProcessMemoryBudget>,
}

impl ReloadWorkerAdmission {
    /// Build one process-wide reload worker gate.
    #[must_use]
    pub fn new(limit: NonZeroUsize) -> Self {
        let allowance = u64::try_from(limit.get())
            .unwrap_or(u64::MAX)
            .saturating_mul(RELOAD_WORKER_MEMORY_RESERVATION_BYTES);
        let allowance = NonZeroU64::new(allowance).unwrap_or(NonZeroU64::MAX);
        Self::with_process_budget(
            limit,
            Arc::new(crate::memory::ProcessMemoryBudget::new(allowance)),
        )
    }

    pub(crate) fn with_process_budget(
        limit: NonZeroUsize,
        process_budget: Arc<crate::memory::ProcessMemoryBudget>,
    ) -> Self {
        // Keep the constructor safe independently of the production config derivation. Tokio
        // panics when `Semaphore::new` is handed more than `MAX_PERMITS`; callers constructing
        // this reusable policy object directly must not be able to turn an extreme, otherwise
        // well-typed count into a process abort.
        let permits = limit.get().min(Semaphore::MAX_PERMITS);
        Self {
            permits: Arc::new(Semaphore::new(permits)),
            process_budget,
        }
    }

    async fn acquire(&self) -> anyhow::Result<ReloadWorkerPermit> {
        let route = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .context("reload worker memory-admission gate closed")?;
        let memory = self
            .process_budget
            .reserve_reload(
                NonZeroU64::new(RELOAD_WORKER_MEMORY_RESERVATION_BYTES).unwrap_or(NonZeroU64::MIN),
            )
            .await;
        Ok(ReloadWorkerPermit {
            _route: route,
            _memory: memory,
        })
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

#[derive(Debug)]
struct ReloadWorkerPermit {
    _route: OwnedSemaphorePermit,
    _memory: crate::memory::ReloadMemoryReservation,
}

/// One table's chunked export (reload §5.3). Owns a side SQL connection; talks to the consume
/// loop only through [`FenceWaiters`]; never touches the replication connection.
#[derive(Debug)]
pub struct ChunkExporter {
    /// The coordinator is temporarily moved into the worker set while COPY runs and restored for H.
    client: Option<tokio_postgres::Client>,
    source_db_url: Redacted<String>,
    fence_waiters: Arc<FenceWaiters>,
    pool: sqlx::PgPool,
    stager: ParquetStager,
    cfg: ChunkExportConfig,
    /// The table shape at the reload's (single) schema version — from the REGISTRY, so files
    /// always match the descriptors their stamped version points at.
    rel: PgRelation,
    /// The `table` metric label (`"<schema>.<table>"`) for this export, precomputed from `rel`.
    series: String,
    schema_version: SchemaVersionNo,
    reload_id: ReloadId,
    /// Exact claim/adoption generation carried into every control-plane mutation.
    lease: control::ExporterLease,
    /// One safe lower fence shared by every chunk in the consistent baseline snapshot.
    start_lsn: Lsn,
    /// Stable source request correlation; synthesized deterministically for legacy callers.
    request_id: Uuid,
}

impl ChunkExporter {
    /// Dial the side connection and resolve the export's fixed shape: the relation from the source
    /// catalog and the schema version from the registry (frozen on the reload row when resuming —
    /// every attempt is single-schema by construction and stays so across DDL).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the source SQL connection, relation description, registry lookup,
    /// frozen schema resolution, or start fence fails.
    pub async fn connect(
        source_db_url: &str,
        pool: sqlx::PgPool,
        fence_waiters: Arc<FenceWaiters>,
        stager: ParquetStager,
        cfg: ChunkExportConfig,
        req: &control::ReloadRow,
    ) -> anyhow::Result<Self> {
        let lease = req
            .exporter_lease(&cfg.instance)
            .context("resolve exporter fencing generation from claimed reload")?;
        let (mut client, connection) = tokio_postgres::connect(source_db_url, NoTls)
            .await
            .context("open chunk-export SQL connection")?;
        // `tokio::spawn` starts its task with an EMPTY span stack — the caller's span is
        // thread-local to whoever polls it, not something a spawn inherits — so this driver's one
        // warning would name no reload at all. `in_current_span` copies the exporter task's span
        // (`reload::spawn_exporter`) onto the new task, which is what makes a dropped side
        // connection attributable to the export it was dialled for.
        let driver = async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = ?e, "chunk-export SQL connection closed");
            }
        };
        tokio::spawn(driver.in_current_span());
        // The registry chain (control-pg) and the live SOURCE catalog hit different servers and
        // neither consumes the other's output, so startup costs the slower branch rather than their
        // sum. Range planning deliberately does not depend on a primary-key index: every plain
        // heap table accepted by preflight can be divided by physical CTID pages.
        let ((rel, schema_version), live_relation) = tokio::try_join!(
            async {
                // A resumed attempt exports at its FROZEN version; a fresh one at the
                // registry's latest.
                let schema_version = match req.schema_version {
                    Some(v) => v,
                    None => control::read_latest_version(
                        &pool,
                        req.epoch,
                        &req.source_schema,
                        &req.source_table,
                    )
                    .await
                    .context("read registry version for reload")?
                    .with_context(|| {
                        format!(
                            "{}.{} has no schema_registry entry — is the extractor streaming it?",
                            req.source_schema, req.source_table
                        )
                    })?,
                };
                // The export shape comes from the REGISTRY at that version — never the live
                // catalog — so every chunk file's columns match the descriptor set the transformer
                // will fetch for its stamped schema_version. (A live `describe` can be ahead of
                // the registry: DDL bumps the registry only when the next Relation message
                // decodes, and e.g. `ADD COLUMN … DEFAULT` materializes values without any DML. Files
                // carrying a shape their version doesn't describe would silently break Phase B's
                // column plan.)
                let registry_row = control::read_registry(
                    &pool,
                    req.epoch,
                    &req.source_schema,
                    &req.source_table,
                    schema_version,
                )
                .await
                .context("read registry row for reload shape")?
                .with_context(|| {
                    format!(
                        "{}.{} has no schema_registry row at version {schema_version}",
                        req.source_schema, req.source_table
                    )
                })?;
                let rel: PgRelation = serde_json::from_value(registry_row.columns)
                    .context("registry columns snapshot is not a PgRelation")?;
                anyhow::Ok((rel, schema_version))
            },
            async {
                crate::source_catalog::describe_source_relation(
                    &client,
                    &req.source_schema,
                    &req.source_table,
                )
                .await
                .context("describe live relation for reload")
            },
        )?;
        if live_relation != rel {
            return Err(ConnectSchemaChanged {
                new_version: await_schema_change(
                    &pool,
                    req.epoch,
                    &req.source_schema,
                    &req.source_table,
                    schema_version,
                    req.reload_id,
                    cfg.echo_timeout,
                )
                .await?,
            }
            .into());
        }
        let request_id = req
            .source_request_id
            .or(req.parent_request_id)
            .with_context(|| {
                format!(
                    "reload {} has no durable source-fence request namespace",
                    req.reload_id
                )
            })?;
        let start_lsn = match req.start_lsn {
            Some(lsn) => lsn,
            None => {
                StartFenceContext {
                    pool: &pool,
                    waiters: &fence_waiters,
                    cfg: &cfg,
                    request_id,
                    req,
                    expected_relation: &rel,
                    schema_version,
                }
                .establish(&mut client)
                .await?
            }
        };
        Ok(ChunkExporter {
            client: Some(client),
            source_db_url: source_db_url.into(),
            fence_waiters,
            pool,
            stager,
            cfg,
            series: format!("{}.{}", rel.schema, rel.name),
            rel,
            schema_version,
            reload_id: req.reload_id,
            lease,
            start_lsn,
            request_id,
        })
    }

    /// Drain one fresh exported snapshot through all ordinary SQL workers, then establish H. The
    /// reload row stays `exporting` until the controller records `export_complete(H)`. An adopted
    /// call may recover durable H, but otherwise returns [`RunOutcome::SnapshotLost`] before
    /// opening or scanning a new snapshot.
    ///
    /// The coordinator's ACCESS SHARE lock freezes the table shape while every COPY runs. A
    /// structural version observed before snapshot commit or at H returns
    /// [`RunOutcome::SchemaChanged`], so every attempt and every output file remains single-schema.
    ///
    /// The tradeoff (H9): restart-on-DDL trades *wasted export work* (bounded by
    /// `reload_max_restarts`, counted by `walrus_reload_restarts_total`) for the transformer never
    /// facing a half-populated table at a version boundary. Letting one baseline straddle versions
    /// was rejected because its failure mode is silent mis-reconciliation rather than visible work.
    ///
    /// ## Cancel safety
    ///
    /// The controller deliberately races this future in
    /// [`lease_guarded_export`](crate::reload::lease_guarded_export). Dropping it aborts worker
    /// transactions and incomplete multipart uploads. A remote object completed just before its
    /// control transaction becomes an orphan; a manifest already committed is purged when adoption
    /// supersedes this snapshot-lost attempt. No physical range is persisted as resumable progress.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if schema checks, chunk export, echo handling, control transitions,
    /// or final export completion fails.
    pub async fn run(&mut self, lost_snapshot_ownership: bool) -> anyhow::Result<RunOutcome> {
        // H is the irreversible upper boundary for this attempt. Decode persists it only after all
        // target WAL through H is durable, but the exporter can crash in the tiny window before its
        // controller flips `exporting -> export_complete`. An adopter must finish from that durable
        // fact: querying the source again could copy a row committed *after* H and stamp it at F,
        // incorrectly pulling future state into the H rebuild.
        //
        // Validate the whole persisted fence identity before trusting H, then repeat the same
        // boundary-scoped DDL check used by the normal drain path. Neither operation reads table data.
        if let Some(final_lsn) = self.recover_durable_end().await? {
            if let Some(new_version) = self.check_schema_changed_through(final_lsn).await? {
                return Ok(RunOutcome::SchemaChanged { new_version });
            }
            tracing::info!(
                reload_id = %self.reload_id,
                final_lsn = %final_lsn,
                "reload recovered durable end marker (controller flips export_complete)"
            );
            return Ok(RunOutcome::Drained { final_lsn });
        }

        // A PostgreSQL snapshot belongs to one backend transaction; neither a file count nor the
        // lease can resurrect it after adoption. This applies even before the first file: the old
        // exporter could still finish an empty/short snapshot and emit H while this adopter starts
        // a later one. Durable H above is the only safe same-attempt recovery fact.
        if lost_snapshot_ownership {
            return Ok(RunOutcome::SnapshotLost);
        }

        let snapshot_outcome = self.export_parallel_snapshot().await?;
        match snapshot_outcome {
            SnapshotExportOutcome::SchemaChanged { new_version } => {
                Ok(RunOutcome::SchemaChanged { new_version })
            }
            SnapshotExportOutcome::Drained { rows } => {
                // The parallel exporter commits every imported transaction and the coordinator
                // (last) before returning. Releasing those locks lets queued DDL win; H's
                // live-shape fence then rejects an obsolete attempt rather than publishing it.
                let final_lsn = match self.establish_end_fence().await? {
                    EndFenceOutcome::Durable(lsn) => lsn,
                    EndFenceOutcome::SchemaChanged(new_version) => {
                        return Ok(RunOutcome::SchemaChanged { new_version });
                    }
                };
                // The catalog shape check inside H catches ordinary column/OID changes. A
                // topology-only ALTER TABLE (for example ATTACH/DETACH PARTITION) can leave
                // that shape byte-for-byte identical while changing which rows a published
                // root contains. Its ddl_audit row precedes H in the same WAL stream, so once
                // H's echo resolves, the committed DDL history through H is authoritative. Reject
                // the attempt if that boundary crossed structural DDL, while ignoring DDL after H.
                if let Some(new_version) = self.check_schema_changed_through(final_lsn).await? {
                    return Ok(RunOutcome::SchemaChanged { new_version });
                }
                tracing::info!(
                    reload_id = %self.reload_id,
                    rows,
                    final_lsn = %final_lsn,
                    "reload export drained (controller flips export_complete)"
                );
                Ok(RunOutcome::Drained { final_lsn })
            }
        }
    }

    /// Freeze one source snapshot, import it into ordinary SQL workers, stream all physical ranges,
    /// then commit child transactions followed by the coordinator. No replication connection or
    /// slot is created anywhere on this path.
    async fn export_parallel_snapshot(&mut self) -> anyhow::Result<SnapshotExportOutcome> {
        let plan = match self.begin_parallel_snapshot().await? {
            SnapshotPlanOutcome::Ready(plan) => *plan,
            SnapshotPlanOutcome::SchemaChanged(new_version) => {
                return Ok(SnapshotExportOutcome::SchemaChanged { new_version });
            }
        };
        let cached = crate::relcache::RelationCache::default()
            .upsert_from_relation(self.rel.clone(), self.schema_version)
            .context("build Arrow schema for streamed reload COPY")?;

        // Initialize every child before any worker begins COPY. If a queued ACCESS EXCLUSIVE DDL
        // makes a child's NOWAIT lock fail, abandon the whole shared snapshot.
        let mut clients = Vec::with_capacity(plan.worker_count);
        for worker_index in 1..plan.worker_count {
            match connect_snapshot_worker(
                self.source_db_url.expose(),
                &plan.snapshot_id,
                &plan.snapshot_identity,
                &self.rel,
                worker_index,
            )
            .await
            {
                Ok(client) => clients.push((worker_index, client)),
                Err(error) => {
                    for (_, client) in &clients {
                        rollback_source_client(client, "prepared COPY worker setup failure").await;
                    }
                    if let Err(rollback_error) = self.rollback_dump_snapshot().await {
                        return Err(error.context(format!(
                            "rolling back snapshot after worker setup failure also failed: {rollback_error:#}"
                        )));
                    }
                    return Err(error);
                }
            }
        }
        let coordinator = self
            .client
            .take()
            .context("reload snapshot coordinator connection is missing")?;
        clients.push((0, coordinator));
        let ctx = CopyWorkerContext {
            ranges: Arc::new(RangeQueue::new(plan.ranges)),
            cached,
            pool: self.pool.clone(),
            stager: self.stager.clone(),
            chunk_rows: self.cfg.chunk_rows,
            router_batch_bytes: self.cfg.router_batch_bytes,
            worker_admission: self.cfg.worker_admission.clone(),
            start_lsn: self.start_lsn,
            schema_version: self.schema_version,
            reload_id: self.reload_id,
            lease: self.lease.clone(),
            epoch: self.cfg.epoch,
            instance: self.cfg.instance.clone(),
            series: self.series.clone(),
        };

        let mut workers = JoinSet::new();
        for (worker_index, client) in clients {
            let worker_ctx = ctx.clone();
            workers.spawn(
                async move { run_copy_worker(worker_index, client, worker_ctx).await }
                    .in_current_span(),
            );
        }

        let mut completed = Vec::with_capacity(plan.worker_count);
        let mut failure = None;
        while let Some(result) = workers.join_next().await {
            match result {
                Ok(Ok(worker)) => completed.push(worker),
                Ok(Err(error)) => {
                    failure = Some(error);
                    break;
                }
                Err(error) => {
                    failure = Some(anyhow::Error::new(error).context("reload COPY worker task"));
                    break;
                }
            }
        }
        if let Some(error) = failure {
            workers.abort_all();
            while workers.join_next().await.is_some() {}
            for worker in &completed {
                rollback_source_client(&worker.client, "sibling COPY worker failure").await;
            }
            return Err(error);
        }

        // A DDL can have committed just before ACCESS SHARE and only now have reached the registry.
        // Even if its final catalog shape compares equal, do not stamp files at an older version.
        if let Some(new_version) = self.check_schema_still_current().await? {
            let mut coordinator = None;
            for worker in completed {
                rollback_source_client(&worker.client, "reload schema version changed").await;
                if worker.worker_index == 0 {
                    coordinator = Some(worker.client);
                }
            }
            self.client = coordinator;
            return Ok(SnapshotExportOutcome::SchemaChanged { new_version });
        }

        let rows = completed
            .iter()
            .fold(0u64, |total, worker| total.saturating_add(worker.rows));
        let coordinator_pos = completed
            .iter()
            .position(|worker| worker.worker_index == 0)
            .context("parallel reload lost its snapshot coordinator worker")?;
        let coordinator = completed.swap_remove(coordinator_pos);

        // Children release first. The coordinator owns the exported snapshot and the table lock
        // that made every range's shape stable, so it commits last.
        for worker in &completed {
            if let Err(error) = worker.client.batch_execute("COMMIT").await {
                rollback_source_client(&coordinator.client, "child snapshot commit failure").await;
                return Err(
                    anyhow::Error::new(error).context("commit imported reload worker snapshot")
                );
            }
        }
        coordinator
            .client
            .batch_execute("COMMIT")
            .await
            .context("commit exported reload coordinator snapshot")?;
        self.client = Some(coordinator.client);
        let mut control_conn = self
            .pool
            .acquire()
            .await
            .context("acquire control connection to seal reload snapshot")?;
        let seal = control::reload::seal_export(
            &mut control_conn,
            &self.lease,
            self.start_lsn,
            self.schema_version,
        )
        .await
        .context("seal complete reload snapshot range plan")?;
        let rows_i64 = i64::try_from(rows).context("reload row count exceeds control bigint")?;
        anyhow::ensure!(
            seal.row_count == rows_i64,
            "sealed reload row count {} disagrees with worker total {rows_i64}",
            seal.row_count
        );
        Ok(SnapshotExportOutcome::Drained { rows })
    }

    /// Open the coordinator transaction, lock the frozen relation shape, export its snapshot, and
    /// plan a bounded number of heap-page ranges. Turning RLS off makes a policy-bound role error
    /// instead of silently producing a partial baseline.
    async fn begin_parallel_snapshot(&mut self) -> anyhow::Result<SnapshotPlanOutcome> {
        let schema = SqlIdent::new(&self.rel.schema).context("validate reload lock schema")?;
        let table = SqlIdent::new(&self.rel.name).context("validate reload lock table")?;
        let lock_sql = format!("LOCK TABLE {schema}.{table} IN ACCESS SHARE MODE");
        let client = self
            .client
            .as_mut()
            .context("reload snapshot coordinator connection is missing")?;
        let setup_sql = coordinator_snapshot_setup_sql();
        if let Err(error) = client.batch_execute(&setup_sql).await {
            rollback_source_client(client, "coordinator snapshot setup failure").await;
            return Err(anyhow::Error::new(error)
                .context("begin read-only repeatable-read reload coordinator snapshot"));
        }
        if let Err(error) = client.batch_execute(&lock_sql).await {
            let rollback_error = self.rollback_dump_snapshot().await.err();
            let error = anyhow::Error::new(error).context("lock reload table for snapshot export");
            return Err(match rollback_error {
                Some(rollback_error) => error.context(format!(
                    "rolling back failed table lock also failed: {rollback_error:#}"
                )),
                None => error,
            });
        }

        let live_relation = crate::source_catalog::describe_source_relation(
            self.client.as_ref().context("coordinator disappeared")?,
            &self.rel.schema,
            &self.rel.name,
        )
        .await
        .context("describe locked live relation for reload")?;
        if live_relation != self.rel {
            self.rollback_dump_snapshot().await?;
            let new_version = await_schema_change(
                &self.pool,
                self.cfg.epoch,
                &self.rel.schema,
                &self.rel.name,
                self.schema_version,
                self.reload_id,
                self.cfg.echo_timeout,
            )
            .await?;
            return Ok(SnapshotPlanOutcome::SchemaChanged(new_version));
        }

        let client = self.client.as_ref().context("coordinator disappeared")?;
        let snapshot = client
            .query_one(
                "SELECT pg_catalog.pg_export_snapshot(),
                        pg_catalog.pg_current_snapshot()::text,
                        pg_catalog.pg_snapshot_xmin(pg_catalog.pg_current_snapshot())::text::bigint,
                        pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())::text::bigint",
                &[],
            )
            .await
            .context("export and identify reload coordinator snapshot")?;
        let snapshot_id: String = snapshot.get(0);
        let snapshot_identity: String = snapshot.get(1);
        let snapshot_xmin: i64 = snapshot.get(2);
        let snapshot_xmax: i64 = snapshot.get(3);
        let storage = client
            .query_one(
                "SELECT am.amname,
                        pg_catalog.pg_relation_size(c.oid, 'main')::bigint,
                        pg_catalog.current_setting('block_size')::bigint
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
                 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'",
                &[&self.rel.schema, &self.rel.name],
            )
            .await
            .context("read locked reload relation storage")?;
        let access_method: Option<String> = storage.get(0);
        let relation_bytes: i64 = storage.get(1);
        let block_bytes: i64 = storage.get(2);
        anyhow::ensure!(
            relation_bytes >= 0 && block_bytes > 0,
            "invalid relation storage size {relation_bytes}/{block_bytes} for {}.{}",
            self.rel.schema,
            self.rel.name
        );
        let relation_bytes =
            u64::try_from(relation_bytes).context("relation size does not fit u64")?;
        let block_bytes = u64::try_from(block_bytes).context("block size does not fit u64")?;
        let blocks = relation_bytes.div_ceil(block_bytes);
        let ranges = if access_method.as_deref() == Some("heap") && blocks > 0 {
            plan_ctid_ranges(blocks, self.cfg.workers_per_table)
        } else {
            // Non-heap access methods are still streamed, but physical CTID ordering is not
            // assumed to be splittable. One worker drains the complete table safely.
            vec![ScanRange::Full]
        };
        let worker_count = self.cfg.workers_per_table.get().min(ranges.len()).max(1);
        let durable_ranges = ranges
            .iter()
            .enumerate()
            .map(|(range_no, range)| {
                let range_no =
                    i64::try_from(range_no).context("reload range number exceeds bigint")?;
                let (full_scan, start_block, end_block) = match range {
                    ScanRange::Full => (true, None, None),
                    ScanRange::Blocks { start, end } => (
                        false,
                        Some(i64::try_from(*start).context("reload block start exceeds bigint")?),
                        end.map(i64::try_from)
                            .transpose()
                            .context("reload block end exceeds bigint")?,
                    ),
                };
                anyhow::Ok(control::ExportRangePlan {
                    range_no,
                    full_scan,
                    start_block,
                    end_block,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut control_conn = self
            .pool
            .acquire()
            .await
            .context("acquire control connection for reload snapshot plan")?;
        if let Err(error) = control::reload::begin_export_plan(
            &mut control_conn,
            &self.lease,
            self.start_lsn,
            self.schema_version,
            control::ExportSnapshot {
                identity: &snapshot_identity,
                xmin: snapshot_xmin,
                xmax: snapshot_xmax,
            },
            &durable_ranges,
        )
        .await
        {
            let rollback_error = self.rollback_dump_snapshot().await.err();
            let error = anyhow::Error::new(error).context("durably record reload snapshot plan");
            return Err(match rollback_error {
                Some(rollback_error) => error.context(format!(
                    "rolling back rejected snapshot plan also failed: {rollback_error:#}"
                )),
                None => error,
            });
        }
        tracing::info!(
            reload_id = %self.reload_id,
            workers = worker_count,
            ranges = ranges.len(),
            blocks,
            access_method = access_method.as_deref().unwrap_or("none"),
            "reload snapshot exported; starting ordinary SQL COPY workers"
        );
        Ok(SnapshotPlanOutcome::Ready(Box::new(SourceSnapshotPlan {
            snapshot_id,
            snapshot_identity,
            ranges,
            worker_count,
        })))
    }

    async fn rollback_dump_snapshot(&self) -> anyhow::Result<()> {
        if let Some(client) = &self.client {
            client
                .batch_execute("ROLLBACK")
                .await
                .context("roll back repeatable-read reload snapshot")?;
        }
        Ok(())
    }

    /// Recover the crash seam after decode persisted H but before the controller recorded
    /// `export_complete(H)`. An end marker is trusted only when the durable reload row, F marker,
    /// and H marker all name this exact attempt, table, schema version, and request namespace.
    async fn recover_durable_end(&self) -> anyhow::Result<Option<Lsn>> {
        let markers = control::reload::read_markers(&self.pool, self.reload_id)
            .await
            .context("read reload markers for end-fence recovery")?;
        if !markers
            .iter()
            .any(|marker| marker.kind == control::ReloadMarkerKind::End)
        {
            return Ok(None);
        }

        let reload = control::reload::get(&self.pool, self.reload_id)
            .await
            .context("read reload row for end-fence recovery")?
            .with_context(|| {
                format!(
                    "reload {} disappeared while recovering its durable end marker",
                    self.reload_id
                )
            })?;
        validate_durable_end(
            &reload,
            &markers,
            self.cfg.epoch,
            &self.rel,
            self.schema_version,
            self.start_lsn,
            self.request_id,
        )
    }

    /// Is the table still at this attempt's frozen `schema_version`? Reads the registry's latest
    /// version (control-pg, a cheap indexed MAX), the extractor's structural source of truth. The source
    /// ACCESS SHARE locks close the scan-time DDL window; this check covers DDL decoded just before
    /// those locks or while H is being established.
    async fn check_schema_still_current(&self) -> anyhow::Result<Option<SchemaVersionNo>> {
        let latest = control::read_latest_version(
            &self.pool,
            self.cfg.epoch,
            &self.rel.schema,
            &self.rel.name,
        )
        .await
        .context("read registry latest version for reload staleness check")?;
        Ok(version_changed(self.schema_version, latest))
    }

    /// Did structural DDL commit inside this attempt's closed source boundary? The end fence's
    /// source lock validates the live shape at H; this LSN-bounded history read catches
    /// topology-only changes that preserve that shape. DDL strictly after H belongs to normal live
    /// processing and must not waste or fail an already-valid F..H snapshot.
    async fn check_schema_changed_through(
        &self,
        through_lsn: Lsn,
    ) -> anyhow::Result<Option<SchemaVersionNo>> {
        let latest = control::read_latest_ddl_version_through(
            &self.pool,
            self.cfg.epoch,
            &self.rel.schema,
            &self.rel.name,
            through_lsn,
        )
        .await
        .context("read boundary-scoped DDL version for reload staleness check")?;
        Ok(version_changed(self.schema_version, latest))
    }

    /// Emit the terminal source event and return only after decode has force-flushed the target and
    /// persisted its durable end marker.
    async fn establish_end_fence(&mut self) -> anyhow::Result<EndFenceOutcome> {
        let client = self
            .client
            .as_mut()
            .context("reload source connection is missing before end fence")?;
        match crate::reload_event::emit_fence(
            client,
            &self.fence_waiters,
            FenceSpec {
                publication: &self.cfg.publication_name,
                request_id: self.request_id,
                reload_id: self.reload_id,
                phase: FencePhase::End,
                target: FenceTarget {
                    schema: &self.rel.schema,
                    table: &self.rel.name,
                    expected_relation: &self.rel,
                    schema_version: self.schema_version,
                },
                timeouts: FenceTimeouts {
                    lock: FENCE_LOCK_TIMEOUT,
                    echo: self.cfg.echo_timeout,
                },
            },
        )
        .await?
        {
            FenceEmission::Observed(echo) => {
                // Decode resolves an end fence only after the target flush. Recording again here is
                // an idempotent crash seam and keeps this exporter usable with a rolling decoder.
                control::reload::record_end_marker(
                    &self.pool,
                    self.reload_id,
                    echo.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(self.request_id),
                        source_schema: &self.rel.schema,
                        source_table: &self.rel.name,
                        schema_version: self.schema_version,
                    },
                )
                .await
                .context("record durable reload end marker")?;
                Ok(EndFenceOutcome::Durable(echo.commit_lsn))
            }
            FenceEmission::AlreadyExists => {
                let lsn = wait_for_marker(
                    &self.pool,
                    self.reload_id,
                    control::ReloadMarkerKind::End,
                    self.cfg.echo_timeout,
                )
                .await?;
                Ok(EndFenceOutcome::Durable(lsn))
            }
            FenceEmission::SchemaChanged => Ok(EndFenceOutcome::SchemaChanged(
                await_schema_change(
                    &self.pool,
                    self.cfg.epoch,
                    &self.rel.schema,
                    &self.rel.name,
                    self.schema_version,
                    self.reload_id,
                    self.cfg.echo_timeout,
                )
                .await?,
            )),
        }
    }
}

/// More ranges than workers smooth out dead-page and row-width skew without creating more source
/// sessions. This is an implementation detail, not a fourth operator control.
const RANGE_TASKS_PER_WORKER: usize = 4;
/// The largest Arrow micro-batch by row count. Remote object size remains operator-controlled by
/// `reload_chunk_rows`; this smaller threshold exists solely to bound router memory.
const ROUTER_BATCH_ROWS: u64 = 1_024;
/// Never let one worker claim more than this much of the process-wide Arrow-buffer allowance.
const ROUTER_BATCH_BYTES_MAX: u64 = 8 * 1024 * 1024;
/// Conservative allowance for one active COPY -> Arrow -> Parquet -> multipart route: one Arrow
/// micro-batch, one 8 MiB multipart part, Parquet's encoded row-group scratch, and bookkeeping.
/// The estimate is intentionally larger than the two fixed 8 MiB buffers. As with every row-based
/// memory guard, a single source value larger than the allowance must still be admitted so the
/// export can make progress.
const RELOAD_WORKER_MEMORY_RESERVATION_BYTES: u64 = 32 * 1024 * 1024;
/// Parquet retains metadata for every row-group/column pair until it writes the footer. Bounding
/// these pairs keeps an arbitrarily large `chunk_rows` value from turning metadata into an
/// object-sized memory buffer. Reaching the bound closes the object early; `chunk_rows` remains the
/// strict row-count maximum.
const MAX_PARQUET_COLUMN_CHUNKS_PER_OBJECT: usize = 4_096;

/// Split the existing process-wide in-flight allowance across the maximum configured COPY streams.
/// A single over-sized source value can exceed an estimate by its own size, but ordinary aggregate
/// Arrow buffering remains within the configured ceiling.
#[must_use]
pub(crate) fn router_batch_bytes(
    max_inflight_bytes: NonZeroU64,
    max_copy_streams: NonZeroUsize,
) -> NonZeroU64 {
    let streams = u64::try_from(max_copy_streams.get()).unwrap_or(u64::MAX);
    let per_worker = max_inflight_bytes.get() / streams;
    NonZeroU64::new(per_worker.clamp(1, ROUTER_BATCH_BYTES_MAX)).unwrap_or(NonZeroU64::MIN)
}

/// Bound the number of concurrently active memory-heavy reload routes. Configured workers remain
/// the source-session ceiling; excess workers wait after importing the common snapshot. At least
/// one route is always admitted so a diagnostic memory setting cannot deadlock reloads.
#[must_use]
pub(crate) fn reload_memory_worker_limit(
    max_inflight_bytes: NonZeroU64,
    max_copy_streams: NonZeroUsize,
) -> NonZeroUsize {
    let budget_workers = max_inflight_bytes.get() / RELOAD_WORKER_MEMORY_RESERVATION_BYTES;
    let budget_workers = usize::try_from(budget_workers).unwrap_or(usize::MAX).max(1);
    let limit = max_copy_streams
        .get()
        .min(budget_workers)
        .min(Semaphore::MAX_PERMITS);
    NonZeroUsize::new(limit).unwrap_or(NonZeroUsize::MIN)
}

fn max_row_groups_per_object(parquet_columns: usize) -> usize {
    MAX_PARQUET_COLUMN_CHUNKS_PER_OBJECT
        .checked_div(parquet_columns)
        .unwrap_or(1)
        .max(1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanRange {
    Full,
    Blocks { start: u64, end: Option<u64> },
}

#[derive(Debug)]
struct SourceSnapshotPlan {
    snapshot_id: String,
    snapshot_identity: String,
    ranges: Vec<ScanRange>,
    worker_count: usize,
}

#[derive(Debug)]
enum SnapshotPlanOutcome {
    Ready(Box<SourceSnapshotPlan>),
    SchemaChanged(SchemaVersionNo),
}

#[derive(Debug)]
struct RangeQueue {
    ranges: Vec<ScanRange>,
    next: AtomicUsize,
}

impl RangeQueue {
    const fn new(ranges: Vec<ScanRange>) -> Self {
        Self {
            ranges,
            next: AtomicUsize::new(0),
        }
    }

    fn claim(&self) -> Option<(i64, ScanRange)> {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let range = self.ranges.get(index).copied()?;
        i64::try_from(index).ok().map(|range_no| (range_no, range))
    }
}

#[derive(Clone, Debug)]
struct CopyWorkerContext {
    ranges: Arc<RangeQueue>,
    cached: Arc<crate::relcache::CachedRelation>,
    pool: sqlx::PgPool,
    stager: ParquetStager,
    chunk_rows: NonZeroU64,
    router_batch_bytes: NonZeroU64,
    worker_admission: ReloadWorkerAdmission,
    start_lsn: Lsn,
    schema_version: SchemaVersionNo,
    reload_id: ReloadId,
    lease: control::ExporterLease,
    epoch: EpochNo,
    instance: String,
    series: String,
}

#[derive(Debug)]
struct CopyWorkerResult {
    worker_index: usize,
    client: tokio_postgres::Client,
    rows: u64,
}

/// Divide a heap into contiguous, disjoint CTID page intervals. The final range is intentionally
/// open-ended: it covers every block visible in S even if the physical relation grew between the
/// size estimate and COPY (newer tuples are still invisible to S).
fn plan_ctid_ranges(blocks: u64, workers: NonZeroUsize) -> Vec<ScanRange> {
    debug_assert!(blocks > 0);
    let max_tasks = workers.get().saturating_mul(RANGE_TASKS_PER_WORKER);
    let max_tasks_u64 = u64::try_from(max_tasks).unwrap_or(u64::MAX);
    let task_count = usize::try_from(blocks.min(max_tasks_u64))
        .unwrap_or(max_tasks)
        .max(1);
    let step = blocks.div_ceil(u64::try_from(task_count).unwrap_or(u64::MAX));
    let mut ranges = Vec::with_capacity(task_count);
    let mut start = 0;
    while start < blocks {
        let next = start.saturating_add(step).min(blocks);
        ranges.push(ScanRange::Blocks {
            start,
            end: (next < blocks).then_some(next),
        });
        start = next;
    }
    ranges
}

fn copy_sql(rel: &PgRelation, range: ScanRange) -> anyhow::Result<String> {
    let columns = rel
        .columns
        .iter()
        .map(|column| {
            common::sql::SqlColumn::new(&column.name)
                .map(|column| format!("_src.{column}::text"))
                .context("validate reload COPY column")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        !columns.is_empty(),
        "reload COPY relation {}.{} has no columns",
        rel.schema,
        rel.name
    );
    let schema = SqlIdent::new(&rel.schema).context("validate reload COPY schema")?;
    let table = SqlIdent::new(&rel.name).context("validate reload COPY table")?;
    let predicate = match range {
        ScanRange::Full => String::new(),
        ScanRange::Blocks { start, end } => match end {
            Some(end) => {
                format!(" WHERE _src.ctid >= '({start},0)'::tid AND _src.ctid < '({end},0)'::tid")
            }
            None => format!(" WHERE _src.ctid >= '({start},0)'::tid"),
        },
    };
    Ok(format!(
        "COPY (SELECT {} FROM ONLY {schema}.{table} AS _src{predicate}) \
         TO STDOUT WITH (FORMAT BINARY)",
        columns.join(", ")
    ))
}

async fn rollback_source_client(client: &tokio_postgres::Client, reason: &'static str) {
    if let Err(error) = client.batch_execute("ROLLBACK").await {
        tracing::warn!(error = ?error, reason, "failed to roll back reload source transaction");
    }
}

fn snapshot_worker_setup_sql(snapshot_id: &str, rel: &PgRelation) -> anyhow::Result<String> {
    let schema = SqlIdent::new(&rel.schema).context("validate reload worker lock schema")?;
    let table = SqlIdent::new(&rel.name).context("validate reload worker lock table")?;
    let snapshot = snapshot_id.to_quoted_literal();
    Ok(format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; \
         SET TRANSACTION SNAPSHOT {snapshot}; \
         {CANONICAL_COPY_GUCS_SQL} \
         SET LOCAL row_security = off; \
         SET LOCAL statement_timeout = 0; \
         SET LOCAL idle_in_transaction_session_timeout = 0; \
         LOCK TABLE {schema}.{table} IN ACCESS SHARE MODE NOWAIT"
    ))
}

async fn connect_snapshot_worker(
    source_db_url: &str,
    snapshot_id: &str,
    expected_snapshot_identity: &str,
    rel: &PgRelation,
    worker_index: usize,
) -> anyhow::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(source_db_url, NoTls)
        .await
        .with_context(|| format!("open reload COPY worker {worker_index} SQL connection"))?;
    let driver = async move {
        if let Err(error) = connection.await {
            tracing::warn!(worker_index, error = ?error, "reload COPY worker SQL connection closed");
        }
    };
    tokio::spawn(driver.in_current_span());

    let setup = snapshot_worker_setup_sql(snapshot_id, rel)?;
    if let Err(error) = client.batch_execute(&setup).await {
        rollback_source_client(&client, "COPY worker snapshot import failure").await;
        return Err(anyhow::Error::new(error).context(format!(
            "import shared snapshot in reload COPY worker {worker_index}"
        )));
    }
    let imported_identity: String = client
        .query_one("SELECT pg_catalog.pg_current_snapshot()::text", &[])
        .await
        .with_context(|| {
            format!("read imported snapshot identity in reload worker {worker_index}")
        })?
        .get(0);
    if imported_identity != expected_snapshot_identity {
        rollback_source_client(&client, "COPY worker imported wrong snapshot").await;
        anyhow::bail!(
            "reload COPY worker {worker_index} imported snapshot {imported_identity}, expected {expected_snapshot_identity}"
        );
    }
    Ok(client)
}

async fn run_copy_worker(
    worker_index: usize,
    client: tokio_postgres::Client,
    ctx: CopyWorkerContext,
) -> anyhow::Result<CopyWorkerResult> {
    // Acquire before constructing the router: this permit accounts for the Arrow builder, Parquet
    // encoder scratch, and multipart buffer. Holding it through `finish` makes remote backpressure
    // release memory capacity before another table's worker can begin polling COPY.
    let _memory_permit = ctx.worker_admission.acquire().await?;
    let column_types = vec![Type::TEXT; ctx.cached.relation.columns.len()];
    let mut tuple = Vec::with_capacity(ctx.cached.relation.columns.len());
    let mut rows = 0u64;
    while let Some((range_no, range)) = ctx.ranges.claim() {
        // One router per physical range makes its durable completion receipt exact: every object
        // counted below contains rows from this range only, and completion is recorded only after
        // the final buffered object manifest is committed.
        let mut router = ReloadRouter::new(ctx.clone())?;
        let mut range_rows = 0u64;
        let sql = copy_sql(&ctx.cached.relation, range)?;
        let copy = client
            .copy_out(&sql)
            .await
            .with_context(|| format!("start reload binary COPY range {range:?}"))?;
        let mut stream = Box::pin(BinaryCopyOutStream::new(copy, &column_types));
        while let Some(row) = stream.next().await {
            let row = row.with_context(|| format!("read reload binary COPY range {range:?}"))?;
            tuple.clear();
            for (index, _) in ctx.cached.relation.columns.iter().enumerate() {
                let value = row
                    .try_get::<Option<String>>(index)
                    .with_context(|| format!("decode reload COPY column {index}"))?;
                tuple.push(value.map_or(TupleValue::Null, TupleValue::Text));
            }
            router.push(&tuple).await?;
            rows = rows.saturating_add(1);
            range_rows = range_rows.saturating_add(1);
        }
        let range_files = router.finish().await?;
        control::reload::record_export_range(
            &ctx.pool,
            &ctx.lease,
            range_no,
            range_files,
            i64::try_from(range_rows).context("reload range row count exceeds control bigint")?,
        )
        .await
        .with_context(|| format!("record completed reload range {range_no}"))?;
    }
    tracing::debug!(worker_index, rows, "reload COPY worker drained");
    Ok(CopyWorkerResult {
        worker_index,
        client,
        rows,
    })
}

struct ReloadRouter {
    ctx: CopyWorkerContext,
    batcher: crate::batch::TableBatcher<crate::batch::SystemClock>,
    writer: Option<crate::staging::ReloadParquetWriter>,
    buffered_rows: u64,
    object_row_groups: usize,
    max_object_row_groups: usize,
    completed_files: i64,
}

impl ReloadRouter {
    fn new(ctx: CopyWorkerContext) -> anyhow::Result<Self> {
        let micro_rows = NonZeroU64::new(ctx.chunk_rows.get().min(ROUTER_BATCH_ROWS))
            .context("reload router row limit became zero")?;
        let max_bytes = ctx.router_batch_bytes;
        let max_object_row_groups =
            max_row_groups_per_object(ctx.cached.arrow_schema.fields().len());
        let batcher = crate::batch::TableBatcher::new(
            Arc::clone(&ctx.cached),
            crate::batch::BatchTriggers {
                max_rows: micro_rows,
                max_bytes,
                max_fill: Duration::from_secs(3600),
            },
            crate::batch::SystemClock,
        )
        .context("create streaming reload Arrow router")?;
        Ok(Self {
            ctx,
            batcher,
            writer: None,
            buffered_rows: 0,
            object_row_groups: 0,
            max_object_row_groups,
            completed_files: 0,
        })
    }

    async fn push(&mut self, tuple: &[TupleValue]) -> anyhow::Result<()> {
        self.batcher
            .push_committed(reload_meta(&self.ctx), tuple)
            .context("append reload COPY row to Arrow micro-batch")?;
        self.buffered_rows = self.buffered_rows.saturating_add(1);
        let object_rows = self
            .writer
            .as_ref()
            .map_or(0, crate::staging::ReloadParquetWriter::row_count);
        if self.batcher.should_flush()
            || object_rows.saturating_add(self.buffered_rows) >= self.ctx.chunk_rows.get()
        {
            self.flush_micro_batch().await?;
        }
        Ok(())
    }

    async fn flush_micro_batch(&mut self) -> anyhow::Result<()> {
        if self.buffered_rows == 0 {
            return Ok(());
        }
        let sealed = self
            .batcher
            .seal()
            .context("seal reload Arrow micro-batch")?;
        let writer = match self.writer.as_mut() {
            Some(writer) => writer,
            None => self.writer.insert(
                self.ctx
                    .stager
                    .begin_reload_stream(
                        Arc::clone(&self.ctx.cached.arrow_schema),
                        self.ctx.cached.relation.schema.clone(),
                        self.ctx.cached.relation.name.clone(),
                        self.ctx.start_lsn,
                        self.ctx.schema_version,
                    )
                    .context("begin streamed reload Parquet object")?,
            ),
        };
        writer
            .write_batch(&sealed.record_batch)
            .await
            .context("flush reload Arrow micro-batch to remote Parquet object")?;
        self.buffered_rows = 0;
        self.object_row_groups = self.object_row_groups.saturating_add(1);
        if writer.row_count() >= self.ctx.chunk_rows.get()
            || self.object_row_groups >= self.max_object_row_groups
        {
            self.finish_object().await?;
        }
        Ok(())
    }

    async fn finish_object(&mut self) -> anyhow::Result<()> {
        let writer = self
            .writer
            .take()
            .context("reload router has no object to finish")?;
        self.object_row_groups = 0;
        let object = writer
            .finish()
            .await
            .context("finish streamed reload Parquet object")?;
        anyhow::ensure!(
            object.row_count <= self.ctx.chunk_rows.get(),
            "reload object exceeded configured row limit: {} > {}",
            object.row_count,
            self.ctx.chunk_rows
        );
        let mut tx = self
            .ctx
            .pool
            .begin()
            .await
            .context("begin reload object manifest transaction")?;
        crate::manifest::record_reload_ready(&mut *tx, self.ctx.epoch, &object, self.ctx.reload_id)
            .await
            .context("record streamed reload object manifest")?;
        let file_no = control::reload::record_exported_file(
            &mut *tx,
            &self.ctx.lease,
            self.ctx.start_lsn,
            self.ctx.schema_version,
        )
        .await
        .context("record completed parallel reload file")?;
        tx.commit()
            .await
            .context("commit reload object manifest and completed-file count")?;
        self.completed_files = self.completed_files.checked_add(1).context(
            "reload range produced more files than the control bigint counter can represent",
        )?;
        common::metrics::record_reload_chunk(&self.ctx.series, object.row_count);
        tracing::debug!(
            reload_id = %self.ctx.reload_id,
            file_no,
            rows = object.row_count,
            key = %object.key,
            "streamed reload object committed"
        );
        Ok(())
    }

    async fn finish(mut self) -> anyhow::Result<i64> {
        self.flush_micro_batch().await?;
        if self.writer.is_some() {
            self.finish_object().await?;
        }
        Ok(self.completed_files)
    }
}

fn reload_meta(ctx: &CopyWorkerContext) -> ExtractorMeta {
    let now = UtcTimestamp::now();
    ExtractorMeta {
        op: Op::Insert,
        // Every baseline row carries F as both LSNs; any overlapping WAL event in (F,H] wins.
        lsn: ctx.start_lsn,
        commit_lsn: ctx.start_lsn,
        commit_ts: now,
        xid: 0,
        epoch: ctx.epoch,
        batch_id: String::new(),
        schema_version: ctx.schema_version,
        source_schema: ctx.cached.relation.schema.clone(),
        source_table: ctx.cached.relation.name.clone(),
        kind: Kind::Reload,
        unchanged_toast: Box::default(),
        extractor_instance: ctx.instance.clone(),
        extractor_processed_at: now,
    }
}

/// Validate that a persisted H belongs to this exact exporter attempt. Kept pure so corruption and
/// identity mismatches are testable without a live control database.
fn validate_durable_end(
    reload: &control::ReloadRow,
    markers: &[control::ReloadMarkerRow],
    expected_epoch: EpochNo,
    expected_relation: &PgRelation,
    expected_schema_version: SchemaVersionNo,
    expected_start_lsn: Lsn,
    expected_request_id: Uuid,
) -> anyhow::Result<Option<Lsn>> {
    let Some(end) = markers
        .iter()
        .find(|marker| marker.kind == control::ReloadMarkerKind::End)
    else {
        return Ok(None);
    };
    let Some(baseline) = markers
        .iter()
        .find(|marker| marker.kind == control::ReloadMarkerKind::Baseline)
    else {
        anyhow::bail!(
            "reload {} has an end marker without its durable baseline marker",
            reload.reload_id
        );
    };
    let end_count = markers
        .iter()
        .filter(|marker| marker.kind == control::ReloadMarkerKind::End)
        .count();
    let baseline_count = markers
        .iter()
        .filter(|marker| marker.kind == control::ReloadMarkerKind::Baseline)
        .count();

    anyhow::ensure!(
        end_count == 1 && baseline_count == 1 && markers.len() == 2,
        "reload {} has an incomplete or duplicate durable F/H marker set",
        reload.reload_id
    );
    anyhow::ensure!(
        reload.status == control::ReloadStatus::Exporting,
        "reload {} durable H recovery requires exporting status, found {:?}",
        reload.reload_id,
        reload.status
    );
    anyhow::ensure!(
        reload.epoch == expected_epoch
            && reload.source_schema == expected_relation.schema
            && reload.source_table == expected_relation.name,
        "reload {} durable H target identity disagrees with the exporter",
        reload.reload_id
    );
    anyhow::ensure!(
        reload.schema_version == Some(expected_schema_version)
            && reload.start_lsn == Some(expected_start_lsn)
            && reload.final_lsn.is_none(),
        "reload {} durable H boundaries disagree with the frozen exporter attempt",
        reload.reload_id
    );
    let Some(durable_request_id) = reload.source_request_id.or(reload.parent_request_id) else {
        anyhow::bail!(
            "reload {} durable H has no source-fence request namespace",
            reload.reload_id
        );
    };
    anyhow::ensure!(
        durable_request_id == expected_request_id,
        "reload {} durable H request identity disagrees with the exporter",
        reload.reload_id
    );
    anyhow::ensure!(
        baseline.reload_id == reload.reload_id
            && baseline.kind == control::ReloadMarkerKind::Baseline
            && baseline.lsn == expected_start_lsn
            && baseline.schema_version == expected_schema_version,
        "reload {} durable baseline marker disagrees with frozen F/schema",
        reload.reload_id
    );
    anyhow::ensure!(
        end.reload_id == reload.reload_id
            && end.kind == control::ReloadMarkerKind::End
            && end.schema_version == expected_schema_version
            && end.lsn >= expected_start_lsn,
        "reload {} durable end marker disagrees with frozen F/schema",
        reload.reload_id
    );
    Ok(Some(end.lsn))
}

struct StartFenceContext<'a> {
    pool: &'a sqlx::PgPool,
    waiters: &'a FenceWaiters,
    cfg: &'a ChunkExportConfig,
    request_id: Uuid,
    req: &'a control::ReloadRow,
    expected_relation: &'a PgRelation,
    schema_version: SchemaVersionNo,
}

impl StartFenceContext<'_> {
    async fn establish(self, client: &mut tokio_postgres::Client) -> anyhow::Result<Lsn> {
        match crate::reload_event::emit_fence(
            client,
            self.waiters,
            FenceSpec {
                publication: &self.cfg.publication_name,
                request_id: self.request_id,
                reload_id: self.req.reload_id,
                phase: FencePhase::Start,
                target: FenceTarget {
                    schema: &self.req.source_schema,
                    table: &self.req.source_table,
                    expected_relation: self.expected_relation,
                    schema_version: self.schema_version,
                },
                timeouts: FenceTimeouts {
                    lock: FENCE_LOCK_TIMEOUT,
                    echo: self.cfg.echo_timeout,
                },
            },
        )
        .await?
        {
            FenceEmission::Observed(echo) => {
                control::reload::record_start_fence(
                    self.pool,
                    self.req.reload_id,
                    echo.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(self.request_id),
                        source_schema: &self.req.source_schema,
                        source_table: &self.req.source_table,
                        schema_version: self.schema_version,
                    },
                )
                .await
                .context("record safe reload start fence")?;
                Ok(echo.commit_lsn)
            }
            FenceEmission::AlreadyExists => {
                wait_for_marker(
                    self.pool,
                    self.req.reload_id,
                    control::ReloadMarkerKind::Baseline,
                    self.cfg.echo_timeout,
                )
                .await
            }
            FenceEmission::SchemaChanged => Err(ConnectSchemaChanged {
                new_version: await_schema_change(
                    self.pool,
                    self.req.epoch,
                    &self.req.source_schema,
                    &self.req.source_table,
                    self.schema_version,
                    self.req.reload_id,
                    self.cfg.echo_timeout,
                )
                .await?,
            }
            .into()),
        }
    }
}

async fn await_schema_change(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    frozen: SchemaVersionNo,
    reload_id: ReloadId,
    timeout: Duration,
) -> anyhow::Result<SchemaVersionNo> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let latest = control::read_latest_version(pool, epoch, schema, table)
            .await
            .context("read registry latest version after live shape change")?;
        if let Some(version) = version_changed(frozen, latest) {
            return Ok(version);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "live source shape changed for reload {reload_id}, but schema_registry did not advance within {timeout:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_marker(
    pool: &sqlx::PgPool,
    reload_id: ReloadId,
    kind: control::ReloadMarkerKind,
    timeout: Duration,
) -> anyhow::Result<Lsn> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(marker) = control::reload::read_markers(pool, reload_id)
                .await
                .context("read reload boundary markers")?
                .into_iter()
                .find(|marker| marker.kind == kind)
            {
                return anyhow::Ok(marker.lsn);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {kind:?} marker on reload {reload_id}"))?
}

#[cfg(test)]
#[path = "reload_export_test.rs"]
mod tests;
