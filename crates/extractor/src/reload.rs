//! The reload controller: pickup, preflight, lease, and concurrency cap (reload H6/H7/H11).
//!
//! The export is a **extractor-owned side task the replication loop never waits for** (H6): if the
//! stream stalled for one table's export, the single slot would lag for *every* table — the exact
//! failure the reload design exists to avoid. So the controller lives entirely off the decode
//! path: its own control-pg pool, its own source SQL connection for catalog preflight, and the
//! only shared state is the `Arc<FenceWaiters>` the decode loop resolves.
//!
//! Each tick (the heartbeat cadence): claim up to *free-permit-count* `requested` rows
//! (`control::reload::claim_requested` — the DB guards double-claims), preflight each (fail fast
//! at request time, not mid-export — H11), and spawn an exporter per survivor under a
//! `tokio::sync::Semaphore` sized `max_concurrent_reloads` — "reload N tables" drains a queue
//! politely. Exporters renew their lease at TTL/3 for as long as they run; a lost lease cancels
//! the exporter. The exporter body is the chunk engine (`crate::reload_export`), driven under the
//! lease guard below.
//!
//! The lease is liveness today and the future fence: under transformer sharding (deferred goal §2),
//! `lease_holder` plus the `table_ownership` fencing-token pattern is how a stale extractor would be
//! kept from double-exporting. Noted, deliberately not built (`replicas=1`).

use crate::reload_event::FenceWaiters;
use anyhow::Context as _;
use common::{EpochNo, Redacted};
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::AsyncFnMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

/// How long shutdown waits for exporters before aborting them. This is deliberately a fixed slice
/// of the Kubernetes termination grace period; a straggler remains recoverable through lease expiry
/// and startup adoption.
const EXPORTER_DRAIN_BUDGET: Duration = Duration::from_secs(5);

/// Convert a duration to the control plane's signed seconds, saturating an oversized TTL.
fn ttl_secs(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX)
}

/// Convert an in-memory count to a signed control-plane bound without wrapping.
fn count_i64(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// Convert an in-memory count to the metrics API's unsigned domain without wrapping.
fn count_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// How an exporter entered the current attempt. Every adopted connection must first recover a
/// durable H and otherwise move to a fresh attempt identity: the abandoned connection may already
/// have appended an as-yet-undecoded marker. Only an adoption with durable chunk progress spends the
/// bounded lost-snapshot restart budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotOwnership {
    Owned,
    AdoptedPristine,
    AdoptedWithProgress,
}

impl SnapshotOwnership {
    const fn was_adopted(self) -> bool {
        !matches!(self, Self::Owned)
    }

    const fn spends_restart_budget(self) -> bool {
        matches!(self, Self::AdoptedWithProgress)
    }
}

const fn adopted_snapshot_ownership(req: &control::ReloadRow) -> SnapshotOwnership {
    if req.has_export_plan || req.chunk_no > 0 || req.cursor_pk.is_some() {
        SnapshotOwnership::AdoptedWithProgress
    } else {
        SnapshotOwnership::AdoptedPristine
    }
}

/// A preflight either genuinely REJECTS the request (typed, terminal, operator-facing) or fails
/// for INFRA reasons (dead connection, timeout) — in which case the claim is released and retried
/// next tick. Conflating the two would let an idle-connection kill terminally fail a valid
/// request with a false "not in the publication" reason.
#[derive(Debug)]
pub enum PreflightOutcome {
    /// The request is genuinely invalid. Terminal: the reload row is failed with this reason.
    Rejected(Box<PreflightRejection>),
    /// Preflight could not reach a verdict. The claim is released and retried next tick, so a
    /// transient fault never fails a valid request with a misleading reason.
    Infra(anyhow::Error),
}

/// An ad-hoc failure raised *inside* a preflight is always infrastructure. Owning that as a `From`
/// impl rather than a per-site `map_err` means `?` cannot accidentally classify a dead connection
/// as a rejection: a rejection is a typed [`PreflightRejection`], constructed explicitly.
impl From<anyhow::Error> for PreflightOutcome {
    fn from(error: anyhow::Error) -> Self {
        PreflightOutcome::Infra(error)
    }
}

/// H11's fail-fast request validation: why a request never becomes an export. The reason lands in
/// `table_reload.error` verbatim, so the operator reads it off the row.
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PreflightRejection {
    /// The table is not published, so no chunk of it could ever be echoed back through the slot.
    #[error("table {0}.{1} is not in the publication")]
    NotPublished(String, String),
    /// The table has no primary key, so the chunk cursor has nothing to page on.
    #[error("table {0}.{1} has no primary key")]
    NoPrimaryKey(String, String),
    /// The table is nominally present, but global actions or a per-table restriction make its WAL
    /// incomplete for full-table reconciliation.
    #[error(transparent)]
    PublicationCoverage(#[from] crate::source_catalog::PublicationCoverageIssue),
}

/// Why a lease-guarded exporter ended (see [`lease_guarded_export`]).
#[derive(Debug)]
pub enum ExporterEnd {
    /// Shutdown: the row is deliberately left `exporting` with its lease running out — an expired
    /// lease on a non-terminal row is exactly what the startup scan adopts and resumes.
    Cancelled,
    /// A renewal found we no longer hold the lease (expired + adopted, or superseded). The export
    /// stops immediately; whoever holds the lease now owns the row.
    LostLease,
    /// The export future itself finished.
    Finished(anyhow::Result<()>),
}

/// How an exporter task itself left the controller-owned set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExporterExit {
    /// The exporter body returned; it already logged its domain outcome.
    Completed,
    /// The exporter panicked.
    Panicked,
    /// The task was aborted during bounded shutdown.
    Aborted,
}

/// Observe and classify one exporter task completion.
pub(crate) fn observe_exporter_end(joined: Result<(), JoinError>) -> ExporterExit {
    match joined {
        Ok(()) => {
            tracing::debug!("reload exporter task joined");
            ExporterExit::Completed
        }
        Err(error) if error.is_panic() => {
            tracing::error!(
                error = ?error,
                "reload exporter panicked; its lease will expire and startup adoption will resume it"
            );
            ExporterExit::Panicked
        }
        Err(error) => {
            tracing::info!(
                error = ?error,
                "reload exporter aborted after drain budget; lease expiry and startup adoption will resume it"
            );
            ExporterExit::Aborted
        }
    }
}

/// Drain all exporters within `budget`, then abort and join any stragglers.
pub(crate) async fn drain_exporters(set: &mut JoinSet<()>, budget: Duration) {
    let drained = tokio::time::timeout(budget, async {
        while let Some(joined) = set.join_next().await {
            observe_exporter_end(joined);
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            exporters = set.len(),
            ?budget,
            "reload exporter drain budget exhausted; aborting stragglers"
        );
        set.abort_all();
        while let Some(joined) = set.join_next().await {
            observe_exporter_end(joined);
        }
    }
}

/// Drive `export` while renewing its lease every `renew_every`; the first failed renewal cancels
/// the export. Pure orchestration — the lease action and the export are injected, so the
/// cancel-on-lost-lease contract is unit-tested without a database.
///
/// `export` is pinned once and polled by `&mut`, so a renewal tick can never restart it from
/// scratch; `renew()` likewise runs inside its selected branch body, where no sibling can tear it.
/// The cancel and lost-lease arms, though, *do* drop `export` mid-flight — so whatever is passed
/// here must be a future whose interrupted work is either already durable or safely replayable. The
/// production one is [`ChunkExporter::run`](crate::reload_export::ChunkExporter::run), which commits
/// each chunk atomically for exactly that reason.
///
/// # Panics
///
/// Panics if `renew_every` is zero — [`tokio::time::interval`] rejects a zero period. The
/// controller passes `ttl / 3`, and config bounds `reload_lease_ttl` at ≥ 15s, so it cannot.
pub async fn lease_guarded_export<R, E>(
    token: CancellationToken,
    renew_every: Duration,
    mut renew: R,
    export: E,
) -> ExporterEnd
where
    R: AsyncFnMut() -> anyhow::Result<bool>,
    E: std::future::Future<Output = anyhow::Result<()>>,
{
    tokio::pin!(export);
    let mut renew_tick = tokio::time::interval(renew_every);
    renew_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    renew_tick.tick().await; // the immediate first tick — the claim just set the lease
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return ExporterEnd::Cancelled,
            res = &mut export => return ExporterEnd::Finished(res),
            _ = renew_tick.tick() => {
                match renew().await {
                    Ok(true) => {}
                    Ok(false) => return ExporterEnd::LostLease,
                    // A transient renewal error is NOT a lost lease: keep exporting — the lease
                    // expiry is the real deadline, and the next tick retries.
                    // `?e` — a bare `%e` on an `anyhow::Error` prints ONLY the outermost context,
                    // dropping every `.context(..)` layer below it. These controller logs are
                    // non-fatal, so this is the only place the chain is ever rendered (`main`
                    // prints the fatal one).
                    Err(e) => tracing::warn!(
                        error = ?e,
                        "reload lease renewal errored; retrying"
                    ),
                }
            }
        }
    }
}

/// The outcome of a bounded exporter-attempt restart.
#[derive(Debug, Clone, Copy)]
pub enum RestartDecision {
    /// A fresh successor with a fresh F; keep exporting under this `reload_id`.
    Restarted(common::ReloadId),
    /// `reload_max_restarts` is spent — the reload is now `failed`; stop.
    Capped,
}

/// Run [`control::reload::restart_for_ddl`] and emit the matching metric. Split from the
/// export loop so a compose test drives this exact path — metric increment included — without
/// standing up the whole controller.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if a control-pool connection cannot be acquired or
/// [`control::ControlError`] prevents atomically failing and restarting the attempt.
pub async fn handle_ddl_restart(
    pool: &sqlx::PgPool,
    old: &control::ReloadRow,
    new_version: common::SchemaVersionNo,
    max_restarts: i32,
) -> anyhow::Result<RestartDecision> {
    let table = format!("{}.{}", old.source_schema, old.source_table);
    let mut conn = pool
        .acquire()
        .await
        .context("acquire a control-pg connection for the reload ddl restart")?;
    match control::reload::restart_for_ddl(&mut conn, old, new_version, max_restarts)
        .await
        .with_context(|| format!("restart reload {} at schema v{new_version}", old.reload_id))?
    {
        Some(new_id) => {
            common::metrics::record_reload_restart(&table);
            tracing::info!(
                old_reload_id = %old.reload_id,
                new_reload_id = %new_id,
                new_version = %new_version,
                restart_count = old.restart_count.saturating_add(1),
                "reload restarted at the new schema (DDL landed between chunks)"
            );
            Ok(RestartDecision::Restarted(new_id))
        }
        None => {
            common::metrics::record_reload_restart_cap_exhausted();
            common::metrics::record_reload_failed(&table);
            tracing::error!(
                reload_id = %old.reload_id,
                new_version = %new_version,
                max_restarts,
                "reload restart cap exhausted — attempt failed (visible waste, not silent corruption)"
            );
            Ok(RestartDecision::Capped)
        }
    }
}

/// Supersede an adopted attempt whose connection-local source snapshot was lost, emitting the
/// same bounded-restart metrics as DDL recovery. The control transaction fails and purges the
/// predecessor before inserting its lease-carrying successor.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if a control-pool connection cannot be acquired or the atomic
/// fail/purge/successor transaction fails.
pub async fn handle_lost_snapshot_restart(
    pool: &sqlx::PgPool,
    old: &control::ReloadRow,
    max_restarts: i32,
) -> anyhow::Result<RestartDecision> {
    let table = format!("{}.{}", old.source_schema, old.source_table);
    let mut conn = pool
        .acquire()
        .await
        .context("acquire a control-pg connection for lost-snapshot restart")?;
    match control::reload::restart_for_lost_snapshot(&mut conn, old, max_restarts)
        .await
        .with_context(|| {
            format!(
                "restart reload {} after source snapshot loss",
                old.reload_id
            )
        })? {
        Some(new_id) => {
            common::metrics::record_reload_restart(&table);
            tracing::info!(
                old_reload_id = %old.reload_id,
                new_reload_id = %new_id,
                restart_count = old.restart_count.saturating_add(1),
                "adopted reload restarted with a fresh source snapshot and F"
            );
            Ok(RestartDecision::Restarted(new_id))
        }
        None => {
            common::metrics::record_reload_restart_cap_exhausted();
            common::metrics::record_reload_failed(&table);
            tracing::error!(
                reload_id = %old.reload_id,
                max_restarts,
                "lost-snapshot restart cap exhausted — attempt failed"
            );
            Ok(RestartDecision::Capped)
        }
    }
}

/// Classify a preflighted target from its two independent catalog answers (H11): not being in the
/// publication outranks having no primary key, so a table that is neither still reports the
/// publication gap the operator must fix first.
///
/// Pure, and split out of `ReloadController::preflight` deliberately: now that the two catalog
/// reads run concurrently, that precedence no longer follows from short-circuiting the second
/// query — it lives here alone, where a test pins it without a database.
fn classify_target(
    published: bool,
    has_pk: bool,
    schema: &str,
    table: &str,
) -> Result<(), PreflightRejection> {
    if !published {
        return Err(PreflightRejection::NotPublished(
            schema.to_string(),
            table.to_string(),
        ));
    }
    if !has_pk {
        return Err(PreflightRejection::NoPrimaryKey(
            schema.to_string(),
            table.to_string(),
        ));
    }
    Ok(())
}

fn classify_publication_issue(
    issue: crate::source_catalog::PublicationCoverageIssue,
    schema: &str,
    table: &str,
) -> PreflightRejection {
    if matches!(
        issue,
        crate::source_catalog::PublicationCoverageIssue::MissingTarget { .. }
    ) {
        PreflightRejection::NotPublished(schema.to_string(), table.to_string())
    } else {
        PreflightRejection::PublicationCoverage(issue)
    }
}

/// The connections + config an exporter needs, bundled so the restart loop takes few args.
struct ExportDeps {
    /// Wrapped for `ReloadController::source_db_url`'s reason — this is a clone of it.
    source_db_url: Redacted<String>,
    pool: sqlx::PgPool,
    waiters: Arc<FenceWaiters>,
    stager: crate::staging::ParquetStager,
    export_cfg: crate::reload_export::ChunkExportConfig,
}

fn connect_schema_change(error: &anyhow::Error) -> Option<common::SchemaVersionNo> {
    error
        .downcast_ref::<crate::reload_export::ConnectSchemaChanged>()
        .map(|changed| changed.new_version)
}

/// Export until drained under bounded fresh-attempt restarts. Structural changes and adopted
/// connection-local snapshot loss both atomically fail/purge the predecessor and reissue from
/// chunk zero with a fresh F — or stop at the cap (the row is already `failed`).
/// `current_reload_id` is shared with the lease-renewal closure: repointing it to the successor
/// BEFORE the next await keeps renewal following the lease onto the new row (which
/// `restart_for_ddl` carried the lease onto), so a renewal tick never fails against the terminal
/// predecessor.
async fn export_with_restarts(
    deps: ExportDeps,
    mut req: control::ReloadRow,
    max_restarts: i32,
    current_reload_id: Arc<AtomicI64>,
    mut snapshot_ownership: SnapshotOwnership,
) -> anyhow::Result<()> {
    use crate::reload_export::{ChunkExporter, RunOutcome};
    let pool = deps.pool;
    loop {
        let mut exporter = match ChunkExporter::connect(
            deps.source_db_url.expose(),
            pool.clone(),
            Arc::clone(&deps.waiters),
            deps.stager.clone(),
            deps.export_cfg.clone(),
            &req,
        )
        .await
        {
            Ok(exporter) => exporter,
            Err(error) => {
                let changed = connect_schema_change(&error);
                let Some(new_version) = changed else {
                    return Err(error).with_context(|| {
                        format!("connect chunk exporter for reload {}", req.reload_id)
                    });
                };
                let Some(successor) = restart_after_schema_change(
                    &pool,
                    &req,
                    new_version,
                    max_restarts,
                    &current_reload_id,
                )
                .await?
                else {
                    return Ok(());
                };
                req = successor;
                snapshot_ownership = SnapshotOwnership::Owned;
                continue;
            }
        };
        match exporter
            .run(snapshot_ownership.was_adopted())
            .await
            .with_context(|| format!("export chunks for reload {}", req.reload_id))?
        {
            RunOutcome::Drained { final_lsn } => {
                // The extractor's last act (H10): flip export_complete carrying H. The TRANSFORMER
                // then flips `complete` once transformed_lsn >= H — the extractor never writes `complete`.
                let lease = req
                    .exporter_lease(&deps.export_cfg.instance)
                    .context("resolve exporter generation for export completion")?;
                control::reload::complete_export(&pool, &lease, final_lsn)
                    .await
                    .with_context(|| format!("mark reload {} export complete", req.reload_id))?;
                tracing::info!(
                    reload_id = %req.reload_id,
                    final_lsn = %final_lsn,
                    "reload export_complete (transformer flips complete once transformed_lsn >= H)"
                );
                return Ok(());
            }
            RunOutcome::SchemaChanged { new_version } => {
                let Some(successor) = restart_after_schema_change(
                    &pool,
                    &req,
                    new_version,
                    max_restarts,
                    &current_reload_id,
                )
                .await?
                else {
                    return Ok(());
                };
                req = successor;
                snapshot_ownership = SnapshotOwnership::Owned;
            }
            RunOutcome::SnapshotLost => {
                let successor = if snapshot_ownership.spends_restart_budget() {
                    let Some(successor) =
                        restart_after_snapshot_loss(&pool, &req, max_restarts, &current_reload_id)
                            .await?
                    else {
                        return Ok(());
                    };
                    successor
                } else {
                    restart_after_pristine_adoption(&pool, &req, &current_reload_id).await?
                };
                req = successor;
                snapshot_ownership = SnapshotOwnership::Owned;
            }
        }
    }
}

async fn restart_after_schema_change(
    pool: &sqlx::PgPool,
    req: &control::ReloadRow,
    new_version: common::SchemaVersionNo,
    max_restarts: i32,
    current_reload_id: &AtomicI64,
) -> anyhow::Result<Option<control::ReloadRow>> {
    match handle_ddl_restart(pool, req, new_version, max_restarts).await? {
        RestartDecision::Restarted(new_id) => {
            // Release publishes the lease-carrying successor before the next await. The renewal
            // shares this task today; the ordering remains correct if it later moves independently.
            current_reload_id.store(new_id.0, Ordering::Release);
            let successor = control::reload::get(pool, new_id)
                .await
                .with_context(|| format!("read successor reload {new_id}"))?
                .ok_or_else(|| {
                    anyhow::anyhow!("successor reload {new_id} vanished after restart")
                })?;
            Ok(Some(successor))
        }
        RestartDecision::Capped => Ok(None),
    }
}

async fn restart_after_snapshot_loss(
    pool: &sqlx::PgPool,
    req: &control::ReloadRow,
    max_restarts: i32,
    current_reload_id: &AtomicI64,
) -> anyhow::Result<Option<control::ReloadRow>> {
    match handle_lost_snapshot_restart(pool, req, max_restarts).await? {
        RestartDecision::Restarted(new_id) => {
            current_reload_id.store(new_id.0, Ordering::Release);
            let successor = control::reload::get(pool, new_id)
                .await
                .with_context(|| format!("read lost-snapshot successor reload {new_id}"))?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "lost-snapshot successor reload {new_id} vanished after restart"
                    )
                })?;
            Ok(Some(successor))
        }
        RestartDecision::Capped => Ok(None),
    }
}

async fn restart_after_pristine_adoption(
    pool: &sqlx::PgPool,
    req: &control::ReloadRow,
    current_reload_id: &AtomicI64,
) -> anyhow::Result<control::ReloadRow> {
    let mut conn = pool
        .acquire()
        .await
        .context("acquire a control-pg connection for pristine-adoption restart")?;
    let new_id = control::reload::restart_pristine_adoption(&mut conn, req)
        .await
        .with_context(|| {
            format!(
                "restart pristine adopted reload {} with a fresh fence identity",
                req.reload_id
            )
        })?;
    current_reload_id.store(new_id.0, Ordering::Release);
    let successor = control::reload::get(pool, new_id)
        .await
        .with_context(|| format!("read pristine-adoption successor reload {new_id}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("pristine-adoption successor reload {new_id} vanished after restart")
        })?;
    tracing::info!(
        old_reload_id = %req.reload_id,
        new_reload_id = %new_id,
        restart_count = req.restart_count,
        "adopted pristine reload moved to a fresh fence identity without spending restart budget"
    );
    Ok(successor)
}

/// Everything the controller needs, cut from [`ExtractorConfig`](crate::config::ExtractorConfig) + bootstrap
/// state.
#[derive(Clone, Debug)]
pub struct ReloadControllerConfig {
    /// Poll cadence — the heartbeat cadence (`heartbeat_idle_after`), per the task's contract.
    pub poll_interval: Duration,
    /// Concurrent exporters — the semaphore's permit count, carried from
    /// [`ExtractorConfig`](crate::config::ExtractorConfig)'s `NonZeroU64` without ever widening back to a
    /// zero-able count. A zero-permit semaphore is not a *paused* controller but a dead one: `tick`
    /// and `adopt_and_resume` would find no free permits, return early every cadence, and leave
    /// `requested` rows queued forever with nothing in the logs to say why.
    pub max_concurrent_reloads: NonZeroUsize,
    /// Maximum source COPY workers assigned to one exporting table. The total configured source
    /// COPY-stream ceiling is this value multiplied by [`Self::max_concurrent_reloads`].
    pub workers_per_table: NonZeroUsize,
    /// Per-worker Arrow router allowance derived from the process-wide in-flight ceiling and the
    /// maximum COPY-stream count. This is internal policy, not an operator-facing fourth knob.
    pub router_batch_bytes: NonZeroU64,
    /// Process-wide memory admission shared by every table export. Its permit count is derived from
    /// `max_inflight_bytes`, so high table/worker settings cannot multiply fixed multipart buffers
    /// without bound.
    pub worker_admission: crate::reload_export::ReloadWorkerAdmission,
    /// How long an exporter's lease stays valid without renewal. Renewal runs at a third of this.
    pub lease_ttl: Duration,
    /// `lease_holder` — the same identity the heartbeat/ownership machinery uses (never a second one).
    pub instance: String,
    /// The publication a requested table must belong to; checked by preflight.
    pub publication_name: String,
    /// The generation exported chunks are stamped with.
    pub epoch: EpochNo,
    /// Records per completed remote reload object; source COPY remains streaming.
    pub chunk_rows: NonZeroU64,
    /// How long a chunk waits for its watermark echo before failing loudly (H11).
    pub echo_timeout: Duration,
    /// How many DDL-restarts a reload may consume before it fails (H9).
    pub reload_max_restarts: i32,
}

/// Extractor-owned reload orchestration (H6). Never on the replication loop's path — it holds a
/// cloned handle of the control-pg pool and dials its OWN source connections; the only shared
/// state is the waiter registry.
#[derive(Debug)]
pub struct ReloadController {
    pool: sqlx::PgPool,
    /// Catalog preflight dials a FRESH ordinary source connection per non-empty tick: reloads are
    /// rare operator events, and a held-forever idle client is exactly what proxies/failovers
    /// silently kill — a dead connection must never masquerade as a preflight rejection.
    ///
    /// Wrapped in [`Redacted`] because this struct derives `Debug` and a libpq URL carries its
    /// password inline.
    source_db_url: Redacted<String>,
    /// Exporters subscribe here before signalling; the decode loop resolves.
    waiters: Arc<FenceWaiters>,
    /// Each exporter clones a handle: chunk Parquet lands in the same epoch-prefixed layout.
    stager: crate::staging::ParquetStager,
    cfg: ReloadControllerConfig,
    semaphore: Arc<Semaphore>,
    token: CancellationToken,
}

/// What woke the controller loop. Handling the event after `select!` releases `JoinSet`'s mutable
/// borrow before a tick can add more tasks.
#[derive(Debug)]
enum ControllerEvent {
    Cancelled,
    Joined(Result<(), JoinError>),
    Tick,
}

impl ReloadController {
    /// Spawn the controller task next to the heartbeat. Failures inside the task are logged and
    /// retried next tick — the controller can degrade, never take the extractor down.
    ///
    /// The returned handle is the only channel the panic below can surface on, so dropping it on the
    /// floor loses that report — hence `#[must_use]`, as on `transformer`'s `spawn_epoch_watch` and
    /// `spawn_renewer`. `clippy::must_use_candidate` cannot demand it here: the `sqlx::PgPool`
    /// argument is not `Freeze`, so that lint reads the signature as side-effecting and skips it.
    ///
    /// # Panics
    ///
    /// The spawned task panics if `cfg.poll_interval` is zero — [`tokio::time::interval`] rejects a
    /// zero period. That one failure is not a degradation the tick loop can absorb: it surfaces on
    /// the returned [`JoinHandle`](tokio::task::JoinHandle), since the interval is built before the
    /// loop starts.
    #[must_use]
    pub fn spawn(
        pool: sqlx::PgPool,
        source_db_url: &str,
        waiters: Arc<FenceWaiters>,
        stager: crate::staging::ParquetStager,
        cfg: ReloadControllerConfig,
        token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let controller = ReloadController {
            pool,
            source_db_url: source_db_url.into(),
            waiters,
            stager,
            semaphore: Arc::new(Semaphore::new(cfg.max_concurrent_reloads.get())),
            token: token.clone(),
            cfg,
        };
        tokio::spawn(async move {
            let mut exporters = JoinSet::new();
            // Startup crash-recovery: adopt + resume our own / orphaned exporting reloads
            // ONCE, before the tick loop, unless we're already shutting down.
            if !token.is_cancelled() {
                controller.adopt_and_resume(&mut exporters, true).await;
            }
            let mut tick = tokio::time::interval(controller.cfg.poll_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let event = tokio::select! {
                    biased;
                    _ = token.cancelled() => ControllerEvent::Cancelled,
                    Some(joined) = exporters.join_next(), if !exporters.is_empty() => {
                        ControllerEvent::Joined(joined)
                    }
                    _ = tick.tick() => ControllerEvent::Tick,
                };

                match event {
                    ControllerEvent::Cancelled => {
                        // Graceful shutdown: exporters see the same token and end Cancelled. Bound
                        // the join so a wedged dependency cannot consume the pod's whole grace period.
                        tracing::info!(
                            exporters = exporters.len(),
                            "reload controller cancelled; draining exporters"
                        );
                        drain_exporters(&mut exporters, EXPORTER_DRAIN_BUDGET).await;
                        return;
                    }
                    ControllerEvent::Joined(joined) => {
                        observe_exporter_end(joined);
                    }
                    ControllerEvent::Tick => {
                        // The tick itself races the token too: a wedged claim/preflight must
                        // never block `handle.await` in the shutdown path. A mid-claim drop can
                        // leave rows `exporting` with a dying lease — lease expiration plus startup adoption
                        // is the designed net for exactly that.
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => {
                                tracing::info!(
                                    exporters = exporters.len(),
                                    "reload controller cancelled mid-tick; draining exporters"
                                );
                                drain_exporters(&mut exporters, EXPORTER_DRAIN_BUDGET).await;
                                return;
                            }
                            res = controller.tick(&mut exporters) => {
                                if let Err(e) = res {
                                    tracing::warn!(
                                        error = ?e,
                                        "reload controller tick failed; retrying next tick"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// One tick: claim ≤ free-permit `requested` rows, preflight each, spawn exporters for the
    /// survivors. Claiming only what can run keeps queued requests in `requested`, where another
    /// (future) extractor instance — or the next tick — can pick them up.
    ///
    /// Error discipline after the claim: **never `?` inside the per-row loop.** The claim already
    /// flipped every row to `exporting`, so an early return would orphan the siblings — claimed,
    /// leased, but with no exporter and no way back (`claim_requested` only sees `requested`).
    /// A typed rejection `fail`s its row; an INFRA error (dead source connection, control-pg
    /// blip) `release_claim`s it back to `requested` for the next tick — an infra failure must
    /// never be recorded as a terminal, operator-misleading preflight rejection.
    async fn tick(&self, exporters: &mut JoinSet<()>) -> anyhow::Result<()> {
        // Surface genuinely stuck exports every tick — independent of free permits, and
        // best-effort so a transient control-pg blip on this read never skips the claim below.
        if let Err(e) = self.warn_stuck().await {
            tracing::debug!(error = ?e, "stuck-reload scan failed this tick");
        }
        self.maybe_complete_bootstrap().await?;
        self.adopt_and_resume(exporters, false).await;
        let free = self.semaphore.available_permits();
        if free == 0 {
            return Ok(());
        }
        // A single guarded UPDATE: if THIS errors, nothing was claimed — safe to propagate.
        let claimed = control::reload::claim_requested(
            &self.pool,
            self.cfg.epoch,
            &self.cfg.instance,
            ttl_secs(self.cfg.lease_ttl),
            count_i64(free),
        )
        .await
        .with_context(|| format!("claim up to {free} reloads in epoch {}", self.cfg.epoch))?;
        if claimed.is_empty() {
            return Ok(());
        }
        // Fresh preflight connection per non-empty tick (see the field doc). If the SOURCE is
        // unreachable, no preflight can be trusted: release every claim and retry next tick.
        let connected = crate::preflight::connect_source(self.source_db_url.expose()).await;
        let Ok(source) = connected.inspect_err(|e| {
            tracing::warn!(
                error = ?e,
                claims = claimed.len(),
                "preflight source connection failed; releasing claims to retry next tick"
            );
        }) else {
            for req in &claimed {
                self.release_row(req).await;
            }
            return Ok(());
        };
        for req in claimed {
            match self.preflight(&source, &req).await {
                Ok(()) => {}
                Err(PreflightOutcome::Rejected(rejection)) => {
                    tracing::warn!(
                        reload_id = %req.reload_id,
                        source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                        reason = %rejection,
                        "reload request rejected at preflight"
                    );
                    common::metrics::record_reload_failed(&format!(
                        "{}.{}",
                        req.source_schema, req.source_table
                    ));
                    if let Err(e) = self.fail_row(&req, &rejection.to_string()).await {
                        tracing::error!(
                            reload_id = %req.reload_id,
                            error = ?e,
                            "could not record the rejection; releasing the claim instead"
                        );
                        self.release_row(&req).await;
                    }
                    continue;
                }
                Err(PreflightOutcome::Infra(e)) => {
                    tracing::warn!(
                        reload_id = %req.reload_id,
                        error = ?e,
                        "preflight infra error (NOT a rejection); releasing the claim to retry"
                    );
                    self.release_row(&req).await;
                    continue;
                }
            }
            // The permit is held INSIDE the spawned task — dropping it on task exit frees the slot.
            let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() else {
                // All permits raced away within this tick (can't happen while this controller
                // is the only claimant; harmless if it ever does): leave the row `exporting`
                // with its lease — lease expiration plus startup adoption recover it.
                tracing::warn!(reload_id = %req.reload_id, "no free permit after claim");
                continue;
            };
            tracing::info!(
                reload_id = %req.reload_id,
                source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                flavor = req.flavor.as_str(),
                "reload claimed → exporting; exporter scheduled"
            );
            self.spawn_exporter(exporters, req, permit, SnapshotOwnership::Owned);
        }
        Ok(())
    }

    /// Spawn the lease-guarded exporter for a claimed or adopted reload, holding `permit` inside the
    /// task so the slot frees on exit. Shared by ordinary pickup ([`tick`]) and crash-recovery
    /// ([`adopt_and_resume`]) — both hand it a row already `exporting` with a fresh lease.
    fn spawn_exporter(
        &self,
        exporters: &mut JoinSet<()>,
        req: control::ReloadRow,
        permit: tokio::sync::OwnedSemaphorePermit,
        snapshot_ownership: SnapshotOwnership,
    ) {
        let pool = self.pool.clone();
        let holder = self.cfg.instance.clone();
        let ttl = self.cfg.lease_ttl;
        let max_restarts = self.cfg.reload_max_restarts;
        let child = self.token.child_token();
        let export_cfg = crate::reload_export::ChunkExportConfig {
            chunk_rows: self.cfg.chunk_rows,
            router_batch_bytes: self.cfg.router_batch_bytes,
            worker_admission: self.cfg.worker_admission.clone(),
            workers_per_table: self.cfg.workers_per_table,
            echo_timeout: self.cfg.echo_timeout,
            instance: self.cfg.instance.clone(),
            epoch: self.cfg.epoch,
            publication_name: self.cfg.publication_name.clone(),
        };
        let source_db_url = self.source_db_url.clone();
        let waiters = Arc::clone(&self.waiters);
        let stager = self.stager.clone();
        // The reload-active gauge: +1 for this exporter task's flavor now, -1 when it
        // ends (any exit path). The flavor is stable across DDL-restarts, so one task = one count.
        let flavor = req.flavor.as_str();
        common::metrics::inc_reload_active(flavor);
        // One span per exporter task: up to `max_concurrent_reloads` of these run at once, and the
        // chunk engine underneath (`reload_export`) logs per chunk, per echo retry and per DDL
        // restart — interleaved, those read as one stream of anonymous chunk lines. The span field
        // is `source_table`, NOT `reload_id`: DDL restart reissues the attempt under a SUCCESSOR
        // reload_id mid-task, so an id frozen at spawn would contradict the very events it labels,
        // while the table being reloaded is invariant for the task's whole life (the events keep
        // spelling the live `reload_id` themselves). `.instrument(span)` and never `span.enter()` —
        // this future parks on chunk I/O and can resume on a different worker thread.
        let table = format!("{}.{}", req.source_schema, req.source_table);
        let span = tracing::info_span!("reload_export", source_table = %table);
        let exporter = async move {
            let _permit = permit;
            // The lease-renewal target: the export loop repoints this on every DDL-restart, so
            // renewal follows the lease onto each successor row.
            let current_reload_id = Arc::new(AtomicI64::new(req.reload_id.0));
            let exporter_generation = req.exporter_generation;
            let renew_pool = pool.clone();
            let renew_id = Arc::clone(&current_reload_id);
            // The chunk engine under bounded restart: dial the side connection and export one
            // repeatable-read snapshot until drained. Adoption without durable H and DDL both
            // create a fresh fenced successor before continuing, then flip export_complete. Echo timeout
            // fails the row inside; any other error leaves it `exporting` for lease-expiry and
            // startup adoption (infra errors are retried, never terminally mis-recorded).
            let export = export_with_restarts(
                ExportDeps {
                    source_db_url,
                    pool,
                    waiters,
                    stager,
                    export_cfg,
                },
                req,
                max_restarts,
                Arc::clone(&current_reload_id),
                snapshot_ownership,
            );
            let end = lease_guarded_export(
                child,
                ttl / 3,
                // An `async` closure, not `move || { .. async move { .. } }`: the `AsyncFnMut`
                // bound links the renewal future's lifetime to the call, so the body borrows the
                // pool and the holder straight from the closure — no per-tick clone of either.
                async move || {
                    // Acquire pairs with the DDL-restart Release that repoints lease renewal.
                    let reload_id = common::ReloadId(renew_id.load(Ordering::Acquire));
                    let lease = control::ExporterLease {
                        reload_id,
                        holder: holder.clone(),
                        generation: exporter_generation,
                    };
                    control::reload::renew_lease(&renew_pool, &lease, ttl_secs(ttl))
                        .await
                        .with_context(|| format!("renew lease for reload {reload_id}"))
                },
                export,
            )
            .await;
            // Acquire pairs with the DDL-restart Release for the final successor-aware log.
            let reload_id = common::ReloadId(current_reload_id.load(Ordering::Acquire));
            match &end {
                ExporterEnd::Cancelled => tracing::info!(
                    reload_id = %reload_id,
                    "exporter cancelled (shutdown); row left for startup-scan resume"
                ),
                ExporterEnd::LostLease => tracing::warn!(
                    reload_id = %reload_id,
                    "exporter lost its lease; stopping (another holder owns the row now)"
                ),
                ExporterEnd::Finished(res) => match res {
                    Ok(()) => tracing::info!(reload_id = %reload_id, "export finished"),
                    Err(e) => tracing::error!(
                        reload_id = %reload_id,
                        error = ?e,
                        "export failed"
                    ),
                },
            }
            common::metrics::dec_reload_active(flavor); // balances the inc above
        };
        exporters.spawn(exporter.instrument(span));
    }

    /// Crash recovery (H7): adopt this extractor's own live leases at startup, and expired orphaned
    /// `exporting` reloads on every later tick (re-acquiring each lease in a race-safe guarded
    /// UPDATE). Durable H finishes the same attempt; otherwise every adoption creates a fresh fenced
    /// successor so an in-flight old marker cannot bind a new snapshot. A pristine pre-F or
    /// pre-chunk attempt preserves its restart count; durable chunk progress spends the bounded
    /// lost-snapshot budget. WAL redelivery is not the recovery mechanism.
    /// `include_own_live_lease` is true only before the tick loop; later scans take expired leases
    /// only, so a live task cannot be adopted twice.
    /// Bounded by the free permits, so it never oversubscribes.
    async fn adopt_and_resume(&self, exporters: &mut JoinSet<()>, include_own_live_lease: bool) {
        let free = self.semaphore.available_permits();
        if free == 0 {
            return;
        }
        let scan = control::reload::adopt_resumable(
            &self.pool,
            self.cfg.epoch,
            &self.cfg.instance,
            ttl_secs(self.cfg.lease_ttl),
            count_i64(free),
            include_own_live_lease,
        )
        .await;
        let Ok(adopted) = scan.inspect_err(|e| {
            tracing::warn!(error = ?e, "reload-adoption scan failed; requested reloads still pick up per tick");
        }) else {
            return;
        };
        for req in adopted {
            let snapshot_ownership = adopted_snapshot_ownership(&req);
            let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() else {
                tracing::warn!(
                    reload_id = %req.reload_id,
                    "no free permit to resume an adopted reload; leaving it (lease re-acquired)"
                );
                continue;
            };
            tracing::info!(
                reload_id = %req.reload_id,
                source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                status = req.status.as_str(),
                cursor_chunk = req.chunk_no,
                ?snapshot_ownership,
                "adopting reload (crash recovery)"
            );
            self.spawn_exporter(exporters, req, permit, snapshot_ownership);
        }
    }

    /// Per-tick surfacing: genuinely stuck exports — `exporting`, lease expired,
    /// nobody renewing — are warned per row AND counted into the `walrus_reload_lease_stale` gauge
    /// the stuck-lease alert reads (a gauge, so the alert never queries control-pg).
    async fn warn_stuck(&self) -> anyhow::Result<()> {
        let stuck = control::reload::stuck_exporting(&self.pool, self.cfg.epoch)
            .await
            .with_context(|| {
                format!("scan for stuck reload exports in epoch {}", self.cfg.epoch)
            })?;
        common::metrics::set_reload_lease_stale(count_u64(stuck.len()));
        for (reload_id, holder) in stuck {
            tracing::warn!(
                reload_id = %reload_id,
                lease_holder = ?holder,
                "reload stuck: exporting with an expired, unadopted lease (no live exporter renewing it)"
            );
        }
        Ok(())
    }

    /// Promote a new epoch only after every child in its durably bound all-table request has
    /// published successfully. This runs on every controller tick, so it also repairs the crash
    /// window where the final transformer cutover committed immediately before the extractor restarted.
    async fn maybe_complete_bootstrap(&self) -> anyhow::Result<()> {
        let Some(progress) = control::read_bootstrap_progress(&self.pool, self.cfg.epoch)
            .await
            .context("read bootstrap group progress")?
        else {
            return Ok(());
        };
        if progress.failed > 0 {
            tracing::error!(
                epoch = %self.cfg.epoch,
                request_id = %progress.request_id,
                expected = progress.expected_tables,
                children = progress.children,
                complete = progress.complete,
                failed = progress.failed,
                "bootstrap reconciliation has failed children; epoch remains bootstrapping"
            );
            return Ok(());
        }
        if !progress.is_ready() {
            tracing::debug!(
                epoch = %self.cfg.epoch,
                request_id = %progress.request_id,
                expected = progress.expected_tables,
                children = progress.children,
                complete = progress.complete,
                "bootstrap reconciliation still in progress"
            );
            return Ok(());
        }
        if control::complete_bootstrap(&self.pool, self.cfg.epoch, progress.request_id)
            .await
            .context("promote completed bootstrap generation")?
        {
            tracing::info!(
                epoch = %self.cfg.epoch,
                request_id = %progress.request_id,
                tables = progress.expected_tables,
                "bootstrap reconciliation complete; epoch promoted to streaming"
            );
        }
        Ok(())
    }

    /// Record a typed rejection on the row (its reason IS the operator UX).
    async fn fail_row(&self, req: &control::ReloadRow, reason: &str) -> anyhow::Result<()> {
        let reload_id = req.reload_id;
        let lease = req
            .exporter_lease(&self.cfg.instance)
            .context("resolve exporter generation for rejected reload")?;
        let mut conn = self.pool.acquire().await.with_context(|| {
            format!("acquire a control-pg connection to fail reload {reload_id}")
        })?;
        control::reload::fail_owned(&mut conn, &lease, reason)
            .await
            .with_context(|| format!("mark reload {reload_id} failed"))?;
        Ok(())
    }

    /// Un-claim a row after an infra failure: back to `requested` for the next tick. If even the
    /// release fails, the row stays `exporting` with a dying lease — lease expiration plus startup adoption
    /// is the recovery net, and the error log is the operator breadcrumb.
    async fn release_row(&self, req: &control::ReloadRow) {
        let lease = match req.exporter_lease(&self.cfg.instance) {
            Ok(lease) => lease,
            Err(error) => {
                tracing::warn!(reload_id = %req.reload_id, error = ?error, "claim row has no valid exporter generation; leaving it for adoption");
                return;
            }
        };
        match control::reload::release_claim(&self.pool, &lease).await {
            Ok(true) => tracing::info!(
                reload_id = %req.reload_id,
                "claim released → requested (retried next tick)"
            ),
            Ok(false) => tracing::warn!(
                reload_id = %req.reload_id,
                "claim no longer ours to release; leaving it"
            ),
            Err(e) => tracing::error!(
                reload_id = %req.reload_id,
                error = ?e,
                "release failed; row stays exporting — lease expiry and startup adoption recover it"
            ),
        }
    }

    /// H11, fail-fast: target in the publication, target has a PK, flavor implementable. Runs
    /// BEFORE a single signal row or chunk is spent on a doomed reload. A catalog query error is
    /// an [`PreflightOutcome::Infra`] failure, NEVER a rejection — a dead connection must not
    /// terminally fail a valid request with a false reason.
    async fn preflight(
        &self,
        source: &tokio_postgres::Client,
        req: &control::ReloadRow,
    ) -> Result<(), PreflightOutcome> {
        // Both accepted flavor spellings preflight identically: in the publication + has a PK.
        // `resync` is only a compatibility alias; it uses the same paused hidden-generation
        // rebuild and fenced publication protocol as `reload`.
        //
        // Publication actions, effective per-target options, and the PK read are independent, so
        // tokio-postgres pipelines them on this one connection. Catalog failures remain Infra;
        // only a successfully observed mismatch becomes a terminal rejection.
        let ((actions, options), has_pk) = tokio::try_join!(
            async {
                tokio::try_join!(
                    async {
                        crate::source_catalog::publication_actions(
                            source,
                            &self.cfg.publication_name,
                        )
                        .await
                        .context("read reload publication action flags")
                    },
                    async {
                        crate::source_catalog::publication_target_options(
                            source,
                            &self.cfg.publication_name,
                            &req.source_schema,
                            &req.source_table,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "publication coverage check for {}.{}",
                                req.source_schema, req.source_table
                            )
                        })
                    },
                )
            },
            async {
                source
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_index i
                                        JOIN pg_class c ON c.oid = i.indrelid
                                        JOIN pg_namespace n ON n.oid = c.relnamespace
                                        WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary)",
                        &[&req.source_schema, &req.source_table],
                    )
                    .await
                    .map(|row| row.get::<_, bool>(0))
                    .with_context(|| {
                        format!("primary-key check for {}.{}", req.source_schema, req.source_table)
                    })
            },
        )?;
        let coverage =
            crate::source_catalog::require_publication_actions(&self.cfg.publication_name, actions)
                .and_then(|()| {
                    crate::source_catalog::require_full_target(
                        &self.cfg.publication_name,
                        &req.source_schema,
                        &req.source_table,
                        options,
                    )
                });
        if let Err(issue) = coverage {
            return Err(PreflightOutcome::Rejected(Box::new(
                classify_publication_issue(issue, &req.source_schema, &req.source_table),
            )));
        }
        classify_target(true, has_pk, &req.source_schema, &req.source_table)
            .map_err(|rejection| PreflightOutcome::Rejected(Box::new(rejection)))
    }
}

#[cfg(test)]
#[path = "reload_test.rs"]
mod tests;
