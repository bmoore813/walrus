//! The extractor's service orchestration — everything the pod does between a validated config and an
//! exit code.
//!
//! [`run`] owns the whole lifecycle: signal handlers, the health server bound before the slow
//! bootstrap, the slot decision (resume vs. fresh-slot reconciliation), the decode loop, and the drain of
//! its side tasks. It hands failures back as `anyhow::Error` values, leaving `src/main.rs` with only
//! what cannot live in a library — the config load, the one `init_tracing` install, the runtime
//! build, and the single `anyhow::Error → ExitCode` mapping. That split is what puts this module
//! behind the `extractor` *library*, so `crates/extractor/tests/` — which links the library and never
//! sees the binary — can reach it.

use crate::config::ExtractorConfig;
use crate::replication::ReplicationStream;
use crate::{bootstrap, consume, health, shutdown};
use anyhow::Context;
use common::EpochNo;
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

/// The lifecycle: install signals, bind health (so probes see 503 during the slow bootstrap), run
/// the shared preflight, mark ready, then stream until SIGTERM.
///
/// # Errors
///
/// Returns the health server's bind, join or serve failure, or whatever the pipeline classified —
/// preflight, slot classification, reconciliation or decode. Each is context-wrapped for the log; the
/// caller recovers the typed exit code with [`crate::exit::code_for`].
pub async fn run(cfg: ExtractorConfig) -> anyhow::Result<()> {
    let token = shutdown::install_signal_handlers();
    let state = health::HealthState::new();
    // Install the Prometheus recorder before anything can serve /metrics or emit a series.
    common::metrics::init();

    // Bind health *after* config validated (no half-open port on a config crash) but *before* the
    // dependency checks, so `/startup` answers 503 while control PG / S3 come up, then flips to 200.
    let listener = tokio::net::TcpListener::bind(cfg.extractor.health_addr)
        .await
        .with_context(|| format!("bind health endpoints on {}", cfg.extractor.health_addr))?;
    let bound = listener.local_addr().context("read health bind address")?;
    tracing::info!(%bound, "health endpoints listening; bootstrapping");
    let server = tokio::spawn(health::serve_on(
        listener,
        Arc::clone(&state),
        token.clone(),
    ));

    let result = shutdown::cancel_on_exit(&token, Box::pin(pipeline(&cfg, &token, &state))).await;
    tracing::info!("draining health server");
    server
        .await
        .context("health server task join")?
        .context("health server")?;
    result
}

/// The fallible middle of the extractor lifecycle. The caller holds a cancellation drop guard for this
/// future's entire lifetime, so every early return winds down token-driven side tasks.
async fn pipeline(
    cfg: &ExtractorConfig,
    token: &tokio_util::sync::CancellationToken,
    state: &health::HealthState,
) -> anyhow::Result<()> {
    // Shared bootstrap steps 2–4. The enclosing drop guard tears down token-driven tasks before a
    // classified error reaches `main`.
    let deadline = Instant::now() + cfg.extractor.startup_deadline;
    let mut ctx = bootstrap::run_shared(cfg, deadline).await?;
    acquire_pipeline_publication_guard(cfg, &ctx.source_client)
        .await
        .context("acquire continuous publication-DDL guard")?;

    const SCHEMA_VERSION: common::SchemaVersionNo = common::SchemaVersionNo(1);
    let triggers = crate::batch::BatchTriggers {
        max_fill: cfg.extractor.max_fill,
        max_rows: cfg.extractor.max_rows,
        max_bytes: cfg.extractor.max_bytes,
    };
    let mut cache = crate::relcache::RelationCache::default();

    // Bootstrap decision (§1.7 / §1.8): a **pre-existing slot** retains WAL, but a new process has
    // lost the continuous publication-DDL guard and therefore opens a reconciled successor at the
    // retained floor. **No slot** means first bootstrap: establish the stream first, then rebuild
    // every table through the same in-band F/dump/H reconciliation used for an operator reload.
    let Bootstrapped {
        mut stream,
        epoch,
        start_lsn,
        stager,
    } = establish_stream(cfg, &mut ctx, &mut cache, SCHEMA_VERSION).await?;
    // DDL state is restartable: hydrate every historical registered shape and processed source
    // audit identity before reading another WAL frame. A replayed transaction then reconstructs
    // its exact old version instead of manufacturing N+1 or falling forward to the latest shape.
    let mut ddl = crate::ddl::DdlConsumer::new(epoch);
    ddl.hydrate_versions(&cache);
    ddl.hydrate_history(
        control::read_all_ddl(&ctx.control_pool, epoch)
            .await
            .context("hydrate DDL history")?,
    );
    // The shared dependency checks are only the first half of bootstrap. Do not advertise ready
    // until slot classification plus generation registration has completed:
    // the transformer is allowed to start as soon as this endpoint flips, and it needs the epoch to exist.
    state.mark_ready();
    tracing::info!("bootstrap complete; ready");
    tracing::info!(slot = %cfg.extractor.slot_name, start_lsn = %start_lsn, epoch = %epoch, "streaming logical replication");

    // ONE wall clock for the whole decode path: the router's and the demux's batchers share it by
    // `Arc` (see `batch::Clock`), so the test seam has a single instant source, not two.
    let clock = Arc::new(crate::batch::SystemClock);
    let mut router =
        consume::BatchRouter::new(triggers, Arc::clone(&clock), epoch, &cfg.extractor.instance);
    let mut checkpoint = crate::checkpoint::DurabilityCheckpoint::new(start_lsn);
    let process_memory_budget = Arc::new(crate::memory::ProcessMemoryBudget::new(
        cfg.extractor.max_inflight_bytes,
    ));
    // Large-transaction demux (§1.6): a txn over logical_decoding_work_mem streams before its commit.
    let mut demux = crate::stream_txn::StreamDemux::with_process_memory_budget(
        triggers,
        clock,
        epoch,
        &cfg.extractor.instance,
        cfg.extractor.max_inflight_bytes,
        Arc::clone(&process_memory_budget),
    );

    // The idle heartbeat rides a SEPARATE ordinary SQL connection (distinct from replication); its
    // beat writes the published `public.walrus_heartbeat`, whose round-trip through the stream advances the
    // slot on an otherwise-idle publication (§1.9).
    let mut heartbeat = crate::heartbeat::Heartbeat::connect(
        cfg.extractor.source_db_url.expose(),
        &cfg.extractor.instance,
        crate::heartbeat::HeartbeatConfig {
            idle_after: cfg.extractor.heartbeat_idle_after,
            roundtrip_deadline: cfg.extractor.heartbeat_roundtrip_deadline,
        },
    )
    .await
    .context("connect heartbeat SQL connection")?;

    // Legacy per-chunk signals remain decodable during rolling upgrades. New exporters share only
    // the start/end fence registry with the decoder.
    let signal_waiters = std::sync::Arc::new(crate::reload_signal::WatermarkWaiters::default());
    let fence_waiters = std::sync::Arc::new(crate::reload_event::FenceWaiters::default());

    // The reload controller: a side task off the decode path — own connections, polls
    // table_reload on the heartbeat cadence, schedules exporters under max_concurrent_reloads.
    let max_concurrent_reloads = NonZeroUsize::try_from(cfg.extractor.max_concurrent_reloads)
        .context("max_concurrent_reloads does not fit usize")?;
    let reload_workers_per_table = NonZeroUsize::try_from(cfg.extractor.reload_workers_per_table)
        .context("reload_workers_per_table does not fit usize")?;
    let max_reload_copy_streams = max_concurrent_reloads
        .get()
        .checked_mul(reload_workers_per_table.get())
        .context("configured reload table/worker product does not fit usize")?;
    let max_reload_copy_streams_nonzero = NonZeroUsize::new(max_reload_copy_streams)
        .context("configured reload COPY-stream limit became zero")?;
    let reload_memory_worker_limit = crate::reload_export::reload_memory_worker_limit(
        cfg.extractor.max_inflight_bytes,
        max_reload_copy_streams_nonzero,
    );
    let reload_router_batch_bytes = crate::reload_export::router_batch_bytes(
        cfg.extractor.max_inflight_bytes,
        reload_memory_worker_limit,
    );
    let reload_worker_admission = crate::reload_export::ReloadWorkerAdmission::with_process_budget(
        reload_memory_worker_limit,
        process_memory_budget,
    );
    tracing::info!(
        max_concurrent_reloads = max_concurrent_reloads.get(),
        reload_workers_per_table = reload_workers_per_table.get(),
        reload_chunk_rows = cfg.extractor.reload_chunk_rows.get(),
        max_reload_copy_streams,
        reload_memory_worker_limit = reload_memory_worker_limit.get(),
        reload_router_batch_bytes = reload_router_batch_bytes.get(),
        "reload extraction limits configured"
    );
    let reload_controller = crate::reload::ReloadController::spawn(
        ctx.control_pool.clone(),
        cfg.extractor.source_db_url.expose(),
        Arc::clone(&fence_waiters),
        stager.clone(),
        crate::reload::ReloadControllerConfig {
            poll_interval: cfg.extractor.heartbeat_idle_after,
            // Narrowing stays inside the non-zero domain: only the 64-bit magnitude can fail to fit
            // a `usize`, never the "at least one exporter" invariant the config already proved.
            max_concurrent_reloads,
            workers_per_table: reload_workers_per_table,
            router_batch_bytes: reload_router_batch_bytes,
            worker_admission: reload_worker_admission,
            lease_ttl: cfg.extractor.reload_lease_ttl,
            instance: cfg.extractor.instance.clone(),
            publication_name: cfg.extractor.publication_name.clone(),
            epoch,
            chunk_rows: cfg.extractor.reload_chunk_rows,
            echo_timeout: cfg.extractor.reload_echo_timeout,
            reload_max_restarts: cfg.extractor.reload_max_restarts,
        },
        token.clone(),
    );
    let slot_guard = crate::guard_monitor::spawn(
        cfg.extractor.source_db_url.expose().clone(),
        cfg.extractor.slot_name.clone(),
        cfg.extractor.heartbeat_idle_after,
        token.clone(),
    );

    let result = consume::DecodeLoop::builder()
        .stream(&mut stream)
        .token(token.clone())
        .cache(&mut cache)
        .router(&mut router)
        .stager(&stager)
        .checkpoint(&mut checkpoint)
        .demux(&mut demux)
        .ddl(&mut ddl)
        .heartbeat(&mut heartbeat)
        .health(state)
        .pool(&ctx.control_pool)
        .epoch(epoch)
        .waiters(&signal_waiters)
        .fence_waiters(&fence_waiters)
        .build()
        .context("wire the decode loop")?
        .run()
        .await;

    // Whatever ended the loop (SIGTERM, stream end, or a decode error), drain the side tasks.
    state.mark_terminating();
    token.cancel();
    let slot_guard_result = slot_guard
        .await
        .context("replication-slot guard task join")?;
    reload_controller
        .await
        .context("reload controller task join")?;
    slot_guard_result?;
    result
}

/// Hold the shared source advisory lock for this SQL session's whole lifetime. Source migration
/// 0002's command-start trigger takes the matching exclusive transaction lock, so publication DDL
/// cannot create an invisible WAL-coverage gap while the replication pipeline or any exporter is
/// online. Revalidating after acquiring closes the startup preflight/lock acquisition race.
async fn acquire_pipeline_publication_guard(
    cfg: &ExtractorConfig,
    client: &tokio_postgres::Client,
) -> anyhow::Result<()> {
    if !crate::source_catalog::try_acquire_publication_ddl_guard(client)
        .await
        .context("try shared publication-DDL advisory lock")?
    {
        return Err(common::Error::SourceDb(
            "publication DDL is in progress; retry startup after the source transaction finishes"
                .to_string(),
        )
        .into());
    }

    // Recheck the binding after lock acquisition so a missing/disabled trigger cannot slip through
    // the startup preflight-to-guard seam. PostgreSQL does not expose event-trigger administration
    // as event-trigger tags, so superuser removal after this check remains a privileged bypass; the
    // guard serializes the publication DDL issued through the supported operational path.
    crate::preflight::SourcePreflight::new(client, cfg)
        .assert_ddl_capture()
        .await
        .context("revalidate publication-DDL guard trigger under its shared lock")?;

    let actions =
        crate::source_catalog::publication_actions(client, &cfg.extractor.publication_name)
            .await
            .context("re-read publication action flags under the pipeline guard")?;
    crate::source_catalog::require_publication_actions(&cfg.extractor.publication_name, actions)
        .map_err(crate::preflight::PreflightError::from)
        .context("publication changed after startup preflight")?;

    let mut targets =
        crate::source_catalog::published_user_tables(client, &cfg.extractor.publication_name)
            .await
            .context("re-read user publication inventory under the pipeline guard")?;
    targets.extend([
        ("public".to_string(), "walrus_heartbeat".to_string()),
        ("public".to_string(), "walrus_ddl_audit".to_string()),
        ("public".to_string(), "walrus_reload_signal".to_string()),
        ("public".to_string(), "walrus_reload_event".to_string()),
    ]);
    validate_publication_targets(client, &cfg.extractor.publication_name, targets.iter()).await?;
    crate::preflight::SourcePreflight::new(client, cfg)
        .assert_table_lock_privileges()
        .await
        .context("revalidate source-table lock privileges under the publication guard")?;
    // The first PK check precedes this session lock. Repeat it here so a key can neither disappear
    // in that seam nor enter the frozen catalog inventory without a real downstream row identity.
    // ExtractorConfig rejects diagnostic-only lenient mode, but spelling Strict here also keeps this
    // safety boundary closed for programmatically constructed configurations.
    crate::preflight::SourcePreflight::new(client, cfg)
        .assert_tables_have_pk(crate::preflight::PkMode::Strict)
        .await
        .context("revalidate source-table keys under the publication guard")?;
    Ok(())
}

async fn validate_publication_targets<'a>(
    client: &tokio_postgres::Client,
    publication: &str,
    targets: impl IntoIterator<Item = &'a (String, String)>,
) -> anyhow::Result<()> {
    for (schema, table) in targets {
        let options =
            crate::source_catalog::publication_target_options(client, publication, schema, table)
                .await
                .with_context(|| format!("inspect publication coverage for {schema}.{table}"))?;
        crate::source_catalog::require_full_target(publication, schema, table, options)
            .map_err(crate::preflight::PreflightError::from)?;
    }
    Ok(())
}

/// The established streaming state after the bootstrap decision.
struct Bootstrapped {
    stream: ReplicationStream,
    epoch: EpochNo,
    start_lsn: common::Lsn,
    stager: crate::staging::ParquetStager,
}

fn generation_can_resume(
    configured_slot: &str,
    recorded_slot: &str,
    status: control::ReplicationStatus,
    publication_guard_continuity_proven: bool,
) -> bool {
    publication_guard_continuity_proven
        && configured_slot == recorded_slot
        && status != control::ReplicationStatus::TotalRestart
}

/// A session advisory lock proves publication semantics only for that source connection's
/// lifetime. Until the source exposes a durable monotonic semantics token, every process startup
/// must assume there was an offline publication interval and rebuild from a new catalog fence.
#[must_use]
const fn publication_guard_continuity_proven_on_startup() -> bool {
    false
}

#[must_use]
const fn resumed_generation_start(
    confirmed_flush: common::Lsn,
    created_lsn: common::Lsn,
) -> common::Lsn {
    if confirmed_flush.as_u64() >= created_lsn.as_u64() {
        confirmed_flush
    } else {
        created_lsn
    }
}

fn assert_generation_slot_name(
    configured_slot: &str,
    recorded_slot: &str,
) -> Result<(), crate::preflight::PreflightError> {
    if configured_slot != recorded_slot {
        return Err(crate::preflight::PreflightError::SlotNameDrift {
            configured: configured_slot.to_owned(),
            recorded: recorded_slot.to_owned(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotLossRecovery {
    CreateIfAbsent,
    ReplaceInvalidated,
}

const fn slot_loss_recovery(status: crate::epoch::SlotStatus) -> Option<SlotLossRecovery> {
    match status {
        crate::epoch::SlotStatus::Absent => Some(SlotLossRecovery::CreateIfAbsent),
        crate::epoch::SlotStatus::Invalidated => Some(SlotLossRecovery::ReplaceInvalidated),
        crate::epoch::SlotStatus::Healthy { .. } | crate::epoch::SlotStatus::Unreachable => None,
    }
}

type SourceTarget = (String, String);

#[derive(Debug, PartialEq, Eq)]
struct InventoryDifference {
    published_without_registry: Vec<SourceTarget>,
    registry_without_publication: Vec<SourceTarget>,
}

impl InventoryDifference {
    const fn is_empty(&self) -> bool {
        self.published_without_registry.is_empty() && self.registry_without_publication.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum InventoryResumeDecision {
    Resume,
    OpenSuccessor(InventoryDifference),
}

#[must_use]
fn inventory_difference(
    published: &BTreeSet<SourceTarget>,
    registered: &BTreeSet<SourceTarget>,
) -> InventoryDifference {
    InventoryDifference {
        published_without_registry: published.difference(registered).cloned().collect(),
        registry_without_publication: registered.difference(published).cloned().collect(),
    }
}

#[must_use]
fn inventory_resume_decision(
    published: &BTreeSet<SourceTarget>,
    registered: &BTreeSet<SourceTarget>,
) -> InventoryResumeDecision {
    let difference = inventory_difference(published, registered);
    if difference.is_empty() {
        InventoryResumeDecision::Resume
    } else {
        InventoryResumeDecision::OpenSuccessor(difference)
    }
}

fn ensure_catalog_fence_version_supported(state: &control::ReplicationState) -> anyhow::Result<()> {
    if state.catalog_fence_version > control::CURRENT_CATALOG_FENCE_VERSION {
        anyhow::bail!(
            "generation {} uses source catalog-fence protocol {}, but this extractor supports only {}; refusing an unsafe binary downgrade",
            state.epoch,
            state.catalog_fence_version,
            control::CURRENT_CATALOG_FENCE_VERSION
        );
    }
    Ok(())
}

/// Resume a pre-existing slot, or open a new generation whose initial state is built by the same
/// source-WAL-triggered reconciliation protocol as an ordinary all-table reload.
async fn establish_stream(
    cfg: &ExtractorConfig,
    ctx: &mut bootstrap::BootstrapCtx,
    cache: &mut crate::relcache::RelationCache,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<Bootstrapped> {
    let make_stager = |epoch| {
        crate::staging::ParquetStager::new(
            Arc::clone(&ctx.object_store),
            cfg.common.object_store.bucket.clone(),
            epoch,
        )
    };

    // One parse, at the top of the one function that names the slot: every use below reads this
    // proven value rather than the raw config text. `ExtractorConfig::validate` ran the same gate at load,
    // so a config that booted cannot fail here — this is the handoff, not a second check.
    let slot = cfg
        .extractor
        .slot()
        .context("parse slot_name for the replication commands")?;

    // Classify the slot on the (already-connected) source before deciding: resume a healthy slot, or —
    // only when the catalog authoritatively says the slot is gone — open a fresh one. A connection
    // hiccup (`Unreachable`) is NOT slot loss: exit so the orchestrator's backoff-restart reconnects,
    // never a total-restart (§1.8, the false-positive guard).
    let status = crate::epoch::classify_slot(&ctx.source_client, slot.as_str()).await;
    match crate::epoch::decide(status) {
        crate::epoch::SlotAction::Retry => {
            anyhow::bail!(
                "could not classify replication slot {slot} (source connection lost mid-bootstrap) \
                 — exiting to retry via backoff; this is NOT slot loss and does NOT bump the epoch"
            );
        }
        crate::epoch::SlotAction::Resume { confirmed_flush } => {
            let Some(state) = control::read_current_epoch(&ctx.control_pool)
                .await
                .context("read current epoch for slot resume")?
            else {
                // The slot creation and control database cannot share a transaction. If the process
                // died in that narrow seam on its very first boot, the retained WAL is still usable;
                // bind it to a new reconciliation generation instead of declaring an empty mirror
                // streaming.
                tracing::warn!(
                    slot = %slot,
                    start_lsn = %confirmed_flush,
                    "healthy slot has no control generation; recovering through all-table reconciliation"
                );
                return establish_bootstrap_generation(
                    cfg,
                    ctx,
                    cache,
                    slot.as_str(),
                    confirmed_flush,
                    None,
                    status,
                    schema_version,
                )
                .await;
            };
            assert_generation_slot_name(slot.as_str(), &state.slot_name)
                .context("configured slot differs from durable generation")?;
            ensure_catalog_fence_version_supported(&state)?;
            if state.catalog_fence_version < control::CURRENT_CATALOG_FENCE_VERSION {
                tracing::error!(
                    old_epoch = %state.epoch,
                    recorded_catalog_fence_version = state.catalog_fence_version,
                    required_catalog_fence_version = control::CURRENT_CATALOG_FENCE_VERSION,
                    "current generation predates atomic source catalog fencing; opening a reconciled successor"
                );
                return establish_bootstrap_generation(
                    cfg,
                    ctx,
                    cache,
                    slot.as_str(),
                    confirmed_flush,
                    Some(state),
                    status,
                    schema_version,
                )
                .await;
            }
            let guard_continuity_proven = publication_guard_continuity_proven_on_startup();
            if !generation_can_resume(
                slot.as_str(),
                &state.slot_name,
                state.status,
                guard_continuity_proven,
            ) {
                if !guard_continuity_proven {
                    tracing::error!(
                        configured_slot = %slot,
                        old_epoch = %state.epoch,
                        start_lsn = %confirmed_flush,
                        "continuous publication guard ended with the prior process; opening a reconciled successor so transient offline publication changes cannot omit rows"
                    );
                } else if state.status == control::ReplicationStatus::TotalRestart {
                    tracing::error!(
                        configured_slot = %slot,
                        generation_slot = %state.slot_name,
                        old_epoch = %state.epoch,
                        "TOTAL-RESTART: source slot is healthy after the durable restart intent; opening a reconciled successor generation"
                    );
                } else {
                    tracing::error!(
                        configured_slot = %slot,
                        generation_slot = %state.slot_name,
                        old_epoch = %state.epoch,
                        "healthy configured slot is not the slot bound to the current generation; opening a reconciled generation"
                    );
                }
                return establish_bootstrap_generation(
                    cfg,
                    ctx,
                    cache,
                    slot.as_str(),
                    confirmed_flush,
                    Some(state),
                    status,
                    schema_version,
                )
                .await;
            }
            let epoch = state.epoch;
            let rows = control::read_all_registry(&ctx.control_pool, epoch)
                .await
                .context("read complete schema_registry history for hydration")?;
            let registered_targets = rows
                .iter()
                .map(|row| (row.source_schema.clone(), row.source_table.clone()))
                .collect::<BTreeSet<_>>();
            let published_targets = crate::source_catalog::published_user_tables(
                &ctx.source_client,
                &cfg.extractor.publication_name,
            )
            .await
            .context("read publication inventory before epoch resume")?
            .into_iter()
            .collect::<BTreeSet<_>>();
            if let InventoryResumeDecision::OpenSuccessor(difference) =
                inventory_resume_decision(&published_targets, &registered_targets)
            {
                // Publication topology can change only while the pipeline guard is absent. Once the
                // extractor returns, a healthy slot still retains every change since confirmed_flush, but
                // the old epoch's frozen registry is no longer a complete target inventory. Open a
                // successor at that retained boundary and rebuild the newly frozen publication rather
                // than exiting into the same mismatch on every orchestrator restart.
                tracing::error!(
                    old_epoch = %epoch,
                    published_without_registry = ?difference.published_without_registry,
                    registry_without_publication = ?difference.registry_without_publication,
                    start_lsn = %confirmed_flush,
                    "publication inventory changed while the extractor was offline; opening a reconciled successor generation"
                );
                return establish_bootstrap_generation(
                    cfg,
                    ctx,
                    cache,
                    slot.as_str(),
                    confirmed_flush,
                    Some(state),
                    status,
                    schema_version,
                )
                .await;
            }
            validate_publication_targets(
                &ctx.source_client,
                &cfg.extractor.publication_name,
                registered_targets.iter(),
            )
            .await
            .context("validate resumed registry targets under publication guard")?;

            // A crash after atomically registering the bootstrap inventory but before inserting (or
            // fully consuming) its source event resumes by re-emitting the exact persisted payload.
            // The UUID and row contents are immutable, so this is an append-only idempotent retry.
            if let Some(progress) = control::read_bootstrap_progress(&ctx.control_pool, epoch)
                .await
                .context("read bootstrap reconciliation progress")?
            {
                let targets: Vec<crate::reload_event::ReloadTarget> =
                    serde_json::from_value(progress.targets.clone())
                        .context("decode frozen bootstrap targets")?;
                let target_count = i64::try_from(targets.len())
                    .context("bootstrap target count does not fit i64")?;
                if target_count != progress.expected_tables {
                    anyhow::bail!(
                        "bootstrap request {} expected {} targets but persisted payload contains {}",
                        progress.request_id,
                        progress.expected_tables,
                        target_count
                    );
                }
                let registered = rows
                    .iter()
                    .map(|row| (row.source_schema.as_str(), row.source_table.as_str()))
                    .collect::<std::collections::BTreeSet<_>>();
                if let Some(missing) = targets.iter().find(|target| {
                    !registered.contains(&(target.schema.as_str(), target.table.as_str()))
                }) {
                    anyhow::bail!(
                        "bootstrap target {}.{} is missing from the atomic schema inventory",
                        missing.schema,
                        missing.table
                    );
                }
                crate::reload_event::request_all_targets(
                    &ctx.source_client,
                    progress.request_id,
                    &targets,
                )
                .await
                .context("re-emit unfinished bootstrap request")?;
            }
            cache.hydrate(rows).context("hydrate relation cache")?;
            let resume_lsn = resumed_generation_start(confirmed_flush, state.created_lsn);
            // Claim the configured slot before deleting anything. PostgreSQL permits only one active
            // consumer per slot, so a rolling replacement fails here while the prior pod can still own
            // unmanifested speculative spills. The sweep then runs while this CopyBoth connection is
            // the source-enforced singleton; no decoder, exporter, or other local writer exists yet.
            let mut stream = ReplicationStream::start(
                cfg.extractor.source_db_url.expose(),
                slot.as_str(),
                resume_lsn,
                &cfg.extractor.publication_name,
            )
            .await
            .context("START_REPLICATION (resume)")?;
            cleanup_epoch_orphans_while_holding_slot(
                &mut stream,
                ctx.object_store.as_ref(),
                &cfg.common.object_store.bucket,
                &ctx.control_pool,
                epoch,
            )
            .await
            .context("collect abandoned epoch objects after claiming resumed slot")?;
            tracing::info!(
                epoch = %epoch,
                cached_relations = cache.len(),
                "relation cache hydrated (resume)"
            );
            return Ok(Bootstrapped {
                stream,
                epoch,
                start_lsn: resume_lsn,
                stager: make_stager(epoch),
            });
        }
        crate::epoch::SlotAction::FreshSlot => {
            // Fall through to the fresh-slot + all-table reconciliation path below.
        }
    }

    // Persist the destructive-slot-lifetime intent before touching the source. If a newer generation
    // won the race after our read, leave the slot untouched and retry from a fresh classification.
    // Re-marking an already-armed epoch is intentionally successful: that is how crashes before the
    // drop, between drop/create, and after create/before generation bump recover.
    let prior = control::read_current_epoch(&ctx.control_pool)
        .await
        .context("read current epoch before replacing the source slot")?;
    if let Some(state) = &prior {
        assert_generation_slot_name(slot.as_str(), &state.slot_name)
            .context("configured slot differs from durable generation")?;
        ensure_catalog_fence_version_supported(state)?;
    }

    let recovery = slot_loss_recovery(status).with_context(|| {
        format!(
            "fresh-slot action requires authoritative absent/invalidated status, found {status:?}"
        )
    })?;
    if recovery == SlotLossRecovery::CreateIfAbsent {
        crate::preflight::SourcePreflight::new(&ctx.source_client, cfg)
            .assert_slot_creation_headroom()
            .await
            .context("check capacity for absent configured replication slot")?;
    }
    if let Some(state) = &prior
        && !control::mark_total_restart(&ctx.control_pool, state.epoch)
            .await
            .context("persist total-restart intent before replacing the source slot")?
    {
        anyhow::bail!(
            "current generation changed after reading epoch {}; source slot {slot} was not touched",
            state.epoch
        );
    }

    // No usable slot: establish WAL retention first. The full baseline is deliberately not copied
    // here; it is requested through the published event stream below, exactly like an operator's
    // later all-table reconciliation.
    let created = match recovery {
        SlotLossRecovery::CreateIfAbsent => {
            crate::slot::verify_or_create_slot(&ctx.source_client, slot.as_str())
                .await
                .context("create snapshot-free logical replication slot")?
        }
        SlotLossRecovery::ReplaceInvalidated => {
            crate::slot::recreate_invalidated_slot(&ctx.source_client, slot.as_str())
                .await
                .context("replace invalidated logical replication slot")?
        }
    };
    let consistent_point = match created {
        crate::slot::SlotResume::Created { consistent_point } => consistent_point,
        crate::slot::SlotResume::Existing(_) => {
            anyhow::bail!(
                "replication slot {slot} appeared after fresh-slot classification; retry bootstrap so it can be classified safely"
            );
        }
    };
    establish_bootstrap_generation(
        cfg,
        ctx,
        cache,
        slot.as_str(),
        consistent_point,
        prior,
        status,
        schema_version,
    )
    .await
}

/// Atomically register one frozen publication inventory in a new control generation, append its
/// source-WAL request, and begin decoding from the slot point that precedes both operations.
#[allow(
    clippy::too_many_arguments,
    reason = "bootstrap binds one immutable generation from its independently established dependencies"
)]
async fn establish_bootstrap_generation(
    cfg: &ExtractorConfig,
    ctx: &mut bootstrap::BootstrapCtx,
    cache: &mut crate::relcache::RelationCache,
    slot: &str,
    retained_floor: common::Lsn,
    prior: Option<control::ReplicationState>,
    slot_status: crate::epoch::SlotStatus,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<Bootstrapped> {
    const CATALOG_FENCE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
    let fence = crate::source_catalog::capture_catalog_fence(
        &mut ctx.source_client,
        &cfg.extractor.publication_name,
        CATALOG_FENCE_LOCK_TIMEOUT,
    )
    .await
    .context("capture writer-drained bootstrap catalog fence")?;
    anyhow::ensure!(
        fence.start_lsn >= retained_floor,
        "source catalog-fence LSN {} precedes retained slot floor {}",
        fence.start_lsn,
        retained_floor
    );
    let start_lsn = fence.start_lsn;
    let targets = fence
        .relations
        .iter()
        .map(|relation| crate::reload_event::ReloadTarget {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
        })
        .collect::<Vec<_>>();
    let expected_tables =
        i64::try_from(targets.len()).context("bootstrap target count does not fit i64")?;
    let targets_json =
        serde_json::to_value(&targets).context("encode bootstrap target inventory")?;
    let request_id = Uuid::new_v4();

    // PostgreSQL is the singleton authority for a slot. Claim it before the control CAS or source
    // request can become visible. Until that CAS commits, periodic feedback keeps flush/apply at
    // the previously durable floor, so a failed contender cannot release WAL through its proposed
    // generation boundary.
    let mut stream = ReplicationStream::start_with_feedback_floor(
        cfg.extractor.source_db_url.expose(),
        slot,
        start_lsn,
        retained_floor,
        &cfg.extractor.publication_name,
    )
    .await
    .context("START_REPLICATION before bootstrap generation registration")?;

    // The epoch row and every registry shape commit together. A restart can therefore see either no
    // generation or a complete inventory plus the exact request payload; never a partial fanout.
    let register = async {
        let mut tx = ctx
            .control_pool
            .begin()
            .await
            .context("begin bootstrap generation registration")?;
        let opened = control::bump_bootstrap_epoch(
            &mut *tx,
            prior.as_ref().map(|state| state.epoch),
            slot,
            start_lsn,
            request_id,
            expected_tables,
            &targets_json,
        )
        .await
        .context("open bootstrapping epoch")?;
        let Some(epoch) = opened else {
            tx.rollback()
                .await
                .context("rollback lost bootstrap-generation compare-and-set")?;
            anyhow::bail!(
                "another extractor changed the successor to {:?}; aborting this contender so a later startup can classify the new current generation",
                prior.as_ref().map(|state| state.epoch)
            );
        };
        for (relation, comments) in fence.relations.into_iter().zip(fence.comments) {
            let relation_oid = relation.oid;
            let mut row = consume::cache_relation(cache, epoch, relation, schema_version)?
                .context("published user relation classified as internal")?;
            comments
                .enrich_registry(&mut row.columns)
                .context("attach bootstrap comment snapshot")?;
            cache.set_registry_columns(relation_oid, schema_version, row.columns.clone());
            consume::persist_registry(&mut *tx, &row)
                .await
                .context("persist bootstrap relation inventory")?;
        }
        tx.commit()
            .await
            .context("commit atomic bootstrap generation inventory")?;
        Ok(epoch)
    };
    let epoch =
        await_while_holding_slot(&mut stream, "bootstrap control registration", register).await?;
    stream.set_durable(start_lsn);

    // `total_restart` can survive the exact crash seam after the old slot was replaced but before
    // its successor was registered. In that case the newly-created slot now classifies healthy,
    // while the durable predecessor status still proves this is destructive recovery.
    let destructive_restart = prior.as_ref().is_some_and(|predecessor| {
        predecessor.status == control::ReplicationStatus::TotalRestart
            || slot_loss_recovery(slot_status).is_some()
    });
    match &prior {
        Some(p) if destructive_restart => tracing::error!(
            old_epoch = %p.epoch,
            new_epoch = %epoch,
            slot,
            ?slot_status,
            "TOTAL-RESTART: opening a new epoch and reconciling every frozen publication target; old-epoch S3 is left to its lifecycle TTL"
        ),
        Some(p) => tracing::warn!(
            old_epoch = %p.epoch,
            new_epoch = %epoch,
            slot,
            ?slot_status,
            "RECONCILED-SUCCESSOR: retaining the healthy replication slot while rebuilding every frozen publication target under a new epoch"
        ),
        None => tracing::info!(epoch = %epoch, "first bootstrap: created slot + established epoch"),
    }
    let request =
        crate::reload_event::request_all_targets(&ctx.source_client, request_id, &targets);
    await_while_holding_slot(&mut stream, "bootstrap source request", request)
        .await
        .context("append initial all-table request to source WAL")?;
    cleanup_epoch_orphans_while_holding_slot(
        &mut stream,
        ctx.object_store.as_ref(),
        &cfg.common.object_store.bucket,
        &ctx.control_pool,
        epoch,
    )
    .await
    .context("collect abandoned bootstrap-epoch objects after claiming slot")?;
    tracing::info!(
        epoch = %epoch,
        tables = targets.len(),
        request_id = %request_id,
        start_lsn = %start_lsn,
        "bootstrap reconciliation requested; streaming WAL and export work in parallel"
    );
    Ok(Bootstrapped {
        stream,
        epoch,
        start_lsn,
        stager: crate::staging::ParquetStager::new(
            Arc::clone(&ctx.object_store),
            cfg.common.object_store.bucket.clone(),
            epoch,
        ),
    })
}

/// Await a bootstrap dependency while retaining the slot and periodically sending liveness
/// feedback. Before the control CAS, the stream's durable position remains the predecessor floor;
/// the caller advances it after that commit and reuses this helper for the source request.
async fn await_while_holding_slot<T>(
    stream: &mut ReplicationStream,
    operation: &'static str,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::pin!(work);
    loop {
        let budget = stream.feedback_budget();
        tokio::select! {
            biased;
            result = &mut work => return result,
            _ = sleep(budget) => stream
                .send_received_feedback(false)
                .await
                .with_context(|| format!("send replication feedback during {operation}"))?,
        }
    }
}

/// Sweep only while PostgreSQL has granted this process the configured replication slot. Entering
/// CopyBoth is the race-free singleton test: a prior/competing pod either still owns the slot (so
/// `START_REPLICATION` failed before this function was reachable) or cannot begin decoding until this
/// connection releases it. Pump feedback while object listing/deletion is in flight so the ownership
/// connection itself cannot time out during a large sweep.
async fn cleanup_epoch_orphans_while_holding_slot(
    stream: &mut ReplicationStream,
    store: &dyn object_store::ObjectStore,
    bucket: &str,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> anyhow::Result<crate::orphan::OrphanCleanupStats> {
    let cleanup = crate::orphan::cleanup_epoch_orphans(store, bucket, pool, epoch);
    tokio::pin!(cleanup);
    loop {
        let budget = stream.feedback_budget();
        tokio::select! {
            biased;
            result = &mut cleanup => return result,
            _ = sleep(budget) => stream
                .send_received_feedback(false)
                .await
                .context("send replication feedback while sweeping epoch orphans")?,
        }
    }
}

#[cfg(test)]
#[path = "app_test.rs"]
mod tests;
