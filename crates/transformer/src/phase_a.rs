//! Phase A — the transformer's ingest half (transformer §4). **Claim** the next `ready` file groups and
//! schema-only stream barriers in commit order, append every file row verbatim into `<table>_raw`,
//! and reconcile every barrier before retiring it. In **one control-DB transaction**, file work
//! advances `raw_appended_lsn` and is deleted while schema-only barriers are marked applied. No
//! transform: this ends with a faithful, idempotent CDC log and an ordered structural frontier.
//!
//! **Two guards, both load-bearing (§4 crash-window):** (1) the queue *deletion* is what advances the
//! frontier (not the watermark alone); (2) the per-file DuckDB ingest ledger commits atomically with
//! each raw append and absorbs a replay. DuckDB and control Postgres cannot share a transaction, so
//! the ordering is strict: the DuckDB append + marker **commit first**, then the Postgres
//! advance+delete txn. A crash between them re-claims the still-`ready` file, finds its marker, and
//! returns zero without rebuilding a per-row index.

use crate::duck::{BeginReload, ReloadBuild, TableDb};
use crate::error::TransformerError;
use crate::health::TransformerState;
use common::{EpochNo, Kind, Lsn, PgRelation, ReloadId, SchemaVersionNo};
use futures_util::StreamExt as _;
use sha2::{Digest as _, Sha256};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

/// Everything one owned table's apply worker needs — **owned** (one [`TableDb`]/DuckDB connection per
/// table, never shared), so it can move into a `spawn_local`'d [`crate::apply_loop::apply_loop`].
#[derive(Debug)]
pub struct TableCtx {
    /// Control-Postgres handle. Cloning a pool shares the same connections, so every worker's copy
    /// draws on one set.
    pub pool: sqlx::PgPool,
    /// The generation this worker was started for. Compared against `epoch_rx` to detect a bump.
    pub epoch: EpochNo,
    /// Latest global control-plane epoch, broadcast by the transformer's one shared poller.
    pub epoch_rx: tokio::sync::watch::Receiver<EpochNo>,
    /// Identity of the table-ownership lease held by this worker.
    pub owner_pod: String,
    /// Monotonic ownership token copied at bootstrap and rechecked for every publication action.
    pub fencing_token: i64,
    /// Staging object store used to download and fingerprint immutable manifest objects.
    pub store: Arc<dyn object_store::ObjectStore>,
    /// Bucket expected in every manifest `s3://` URI; the configured client is scoped to it.
    pub staging_bucket: String,
    /// Source schema of the table this worker owns.
    pub schema: String,
    /// Source table this worker owns.
    pub table: String,
    /// The `table` metric label (`"<schema>.<table>"`), precomputed at construction. Cardinality is
    /// bounded by the tables this pod owns, and the value is constant per worker.
    pub series: String,
    /// The table shape — the transform (Phase B) renders its SQL from this.
    pub rel: PgRelation,
    /// This table's transient DuckDB connection attached to its isolated DuckLake schema.
    /// `app::pipeline` joins every worker and drops these connections before it releases the shared
    /// catalog advisory-lock session and then the control leases.
    pub db: TableDb,
    /// The process-wide probe state. Shared, not owned: one table's quarantine degrades the pod.
    pub state: Arc<TransformerState>,
    /// Files claimed per cycle.
    pub max_files: NonZeroI64,
    /// Fresh full-table snapshots permitted after an immutable object fails verification.
    pub max_integrity_resnapshots: u32,
    /// The apply-loop poll cadence.
    pub poll_interval: Duration,
    /// The compaction cadence — full-rebuild + prune, on this worker thread after an apply cycle.
    pub compaction_interval: Duration,
    /// Raw retention as an LSN-byte lag behind `transformed_lsn` (the prune floor).
    pub retention_lsn_lag: u64,
    /// The reload_id whose claim pause was already logged — a paused table says *why* it
    /// is idle once per pause, not once per poll. Per-table by construction (one [`TableCtx`] per
    /// worker, with `!Sync` interior state), so this needs interior mutability behind
    /// `run_phase_a(&ctx)`, not synchronisation. `Option<ReloadId>` is `Copy`, so `Cell` has no borrow
    /// flag or runtime panic path.
    pub pause_logged: Cell<Option<ReloadId>>,
}

/// The once-per-pause transition: `Some(reload_id)` exactly when a NEW pause begins (a different
/// reload than last logged, or the first). A lifted pause (no live rebuild) clears the latch so
/// the next reload logs again.
pub(crate) fn pause_began(
    logged: &Cell<Option<ReloadId>>,
    live: Option<ReloadId>,
) -> Option<ReloadId> {
    match (logged.get(), live) {
        (prev, Some(id)) if prev != Some(id) => {
            logged.set(Some(id));
            Some(id)
        }
        (_, None) => {
            logged.set(None);
            None
        }
        _ => None,
    }
}

/// Read the two independent inputs to the Phase-A backlog gauge concurrently.
///
/// Neither indexed control-plane read consumes the other's output or runs inside a transaction, so
/// concurrency changes their latency from the sum of two round trips to the maximum while retaining
/// two SQL queries. This uses one task rather than spawning, which remains valid on the transformer's
/// `LocalSet`.
///
/// Each read may hold one of `control::connect`'s five default pool connections. With many owned
/// tables that can queue, but it cannot deadlock here: neither future holds an open transaction while
/// waiting to acquire another connection.
async fn read_lag_inputs(ctx: &TableCtx) -> Result<(Option<Lsn>, Lsn), TransformerError> {
    let (max_ready, checkpoint) = tokio::try_join!(
        control::max_ready_lsn_end(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table),
        control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table),
    )?;
    Ok((
        max_ready,
        checkpoint.map_or(Lsn::ZERO, |cp| cp.raw_appended_lsn),
    ))
}

/// One Phase-A pass. Returns the max `lsn_end` appended, or `None` if the queue was empty.
///
/// # Errors
///
/// Returns [`TransformerError::Control`] or [`TransformerError::ControlTxn`] for control-plane reads and the
/// advance/delete transaction, [`TransformerError::Duck`] for local append/reconcile failures,
/// [`TransformerError::RegistryDecode`] for an invalid stored shape, [`TransformerError::Quarantine`] for an
/// unsafe DDL cast, or [`TransformerError::Internal`] for an inconsistent reload manifest.
pub async fn run_phase_a(ctx: &TableCtx) -> Result<Option<Lsn>, TransformerError> {
    // A crash can land after control Postgres fenced a corrupt publication but before the local
    // hidden generation was removed. Clean only that exact failed/building identity before the
    // durable recovery state below decides whether this table may continue.
    discard_failed_reload_build(ctx).await?;
    enforce_integrity_recovery_state(ctx).await?;

    // Observability: set the Phase-A backlog gauge every poll (0 when caught up) —
    // `max(lsn_end over ready files) − raw_appended_lsn`. Both operands are cheap indexed control-DB
    // reads; doing this before the claim means idle polls report a truthful 0.
    let (max_ready, raw_appended) = read_lag_inputs(ctx).await?;
    common::metrics::set_raw_append_lag(&ctx.series, raw_append_lag_bytes(max_ready, raw_appended));

    // A source-driven attempt is discoverable from its durable marker rows even when the table
    // exported zero rows and therefore produced no reload manifest. Start (or resume) its hidden
    // generation before looking at the file queue; this also purges the pre-fence backlog so a
    // large backlog cannot keep the first reload chunk outside every bounded claim forever.
    let mut reload_build = prepare_ready_reload(ctx).await?;

    // 1. Claim complete work units in commit order — NEVER filter on
    //    `lsn_end > raw_appended_lsn` (that skips equal-lsn_end snapshot files forever). A reload
    //    publication re-runs its fenced pre-F purge before every claim so a late zero-file stream
    //    group is superseded rather than incorrectly applied to the replacement generation.
    let claimed = if let Some(build) = &reload_build {
        if build.phase == crate::duck::ReloadPhase::Published {
            Vec::new()
        } else {
            let publication = publication_for_build(ctx, build).await?;
            control::delete_publication_superseded(
                &ctx.pool,
                &publication,
                &ctx.owner_pod,
                ctx.fencing_token,
            )
            .await?;
            control::reload::claim_publication_ready_units(
                &ctx.pool,
                &publication,
                &ctx.owner_pod,
                ctx.fencing_token,
                ctx.max_files.get(),
            )
            .await?
        }
    } else {
        control::claim_ready_units(
            &ctx.pool,
            ctx.epoch,
            &ctx.schema,
            &ctx.table,
            ctx.max_files.get(),
        )
        .await?
    };

    // Close the readiness/claim check-use seam. If the first read saw `exporting` but
    // `complete_export` became visible to the claim and lifted its pause, the newer read must
    // create the hidden generation before any claimed (F,H] WAL is routed. This second read is
    // needed even when `claimed` is empty: a zero-row dump still has marker-only work to publish.
    // A completion after this read is safe: a claim that saw the existing attempt as requested or
    // exporting returned no rows, while files claimed before a brand-new request all precede its F.
    if reload_build.is_none() {
        reload_build = prepare_ready_reload(ctx).await?;
    }

    if claimed.is_empty() {
        // Distinguish IDLE from PAUSED: a live reload attempt withholds this
        // table's claims (reload §2 — claiming would retire post-`W` files the rebuild must
        // replay). Only probe when a backlog exists, and log the reason once per pause.
        if max_ready.is_some() {
            let live = control::reload::active_rebuilds(&ctx.pool, ctx.epoch)
                .await?
                .into_iter()
                .find(|r| r.source_schema == ctx.schema && r.source_table == ctx.table)
                .map(|r| r.reload_id);
            if let Some(reload_id) = pause_began(&ctx.pause_logged, live) {
                tracing::info!(
                    table = %format_args!("{}.{}", ctx.schema, ctx.table),
                    reload_id = %reload_id,
                    reason = "rebuild-in-flight",
                    "claims paused: ready rows accumulate (frontier frozen at W) until export_complete"
                );
            }
        } else {
            pause_began(&ctx.pause_logged, None); // caught up — clear the latch
        }
        // An upgraded database keeps its legacy row-level replay PK until the control queue is
        // empty. Only then can we prove there is no old-version crash-window append lacking a file
        // marker. New files arriving after this observation have not been appended and are safe.
        if reload_build.is_none()
            && ctx.db.has_legacy_replay_fence()
            && !control::manifest_work_exists(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?
            && ctx.db.migrate_legacy_replay_fence(&ctx.table)?
        {
            tracing::info!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                "migrated raw replay fence from a per-row primary key to the file ingest ledger"
            );
        }
        return Ok(None);
    }
    pause_began(&ctx.pause_logged, None); // claiming again — any pause has lifted

    // 2. Append each file verbatim to <table>_raw (DuckDB auto-commits each statement). Idempotent.
    //    Files are claimed in (lsn_end, id) = commit order, and the extractor cuts a fresh homogeneous file at
    //    every structural change, so schema_version is monotonic across `claimed`. Before appending a
    //    file at a NEWER version, reconcile both tables UP TO it — so `<table>_raw` always has
    //    exactly the file's columns and the verbatim `SELECT *` append lines up; already-appended older
    //    rows read NULL for the freshly-added column (additive superset).
    // A pending reload will publish a replacement mirror at the new schema
    // and `delete_superseded` every non-reload file at `lsn_end <= F`. Such a file must NOT
    // reconcile here: a lossy cast would re-quarantine the transformer on every restart BEFORE it ever
    // reaches the reload chunk file that clears the quarantine (the claim order puts the low-`lsn_end`
    // blocker first). Compute the floor once; skip superseded version-crossing files in the loop.
    let supersede_floor =
        control::reload::reload_supersede_floor(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
            .await?;

    let mut max_lsn = reload_build
        .as_ref()
        .map_or(raw_appended, |build| build.raw_appended_lsn);
    let mut ids = Vec::with_capacity(claimed.len());
    let mut barriers = Vec::with_capacity(claimed.len());
    let mut appended = 0u64;
    let mut version_schemas = BTreeMap::<SchemaVersionNo, VersionSchema>::new();
    for work in &claimed {
        let work_lsn = match work {
            control::ReadyManifestUnit::Files(files) => {
                files
                    .first()
                    .ok_or_else(|| TransformerError::ManifestInvariant {
                        message: "claim produced an empty manifest unit".to_string(),
                    })?
                    .lsn_end
            }
            control::ReadyManifestUnit::SchemaBarrier(barrier) => barrier.commit_lsn,
        };
        // H is a unit boundary. Protocol-v2 group children share lsn_end, and no part of a group is
        // routed or retired when the entire unit belongs after the active publication barrier.
        if reload_build
            .as_ref()
            .is_some_and(|build| work_lsn > build.final_lsn)
        {
            break;
        }

        let files = match work {
            control::ReadyManifestUnit::Files(files) => files,
            control::ReadyManifestUnit::SchemaBarrier(barrier) => {
                validate_schema_barrier(ctx, barrier)?;
                if let Some(build) = &reload_build {
                    if barrier.commit_lsn <= build.start_lsn {
                        // A publication can race one late pre-F control-plane insert after the purge
                        // immediately preceding its SELECT claim. Re-run the fenced purge and do not
                        // acknowledge that structural receipt as applied: the baseline supersedes it.
                        let publication = publication_for_build(ctx, build).await?;
                        control::delete_publication_superseded(
                            &ctx.pool,
                            &publication,
                            &ctx.owner_pod,
                            ctx.fencing_token,
                        )
                        .await?;
                        continue;
                    }
                    if barrier.final_schema_version != build.schema_version {
                        return Err(TransformerError::ManifestInvariant {
                            message: format!(
                                "schema-only stream group {} reaches schema {} across frozen reload {} schema {}; shadow schema evolution inside (F,H] is not yet supported",
                                barrier.id.0,
                                barrier.final_schema_version,
                                build.reload_id,
                                build.schema_version,
                            ),
                        });
                    }
                } else {
                    reconcile_schema_barrier(ctx, barrier, &mut version_schemas).await?;
                }
                barriers.push(barrier.clone());
                continue;
            }
        };
        let unit = files.iter().collect::<Vec<_>>();
        let first = unit
            .first()
            .copied()
            .ok_or_else(|| TransformerError::ManifestInvariant {
                message: "claim produced an empty manifest unit".to_string(),
            })?;

        let mut route = None;
        for f in &unit {
            if let Some(build) = &reload_build
                && f.kind == control::ManifestKind::Reload
                && f.reload_id == Some(build.reload_id)
            {
                validate_reload_manifest_at_f(f, build)?;
            }
            let file_route = if f.kind == control::ManifestKind::Reload {
                let file_route = route_reload_file(ctx, f).await?;
                reload_build = ctx.db.reload_build()?;
                file_route
            } else if let Some(build) = &reload_build {
                if f.lsn_end <= build.start_lsn {
                    FileRoute::Superseded
                } else {
                    FileRoute::Shadow(build.shadow_table.clone())
                }
            } else {
                FileRoute::Live
            };
            if route
                .as_ref()
                .is_some_and(|expected| expected != &file_route)
            {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "stream group {:?} would split across transformer destinations",
                        first.stream_group_id
                    ),
                });
            }
            route = Some(file_route);
        }
        let route = route.ok_or_else(|| TransformerError::ManifestInvariant {
            message: "manifest unit has no route".to_string(),
        })?;

        if route == FileRoute::Retire {
            ids.extend(unit.iter().map(|file| file.id));
            continue;
        }
        if route == FileRoute::Superseded {
            let build =
                reload_build
                    .as_ref()
                    .ok_or_else(|| TransformerError::ManifestInvariant {
                        message: "superseded manifest unit has no reload build".to_string(),
                    })?;
            let publication = publication_for_build(ctx, build).await?;
            control::delete_publication_superseded(
                &ctx.pool,
                &publication,
                &ctx.owner_pod,
                ctx.fencing_token,
            )
            .await?;
            continue;
        }
        let current_schema = ctx.db.schema_version()?;
        let target = manifest_unit_target(&unit)?;
        if route == FileRoute::Live
            && target > current_schema
            && unit.iter().any(|f| {
                f.kind != control::ManifestKind::Reload
                    && supersede_floor.is_some_and(|floor| f.lsn_end <= floor)
            })
        {
            continue;
        }
        if let FileRoute::Shadow(_) = &route {
            let build =
                reload_build
                    .as_ref()
                    .ok_or_else(|| TransformerError::ManifestInvariant {
                        message: "shadow-routed manifest unit has no reload build".to_string(),
                    })?;
            if target != build.schema_version
                || unit
                    .iter()
                    .any(|file| file.schema_version != build.schema_version)
            {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "stream group {:?} reaches schema {target} across frozen reload {} schema {}; shadow schema evolution inside [F,H] is not yet supported",
                        first.stream_group_id, build.reload_id, build.schema_version,
                    ),
                });
            }
        }

        // One protocol-v2 transaction group can contain children from several structural schema
        // versions. The append is atomic, so the raw destination must be reconciled to the group's
        // final schema barrier before any child is inserted; that barrier can be newer than every
        // child when DDL follows the last data row. Preserve exact validation for every older child
        // by binding its immutable registry plan now; the final raw-table superset is not an
        // authoritative description of historical Parquet shape. Load every intervening registry
        // version too: composing only the endpoints would lose rename chains and would position-zip
        // survivors across a DROP-induced attnum shift.
        let source_floor = unit
            .iter()
            .map(|file| file.schema_version)
            .min()
            .ok_or_else(|| TransformerError::ManifestInvariant {
                message: "manifest unit has no schema version".to_string(),
            })?;
        let destination_version = match &route {
            FileRoute::Live => current_schema.max(target),
            FileRoute::Shadow(_) => target,
            FileRoute::Retire => {
                return Err(TransformerError::ManifestInvariant {
                    message: "retired manifest unit reached schema binding".to_string(),
                });
            }
            FileRoute::Superseded => {
                return Err(TransformerError::ManifestInvariant {
                    message: "superseded manifest unit reached schema binding".to_string(),
                });
            }
        };
        let expected_count = destination_version
            .0
            .checked_sub(source_floor.0)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| TransformerError::ManifestInvariant {
                message: format!(
                    "invalid schema lineage range {source_floor}..={destination_version}"
                ),
            })?;
        let cached_count = i64::try_from(
            version_schemas
                .range(source_floor..=destination_version)
                .count(),
        )
        .unwrap_or(i64::MAX);
        if cached_count != expected_count {
            for (version, schema) in schema_lineage(ctx, source_floor, destination_version).await? {
                version_schemas.insert(version, schema);
            }
        }
        let source_versions = unit
            .iter()
            .map(|file| file.schema_version)
            .collect::<BTreeSet<_>>();
        let mut destination_columns = BTreeMap::<SchemaVersionNo, Vec<String>>::new();
        for version in source_versions {
            let schema = version_schema(&version_schemas, version)?;
            ctx.db.cache_staged_schema(version, &schema.plan)?;
            destination_columns.insert(
                version,
                destination_columns_between(version, destination_version, &version_schemas)?,
            );
        }

        if route == FileRoute::Live
            && target > current_schema
            && let Err(error) = crate::ddl::reconcile_to_version(
                &ctx.db,
                &ctx.pool,
                ctx.epoch,
                &ctx.schema,
                &ctx.table,
                target,
            )
            .await
        {
            if matches!(error, TransformerError::Quarantine { .. }) {
                ctx.state.quarantine_table(&ctx.schema, &ctx.table);
                tracing::error!(
                    table = %format_args!("{}.{}", ctx.schema, ctx.table),
                    "QUARANTINE: schema change could not be applied before atomic stream-group append"
                );
            }
            return Err(error);
        }

        let destination = match &route {
            FileRoute::Live => ctx.table.as_str(),
            FileRoute::Shadow(table) => table.as_str(),
            FileRoute::Retire => {
                return Err(TransformerError::ManifestInvariant {
                    message: "retired manifest unit reached append preparation".to_string(),
                });
            }
            FileRoute::Superseded => {
                return Err(TransformerError::ManifestInvariant {
                    message: "superseded manifest unit reached append preparation".to_string(),
                });
            }
        };
        let overrides: Vec<Option<String>> = unit
            .iter()
            .map(|file| {
                (file.kind == control::ManifestKind::Spill).then(|| file.lsn_end.to_string())
            })
            .collect();
        let probes: Vec<crate::duck::ManifestAppend<'_>> = unit
            .iter()
            .zip(&overrides)
            .map(|(file, lsn_override)| {
                let schema = version_schema(&version_schemas, file.schema_version)?;
                let mapped = destination_columns
                    .get(&file.schema_version)
                    .ok_or_else(|| TransformerError::ManifestInvariant {
                        message: format!(
                            "manifest {} has no destination mapping for schema {}",
                            file.id, file.schema_version
                        ),
                    })?;
                Ok(crate::duck::ManifestAppend {
                    manifest_id: file.id,
                    original_uri: &file.s3_uri,
                    verified_uri: None,
                    object_size: file.object_size,
                    sha256: &file.sha256,
                    stream_group_id: file.stream_group_id.map(|id| id.0),
                    schema_version: file.schema_version,
                    commit_lsn_override: lsn_override.as_deref(),
                    destination_columns: Some(mapped),
                    expectation: Some(manifest_expectation(file, &schema.relation)),
                })
            })
            .collect::<Result<_, TransformerError>>()?;
        let mut states = Vec::with_capacity(probes.len());
        for (file, probe) in unit.iter().zip(&probes) {
            match ctx.db.ingest_receipt_state(probe) {
                Ok(state) => states.push(state),
                Err(error @ TransformerError::ManifestInvariant { .. }) => {
                    recover_object_integrity_failure(ctx, file, reload_build.as_ref(), &error)
                        .await?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        let verified = if states
            .iter()
            .all(|state| *state == crate::duck::IngestReceiptState::Ingested)
        {
            Vec::new()
        } else if states
            .iter()
            .all(|state| *state == crate::duck::IngestReceiptState::Missing)
        {
            let mut downloads = Vec::with_capacity(unit.len());
            for file in &unit {
                match download_verified(ctx, file).await {
                    Ok(object) => downloads.push(object),
                    Err(error @ TransformerError::ObjectIntegrity { .. }) => {
                        recover_object_integrity_failure(ctx, file, reload_build.as_ref(), &error)
                            .await?;
                        // Nothing in this claim is retired. Earlier unit appends are protected by
                        // their Duck receipts and safely replay after the replacement snapshot.
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            }
            downloads
        } else {
            let error = TransformerError::ManifestInvariant {
                message: format!(
                    "stream group {:?} has a partial durable ingest receipt",
                    first.stream_group_id
                ),
            };
            recover_object_integrity_failure(ctx, first, reload_build.as_ref(), &error).await?;
            return Ok(None);
        };
        let appends: Vec<crate::duck::ManifestAppend<'_>> = unit
            .iter()
            .zip(&overrides)
            .enumerate()
            .map(|(index, (file, lsn_override))| {
                let schema = version_schema(&version_schemas, file.schema_version)?;
                let mapped = destination_columns
                    .get(&file.schema_version)
                    .ok_or_else(|| TransformerError::ManifestInvariant {
                        message: format!(
                            "manifest {} has no destination mapping for schema {}",
                            file.id, file.schema_version
                        ),
                    })?;
                let verified_uri = verified
                    .get(index)
                    .map(|object| object.uri(&file.s3_uri))
                    .transpose()?;
                Ok(crate::duck::ManifestAppend {
                    manifest_id: file.id,
                    original_uri: &file.s3_uri,
                    verified_uri,
                    object_size: file.object_size,
                    sha256: &file.sha256,
                    stream_group_id: file.stream_group_id.map(|id| id.0),
                    schema_version: file.schema_version,
                    commit_lsn_override: lsn_override.as_deref(),
                    destination_columns: Some(mapped),
                    expectation: Some(manifest_expectation(file, &schema.relation)),
                })
            })
            .collect::<Result<_, TransformerError>>()?;
        match ctx.db.append_manifest_unit(destination, &appends) {
            Ok(rows) => appended = appended.saturating_add(rows),
            Err(error @ TransformerError::ObjectIntegrity { .. }) => {
                let failed = match &error {
                    TransformerError::ObjectIntegrity { uri, .. } => unit
                        .iter()
                        .copied()
                        .find(|file| file.s3_uri == *uri)
                        .unwrap_or(first),
                    _ => first,
                };
                recover_object_integrity_failure(ctx, failed, reload_build.as_ref(), &error)
                    .await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        max_lsn = unit.iter().fold(max_lsn, |max, file| max.max(file.lsn_end));
        ids.extend(unit.iter().map(|file| file.id));
    }

    // 3. ONE control-DB txn: advance/delete file work and retire schema-only barriers together.
    //    Duck append and schema reconciliation are already durable. A zero-file barrier does not
    //    advance a data checkpoint: no raw row exists at its commit LSN for Phase B to transform.
    if ids.is_empty() && barriers.is_empty() {
        return Ok(None);
    }
    let publication = match &reload_build {
        Some(build) => {
            if !ids.is_empty() {
                ctx.db
                    .advance_reload_raw(build.reload_id, build.publication_nonce, max_lsn)?;
            }
            Some(publication_for_build(ctx, build).await?)
        }
        None => None,
    };
    let mut tx = ctx
        .pool
        .begin()
        .await
        .map_err(|source| TransformerError::ControlTxn {
            op: "begin advance+delete txn",
            source,
        })?;
    if let Some(publication) = &publication {
        control::reload::lock_publication(&mut *tx, publication, &ctx.owner_pod, ctx.fencing_token)
            .await?;
    } else if !ids.is_empty() {
        control::advance_raw_appended(&mut *tx, ctx.epoch, &ctx.schema, &ctx.table, max_lsn)
            .await?;
    }
    // Both retirement statements lock stream-group parents. Resolve their union first and acquire
    // one global ascending lock order, otherwise a mixed file+barrier batch can deadlock another
    // integrity/publication transaction that touches the same parents in the opposite subset order.
    control::lock_manifest_work_groups(&mut *tx, &ids, &barriers).await?;
    if !ids.is_empty() {
        let deleted = control::delete_claimed(&mut *tx, &ids).await?;
        if deleted != ids.len() as u64 {
            return Err(TransformerError::Internal(format!(
                "claimed manifest retirement removed {deleted} rows, expected {}",
                ids.len()
            )));
        }
    }
    if !barriers.is_empty() {
        let completed = control::complete_schema_barriers(&mut *tx, &barriers).await?;
        if completed != barriers.len() as u64 {
            return Err(TransformerError::Internal(format!(
                "schema barrier retirement completed {completed} rows, expected {}",
                barriers.len()
            )));
        }
    }
    tx.commit()
        .await
        .map_err(|source| TransformerError::ControlTxn {
            op: "commit advance+delete txn",
            source,
        })?;

    // Migration is intentionally deferred until a fresh control-plane read proves the queue is
    // empty. A prior release may have appended more files than this process's current `max_files`,
    // then crashed before deleting them; migrating after merely one batch would duplicate the rest.
    if reload_build.is_none()
        && ctx.db.has_legacy_replay_fence()
        && !control::manifest_work_exists(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?
        && ctx.db.migrate_legacy_replay_fence(&ctx.table)?
    {
        tracing::info!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            "migrated raw replay fence from a per-row primary key to the file ingest ledger"
        );
    }

    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        files = ids.len(),
        schema_barriers = barriers.len(),
        rows = appended,
        raw_appended = %max_lsn,
        "Phase A: appended to <table>_raw, watermark advanced, queue drained"
    );
    Ok(Some(max_lsn))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileRoute {
    Live,
    Shadow(String),
    Retire,
    Superseded,
}

#[derive(Debug)]
struct VerifiedObject {
    temp: tempfile::NamedTempFile,
}

impl VerifiedObject {
    fn uri(&self, original_uri: &str) -> Result<&str, TransformerError> {
        self.temp
            .path()
            .to_str()
            .ok_or_else(|| TransformerError::ManifestInvariant {
                message: format!("temporary path for {original_uri} is not UTF-8"),
            })
    }
}

fn validate_schema_barrier(
    ctx: &TableCtx,
    barrier: &control::StreamSchemaBarrier,
) -> Result<(), TransformerError> {
    if barrier.epoch != ctx.epoch
        || barrier.source_schema != ctx.schema
        || barrier.source_table != ctx.table
        || barrier.final_schema_version.0 <= 0
    {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "schema barrier {} identity/version does not match worker epoch {} table {}.{}",
                barrier.id.0, ctx.epoch, ctx.schema, ctx.table
            ),
        });
    }
    Ok(())
}

/// Reconcile a zero-file structural commit before its durable control receipt is retired. Loading
/// the complete range up front bounds the older stepwise reconciler even if a corrupt barrier names
/// `i64::MAX`; a missing version fails before that loop starts. A crash after Duck commits but before
/// control retirement reclaims the barrier and takes the `target <= current` idempotent path.
async fn reconcile_schema_barrier(
    ctx: &TableCtx,
    barrier: &control::StreamSchemaBarrier,
    version_schemas: &mut BTreeMap<SchemaVersionNo, VersionSchema>,
) -> Result<(), TransformerError> {
    let current = ctx.db.schema_version()?;
    if barrier.final_schema_version <= current {
        return Ok(());
    }
    for (version, schema) in schema_lineage(ctx, current, barrier.final_schema_version).await? {
        version_schemas.insert(version, schema);
    }
    if let Err(error) = crate::ddl::reconcile_to_version(
        &ctx.db,
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        barrier.final_schema_version,
    )
    .await
    {
        if matches!(error, TransformerError::Quarantine { .. }) {
            ctx.state.quarantine_table(&ctx.schema, &ctx.table);
            tracing::error!(
                table = %ctx.series,
                stream_group_id = barrier.id.0,
                target_schema = %barrier.final_schema_version,
                "QUARANTINE: schema-only streamed commit could not be reconciled"
            );
        }
        return Err(error);
    }
    Ok(())
}

/// The structural barrier an atomic append must reach before inserting any child. Protocol-v2
/// groups carry their table's final schema at `StreamCommit`; it can be newer than every Parquet
/// child when the transaction performs DDL after its last data row. Singleton files have no group
/// barrier and retain their own homogeneous schema version.
fn manifest_unit_target(
    unit: &[&control::ManifestRow],
) -> Result<SchemaVersionNo, TransformerError> {
    let first = unit
        .first()
        .copied()
        .ok_or_else(|| TransformerError::ManifestInvariant {
            message: "manifest unit has no schema version".to_string(),
        })?;
    let child_target = unit
        .iter()
        .map(|file| file.schema_version)
        .max()
        .ok_or_else(|| TransformerError::ManifestInvariant {
            message: "manifest unit has no schema version".to_string(),
        })?;
    let Some(group_id) = first.stream_group_id else {
        if first.stream_group_final_schema_version.is_some() {
            return Err(TransformerError::ManifestInvariant {
                message: format!(
                    "ungrouped manifest {} carries a final stream schema barrier",
                    first.id
                ),
            });
        }
        return Ok(child_target);
    };
    let final_schema_version = first.stream_group_final_schema_version.ok_or_else(|| {
        TransformerError::ManifestInvariant {
            message: format!("stream group {} has no final schema version", group_id.0),
        }
    })?;
    if final_schema_version < child_target
        || unit.iter().any(|file| {
            file.stream_group_id != Some(group_id)
                || file.stream_group_final_schema_version != Some(final_schema_version)
        })
    {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "stream group {} has an inconsistent final schema version {final_schema_version} below child target {child_target}",
                group_id.0
            ),
        });
    }
    Ok(final_schema_version)
}

fn manifest_expectation<'a>(
    file: &'a control::ManifestRow,
    relation: &'a PgRelation,
) -> crate::duck::ManifestExpectation<'a> {
    let kind = match file.kind {
        control::ManifestKind::Snapshot => Kind::Snapshot,
        control::ManifestKind::Reload => Kind::Reload,
        control::ManifestKind::Stream | control::ManifestKind::Spill => Kind::Stream,
    };
    crate::duck::ManifestExpectation {
        row_count: file.row_count,
        epoch: file.epoch,
        source_schema: &file.source_schema,
        source_table: &file.source_table,
        source_columns: &relation.columns,
        schema_version: file.schema_version,
        kind,
        lsn_start: file.lsn_start,
        lsn_end: file.lsn_end,
        speculative_commit_lsn: file.kind == control::ManifestKind::Spill,
    }
}

async fn download_verified(
    ctx: &TableCtx,
    manifest: &control::ManifestRow,
) -> Result<VerifiedObject, TransformerError> {
    let rest = manifest.s3_uri.strip_prefix("s3://").ok_or_else(|| {
        TransformerError::ManifestInvariant {
            message: format!("manifest {} has a non-S3 URI", manifest.id),
        }
    })?;
    let (bucket, key) =
        rest.split_once('/')
            .ok_or_else(|| TransformerError::ManifestInvariant {
                message: format!("manifest {} has an S3 URI with no object key", manifest.id),
            })?;
    if bucket != ctx.staging_bucket || key.is_empty() {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "manifest {} URI bucket/key does not match configured staging bucket",
                manifest.id
            ),
        });
    }
    let object_path = object_store::path::Path::from(key);
    let result = match ctx.store.get(&object_path).await {
        Ok(result) => result,
        Err(source) => return Err(classify_manifest_get_error(&manifest.s3_uri, source)),
    };
    let temp = tempfile::NamedTempFile::new().map_err(|source| TransformerError::File {
        op: "create verified manifest temp file",
        path: std::env::temp_dir().display().to_string(),
        source,
    })?;
    let path = temp.path().to_path_buf();
    let output = tokio::fs::File::create(&path)
        .await
        .map_err(|source| TransformerError::File {
            op: "open verified manifest temp file",
            path: path.display().to_string(),
            source,
        })?;
    // Object-store streams may yield many small chunks. Buffer their filesystem writes so those
    // chunks do not each require a blocking-pool file operation.
    let mut output = tokio::io::BufWriter::with_capacity(64 * 1024, output);
    let mut stream = result.into_stream();
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|source| {
            classify_manifest_object_error(&manifest.s3_uri, "stream manifest object", source)
        })?;
        size = size
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| TransformerError::ObjectIntegrity {
                uri: manifest.s3_uri.clone(),
                reason: "downloaded byte count overflowed u64".to_string(),
            })?;
        hasher.update(&chunk);
        output
            .write_all(&chunk)
            .await
            .map_err(|source| TransformerError::File {
                op: "write verified manifest temp file",
                path: path.display().to_string(),
                source,
            })?;
    }
    output
        .flush()
        .await
        .map_err(|source| TransformerError::File {
            op: "flush verified manifest temp file",
            path: path.display().to_string(),
            source,
        })?;
    drop(output);
    let actual_size = i64::try_from(size).map_err(|_| TransformerError::ObjectIntegrity {
        uri: manifest.s3_uri.clone(),
        reason: "downloaded object is larger than bigint".to_string(),
    })?;
    let actual_sha = hasher.finalize();
    validate_object_fingerprint(
        &manifest.s3_uri,
        manifest.object_size,
        &manifest.sha256,
        actual_size,
        actual_sha.as_slice(),
    )?;
    Ok(VerifiedObject { temp })
}

fn classify_manifest_get_error(uri: &str, source: object_store::Error) -> TransformerError {
    classify_manifest_object_error(uri, "download manifest object", source)
}

fn classify_manifest_object_error(
    uri: &str,
    op: &'static str,
    source: object_store::Error,
) -> TransformerError {
    match source {
        object_store::Error::NotFound { .. } => TransformerError::ObjectIntegrity {
            uri: uri.to_string(),
            reason: "durable manifest object is missing".to_string(),
        },
        source => TransformerError::ObjectStore {
            op,
            source: Box::new(source),
        },
    }
}

async fn enforce_integrity_recovery_state(ctx: &TableCtx) -> Result<(), TransformerError> {
    let Some(recovery) =
        control::read_integrity_recovery(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?
    else {
        return Ok(());
    };
    match recovery.status {
        control::IntegrityRecoveryStatus::Retrying => {
            ctx.state.quarantine_table(&ctx.schema, &ctx.table);
            tracing::debug!(
                table = %ctx.series,
                attempt = recovery.attempt_count,
                recovery_reload_id = ?recovery.recovery_reload_id,
                "table readiness remains degraded while an integrity replacement is pending"
            );
            Ok(())
        }
        control::IntegrityRecoveryStatus::Quarantined => {
            ctx.state.quarantine_table(&ctx.schema, &ctx.table);
            Err(TransformerError::Quarantine {
                table: ctx.series.clone(),
                reason: recovery.last_error,
            })
        }
        control::IntegrityRecoveryStatus::Recovered => {
            ctx.state.clear_table_quarantine(&ctx.schema, &ctx.table);
            Ok(())
        }
    }
}

async fn discard_failed_reload_build(ctx: &TableCtx) -> Result<(), TransformerError> {
    let Some(build) = ctx.db.reload_build()? else {
        return Ok(());
    };
    if build.phase != crate::duck::ReloadPhase::Building {
        return Ok(());
    }
    let Some(reload) = control::reload::get(&ctx.pool, build.reload_id).await? else {
        return Ok(());
    };
    if reload.epoch != ctx.epoch
        || reload.source_schema != ctx.schema
        || reload.source_table != ctx.table
    {
        return Err(TransformerError::Internal(format!(
            "local reload {} belongs to {}.{}, but its control row belongs to epoch {} {}.{}",
            build.reload_id,
            ctx.schema,
            ctx.table,
            reload.epoch,
            reload.source_schema,
            reload.source_table
        )));
    }
    if reload.status != control::ReloadStatus::Failed {
        return Ok(());
    }
    if !ctx
        .db
        .abandon_reload_build(build.reload_id, build.publication_nonce)?
    {
        return Err(TransformerError::Internal(format!(
            "failed reload {} changed while cleaning its stale local shadow",
            build.reload_id
        )));
    }
    tracing::warn!(
        table = %ctx.series,
        reload_id = %build.reload_id,
        "removed a stale hidden generation whose control publication is failed"
    );
    Ok(())
}

async fn recover_object_integrity_failure(
    ctx: &TableCtx,
    manifest: &control::ManifestRow,
    build: Option<&ReloadBuild>,
    error: &TransformerError,
) -> Result<(), TransformerError> {
    let reason = error.to_string();
    let publication = build.map(|build| control::IntegrityPublicationFence {
        reload_id: build.reload_id,
        publication_nonce: build.publication_nonce,
    });
    let outcome = control::handle_integrity_failure(
        &ctx.pool,
        &control::IntegrityFailure {
            epoch: ctx.epoch,
            source_schema: &ctx.schema,
            source_table: &ctx.table,
            manifest_id: manifest.id,
            reason: &reason,
            owner_pod: &ctx.owner_pod,
            fencing_token: ctx.fencing_token,
            publication,
            max_resnapshots: ctx.max_integrity_resnapshots,
        },
    )
    .await?;

    if let Some(build) = build
        && !ctx
            .db
            .abandon_reload_build(build.reload_id, build.publication_nonce)?
    {
        return Err(TransformerError::Internal(format!(
            "integrity handler fenced reload {}, but its local building receipt changed before cleanup",
            build.reload_id
        )));
    }
    ctx.state.quarantine_table(&ctx.schema, &ctx.table);
    match outcome {
        control::IntegrityFailureOutcome::RecoveryScheduled { reload_id, attempt } => {
            tracing::error!(
                table = %ctx.series,
                manifest_id = %manifest.id,
                recovery_reload_id = %reload_id,
                attempt,
                reason,
                "staged object failed verification; scheduled a bounded replacement snapshot"
            );
            Ok(())
        }
        control::IntegrityFailureOutcome::Quarantined { attempt } => {
            tracing::error!(
                table = %ctx.series,
                manifest_id = %manifest.id,
                attempt,
                reason,
                "staged object failed verification; integrity replacement budget exhausted"
            );
            Err(TransformerError::Quarantine {
                table: ctx.series.clone(),
                reason,
            })
        }
    }
}

fn validate_object_fingerprint(
    uri: &str,
    expected_size: i64,
    expected_sha: &[u8],
    actual_size: i64,
    actual_sha: &[u8],
) -> Result<(), TransformerError> {
    if actual_size == expected_size && actual_sha == expected_sha {
        return Ok(());
    }
    Err(TransformerError::ObjectIntegrity {
        uri: uri.to_string(),
        reason: format!(
            "expected {expected_size} bytes / {}, got {actual_size} bytes / {}",
            hex::encode(expected_sha),
            hex::encode(actual_sha)
        ),
    })
}

fn validate_reload_manifest_at_f(
    file: &control::ManifestRow,
    build: &ReloadBuild,
) -> Result<(), TransformerError> {
    if file.reload_id != Some(build.reload_id)
        || file.lsn_start != build.start_lsn
        || file.lsn_end != build.start_lsn
    {
        return Err(TransformerError::Internal(format!(
            "reload manifest {} identity/boundaries ({:?}, [{}, {}]) do not equal active reload {} at frozen F {}",
            file.id, file.reload_id, file.lsn_start, file.lsn_end, build.reload_id, build.start_lsn
        )));
    }
    Ok(())
}

/// Discover a marker-delimited, source-driven rebuild without depending on a data file.
async fn prepare_ready_reload(ctx: &TableCtx) -> Result<Option<ReloadBuild>, TransformerError> {
    discard_failed_reload_build(ctx).await?;
    let existing = ctx.db.reload_build()?;
    if let Some(build) = &existing
        && build.phase == crate::duck::ReloadPhase::Published
    {
        let receipt = control::reload::read_publication(&ctx.pool, build.reload_id)
            .await?
            .ok_or_else(|| {
                TransformerError::Internal(format!(
                    "Duck has published reload {} but control has no publication receipt",
                    build.reload_id
                ))
            })?;
        validate_build_publication(ctx, build, &receipt)?;
        if receipt.status == control::ReloadStatus::Complete {
            ctx.db
                .clear_reload_publication(build.reload_id, build.publication_nonce)?;
            return Ok(None);
        }
    }

    let Some(publication) = control::reload::claim_publication(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?
    else {
        if let Some(build) = existing {
            return Err(TransformerError::Internal(format!(
                "Duck reload {} has no export_complete/publishing control owner",
                build.reload_id
            )));
        }
        return Ok(None);
    };

    if let Some(build) = existing {
        validate_build_publication(ctx, &build, &publication)?;
        return Ok(Some(build));
    }

    let plan = plan_at_version(ctx, publication.schema_version).await?;
    let BeginReload::Ready(build) = ctx.db.begin_reload_shadow(
        &plan,
        publication.schema_version,
        publication.reload_id,
        publication.start_lsn,
        publication.final_lsn,
        publication.publication_nonce,
    )?
    else {
        return Ok(None);
    };
    let purged = control::delete_publication_superseded(
        &ctx.pool,
        &publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %publication.reload_id,
        schema_version = %publication.schema_version,
        start_lsn = %publication.start_lsn,
        final_lsn = %publication.final_lsn,
        purged,
        "fenced reload publication started in a hidden generation"
    );
    Ok(Some(*build))
}

pub(crate) fn validate_build_publication(
    ctx: &TableCtx,
    build: &ReloadBuild,
    publication: &control::ReloadPublication,
) -> Result<(), TransformerError> {
    if publication.epoch != ctx.epoch
        || publication.source_schema != ctx.schema
        || publication.source_table != ctx.table
        || publication.reload_id != build.reload_id
        || publication.start_lsn != build.start_lsn
        || publication.final_lsn != build.final_lsn
        || publication.schema_version != build.schema_version
        || publication.publication_nonce != build.publication_nonce
    {
        return Err(TransformerError::Internal(format!(
            "Duck reload {} does not match its durable control publication receipt",
            build.reload_id
        )));
    }
    Ok(())
}

pub(crate) async fn publication_for_build(
    ctx: &TableCtx,
    build: &ReloadBuild,
) -> Result<control::ReloadPublication, TransformerError> {
    let publication = control::reload::claim_publication(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?
    .ok_or_else(|| {
        TransformerError::Internal(format!(
            "reload {} lost its publishing control receipt",
            build.reload_id
        ))
    })?;
    validate_build_publication(ctx, build, &publication)?;
    Ok(publication)
}

/// Route one claimed `kind='reload'` file to the live generation, the matching hidden generation,
/// or retirement when it belongs to a stale attempt.
async fn route_reload_file(
    ctx: &TableCtx,
    f: &control::ManifestRow,
) -> Result<FileRoute, TransformerError> {
    let file_reload_id = f.reload_id.ok_or_else(|| {
        TransformerError::Internal(format!(
            "manifest row {} is kind='reload' but carries no reload_id",
            f.id
        ))
    })?;
    if let Some(build) = ctx.db.reload_build()? {
        if file_reload_id < build.reload_id {
            return Ok(FileRoute::Retire);
        }
        if file_reload_id == build.reload_id {
            return Ok(FileRoute::Shadow(build.shadow_table));
        }
    }
    // `None` = the latch was never set, so this file can only be a NEW attempt: fall through to
    // the "greater" arm below rather than compare it against a stand-in id.
    if let Some(recorded) = ctx.db.recorded_reload_id()? {
        if file_reload_id < recorded {
            return Ok(FileRoute::Retire); // a superseded attempt whose purge raced the claim (H9)
        }
        if file_reload_id == recorded {
            return Ok(FileRoute::Retire); // a crash-window chunk discovered after publication
        }
    }

    // The ordinary claim can observe `export_complete` in the narrow interval before the second
    // preparation read. Claim publication here too; no shadow may be created from a file alone.
    let publication = control::reload::claim_publication(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?
    .ok_or_else(|| {
        TransformerError::Internal(format!(
            "reload manifest {} has no fenced publication attempt",
            f.id
        ))
    })?;
    if publication.reload_id != file_reload_id {
        return if file_reload_id < publication.reload_id {
            Ok(FileRoute::Retire)
        } else {
            Err(TransformerError::Internal(format!(
                "reload manifest {} belongs to future attempt {file_reload_id} while {} is publishing",
                f.id, publication.reload_id
            )))
        };
    }
    if f.schema_version != publication.schema_version {
        return Err(TransformerError::Internal(format!(
            "reload {file_reload_id} chunk schema {} differs from frozen schema {}",
            f.schema_version, publication.schema_version
        )));
    }
    if f.lsn_end > publication.final_lsn {
        return Err(TransformerError::Internal(format!(
            "reload {file_reload_id} chunk {} lies beyond end marker {}",
            f.id, publication.final_lsn
        )));
    }

    // Build both replacement tables under a hidden deterministic name. The public/live generation
    // remains untouched until Phase B reaches the explicit H barrier.
    let plan = plan_at_version(ctx, publication.schema_version).await?;
    let BeginReload::Ready(build) = ctx.db.begin_reload_shadow(
        &plan,
        publication.schema_version,
        file_reload_id,
        publication.start_lsn,
        publication.final_lsn,
        publication.publication_nonce,
    )?
    else {
        return Ok(FileRoute::Retire);
    };
    // Purge superseded pending rows: every non-reload file at lsn_end <= the start fence describes a
    // commit the consistent baseline re-covers; applying it after replacement would only churn.
    // Post-F stream files survive and apply after the baseline chunks in (lsn_end, id) order.
    let purged = control::delete_publication_superseded(
        &ctx.pool,
        &publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %file_reload_id,
        schema_version = %publication.schema_version,
        start_lsn = %publication.start_lsn,
        final_lsn = %publication.final_lsn,
        purged,
        "reload reconciliation started in a hidden generation"
    );
    Ok(FileRoute::Shadow(build.shadow_table))
}

struct VersionSchema {
    relation: PgRelation,
    plan: crate::plan::TablePlan,
    emit_groups: Vec<Vec<VersionEmitColumn>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionEmitColumn {
    name: String,
    duckdb_type: String,
}

impl VersionSchema {
    fn from_registry_parts(
        relation: PgRelation,
        descriptors: &[common::TypeDescriptor],
    ) -> Result<Self, TransformerError> {
        // Re-plan one logical source column at a time through the same TablePlan implementation.
        // The resulting groups retain the attnum boundary that the flat full-table plan omits, so
        // rename lineage can map every Tier-2 sibling without guessing from name prefixes.
        let plan = crate::plan::TablePlan::from_registry(&relation, descriptors)?;
        let descriptors_by_column = descriptors
            .iter()
            .map(|descriptor| (descriptor.column.as_str(), descriptor))
            .collect::<HashMap<_, _>>();
        let emit_groups = relation
            .columns
            .iter()
            .map(|column| {
                let descriptor = descriptors_by_column.get(column.name.as_str()).copied();
                crate::plan::TablePlan::for_registry_column(&relation, column, descriptor)
                    .raw_cols
                    .iter()
                    .map(|raw| VersionEmitColumn {
                        name: raw.name.clone(),
                        duckdb_type: raw.duckdb_type.clone(),
                    })
                    .collect()
            })
            .collect();
        Ok(Self {
            relation,
            plan,
            emit_groups,
        })
    }
}

fn version_schema(
    schemas: &BTreeMap<SchemaVersionNo, VersionSchema>,
    version: SchemaVersionNo,
) -> Result<&VersionSchema, TransformerError> {
    schemas
        .get(&version)
        .ok_or_else(|| TransformerError::ManifestInvariant {
            message: format!("schema lineage is missing version {version}"),
        })
}

/// Map one historical Parquet's physical emit columns onto the raw table at `destination_version`.
/// Every immutable registry step is inspected. ADD does not touch older mappings; DROP deliberately
/// leaves the removed column's raw destination frozen because raw is a historical superset even
/// though later source positions shift. A common-position name substitution fails closed because
/// the persisted protocol cannot prove whether it was RENAME or DROP+ADD.
fn destination_columns_between(
    source_version: SchemaVersionNo,
    destination_version: SchemaVersionNo,
    schemas: &BTreeMap<SchemaVersionNo, VersionSchema>,
) -> Result<Vec<String>, TransformerError> {
    if source_version > destination_version {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "cannot map future schema {source_version} into older raw schema {destination_version}"
            ),
        });
    }
    let source = version_schema(schemas, source_version)?;
    let mut destinations = source
        .plan
        .raw_cols
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let mut current = source_version;
    while current < destination_version {
        let next = SchemaVersionNo(current.0.checked_add(1).ok_or_else(|| {
            TransformerError::ManifestInvariant {
                message: format!("schema lineage overflows after version {current}"),
            }
        })?);
        compose_schema_step(
            version_schema(schemas, current)?,
            version_schema(schemas, next)?,
            current,
            next,
        )?;
        current = next;
    }
    destinations.push("walrus_extractor_meta".to_string());
    let mut seen = std::collections::HashSet::with_capacity(destinations.len());
    if destinations
        .iter()
        .any(|column| !seen.insert(column.as_str()))
    {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "schema {source_version} maps duplicate columns into raw schema {destination_version}"
            ),
        });
    }
    Ok(destinations)
}

fn compose_schema_step(
    old: &VersionSchema,
    new: &VersionSchema,
    old_version: SchemaVersionNo,
    new_version: SchemaVersionNo,
) -> Result<(), TransformerError> {
    let old_shape = crate::ddl::SchemaVersion {
        version: old_version,
        relation: old.relation.clone(),
    };
    let new_shape = crate::ddl::SchemaVersion {
        version: new_version,
        relation: new.relation.clone(),
    };
    let diff = crate::ddl::diff(&old_shape, &new_shape)?;

    if new.relation.columns.len() < old.relation.columns.len() {
        // A drop shifts later attnums, but raw retains the dropped physical column. Match surviving
        // logical columns by name solely to prove their emit names did not also change in this
        // drop step; never zip the shifted positions.
        for (old_index, old_column) in old.relation.columns.iter().enumerate() {
            let Some(new_index) = new
                .relation
                .columns
                .iter()
                .position(|column| column.name == old_column.name)
            else {
                continue;
            };
            if old.emit_groups.get(old_index) != new.emit_groups.get(new_index) {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema step {old_version}->{new_version} changes emit shape while dropping columns"
                    ),
                });
            }
        }
        return Ok(());
    }

    for change in diff.additive {
        let crate::ddl::AdditiveChange::RenameColumn { position, from, to } = change else {
            continue;
        };
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "schema step {old_version}->{new_version} substitutes common position {position} {from:?}->{to:?}; a genuine RENAME and same-statement DROP+ADD are intentionally indistinguishable until stable column-lineage evidence is persisted"
            ),
        });
    }

    // With source names unchanged, emitted names must also stay unchanged. This rejects descriptor
    // drift instead of silently position-zipping an unsupported physical-shape change.
    for (position, old_column) in old.relation.columns.iter().enumerate() {
        let old_emit = old.emit_groups.get(position);
        let new_emit = new.emit_groups.get(position);
        let emit_names_match = old_emit.zip(new_emit).is_some_and(|(old, new)| {
            old.iter()
                .map(|column| column.name.as_str())
                .eq(new.iter().map(|column| column.name.as_str()))
        });
        if old_column.name == new.relation.columns[position].name && !emit_names_match {
            return Err(TransformerError::ManifestInvariant {
                message: format!(
                    "schema step {old_version}->{new_version} changes emit shape without a column rename at position {position}"
                ),
            });
        }
    }
    Ok(())
}

/// The exact registry relation and physical emit plan at `version`, falling back to the bootstrap
/// relation's scalar shape for hermetic single-version setups —
/// `phase_b::current_transform`'s precedent. Keeping both views together prevents file-schema
/// validation and `unchanged_toast` validation from accidentally binding different versions.
async fn schema_at_version(
    ctx: &TableCtx,
    version: SchemaVersionNo,
) -> Result<VersionSchema, TransformerError> {
    match control::read_registry(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, version).await? {
        Some(row) => decode_version_schema(ctx, row),
        None => VersionSchema::from_registry_parts(ctx.rel.clone(), &[]),
    }
}

fn decode_version_schema(
    ctx: &TableCtx,
    row: control::RegistryRow,
) -> Result<VersionSchema, TransformerError> {
    let version = row.schema_version;
    // Label built inside the closure (`current_transform`'s precedent): only a decode failure pays
    // for it.
    let relation: PgRelation =
        serde_json::from_value(row.columns).map_err(|source| TransformerError::RegistryDecode {
            table: format!("{}.{}", ctx.schema, ctx.table),
            version: version.0,
            source,
        })?;
    VersionSchema::from_registry_parts(relation, &row.descriptors)
}

/// Load and validate an inclusive contiguous registry range with one indexed query. A malicious
/// manifest version near `i64::MAX` therefore causes one missing-lineage error instead of billions
/// of sequential point reads. The one-version empty fallback preserves hermetic Phase-A fixtures
/// that intentionally omit a control registry; production history is immutable and contiguous.
async fn schema_lineage(
    ctx: &TableCtx,
    first: SchemaVersionNo,
    last: SchemaVersionNo,
) -> Result<Vec<(SchemaVersionNo, VersionSchema)>, TransformerError> {
    if first > last {
        return Err(TransformerError::ManifestInvariant {
            message: format!("invalid schema lineage range {first}..={last}"),
        });
    }
    let rows =
        control::read_registry_range(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, first, last)
            .await?;
    // Bare-relation fixtures have no registry, but they are only allowed to describe the schema the
    // local DuckDB has already bootstrapped. Without this equality guard, a fabricated one-file
    // manifest at i64::MAX would take the fixture fallback and then pin `reconcile_to_version` in an
    // effectively unbounded integer walk.
    if rows.is_empty() && first == last && first == ctx.db.schema_version()? {
        return Ok(vec![(
            first,
            VersionSchema::from_registry_parts(ctx.rel.clone(), &[])?,
        )]);
    }

    let mut expected = first;
    let mut lineage = Vec::with_capacity(rows.len());
    for row in rows {
        if row.schema_version != expected {
            return Err(TransformerError::ManifestInvariant {
                message: format!("schema lineage {first}..={last} is missing version {expected}"),
            });
        }
        let version = row.schema_version;
        lineage.push((version, decode_version_schema(ctx, row)?));
        if version == last {
            return Ok(lineage);
        }
        expected = SchemaVersionNo(version.0.checked_add(1).ok_or_else(|| {
            TransformerError::ManifestInvariant {
                message: format!("schema lineage overflows after version {version}"),
            }
        })?);
    }
    Err(TransformerError::ManifestInvariant {
        message: format!("schema lineage {first}..={last} is missing version {expected}"),
    })
}

/// The registry shape at `version` as a [`crate::plan::TablePlan`] (the Tier-2 emit/recombine path).
async fn plan_at_version(
    ctx: &TableCtx,
    version: SchemaVersionNo,
) -> Result<crate::plan::TablePlan, TransformerError> {
    Ok(schema_at_version(ctx, version).await?.plan)
}

/// The raw-append backlog in LSN-bytes: how far the newest ready file's commit LSN leads the Phase-A
/// frontier. An empty queue (`None`) is 0; a frontier already at/after the head is 0. This is the
/// value of `walrus_transformer_raw_append_lag_bytes`.
///
/// `max` bounds the head **up to** the frontier rather than branching on the same comparison twice:
/// the lower bound is what makes `-` reachable only inside its defined ordered domain, and the
/// clamped-equal case is exactly the 0 a caught-up (or momentarily stale) read should report.
fn raw_append_lag_bytes(max_ready_lsn_end: Option<Lsn>, raw_appended: Lsn) -> u64 {
    max_ready_lsn_end.map_or(0, |head| head.max(raw_appended) - raw_appended)
}

#[cfg(test)]
#[path = "phase_a_test.rs"]
mod tests;
