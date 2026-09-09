//! The decode loop: join the live [`ReplicationStream`] to the sync, pure [`pgoutput`]
//! decoder. The Rust analogue of the proof harness's `run-tests.sh` — an `INSERT` now
//! decodes to `Begin → Relation → Insert → Commit` against a real Postgres. No Arrow / batching / S3.
//!
//! **The seam that kept the decoder testable:** [`pgoutput::parse_message`] stays **sync + pure**;
//! this loop owns the I/O (`.await`s a frame) and calls the decoder synchronously on the returned
//! `Bytes`. The [`StreamCtx`] (are we inside a `Stream Start`/`Stop` block?) is threaded across
//! frames by the loop, since a v2 sub-xid prefix appears *only inside* a stream. Small txns still
//! arrive whole at commit (no stream frames), and [`StreamCtx`] handles both shapes with no
//! special-casing here.

use crate::batch::{BatchTriggers, Clock, SealedBatch, TableBatcher};
use crate::health::HealthState;
use crate::heartbeat::{Heartbeat, InternalTables};
use crate::pgoutput::{self, Message, Reader, StreamCtx};
use crate::relcache::{RelationCache, is_internal_table};
use crate::replication::{ReplicationMessage, ReplicationStream};
use anyhow::Context;
use common::{EpochNo, ExtractorMeta, Kind, Lsn, Op, TupleValue, UtcTimestamp};
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A missing required field at decode-loop build time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("decode loop builder: missing required field `{0}`")]
pub struct DecodeLoopError(&'static str);

/// The outcome carried beyond the pinned frame future's borrow of the replication stream.
enum FrameEvent {
    Cancelled,
    Frame(anyhow::Result<Option<ReplicationMessage>>),
}

fn transaction_scope(
    xid: Option<u32>,
    current_top: Option<u32>,
) -> anyhow::Result<crate::ddl::TransactionScope> {
    match xid {
        Some(sub_xid) => Ok(crate::ddl::TransactionScope::Streamed {
            top_xid: current_top.context("xid-prefixed message arrived outside StreamStart")?,
            sub_xid,
        }),
        None => Ok(crate::ddl::TransactionScope::Ordinary),
    }
}

fn decode_reload_event(
    rel: &common::PgRelation,
    new: &[TupleValue],
    xid: Option<u32>,
    current_top: Option<u32>,
) -> anyhow::Result<crate::reload_event::PendingReloadEvent> {
    let top_xid = match transaction_scope(xid, current_top)? {
        crate::ddl::TransactionScope::Ordinary => None,
        crate::ddl::TransactionScope::Streamed { top_xid, .. } => Some(top_xid),
    };
    crate::reload_event::PendingReloadEvent::from_tuple(rel, new, xid, top_xid)
        .context("parse public.walrus_reload_event tuple")
}

/// Result of resolving one audit row against the exact identities frozen into this epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TrackedDdlResolution {
    Tracked(Box<common::PgRelation>),
    Unresolved,
    Unrelated,
}

/// Resolve a DDL audit row only against the relation identities frozen into this epoch.
///
/// The source trigger sees every user table, not just publication members. Schema/name lookup is
/// permitted only for a legacy null-OID row when the qualified name maps to one distinct frozen OID.
/// A supplied but different OID at the same tracked name is returned as tracked so commit validation
/// can reject the recreation. A structural null-OID row without one unique match is unresolved, not
/// unrelated: it may be a tracked rename/schema move whose post-change name no longer matches.
fn tracked_relation_for_ddl(
    cache: &RelationCache,
    event: &crate::ddl::DdlEvent,
) -> TrackedDdlResolution {
    if let Some(oid) = event.c_rel_oid
        && let Some(cached) = cache.latest_for(oid)
    {
        return TrackedDdlResolution::Tracked(Box::new(cached.relation.clone()));
    }

    let mut matching_oids = cache
        .iter()
        .filter(|cached| {
            cached.relation.schema == event.source_schema
                && cached.relation.name == event.source_table
        })
        .map(|cached| cached.relation.oid)
        .collect::<Vec<_>>();
    matching_oids.sort_unstable();
    matching_oids.dedup();
    if let [oid] = matching_oids.as_slice()
        && let Some(cached) = cache
            .iter()
            .filter(|cached| {
                cached.relation.oid == *oid
                    && cached.relation.schema == event.source_schema
                    && cached.relation.name == event.source_table
            })
            .max_by_key(|cached| cached.schema_version)
    {
        return TrackedDdlResolution::Tracked(Box::new(cached.relation.clone()));
    }

    if event.is_structural() && (event.c_rel_oid.is_none() || !matching_oids.is_empty()) {
        TrackedDdlResolution::Unresolved
    } else {
        TrackedDdlResolution::Unrelated
    }
}

/// The fully wired decode loop. Construct it with [`DecodeLoop::builder`]; [`DecodeLoop::run`]
/// consumes the wiring so its mutable borrows cannot escape or be reused concurrently.
#[derive(Debug)]
pub struct DecodeLoop<'a, C> {
    stream: &'a mut ReplicationStream,
    token: CancellationToken,
    cache: &'a mut RelationCache,
    router: &'a mut BatchRouter<C>,
    stager: &'a crate::staging::ParquetStager,
    checkpoint: &'a mut crate::checkpoint::DurabilityCheckpoint,
    demux: &'a mut crate::stream_txn::StreamDemux<C>,
    ddl: &'a mut crate::ddl::DdlConsumer,
    heartbeat: &'a mut Heartbeat,
    health: &'a HealthState,
    pool: &'a sqlx::PgPool,
    epoch: EpochNo,
    waiters: &'a crate::reload_signal::WatermarkWaiters,
    fence_waiters: &'a crate::reload_event::FenceWaiters,
}

/// Wires a [`DecodeLoop`]. Every setter consumes and returns the builder, so the chain must be kept:
/// a dropped intermediate silently discards that field.
///
/// Ignoring a setter's return value is a compile error:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # let builder = extractor::consume::DecodeLoop::<
/// #     std::sync::Arc<extractor::batch::SystemClock>,
/// # >::builder();
/// builder.epoch(common::EpochNo(7));
/// ```
#[must_use = "a builder does nothing until you call build()"]
#[derive(Debug)]
pub struct DecodeLoopBuilder<'a, C> {
    stream: Option<&'a mut ReplicationStream>,
    token: Option<CancellationToken>,
    cache: Option<&'a mut RelationCache>,
    router: Option<&'a mut BatchRouter<C>>,
    stager: Option<&'a crate::staging::ParquetStager>,
    checkpoint: Option<&'a mut crate::checkpoint::DurabilityCheckpoint>,
    demux: Option<&'a mut crate::stream_txn::StreamDemux<C>>,
    ddl: Option<&'a mut crate::ddl::DdlConsumer>,
    heartbeat: Option<&'a mut Heartbeat>,
    health: Option<&'a HealthState>,
    pool: Option<&'a sqlx::PgPool>,
    epoch: Option<EpochNo>,
    waiters: Option<&'a crate::reload_signal::WatermarkWaiters>,
    fence_waiters: Option<&'a crate::reload_event::FenceWaiters>,
}

impl<C> Default for DecodeLoopBuilder<'_, C> {
    fn default() -> Self {
        Self {
            stream: None,
            token: None,
            cache: None,
            router: None,
            stager: None,
            checkpoint: None,
            demux: None,
            ddl: None,
            heartbeat: None,
            health: None,
            pool: None,
            epoch: None,
            waiters: None,
            fence_waiters: None,
        }
    }
}

impl<'a, C> DecodeLoop<'a, C> {
    /// An empty [`DecodeLoopBuilder`]. Every field is required, so this is the only constructor —
    /// there is no `new` that could leave a collaborator unwired.
    pub fn builder() -> DecodeLoopBuilder<'a, C> {
        DecodeLoopBuilder::default()
    }
}

impl<C: Clock + Clone> DecodeLoop<'_, C> {
    /// Drive the stream: decode each `XLogData`, register each `Relation` (cache + schema_registry),
    /// route I/U/D plus table-level TRUNCATE boundaries into per-table batchers (sealing at commit
    /// boundaries), PUT sealed batches to S3,
    /// keep keepalives answered, and exit cleanly on cancel or stream end.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] when replication I/O or decoding fails, a relation/DDL/reload
    /// invariant is invalid, Arrow batching or S3/manifest durability fails, or a control-plane
    /// operation cannot complete. Context on the returned chain identifies the failed stage.
    ///
    /// # Panics
    ///
    /// Panics if the wired [`Heartbeat`]'s `idle_after` is zero: it is this loop's idle-check
    /// cadence, and [`tokio::time::interval`] rejects a zero period. `config.rs` bounds the
    /// configured value above zero.
    pub async fn run(self) -> anyhow::Result<()> {
        let DecodeLoop {
            stream,
            token,
            cache,
            router,
            stager,
            checkpoint,
            demux,
            ddl,
            heartbeat,
            health,
            pool,
            epoch,
            waiters,
            fence_waiters,
        } = self;
        // `BatchRouter::route` retains its integration-test seam, but the cached relation owns the
        // structural version now; this compatibility argument is intentionally ignored there.
        let schema_version = common::SchemaVersionNo(0);
        let mut ctx = StreamCtx::default();
        let mut internal = InternalTables::default();
        // reload_signal echoes buffered between their Insert and their transaction's fate:
        // the watermark is the COMMIT LSN, which only the Commit message carries.
        let mut pending_signals = crate::reload_signal::PendingSignals::default();
        // Source reload events are also provisional until their transaction commits.  End fences
        // take the stronger path below: their waiter is not resolved until the target's committed
        // rows have crossed object-store + manifest durability.
        let mut pending_reload_events = crate::reload_event::PendingReloadEvents::default();
        // Idle windows are monotonic (`tokio::time::Instant`); `last_activity` moves on every user change,
        // never on keepalives or the heartbeat's own round-trip.
        let mut last_activity = Instant::now();
        // Whether the transaction currently decoding carries the heartbeat change (its Commit lets the
        // checkpoint advance on an idle publication).
        let mut txn_has_heartbeat = false;
        // pgoutput's ordinary Commit carries no xid, so retain the real xid and final LSN from its
        // matching Begin. This is deliberately separate from BatchRouter's row-routing context: the
        // durable schema publication receipt must reject a missing/overlapping Begin instead of
        // reusing stale transaction identity.
        let mut ordinary_txn = None;
        // Relation XLogData frames do not carry a trustworthy schema-history position (PostgreSQL
        // can emit wal_start=0). Instead, retain the feedback cursor immediately after the last
        // source commit this decode pass completed successfully. Schema history is queried strictly
        // before that cursor. This also repairs a legacy restart exactly at commit_lsn: both ordinary
        // and protocol-v2 streamed rows bind before their own already-durable control history.
        let mut processed_through_lsn = checkpoint.confirmed_flush();
        // Check idleness at the beat cadence; the first (immediate) tick is a no-op (just-started).
        let mut beat_check = tokio::time::interval(heartbeat.idle_after());
        beat_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut observed_open_stream_txns = 0;
        refresh_open_stream_guards(demux, &mut observed_open_stream_txns);
        loop {
            let event = {
                // Keep one frame future alive across heartbeat ticks. `next()` may be parked in a
                // feedback write, where dropping it would leave a partial CopyBoth frame.
                let frame = stream.next();
                tokio::pin!(frame);
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break FrameEvent::Cancelled,
                        _ = beat_check.tick() => {
                            // Refresh age even while the stream is idle between protocol-v2 segments.
                            // This reads demux state only; it cannot advance the durability checkpoint.
                            refresh_open_stream_guards(demux, &mut observed_open_stream_txns);
                            // The beat fires over a SEPARATE SQL connection only when idle on both clocks; a
                            // failure is logged and surfaced as `degraded`, never fatal (liveness never self-harms).
                            let now = Instant::now();
                            match heartbeat.maybe_beat(now, last_activity).await {
                                Ok(Some(seq)) => tracing::info!(beat_seq = seq, "fired idle heartbeat"),
                                Ok(None) => {}
                                Err(e) => tracing::warn!(error = ?e, "heartbeat beat failed"),
                            }
                            health.set_degraded(heartbeat.is_degraded(now));
                        }
                        res = &mut frame => break FrameEvent::Frame(res),
                    }
                }
            };

            match event {
                FrameEvent::Cancelled => {
                    // SIGTERM/SIGINT: the loop has stopped consuming — now run the ordered drain (NOT
                    // cancellable; the caller bounds it by the K8s grace period). The slot is never dropped.
                    tracing::info!("decode loop cancelled; draining");
                    health.mark_terminating();
                    let outcome =
                        crate::shutdown::drain(stream, router, stager, checkpoint, pool, epoch)
                            .await
                            .context("graceful drain")?;
                    tracing::info!(
                        ?outcome,
                        "drain complete; slot left in place, resume on restart"
                    );
                    return Ok(());
                }
                FrameEvent::Frame(frame) => {
                    match frame.context("read replication frame")? {
                        None => {
                            tracing::info!("replication stream ended");
                            return Ok(());
                        }
                        Some(frame) => {
                            // The change's LSN is the XLogData frame's start (pgoutput change messages
                            // carry no per-change LSN of their own).
                            let frame_lsn = match &frame {
                                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
                            };
                            if let Some(msg) = on_frame(&mut ctx, frame)? {
                                trace_message(&msg);
                                match &msg {
                                    m @ Message::Begin { xid, final_lsn, .. } => {
                                        anyhow::ensure!(
                                            ordinary_txn.is_none(),
                                            "ordinary Begin for xid {xid} arrived while xid {} is still open",
                                            ordinary_txn.map_or(0, |(open_xid, _)| open_xid)
                                        );
                                        ordinary_txn = Some((*xid, *final_lsn));
                                        flush_sealed(
                                            router.route(cache, m, frame_lsn, schema_version)?,
                                            router.undurable_floor(),
                                            stream,
                                            stager,
                                            checkpoint,
                                            pool,
                                            epoch,
                                        )
                                        .await?;
                                    }
                                    Message::Relation { relation, xid } => {
                                        // Learn every walrus internal table's OID and relation shape BEFORE
                                        // its change arrives (Relation always precedes the change in the same txn).
                                        internal.note_relation(relation);
                                        if !is_internal_table(&relation.schema, &relation.name) {
                                            let scope =
                                                transaction_scope(*xid, demux.current_top())?;
                                            // At an exact-boundary replay, durable control history can already
                                            // contain this transaction's DDL at C. Querying strictly before
                                            // the processed feedback cursor excludes that future for streamed
                                            // replay too; ordinary Begin.final_lsn supplies an additional exact
                                            // upper bound for its own transaction.
                                            let ordinary_final_lsn = match scope {
                                                crate::ddl::TransactionScope::Ordinary => {
                                                    let (_, final_lsn) = ordinary_txn.context(
                                                        "ordinary Relation arrived without a matching Begin",
                                                    )?;
                                                    Some(final_lsn)
                                                }
                                                crate::ddl::TransactionScope::Streamed {
                                                    ..
                                                } => None,
                                            };
                                            let relation_history_lsn = relation_history_cutoff(
                                                processed_through_lsn,
                                                ordinary_final_lsn,
                                            );
                                            let version = ddl
                                                .relation_version_for(
                                                    scope,
                                                    relation,
                                                    relation_history_lsn,
                                                    cache,
                                                )
                                                .context("bind Relation to exact schema history")?;
                                            let row = cache_relation(
                                                cache,
                                                epoch,
                                                relation.clone(),
                                                version,
                                            )?
                                            .context(
                                                "user Relation unexpectedly classified internal",
                                            )?;
                                            if let crate::ddl::TransactionScope::Streamed {
                                                top_xid,
                                                sub_xid,
                                            } = scope
                                            {
                                                demux.bind_relation(
                                                    top_xid,
                                                    sub_xid,
                                                    relation.oid,
                                                    version,
                                                );
                                            } else {
                                                router.bind_relation(relation.oid, version);
                                            }
                                            if ddl.is_provisional(
                                                &relation.schema,
                                                &relation.name,
                                                version,
                                            ) {
                                                ddl.stage_registry(scope, row);
                                            } else {
                                                persist_registry(pool, &row).await?;
                                            }
                                        }
                                    }
                                    // The DDL signal is provisional until this source transaction commits.
                                    // Its structured snapshot is usable immediately INSIDE the transaction,
                                    // while manifest/registry publication waits for Commit/StreamCommit.
                                    Message::Insert {
                                        relation_oid,
                                        new,
                                        xid,
                                    } if internal.is_ddl_audit(*relation_oid) => {
                                        if let Some(rel) = internal.ddl_audit_rel() {
                                            let ev = crate::ddl::DdlEvent::from_tuple(rel, new)
                                                .context("parse ddl_audit tuple")?;
                                            let scope =
                                                transaction_scope(*xid, demux.current_top())?;
                                            let resolution = tracked_relation_for_ddl(cache, &ev);
                                            let previous_for_oid = match resolution {
                                                TrackedDdlResolution::Tracked(previous) => {
                                                    Some(*previous)
                                                }
                                                TrackedDdlResolution::Unresolved => None,
                                                TrackedDdlResolution::Unrelated => {
                                                    // A nonmatching explicit OID proves this event belongs to
                                                    // an untracked relation. Null-OID structural events never
                                                    // take this branch because they could hide a rename/move.
                                                    tracing::debug!(
                                                        relation_oid = ?ev.c_rel_oid,
                                                        source_table = %format_args!("{}.{}", ev.source_schema, ev.source_table),
                                                        c_tag = %ev.c_tag,
                                                        "ignoring DDL audit for an untracked relation"
                                                    );
                                                    continue;
                                                }
                                            };
                                            let observation = if previous_for_oid.is_some() {
                                                ddl.observe(
                                                    scope,
                                                    ev.clone(),
                                                    previous_for_oid.as_ref(),
                                                )
                                            } else {
                                                ddl.observe_unresolved_identity(scope, ev.clone())
                                            };
                                            if let Some(new_version) =
                                                observation.structural_version
                                            {
                                                // A sql_drop sentinel has no post-change relation. Do not
                                                // try to build/cache its empty shape or cut files before
                                                // transaction fate is known; a tracked drop fails at commit,
                                                // while StreamAbort discards it without decode side effects.
                                                if !ev.is_table_drop() {
                                                    // Cache the trigger's authoritative post-change catalog
                                                    // snapshot now, but commit-gate its registry row.
                                                    let relation_after = ev.relation_after(
                                                        previous_for_oid.as_ref(),
                                                    )?;
                                                    let row = if observation.replay
                                                        && relation_after.is_some()
                                                    {
                                                        let oid = relation_after
                                                            .as_ref()
                                                            .map(|relation| relation.oid)
                                                            .context(
                                                                "replayed structural DDL has no resolved relation identity",
                                                            )?;
                                                        let cached = cache
                                                            .get(oid, new_version)
                                                            .with_context(|| {
                                                                format!(
                                                                    "replayed DDL audit {} has no exact hydrated registry version {} for relation OID {}",
                                                                    ev.source_audit_id, new_version, oid
                                                                )
                                                            })?;
                                                        Some(registry_row_from_cached(
                                                            epoch,
                                                            cached.as_ref(),
                                                        )?)
                                                    } else if observation.replay {
                                                        None
                                                    } else {
                                                        relation_after
                                                            .map(|after| {
                                                                cache_relation(
                                                                    cache,
                                                                    epoch,
                                                                    after,
                                                                    new_version,
                                                                )
                                                            })
                                                            .transpose()?
                                                            .flatten()
                                                    };
                                                    if let Some(row) = row {
                                                        // Replays are commit-gated too: StreamCommit must
                                                        // reproduce the exact durable DDL/registry shape so
                                                        // the atomic receipt validates after a lost ACK.
                                                        ddl.stage_registry(scope, row);
                                                    }
                                                    // Cut committed old-version rows now; preserve any open
                                                    // ordinary pre-DDL rows as a commit-gated segment.
                                                    let sealed = router.cut_table(
                                                        cache,
                                                        &ev.source_schema,
                                                        &ev.source_table,
                                                    )?;
                                                    flush_sealed(
                                                        sealed,
                                                        router.undurable_floor(),
                                                        stream,
                                                        stager,
                                                        checkpoint,
                                                        pool,
                                                        epoch,
                                                    )
                                                    .await?;
                                                }
                                                tracing::info!(
                                                    source_table = %format_args!("{}.{}", ev.source_schema, ev.source_table),
                                                    c_tag = %ev.c_tag,
                                                    schema_version = %new_version,
                                                    capture_lsn = %ev.capture_lsn,
                                                    replay = observation.replay,
                                                    "DDL staged: provisional version + file boundary"
                                                );
                                            } else {
                                                tracing::info!(
                                                    c_tag = %ev.c_tag,
                                                    replay = observation.replay,
                                                    "DDL staged: metadata-only"
                                                );
                                            }
                                        }
                                    }
                                    // Append-only reload request/fence events are control-plane rows,
                                    // never user data.  Buffer until transaction fate is known; the
                                    // Commit paths below enforce the stronger end-fence durability gate.
                                    Message::Insert {
                                        relation_oid,
                                        new,
                                        xid,
                                    } if internal.is_reload_event(*relation_oid) => {
                                        let rel = internal.reload_event_rel().context(
                                            "reload_event OID known without its relation shape",
                                        )?;
                                        let event = decode_reload_event(
                                            rel,
                                            new,
                                            *xid,
                                            demux.current_top(),
                                        )?;
                                        pending_reload_events.push(event);
                                    }
                                    // The reload echo: the extractor's own signal INSERT returning
                                    // through the stream. Buffered here; the waiter resolves at the
                                    // transaction's Commit with its commit LSN (= the chunk watermark
                                    // L_i). NEVER batched, never a Parquet file or manifest row.
                                    Message::Insert {
                                        relation_oid,
                                        new,
                                        xid,
                                    } if internal.is_reload_signal(*relation_oid) => {
                                        if let Some(rel) = internal.reload_signal_rel() {
                                            let top_xid =
                                                match transaction_scope(*xid, demux.current_top())?
                                                {
                                                    crate::ddl::TransactionScope::Ordinary => None,
                                                    crate::ddl::TransactionScope::Streamed {
                                                        top_xid,
                                                        ..
                                                    } => Some(top_xid),
                                                };
                                            match crate::reload_signal::PendingSignal::from_tuple(
                                                rel, new, *xid, top_xid,
                                            ) {
                                                Ok(sig) => pending_signals.push(sig),
                                                Err(error) => tracing::warn!(
                                                    error = ?error,
                                                    "malformed public.walrus_reload_signal tuple; ignoring"
                                                ),
                                            }
                                        }
                                    }
                                    // The heartbeat round-trip: record it, mark the txn, and NEVER stage it
                                    // to S3 / a manifest row — it is control-plane, not user data. Other
                                    // internal tables' non-signal ops (a ddl_audit UPDATE, a reload_signal
                                    // UPDATE — neither should happen) are consumed-and-ignored here: only
                                    // the heartbeat's tuple carries a beat_seq.
                                    Message::Insert {
                                        relation_oid, new, ..
                                    }
                                    | Message::Update {
                                        relation_oid, new, ..
                                    } if internal.is_internal(*relation_oid) => {
                                        if internal.is_heartbeat(*relation_oid) {
                                            if let Some(seq) = internal.beat_seq_of(new) {
                                                heartbeat.observe_return(seq, Instant::now());
                                                tracing::info!(
                                                    beat_seq = seq,
                                                    "heartbeat round-trip observed"
                                                );
                                            }
                                            txn_has_heartbeat = true;
                                        }
                                    }
                                    // Non-insert ops on internal tables — e.g. operator-run
                                    // reload_signal pruning DELETEs — are consumed-and-ignored:
                                    // acked like any record, never routed toward a batcher.
                                    Message::Delete { relation_oid, .. }
                                        if internal.is_internal(*relation_oid) => {}
                                    Message::Commit {
                                        commit_lsn,
                                        end_lsn,
                                        commit_ts,
                                        ..
                                    } => {
                                        validate_commit_order(processed_through_lsn, *commit_lsn)?;
                                        checkpoint
                                            .observe_commit(*commit_lsn, *end_lsn)
                                            .context("record ordinary pgoutput commit boundary")?;
                                        // `take` both validates Begin/Commit sequencing and guarantees a
                                        // later malformed transaction cannot inherit this commit's xid.
                                        let (top_xid, begin_final_lsn) =
                                            ordinary_txn.take().context(
                                                "ordinary Commit arrived without a matching Begin",
                                            )?;
                                        anyhow::ensure!(
                                            begin_final_lsn == *commit_lsn,
                                            "ordinary Begin/Commit final LSN mismatch for xid {top_xid}: begin={begin_final_lsn}, commit={commit_lsn}"
                                        );
                                        let real_commit_ts =
                                            UtcTimestamp::from_pg_micros(*commit_ts)?;
                                        // Capture this before Commit promotes the open rows. A structural
                                        // DDL transaction force-seals exactly these transaction-local
                                        // segments so its files and schema history share one receipt.
                                        let has_routed_data = router.has_open_ordinary_data();
                                        // Preparing is read-only: a failed identity check or later control
                                        // publication leaves the provisional DDL intact for WAL replay.
                                        let prepared_ddl = ddl
                                            .prepare_ordinary_commit(*commit_lsn)
                                            .context("prepare ordinary DDL state")?;
                                        let has_structural_ddl = prepared_ddl.has_structural_ddl();
                                        let committed_ddl_bindings =
                                            ddl_relation_bindings(prepared_ddl.ddl_rows(), cache)?;
                                        let committed_reload_effects =
                                            pending_reload_events.ordinary_effect_count();
                                        if has_structural_ddl && committed_reload_effects != 0 {
                                            return Err(crate::ddl::DdlError::MixedOrdinaryReloadEffectsAndStructuralDdl {
                                                top_xid,
                                                commit_lsn: *commit_lsn,
                                                committed_reload_effects,
                                            }
                                            .into());
                                        }
                                        // Pre-DDL committed prefixes were flushed at the first structural
                                        // cut. Promote this transaction and, for the mixed DDL/data path,
                                        // force every touched segment into the atomic receipt even when the
                                        // normal size/cadence triggers stayed idle.
                                        let sealed = router.commit(
                                            *commit_lsn,
                                            real_commit_ts,
                                            has_structural_ddl && has_routed_data,
                                        )?;
                                        let committed_reload_events =
                                            pending_reload_events.on_commit(*commit_lsn);
                                        debug_assert_eq!(
                                            committed_reload_events.len(),
                                            committed_reload_effects
                                        );
                                        let persisted_reload_events = if has_structural_ddl {
                                            if has_routed_data {
                                                publish_ordinary_structural_commit(
                                                    OrdinaryStructuralCommit {
                                                        ddl,
                                                        prepared_ddl,
                                                        top_xid,
                                                        commit_lsn: *commit_lsn,
                                                        commit_ts: real_commit_ts,
                                                        sealed,
                                                        undurable_floor: router.undurable_floor(),
                                                        stream,
                                                        stager,
                                                        checkpoint,
                                                        pool,
                                                        epoch,
                                                    },
                                                )
                                                .await?;
                                                PersistedReloadEvents { events: Vec::new() }
                                            } else {
                                                anyhow::ensure!(
                                                    sealed.is_empty(),
                                                    "ordinary structural DDL-only transaction xid {top_xid} at {commit_lsn} unexpectedly sealed {} older batch(es)",
                                                    sealed.len()
                                                );
                                                let publication = stream_commit_publication(
                                                    epoch,
                                                    top_xid,
                                                    *commit_lsn,
                                                    real_commit_ts,
                                                    &[],
                                                    prepared_ddl.ddl_rows(),
                                                    prepared_ddl.registry_rows(),
                                                );
                                                let _outcome = publish_stream_commit_keepalive(
                                                    stream,
                                                    pool,
                                                    &publication,
                                                )
                                                .await
                                                .context(
                                                    "atomically publish ordinary structural DDL",
                                                )?;
                                                ddl.finalize_ordinary_commit(prepared_ddl);
                                                persist_committed_reload_events(
                                                    committed_reload_events,
                                                    sealed,
                                                    router,
                                                    stream,
                                                    stager,
                                                    checkpoint,
                                                    pool,
                                                )
                                                .await?
                                            }
                                        } else {
                                            let ddl_publication =
                                                await_with_received_feedback(
                                                    stream,
                                                    ddl.on_commit(
                                                        pool,
                                                        top_xid,
                                                        *commit_lsn,
                                                        real_commit_ts,
                                                        has_routed_data,
                                                        committed_reload_effects,
                                                    ),
                                                    "keepalive while ordinary DDL control publication is blocked",
                                                )
                                                .await;
                                            persist_after_ordinary_schema_publication(
                                                ddl_publication,
                                                persist_committed_reload_events(
                                                    committed_reload_events,
                                                    sealed,
                                                    router,
                                                    stream,
                                                    stager,
                                                    checkpoint,
                                                    pool,
                                                ),
                                            )
                                            .await?
                                        };
                                        // Start resolves after normal commit durability. End resolves only
                                        // after the helper force-flushed its target AND recorded H.
                                        let persisted_requests =
                                            persisted_reload_events.resolve_fences(fence_waiters);
                                        for request in persisted_requests {
                                            tracing::info!(
                                                event_id = %request.event.event_id,
                                                request_id = %request.event.request_id,
                                                commit_lsn = %request.commit_lsn,
                                                "source reload request durably persisted in control"
                                            );
                                        }
                                        router.commit_ddl_bindings(&committed_ddl_bindings);
                                        // Resolve any signal echoes this transaction carried: its commit
                                        // LSN IS the chunk watermark L_i. The signal txn needs no
                                        // special ack — confirmed_flush passes it like any consumed record.
                                        pending_signals.on_commit(*commit_lsn, waiters);
                                        // Then, for an idle heartbeat-only txn, mark its commit LSN durable;
                                        // the checkpoint translates that identity to its observed end LSN —
                                        // but never past un-durable user data (a floor the flush above just
                                        // cleared if it was eligible).
                                        if std::mem::take(&mut txn_has_heartbeat) {
                                            if let Some(floor) = router.undurable_floor() {
                                                tracing::warn!(
                                                    floor = %floor,
                                                    "heartbeat: un-durable buffered data precedes the beat; holding confirmed_flush"
                                                );
                                            } else {
                                                checkpoint.on_commit_durable(*commit_lsn)?;
                                                checkpoint
                                                    .send(stream, false)
                                                    .await
                                                    .context("send heartbeat standby status")?;
                                                tracing::info!(
                                                    confirmed_flush = %checkpoint.confirmed_flush(),
                                                    "idle heartbeat advanced confirmed_flush"
                                                );
                                            }
                                            health.set_degraded(
                                                heartbeat.is_degraded(Instant::now()),
                                            );
                                        }
                                        advance_processed_through(
                                            &mut processed_through_lsn,
                                            *commit_lsn,
                                            *end_lsn,
                                        )?;
                                    }
                                    // --- Large-transaction streaming (§1.6). A txn over
                                    // logical_decoding_work_mem arrives BEFORE its commit as interleaved
                                    // Stream blocks; the demux stages speculatively and commit-gates.
                                    Message::StreamStart { xid, first_segment } => {
                                        // Capture the last already-confirmed position before accepting a
                                        // first StreamStart. The StreamStart record itself is not a safe
                                        // feedback position: replay must see it again after a crash.
                                        let pre_start_ceiling =
                                            checkpoint.capture_pre_stream_start_ceiling();
                                        demux.on_stream_start(*xid, *first_segment, frame_lsn)?;
                                        if *first_segment {
                                            checkpoint.on_stream_start(*xid, pre_start_ceiling)?;
                                        }
                                    }
                                    Message::StreamStop => demux.on_stream_stop()?,
                                    m @ (Message::Insert { xid: Some(_), .. }
                                    | Message::Update { xid: Some(_), .. }
                                    | Message::Delete { xid: Some(_), .. }
                                    | Message::Truncate { xid: Some(_), .. }) => {
                                        last_activity = Instant::now();
                                        demux.on_change(cache, m, stager, frame_lsn).await?;
                                    }
                                    Message::StreamCommit {
                                        xid,
                                        commit_lsn,
                                        end_lsn,
                                        commit_ts,
                                        ..
                                    } => {
                                        validate_commit_order(processed_through_lsn, *commit_lsn)?;
                                        checkpoint
                                            .observe_commit(*commit_lsn, *end_lsn)
                                            .context("record streamed pgoutput commit boundary")?;
                                        // Reject an unknown xid or a missing StreamStop before the
                                        // ordering fence mutates any ordinary table batch.
                                        demux.validate_stream_commit(*xid)?;
                                        let prepared_ddl = ddl
                                            .prepare_stream_commit(*xid, *commit_lsn)
                                            .context("prepare streamed DDL state")?;
                                        let committed_reload_effects =
                                            pending_reload_events.stream_effect_count(*xid);
                                        if prepared_ddl.has_structural_ddl()
                                            && committed_reload_effects != 0
                                        {
                                            return Err(crate::ddl::DdlError::MixedStreamedReloadEffectsAndStructuralDdl {
                                                top_xid: *xid,
                                                commit_lsn: *commit_lsn,
                                                committed_reload_effects,
                                            }
                                            .into());
                                        }
                                        let committed_ddl_bindings =
                                            ddl_relation_bindings(prepared_ddl.ddl_rows(), cache)?;
                                        // Commit-order fence (architecture.md §1.6): any regular (non-streamed)
                                        // txn that committed WHILE this large txn was streaming is still buffered
                                        // in the router — small batches flush on the `max_fill` cadence, not per
                                        // commit — and its commit LSN is LOWER than this one. Flush those `ready`
                                        // rows FIRST so the manifest stays in commit-LSN order. Otherwise this
                                        // txn's file (higher `lsn_end`) becomes `ready` first, the transformer
                                        // transforms + advances `transformed_lsn` past it, and the late,
                                        // lower-LSN regular file is then permanently skipped by the `>= ` window.
                                        // (The slot stays clamped to this still-open txn's floor until its
                                        // durable StreamCommit below, so draining early never advances past it.)
                                        flush_sealed(
                                            router.seal_stream_commit_boundary()?,
                                            router.undurable_floor(),
                                            stream,
                                            stager,
                                            checkpoint,
                                            pool,
                                            epoch,
                                        )
                                        .await?;
                                        // Materialise the survivors (aborted sub-xids excluded), then
                                        // atomically publish their per-table file groups at
                                        // `lsn_end = commit_lsn`; after publication the checkpoint
                                        // translates that logical frontier to this commit's `end_lsn`.
                                        let real_commit_ts =
                                            UtcTimestamp::from_pg_micros(*commit_ts)?;
                                        let objs = demux
                                            .on_stream_commit(
                                                *xid,
                                                *commit_lsn,
                                                real_commit_ts,
                                                cache,
                                                stager,
                                            )
                                            .await?;
                                        let publication = stream_commit_publication(
                                            epoch,
                                            *xid,
                                            *commit_lsn,
                                            real_commit_ts,
                                            &objs,
                                            prepared_ddl.ddl_rows(),
                                            prepared_ddl.registry_rows(),
                                        );
                                        let outcome = publish_stream_commit_keepalive(
                                            stream,
                                            pool,
                                            &publication,
                                        )
                                        .await
                                        .context("atomically publish streamed transaction")?;
                                        // Both outcomes prove the complete control transaction is durable:
                                        // `AlreadyPublished` is the lost-source-ACK replay of that same receipt.
                                        ddl.finalize_stream_commit(prepared_ddl);
                                        router.commit_ddl_bindings(&committed_ddl_bindings);
                                        match outcome {
                                            control::PublishStreamOutcome::Published => {}
                                            control::PublishStreamOutcome::AlreadyPublished => {
                                                tracing::warn!(
                                                    xid,
                                                    commit_lsn = %commit_lsn,
                                                    duplicate_files = objs.len(),
                                                    "streamed transaction was already published; cleaning replay objects before ACK"
                                                );
                                                delete_replayed_stream_objects(stager, &objs).await;
                                            }
                                        }
                                        // Defensive path: a one-row internal event should not stream, but
                                        // if it does, target objects from this transaction are now manifested.
                                        // Persist its End marker before ACKing the StreamCommit; resolve only
                                        // after the ordinary checkpoint send below succeeds.
                                        let committed_reload_events = pending_reload_events
                                            .on_stream_commit(*xid, *commit_lsn);
                                        let persisted_reload_events =
                                            persist_committed_reload_events(
                                                committed_reload_events,
                                                Vec::new(),
                                                router,
                                                stream,
                                                stager,
                                                checkpoint,
                                                pool,
                                            )
                                            .await?;
                                        checkpoint.on_stream_end(*xid)?;
                                        checkpoint.on_commit_durable(*commit_lsn)?;
                                        checkpoint
                                            .send(stream, false)
                                            .await
                                            .context("send streamed-commit standby status")?;
                                        let persisted_requests =
                                            persisted_reload_events.resolve_fences(fence_waiters);
                                        for request in persisted_requests {
                                            tracing::info!(
                                                event_id = %request.event.event_id,
                                                request_id = %request.event.request_id,
                                                commit_lsn = %request.commit_lsn,
                                                "streamed source reload request durably persisted in control"
                                            );
                                        }
                                        // Can't-happen defense: a single-row signal txn never
                                        // streams, but if one somehow did, its surviving echo resolves here.
                                        pending_signals.on_stream_commit(
                                            *xid,
                                            *commit_lsn,
                                            waiters,
                                        );
                                        tracing::info!(
                                            xid,
                                            files = objs.len(),
                                            commit_lsn = %commit_lsn,
                                            confirmed_flush = %checkpoint.confirmed_flush(),
                                            "streamed txn committed → ready"
                                        );
                                        advance_processed_through(
                                            &mut processed_through_lsn,
                                            *commit_lsn,
                                            *end_lsn,
                                        )?;
                                    }
                                    Message::StreamAbort { top_xid, sub_xid } => {
                                        // sub == top → whole-txn drop; sub != top → exclude the rolled-back
                                        // savepoint's rows (proto §9b) while the top-level txn commits on.
                                        demux.on_stream_abort(*top_xid, *sub_xid, stager).await?;
                                        for (schema, table, version) in
                                            ddl.on_stream_abort(*top_xid, *sub_xid)
                                        {
                                            cache.remove_version(&schema, &table, version);
                                        }
                                        let floor_lifted = if top_xid == sub_xid {
                                            checkpoint.on_stream_end(*top_xid)?
                                        } else {
                                            false
                                        };
                                        if floor_lifted {
                                            checkpoint
                                                .send(stream, false)
                                                .await
                                                .context(
                                                    "send streamed-abort standby status after floor lift",
                                                )?;
                                        }
                                        // An aborted (sub)transaction's signal echo must never resolve a
                                        // waiter — the commit never carried it.
                                        pending_signals.on_stream_abort(*top_xid, *sub_xid);
                                        pending_reload_events.on_stream_abort(*top_xid, *sub_xid);
                                    }
                                    other => {
                                        // A user change is activity — it suppresses the idle beat.
                                        if matches!(
                                            other,
                                            Message::Insert { .. }
                                                | Message::Update { .. }
                                                | Message::Delete { .. }
                                                | Message::Truncate { .. }
                                        ) {
                                            last_activity = Instant::now();
                                        }
                                        flush_sealed(
                                            router.route(
                                                cache,
                                                other,
                                                frame_lsn,
                                                schema_version,
                                            )?,
                                            router.undurable_floor(),
                                            stream,
                                            stager,
                                            checkpoint,
                                            pool,
                                            epoch,
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            refresh_open_stream_guards(demux, &mut observed_open_stream_txns);
        }
    }
}

fn refresh_open_stream_guards<C: Clock + Clone>(
    demux: &crate::stream_txn::StreamDemux<C>,
    prior_count: &mut usize,
) {
    let stats = demux.open_stats();
    common::metrics::set_open_stream_txns(
        stats.count,
        stats.oldest_age.map_or(0.0, |age| age.as_secs_f64()),
    );
    if stats.count != *prior_count {
        tracing::info!(
            open_stream_txns = stats.count,
            oldest_open_first_lsn = ?stats.oldest_floor,
            oldest_open_age_seconds = stats.oldest_age.map(|age| age.as_secs_f64()),
            "open protocol-v2 transaction guard changed"
        );
        *prior_count = stats.count;
    }
}

impl<'a, C> DecodeLoopBuilder<'a, C> {
    /// The started replication stream the loop pulls CopyBoth frames from. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn stream(mut self, stream: &'a mut ReplicationStream) -> Self {
        self.stream = Some(stream);
        self
    }

    /// The shutdown token. Cancelling it makes the loop return after the current frame rather than
    /// mid-message, which is what lets the drain publish what is already committed. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub fn token(mut self, token: CancellationToken) -> Self {
        self.token = Some(token);
        self
    }

    /// The relation cache the loop resolves each message's OID against, and updates when a Relation
    /// message announces a new shape. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn cache(mut self, cache: &'a mut RelationCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// The per-table batcher set that decoded rows are routed into. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn router(mut self, router: &'a mut BatchRouter<C>) -> Self {
        self.router = Some(router);
        self
    }

    /// Where sealed batches are written as Parquet. Shared (`&`) rather than exclusive: the object
    /// store handles concurrent PUTs itself. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn stager(mut self, stager: &'a crate::staging::ParquetStager) -> Self {
        self.stager = Some(stager);
        self
    }

    /// The durability checkpoint that decides what LSN may be confirmed back to the source. Only it
    /// may advance `confirmed_flush`, and only after a batch is durable. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn checkpoint(
        mut self,
        checkpoint: &'a mut crate::checkpoint::DurabilityCheckpoint,
    ) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// The streamed-transaction demultiplexer, which buffers in-progress transactions separately so
    /// an uncommitted one is never published. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn demux(mut self, demux: &'a mut crate::stream_txn::StreamDemux<C>) -> Self {
        self.demux = Some(demux);
        self
    }

    /// The DDL consumer that turns `ddl_audit` rows into schema-version bumps. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn ddl(mut self, ddl: &'a mut crate::ddl::DdlConsumer) -> Self {
        self.ddl = Some(ddl);
        self
    }

    /// The heartbeat, which both fires beats while the source is idle and observes their return.
    /// Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn heartbeat(mut self, heartbeat: &'a mut Heartbeat) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// The probe state the loop marks degraded or live. Shared, since the health server reads it
    /// concurrently. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn health(mut self, health: &'a HealthState) -> Self {
        self.health = Some(health);
        self
    }

    /// Control-Postgres handle for manifest and registry writes. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn pool(mut self, pool: &'a sqlx::PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// The generation every row and manifest row this loop produces is stamped with. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn epoch(mut self, epoch: EpochNo) -> Self {
        self.epoch = Some(epoch);
        self
    }

    /// The watermark waiters a decoded reload signal resolves, which is how a chunk export learns
    /// its commit LSN. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn waiters(mut self, waiters: &'a crate::reload_signal::WatermarkWaiters) -> Self {
        self.waiters = Some(waiters);
        self
    }

    /// The start/end-fence waiters resolved from committed `public.walrus_reload_event` rows.  End waiters
    /// are resolved only after this loop has durably flushed their target table. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn fence_waiters(
        mut self,
        fence_waiters: &'a crate::reload_event::FenceWaiters,
    ) -> Self {
        self.fence_waiters = Some(fence_waiters);
        self
    }

    /// Construct the fully wired loop, naming the first missing required field.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeLoopError`] naming the first setter omitted from the builder chain.
    pub fn build(self) -> Result<DecodeLoop<'a, C>, DecodeLoopError> {
        Ok(DecodeLoop {
            stream: self.stream.ok_or(DecodeLoopError("stream"))?,
            token: self.token.ok_or(DecodeLoopError("token"))?,
            cache: self.cache.ok_or(DecodeLoopError("cache"))?,
            router: self.router.ok_or(DecodeLoopError("router"))?,
            stager: self.stager.ok_or(DecodeLoopError("stager"))?,
            checkpoint: self.checkpoint.ok_or(DecodeLoopError("checkpoint"))?,
            demux: self.demux.ok_or(DecodeLoopError("demux"))?,
            ddl: self.ddl.ok_or(DecodeLoopError("ddl"))?,
            heartbeat: self.heartbeat.ok_or(DecodeLoopError("heartbeat"))?,
            health: self.health.ok_or(DecodeLoopError("health"))?,
            pool: self.pool.ok_or(DecodeLoopError("pool"))?,
            epoch: self.epoch.ok_or(DecodeLoopError("epoch"))?,
            waiters: self.waiters.ok_or(DecodeLoopError("waiters"))?,
            fence_waiters: self.fence_waiters.ok_or(DecodeLoopError("fence_waiters"))?,
        })
    }
}

/// Build the one control-plane publication for a protocol-v2 commit. Keeping the conversion at this
/// boundary makes it impossible to publish only a prefix of the materialised objects.
#[must_use]
fn stream_commit_publication(
    epoch: EpochNo,
    top_xid: u32,
    commit_lsn: Lsn,
    commit_ts: UtcTimestamp,
    objects: &[crate::staging::WrittenObject],
    ddl_rows: &[control::DdlRow],
    registry_rows: &[control::RegistryRow],
) -> control::NewStreamCommitPublication {
    control::NewStreamCommitPublication {
        epoch,
        top_xid,
        commit_lsn,
        commit_ts,
        ddl_rows: ddl_rows.to_vec(),
        registry_rows: registry_rows.to_vec(),
        files: objects
            .iter()
            .map(|object| crate::manifest::to_ready_row(epoch, object, None))
            .collect(),
    }
}

/// Extract and validate the global relation bindings made safe by one committed DDL receipt.
/// COMMENT events do not create schema versions, and a dropped/unknown identity has no surviving
/// OID to route. Multiple DDLs for one relation in a transaction promote only its final version.
fn ddl_relation_bindings(
    rows: &[control::DdlRow],
    cache: &RelationCache,
) -> anyhow::Result<Vec<(u32, common::SchemaVersionNo)>> {
    let mut final_versions = HashMap::new();
    for row in rows
        .iter()
        .filter(|row| !row.c_tag.eq_ignore_ascii_case("COMMENT"))
    {
        let oid = if let Some(oid) = row.c_rel_oid {
            oid
        } else {
            // Older source triggers omitted c_rel_oid. Preserve that supported replay path by
            // resolving the exact frozen table/version, never by taking a global latest entry.
            let mut candidates = cache
                .iter()
                .filter(|cached| {
                    cached.relation.schema == row.source_schema
                        && cached.relation.name == row.source_table
                        && cached.schema_version == row.schema_version
                })
                .map(|cached| cached.relation.oid)
                .collect::<Vec<_>>();
            candidates.sort_unstable();
            candidates.dedup();
            let [oid] = candidates.as_slice() else {
                anyhow::bail!(
                    "committed DDL without relation OID has {} exact cached identities for {}.{} version {}",
                    candidates.len(),
                    row.source_schema,
                    row.source_table,
                    row.schema_version
                );
            };
            *oid
        };
        final_versions
            .entry(oid)
            .and_modify(|version: &mut common::SchemaVersionNo| {
                *version = (*version).max(row.schema_version);
            })
            .or_insert(row.schema_version);
    }
    let mut bindings = final_versions.into_iter().collect::<Vec<_>>();
    bindings.sort_unstable_by_key(|&(oid, _)| oid);
    for &(oid, version) in &bindings {
        anyhow::ensure!(
            cache.get(oid, version).is_some(),
            "committed DDL relation version is not cached: oid={oid} version={version}"
        );
    }
    Ok(bindings)
}

/// Reject a source commit-order regression before any object/control side effect for that commit.
fn validate_commit_order(current: Lsn, incoming: Lsn) -> anyhow::Result<()> {
    anyhow::ensure!(
        incoming >= current,
        "source commit LSN regressed behind decode frontier: incoming={incoming}, frontier={current}"
    );
    Ok(())
}

/// Last position eligible for an unscoped Relation's durable schema-history lookup. The processed
/// cursor normally points just after the previous commit record, so subtracting one retains that
/// commit. A legacy cursor parked exactly at the replayed commit instead excludes its already-written
/// future history. Ordinary transactions additionally expose their own final/commit LSN up front.
#[must_use]
fn relation_history_cutoff(processed_through: Lsn, ordinary_final_lsn: Option<Lsn>) -> Lsn {
    let prior_processed = Lsn::new(processed_through.as_u64().saturating_sub(1));
    ordinary_final_lsn.map_or(prior_processed, |final_lsn| {
        prior_processed.min(Lsn::new(final_lsn.as_u64().saturating_sub(1)))
    })
}

/// Move the relation-history cursor to the first position after a complete source commit, and only
/// after all handling for that commit succeeded. Retaining `end_lsn` lets the next history lookup use
/// `cursor - 1`: prior committed DDL remains visible, while a legacy replay starting exactly at its
/// own `commit_lsn` cannot see its future control row.
fn advance_processed_through(
    current: &mut Lsn,
    commit_lsn: Lsn,
    end_lsn: Lsn,
) -> anyhow::Result<()> {
    validate_commit_order(*current, commit_lsn)?;
    anyhow::ensure!(
        end_lsn > commit_lsn,
        "source commit end LSN did not follow its commit LSN: commit={commit_lsn}, end={end_lsn}"
    );
    *current = end_lsn;
    Ok(())
}

/// Durably publish one ordinary transaction whose rows straddle structural DDL.
///
/// Every supplied batch must contain only this transaction. The caller established that invariant
/// by flushing all older committed prefixes at the first structural DDL cut and force-sealing each
/// batcher touched by the current transaction at Commit. Objects are materialised first, then their
/// files, DDL history, registry rows, and schema barriers become visible in one control transaction.
/// Source durability advances only after that transaction succeeds (or an exact receipt replay is
/// proven); replay objects are deleted only after control has verified that none of their fresh URIs
/// is referenced by live manifest work.
struct OrdinaryStructuralCommit<'a> {
    ddl: &'a mut crate::ddl::DdlConsumer,
    prepared_ddl: crate::ddl::PreparedOrdinaryDdl,
    top_xid: u32,
    commit_lsn: Lsn,
    commit_ts: UtcTimestamp,
    sealed: Vec<SealedBatch>,
    undurable_floor: Option<Lsn>,
    stream: &'a mut ReplicationStream,
    stager: &'a crate::staging::ParquetStager,
    checkpoint: &'a mut crate::checkpoint::DurabilityCheckpoint,
    pool: &'a sqlx::PgPool,
    epoch: EpochNo,
}

async fn publish_ordinary_structural_commit(
    commit: OrdinaryStructuralCommit<'_>,
) -> anyhow::Result<()> {
    let OrdinaryStructuralCommit {
        ddl,
        prepared_ddl,
        top_xid,
        commit_lsn,
        commit_ts,
        sealed,
        undurable_floor,
        stream,
        stager,
        checkpoint,
        pool,
        epoch,
    } = commit;
    anyhow::ensure!(
        !sealed.is_empty(),
        "ordinary structural transaction xid {top_xid} at {commit_lsn} routed data but sealed no batches"
    );
    for batch in &sealed {
        anyhow::ensure!(
            batch.lsn_start == commit_lsn && batch.lsn_end == commit_lsn,
            "ordinary structural transaction xid {top_xid} at {commit_lsn} retained non-local batch {}.{} range {}..={}",
            batch.schema,
            batch.table,
            batch.lsn_start,
            batch.lsn_end
        );
    }

    let objects = materialize_sealed_keepalive(sealed, stream, stager).await?;
    let publication = stream_commit_publication(
        epoch,
        top_xid,
        commit_lsn,
        commit_ts,
        &objects,
        prepared_ddl.ddl_rows(),
        prepared_ddl.registry_rows(),
    );
    let outcome = publish_stream_commit_keepalive(stream, pool, &publication)
        .await
        .context("atomically publish ordinary structural transaction")?;

    // Both outcomes prove the complete control transaction is durable. A changed replay or a replay
    // URI still referenced by live work fails inside control and never reaches either operation.
    ddl.finalize_ordinary_commit(prepared_ddl);
    match outcome {
        control::PublishStreamOutcome::Published => {}
        control::PublishStreamOutcome::AlreadyPublished => {
            tracing::warn!(
                top_xid,
                commit_lsn = %commit_lsn,
                duplicate_files = objects.len(),
                "ordinary structural transaction was already published; cleaning replay objects before ACK"
            );
            delete_replayed_stream_objects(stager, &objects).await;
        }
    }

    advance_durable_frontier(Some(commit_lsn), undurable_floor, stream, checkpoint).await
}

/// PUT all batches for an atomic commit receipt while pumping received-only feedback. No manifest
/// child is visible until the caller has materialised the complete vector and publishes it once.
async fn materialize_sealed_keepalive(
    sealed: Vec<SealedBatch>,
    stream: &mut ReplicationStream,
    stager: &crate::staging::ParquetStager,
) -> anyhow::Result<Vec<crate::staging::WrittenObject>> {
    let mut objects = Vec::with_capacity(sealed.len());
    for batch in sealed {
        objects.push(put_batch_keepalive(stream, stager, batch).await?);
    }
    Ok(objects)
}

/// Await an atomic control publication while pumping received-only feedback. Publication can block
/// behind the table advisory locks shared with reload cutover; keeping the walsender alive here is
/// as important as it is during object upload, while the old durable flush/apply positions remain
/// untouched until the caller handles the successful outcome.
async fn publish_stream_commit_keepalive(
    stream: &mut ReplicationStream,
    pool: &sqlx::PgPool,
    publication: &control::NewStreamCommitPublication,
) -> anyhow::Result<control::PublishStreamOutcome> {
    await_with_received_feedback(
        stream,
        control::publish_stream_commit(pool, publication),
        "keepalive while atomic control publication is blocked",
    )
    .await
}

/// Await a non-replication operation while periodically reporting the receive position on the
/// replication connection. The operation must not touch `stream`; its durable result remains the
/// caller's gate for advancing flush/apply feedback.
async fn await_with_received_feedback<T, E>(
    stream: &mut ReplicationStream,
    operation: impl std::future::Future<Output = Result<T, E>>,
    keepalive_context: &'static str,
) -> anyhow::Result<T>
where
    E: Into<anyhow::Error>,
{
    tokio::pin!(operation);
    loop {
        let budget = stream.feedback_budget();
        tokio::select! {
            biased;
            outcome = &mut operation => return outcome.map_err(Into::into),
            _ = sleep(budget) => stream
                .send_received_feedback(false)
                .await
                .context(keepalive_context)?,
        }
    }
}

/// A receipt from a prior attempt already owns its original object keys. This replay generated new
/// UUID keys, so none of these objects are referenced and every one can be deleted before the ACK.
/// Cleanup is deliberately best-effort: the durable receipt is authoritative and an object-store
/// delete failure leaves garbage, not missing data.
async fn delete_replayed_stream_objects(
    stager: &crate::staging::ParquetStager,
    objects: &[crate::staging::WrittenObject],
) {
    for object in objects {
        if let Err(error) = stager.delete(&object.key).await {
            tracing::warn!(
                s3_uri = %object.s3_uri,
                error = ?error,
                "failed to delete duplicate object from replayed StreamCommit"
            );
        }
    }
}

/// A committed reload seal is the durable owner of every source change through its H. An ordinary
/// replay covered by that seal wrote a new random object before consulting control Postgres, so the
/// object is unreferenced and can be removed. Cleanup failure leaves garbage, not missing data, and
/// must not turn a safely covered source commit back into an ACK-blocking error.
async fn delete_seal_covered_object(
    stager: &crate::staging::ParquetStager,
    object: &crate::staging::WrittenObject,
    sealed_through: Lsn,
) {
    if let Err(error) = stager.delete(&object.key).await {
        tracing::warn!(
            s3_uri = %object.s3_uri,
            lsn_end = %object.lsn_end,
            %sealed_through,
            error = ?error,
            "failed to delete ordinary replay object covered by reload seal"
        );
    }
}

/// PUT every sealed batch and durably publish its manifest disposition before advancing the
/// durability checkpoint. A lost-ACK replay already covered by a reload seal has the same durable
/// frontier without recreating queue work.
/// Several files can share one transaction's commit LSN (multiple tables, or the pre/post-DDL schema
/// segments of one table); acknowledging after only the first sibling would make a crash lose the
/// rest. An older committed-but-unsealed batch is an additional fence: its commit has not reached S3,
/// so the slot cannot advance through its LSN yet.
async fn flush_sealed(
    sealed: Vec<SealedBatch>,
    undurable_floor: Option<Lsn>,
    stream: &mut ReplicationStream,
    stager: &crate::staging::ParquetStager,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> anyhow::Result<()> {
    let max_durable = persist_sealed(sealed, stream, stager, pool, epoch).await?;
    advance_durable_frontier(max_durable, undurable_floor, stream, checkpoint).await
}

/// Complete durability steps (a) object PUT and (b) manifest commit, but deliberately do not ACK
/// the source yet.  End-fence handling uses this seam to persist its data-free control marker after
/// every target file is durable and before an ACK could make the source event non-replayable.
async fn persist_sealed(
    sealed: Vec<SealedBatch>,
    stream: &mut ReplicationStream,
    stager: &crate::staging::ParquetStager,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> anyhow::Result<Option<Lsn>> {
    let mut max_durable = None;
    for batch in sealed {
        // Durability steps (a) PUT then (b) commit the manifest row — pumping unconditional keepalive
        // throughout so a slow or stalled S3 flush can't starve the walsender past `wal_sender_timeout`
        // (§1.9). The flush touches the object store + control DB, never the replication socket.
        let written = flush_batch_keepalive(stream, stager, pool, epoch, batch).await?;
        max_durable =
            Some(max_durable.map_or(written.lsn_end, |lsn: Lsn| lsn.max(written.lsn_end)));
        tracing::info!(
            uri = %written.s3_uri,
            lsn_end = %written.lsn_end,
            "durable: ordinary batch published against manifest/reload fence"
        );
    }
    Ok(max_durable)
}

/// Confirm ordinary DDL state before polling any downstream file/control durability work.
///
/// The caller awaits DDL persistence through [`await_with_received_feedback`] first, then passes its
/// completed result here. A failed or ambiguous publication therefore leaves every sealed batch and
/// the source ACK path untouched so replay can retry the exact source transaction.
async fn persist_after_ordinary_schema_publication<F, T>(
    publication: anyhow::Result<()>,
    persistence: F,
) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    publication.context("commit ordinary DDL state")?;
    persistence.await
}

/// Apply the existing committed-but-unsealed floor and send feedback for a newly durable file
/// group.  Kept separate from [`persist_sealed`] so control markers can be crash-durable before an
/// event commit becomes ACK-eligible without changing the normal floor calculation.
async fn advance_durable_frontier(
    max_durable: Option<Lsn>,
    undurable_floor: Option<Lsn>,
    stream: &mut ReplicationStream,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
) -> anyhow::Result<()> {
    let Some(frontier) = record_durable_frontier(max_durable, undurable_floor, checkpoint)? else {
        if let (Some(durable), Some(floor)) = (max_durable, undurable_floor)
            && floor <= durable
        {
            tracing::info!(
                durable_through = %durable,
                undurable_floor = %floor,
                confirmed_flush = %checkpoint.confirmed_flush(),
                "slot held behind an older committed-but-unsealed batch"
            );
        }
        return Ok(());
    };
    checkpoint
        .send(stream, false)
        .await
        .context("send durability standby status")?;
    tracing::info!(
        durable_through = %frontier,
        confirmed_flush = %checkpoint.confirmed_flush(),
        "durable file group complete; slot advanced"
    );
    Ok(())
}

/// Committed reload events whose end-fence target files and end markers are durable.  Keeping this
/// as a distinct state prevents the decode loop from resolving a fence directly from tuple decode
/// or transaction commit: the only constructor is [`persist_committed_reload_events`].
#[derive(Debug)]
struct PersistedReloadEvents {
    events: Vec<crate::reload_event::CommittedReloadEvent>,
}

impl PersistedReloadEvents {
    /// Resolve committed start/end waiters and return already-persisted request events for logging.
    /// End events can reach this method only after their target flush and marker commit succeeded.
    fn resolve_fences(
        self,
        waiters: &crate::reload_event::FenceWaiters,
    ) -> Vec<crate::reload_event::CommittedReloadEvent> {
        let mut requests = Vec::new();
        for committed in self.events {
            let Some(phase) = committed.event.fence_phase() else {
                requests.push(committed);
                continue;
            };
            let Some(reload_id) = committed.event.reload_id else {
                tracing::error!(
                    event_id = %committed.event.event_id,
                    ?phase,
                    "committed reload fence has no reload_id; not resolving"
                );
                continue;
            };
            waiters.resolve(
                reload_id,
                phase,
                crate::reload_event::FenceEcho {
                    commit_lsn: committed.commit_lsn,
                    embedded_lsn: committed.event.embedded_lsn,
                },
            );
        }
        requests
    }
}

/// Force-seal every end fence's target, persist all supplied batches, and commit every data-free
/// end marker before allowing the ordinary durability checkpoint to advance.
///
/// Start fences use the same path without a force-seal or marker; callers resolve both phases only
/// from the returned [`PersistedReloadEvents`].  The router's post-seal undurable floor is passed to
/// the unchanged frontier calculation, so an unrelated table can still hold the slot behind its
/// older committed rows even though this target's end fence is safe to publish.
async fn persist_committed_reload_events<C: Clock + Clone>(
    events: Vec<crate::reload_event::CommittedReloadEvent>,
    mut sealed: Vec<SealedBatch>,
    router: &mut BatchRouter<C>,
    stream: &mut ReplicationStream,
    stager: &crate::staging::ParquetStager,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
    pool: &sqlx::PgPool,
) -> anyhow::Result<PersistedReloadEvents> {
    let epoch = router.epoch;
    for committed in &events {
        if committed.event.kind != crate::reload_event::ReloadEventKind::EndFence {
            continue;
        }
        let (schema, table) = committed.event.target().with_context(|| {
            format!(
                "end-fence event {} has no concrete target",
                committed.event.event_id
            )
        })?;
        sealed.extend(
            router
                .force_flush_table(schema, table)
                .with_context(|| format!("force-flush end fence for {schema}.{table}"))?,
        );
    }

    let undurable_floor = router.undurable_floor();
    let mut max_durable = persist_sealed(sealed, stream, stager, pool, epoch).await?;

    // Persist every control-plane consequence before ACK. A crash can therefore replay the source
    // event idempotently, while a successful ACK can never strand a request/fence only in memory.
    for committed in &events {
        match committed.event.kind {
            crate::reload_event::ReloadEventKind::Request => {
                let request_id = committed.event.request_id;
                match committed.event.scope {
                    crate::reload_event::ReloadScope::Table => {
                        let (schema, table) = committed.event.target().with_context(|| {
                            format!(
                                "table reload request event {} has no target",
                                committed.event.event_id
                            )
                        })?;
                        control::reload::request_from_source(
                            pool,
                            &control::SourceReloadRequest {
                                epoch,
                                source_request_id: request_id,
                                parent_request_id: None,
                                scope: control::ReloadScope::Table,
                                source_schema: schema,
                                source_table: table,
                                flavor: control::ReloadFlavor::Reload,
                            },
                        )
                        .await
                        .context("persist table reload request decoded from source WAL")?;
                    }
                    crate::reload_event::ReloadScope::AllPublished => {
                        // This inventory was frozen into the source WAL row. A replay after a
                        // publication/DDL change must fan out exactly the same children.
                        let targets = dedupe_reload_targets(&committed.event.targets);
                        for (schema, table) in targets {
                            control::reload::request_from_source(
                                pool,
                                &control::SourceReloadRequest {
                                    epoch,
                                    source_request_id: request_id,
                                    parent_request_id: Some(request_id),
                                    scope: control::ReloadScope::AllPublished,
                                    source_schema: schema,
                                    source_table: table,
                                    flavor: control::ReloadFlavor::Reload,
                                },
                            )
                            .await
                            .with_context(|| {
                                format!("persist all-published reload child for {schema}.{table}")
                            })?;
                        }
                    }
                }
            }
            crate::reload_event::ReloadEventKind::StartFence => {
                let reload_id = committed.event.reload_id.with_context(|| {
                    format!(
                        "start-fence event {} has no reload attempt",
                        committed.event.event_id
                    )
                })?;
                let schema_version = committed.event.schema_version.with_context(|| {
                    format!(
                        "start-fence event {} has no schema version",
                        committed.event.event_id
                    )
                })?;
                let (source_schema, source_table) =
                    committed.event.target().with_context(|| {
                        format!(
                            "start-fence event {} has no concrete target",
                            committed.event.event_id
                        )
                    })?;
                let recorded = control::reload::record_start_fence(
                    pool,
                    reload_id,
                    committed.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(committed.event.request_id),
                        source_schema,
                        source_table,
                        schema_version,
                    },
                )
                .await;
                accept_failed_attempt_fence(
                    pool,
                    committed,
                    reload_id,
                    crate::reload_event::FencePhase::Start,
                    recorded,
                )
                .await
                .context("record durable reload start fence")?;
            }
            crate::reload_event::ReloadEventKind::EndFence => {
                let reload_id = committed.event.reload_id.with_context(|| {
                    format!(
                        "end-fence event {} has no reload attempt",
                        committed.event.event_id
                    )
                })?;
                let schema_version = committed.event.schema_version.with_context(|| {
                    format!(
                        "end-fence event {} has no schema version",
                        committed.event.event_id
                    )
                })?;
                let (source_schema, source_table) =
                    committed.event.target().with_context(|| {
                        format!(
                            "end-fence event {} has no concrete target",
                            committed.event.event_id
                        )
                    })?;
                let recorded = control::reload::record_end_marker(
                    pool,
                    reload_id,
                    committed.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(committed.event.request_id),
                        source_schema,
                        source_table,
                        schema_version,
                    },
                )
                .await;
                accept_failed_attempt_fence(
                    pool,
                    committed,
                    reload_id,
                    crate::reload_event::FencePhase::End,
                    recorded,
                )
                .await
                .context("record durable reload end marker")?;
            }
        }
    }

    // The event row itself carries no user-data batch. Once every consequence above is durable in
    // control Postgres, its commit is nevertheless a durable point just like a manifested file.
    // Including it lets an empty-table start/end request advance confirmed_flush without waiting
    // for an unrelated future heartbeat; the ordinary undurable floor below still holds ACK behind
    // any older committed user rows on another table.
    for committed in &events {
        max_durable = Some(max_durable.map_or(committed.commit_lsn, |durable| {
            durable.max(committed.commit_lsn)
        }));
    }

    advance_durable_frontier(max_durable, undurable_floor, stream, checkpoint).await?;
    Ok(PersistedReloadEvents { events })
}

/// A fence can be irrevocably present in source WAL while DDL supersedes its attempt in control.
/// Treat exactly that durable terminal race as a stale no-op; every other transition failure stays
/// loud so a missing row, wrong target, or illegal live-state transition cannot be ACKed away.
async fn accept_failed_attempt_fence(
    pool: &sqlx::PgPool,
    committed: &crate::reload_event::CommittedReloadEvent,
    reload_id: common::ReloadId,
    phase: crate::reload_event::FencePhase,
    result: Result<(), control::ControlError>,
) -> anyhow::Result<()> {
    let Err(error) = result else {
        return Ok(());
    };
    if !matches!(&error, control::ControlError::ReloadTransition { .. }) {
        return Err(error.into());
    }

    let row = control::reload::get(pool, reload_id)
        .await
        .context("classify rejected reload fence transition")?;
    let Some(row) = row else {
        return Err(anyhow::Error::new(error).context(format!(
            "reload fence {phase:?} references missing attempt {reload_id}"
        )));
    };
    let target = committed.event.target();
    if !is_matching_failed_fence(
        FailedReloadAttempt {
            status: row.status,
            target: (&row.source_schema, &row.source_table),
            request_id: row.source_request_id.or(row.parent_request_id),
            schema_version: row.schema_version,
        },
        DecodedFenceIdentity {
            target,
            request_id: committed.event.request_id,
            schema_version: committed.event.schema_version,
        },
    ) {
        return Err(anyhow::Error::new(error).context(format!(
            "reload fence {phase:?} rejected for attempt {reload_id}: status={:?}, event_target={target:?}, row_target={}.{}",
            row.status, row.source_schema, row.source_table
        )));
    }

    tracing::info!(
        %reload_id,
        ?phase,
        event_id = %committed.event.event_id,
        source_table = %format_args!("{}.{}", row.source_schema, row.source_table),
        "ignoring stale fence for DDL-superseded failed reload"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct FailedReloadAttempt<'a> {
    status: control::ReloadStatus,
    target: (&'a str, &'a str),
    request_id: Option<Uuid>,
    schema_version: Option<common::SchemaVersionNo>,
}

#[derive(Debug, Clone, Copy)]
struct DecodedFenceIdentity<'a> {
    target: Option<(&'a str, &'a str)>,
    request_id: Uuid,
    schema_version: Option<common::SchemaVersionNo>,
}

#[must_use]
fn is_matching_failed_fence(
    attempt: FailedReloadAttempt<'_>,
    fence: DecodedFenceIdentity<'_>,
) -> bool {
    attempt.status == control::ReloadStatus::Failed
        && fence.target == Some(attempt.target)
        && attempt
            .request_id
            .is_none_or(|request_id| request_id == fence.request_id)
        && attempt
            .schema_version
            .is_none_or(|version| fence.schema_version == Some(version))
}

#[must_use]
fn dedupe_reload_targets(
    targets: &[crate::reload_event::ReloadTarget],
) -> std::collections::BTreeSet<(&str, &str)> {
    targets
        .iter()
        .map(|target| (target.schema.as_str(), target.table.as_str()))
        .collect()
}

/// Highest newly-durable LSN that is strictly before any committed-but-unsealed row.
fn durable_frontier(max_durable: Option<Lsn>, undurable_floor: Option<Lsn>) -> Option<Lsn> {
    match (max_durable, undurable_floor) {
        (Some(durable), Some(floor)) if floor <= durable => None,
        (durable, _) => durable,
    }
}

/// Apply a newly durable file/control frontier to the checkpoint. A schema-barrier publication is
/// deliberately absent from both inputs, so a DDL-only commit cannot advance slot feedback by
/// itself.
fn record_durable_frontier(
    max_durable: Option<Lsn>,
    undurable_floor: Option<Lsn>,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
) -> anyhow::Result<Option<Lsn>> {
    let Some(frontier) = durable_frontier(max_durable, undurable_floor) else {
        return Ok(None);
    };
    // Step (c): all files/control markers at/below this frontier are durably represented.
    checkpoint.on_commit_durable(frontier)?;
    Ok(Some(frontier))
}

/// Await the durable flush ([`flush_batch`]: S3 PUT + manifest commit) while pumping **unconditional**
/// keepalive feedback on the stream every feedback interval — so a slow or stalled S3 PUT can't starve
/// the walsender past `wal_sender_timeout` (§1.9). The flush future touches the object store and control
/// DB, never the replication socket, so the keepalive rides concurrently; and `tokio::select!` runs the
/// *chosen* branch's body to completion (the flush future is parked, never dropped, while a keepalive
/// sends), so a feedback frame is never cancelled mid-write. `confirmed_flush` is untouched here — it
/// advances only after this returns (the caller's `on_commit_durable`), so the pumped feedback
/// carries the pre-flush durable baseline (received advances as `write`, `flush`/`apply` hold — the
/// two-LSN rule).
async fn flush_batch_keepalive(
    stream: &mut ReplicationStream,
    stager: &crate::staging::ParquetStager,
    ex: &sqlx::PgPool,
    epoch: EpochNo,
    batch: SealedBatch,
) -> anyhow::Result<crate::staging::WrittenObject> {
    await_with_received_feedback(
        stream,
        flush_batch(stager, ex, epoch, batch),
        "keepalive during a stalled flush",
    )
    .await
}

/// Await only the durable object PUT while pumping received-only keepalives. Unlike
/// [`flush_batch_keepalive`], this deliberately creates no individual manifest row: the caller owns
/// all objects until it can publish their one atomic commit receipt.
async fn put_batch_keepalive(
    stream: &mut ReplicationStream,
    stager: &crate::staging::ParquetStager,
    batch: SealedBatch,
) -> anyhow::Result<crate::staging::WrittenObject> {
    await_with_received_feedback(
        stream,
        stager.put(batch),
        "keepalive during a stalled atomic object PUT",
    )
    .await
}

/// Flush a sealed batch durably: **(a)** PUT the Parquet object to S3, **then (b)** either commit a
/// `file_manifest` `ready` row or atomically prove that a reload seal covers its commit — never the
/// other way round (§1.5). Step (c) — translating `obj.lsn_end` (the commit-order LSN) to the
/// matching pgoutput end LSN and advancing the slot — happens in the checkpoint caller. A crash
/// between (a) and (b) is safe: the batch re-streams because neither manifest nor
/// seal disposition was acknowledged.
///
/// ## Cancel safety
///
/// **Not cancel-safe as a composite durability operation.** The object PUT may finish before the
/// manifest insert, so dropping this future can leave an unreferenced object. The keepalive path
/// pins this future and polls it by mutable reference; other callers await it directly. A retry is
/// safe because a manifest is never recorded before its object is durable.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if Parquet encoding/S3 durability or the subsequent table-fenced
/// control publication fails. A best-effort failure to delete a seal-covered replay object is only
/// logged because it leaves garbage, not missing data.
pub async fn flush_batch<'a>(
    stager: &crate::staging::ParquetStager,
    acquire: impl sqlx::Acquire<'a, Database = sqlx::Postgres>,
    epoch: EpochNo,
    batch: crate::batch::SealedBatch,
) -> anyhow::Result<crate::staging::WrittenObject> {
    flush_batch_kind(
        stager,
        acquire,
        epoch,
        batch,
        crate::staging::FileKind::Stream,
    )
    .await
}

fn validate_ordinary_flush_kind(kind: crate::staging::FileKind) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(
            kind,
            crate::staging::FileKind::Stream | crate::staging::FileKind::Snapshot
        ),
        "ordinary batch publisher does not accept {} objects",
        kind.as_str()
    );
    Ok(())
}

/// Flush a sealed ordinary batch with an explicit Stream or legacy Snapshot provenance. Reload
/// objects require a reload attempt identity and streamed-transaction spills require an atomic
/// group receipt, so both are rejected before any object-store write.
///
/// # Errors
///
/// Returns [`anyhow::Error`] before PUT for a non-ordinary kind, or if object/control durability
/// fails.
pub async fn flush_batch_kind<'a>(
    stager: &crate::staging::ParquetStager,
    acquire: impl sqlx::Acquire<'a, Database = sqlx::Postgres>,
    epoch: EpochNo,
    batch: crate::batch::SealedBatch,
    kind: crate::staging::FileKind,
) -> anyhow::Result<crate::staging::WrittenObject> {
    validate_ordinary_flush_kind(kind)?;
    // (a) durable in S3.
    let obj = stager
        .put_with_kind(batch, kind)
        .await
        .context("PUT parquet object to S3 (durability a)")?;
    // (b) either a fresh manifest is committed, or a durable reload seal proves that this replayed
    // source prefix already belongs to the replacement generation.
    let outcome = crate::manifest::publish_ordinary(acquire, epoch, &obj)
        .await
        .context("publish ordinary manifest against reload seal (durability b)")?;
    if let control::PublishManifestOutcome::CoveredBySeal(sealed_through) = outcome {
        delete_seal_covered_object(stager, &obj, sealed_through).await;
        tracing::info!(
            s3_uri = %obj.s3_uri,
            lsn_end = %obj.lsn_end,
            %sealed_through,
            "ordinary replay is durable through reload seal; discarded redundant object"
        );
    }
    Ok(obj)
}

/// Routes decoded changes into per-table [`TableBatcher`]s and seals at commit boundaries. Owns the
/// per-table batchers + the extractor context stamped into each row's `walrus_extractor_meta`.
#[derive(Debug)]
pub struct BatchRouter<C> {
    // HASHER-CHOICE: std's default. This is the tree's densest map — one `entry` per decoded row —
    // but no profile implicates hashing, and the keys are source-derived, so collision resistance
    // remains preferable to a speculative faster hasher.
    batchers: HashMap<u32, TableBatcher<C>>,
    /// Relation version currently bound by pgoutput's latest ordinary `Relation` message for an OID.
    bindings: HashMap<u32, common::SchemaVersionNo>,
    /// Pre-DDL segments from the current ordinary transaction. They are promoted and force-sealed only
    /// when that transaction's Commit arrives, so a boundary never discards speculative rows.
    pending_cuts: Vec<TableBatcher<C>>,
    triggers: BatchTriggers,
    clock: C,
    epoch: EpochNo,
    extractor_instance: String,
    /// The current transaction's top-level xid (from `Begin`), used when a change carries no xid
    /// (non-streamed txns).
    txn_xid: u32,
}

/// One decoded change's live provenance before its cached relation supplies the structural shape.
struct RowSource<'a> {
    oid: u32,
    op: Op,
    values: &'a [TupleValue],
    frame_lsn: Lsn,
    xid: u32,
}

/// Whether an UPDATE's old and new images address different replication keys.
///
/// pgoutput supplies an old tuple for `REPLICA IDENTITY FULL` updates even when the key did not
/// move, so the presence of `old` alone is not enough. Compare only the relation's key columns.
/// The preflight requires a real primary key; an absent key or malformed tuple is therefore a
/// protocol/catalog invariant violation and must stop acknowledgement rather than leak a ghost row.
///
/// # Errors
///
/// Returns an error if either tuple width differs from the relation, the relation has no key, or an
/// old key arrives as an unresolved TOAST placeholder. A `new` unchanged-TOAST marker is explicitly
/// equal to the old value: that is the marker's wire meaning.
pub(crate) fn update_changes_key(
    relation: &common::PgRelation,
    old: &[TupleValue],
    new: &[TupleValue],
) -> anyhow::Result<bool> {
    let expected = relation.columns.len();
    anyhow::ensure!(
        old.len() == expected && new.len() == expected,
        "UPDATE tuple width mismatch for {}.{}: relation={expected}, old={}, new={}",
        relation.schema,
        relation.name,
        old.len(),
        new.len()
    );

    let mut saw_key = false;
    let mut changed = false;
    for (index, column) in relation.columns.iter().enumerate() {
        if !column.is_key {
            continue;
        }
        saw_key = true;
        // Width was checked above, so zipped iteration would also be safe; checked access keeps
        // this helper valid under the crate's no-indexing lint if it is enabled here later.
        let old_value = old.get(index).context("checked UPDATE old tuple width")?;
        let new_value = new.get(index).context("checked UPDATE new tuple width")?;
        anyhow::ensure!(
            !matches!(old_value, TupleValue::UnchangedToast),
            "UPDATE old key column {}.{}.{} arrived as unchanged TOAST",
            relation.schema,
            relation.name,
            column.name
        );
        if matches!(new_value, TupleValue::UnchangedToast) {
            continue;
        }
        changed |= old_value != new_value;
    }
    anyhow::ensure!(
        saw_key,
        "UPDATE relation {}.{} has no replication key",
        relation.schema,
        relation.name
    );
    Ok(changed)
}

/// Replace an unchanged-TOAST marker in an UPDATE's replication-key columns with the actual old
/// key value. Arrow represents the marker as NULL and the transformer partitions by these columns, so
/// allowing the sentinel to cross the extractor boundary would address a NULL key instead of the source
/// row. Non-key sentinels deliberately remain untouched for the transformer's value back-scan.
///
/// The common case borrows `new` without allocating; an owned copy is made only when a key marker
/// actually needs substitution.
///
/// # Errors
///
/// Returns an error if a tuple has the wrong width, the relation has no replication key, a key
/// sentinel has no old image to resolve it from, or the old key is itself unresolved.
pub(crate) fn normalize_update_keys<'a>(
    relation: &common::PgRelation,
    old: Option<&[TupleValue]>,
    new: &'a [TupleValue],
) -> anyhow::Result<Cow<'a, [TupleValue]>> {
    let expected = relation.columns.len();
    anyhow::ensure!(
        new.len() == expected,
        "UPDATE new tuple width mismatch for {}.{}: relation={expected}, new={}",
        relation.schema,
        relation.name,
        new.len()
    );
    if let Some(old) = old {
        anyhow::ensure!(
            old.len() == expected,
            "UPDATE old tuple width mismatch for {}.{}: relation={expected}, old={}",
            relation.schema,
            relation.name,
            old.len()
        );
    }

    let mut normalized = Cow::Borrowed(new);
    let mut saw_key = false;
    for (index, column) in relation.columns.iter().enumerate() {
        if !column.is_key {
            continue;
        }
        saw_key = true;
        let new_value = normalized
            .as_ref()
            .get(index)
            .context("checked UPDATE new tuple width")?;
        if !matches!(new_value, TupleValue::UnchangedToast) {
            continue;
        }
        let old_value = old
            .context("UPDATE key arrived as unchanged TOAST without an old image")?
            .get(index)
            .context("checked UPDATE old tuple width")?;
        anyhow::ensure!(
            !matches!(old_value, TupleValue::UnchangedToast),
            "UPDATE old key column {}.{}.{} arrived as unchanged TOAST",
            relation.schema,
            relation.name,
            column.name
        );
        normalized
            .to_mut()
            .get_mut(index)
            .context("checked normalized UPDATE tuple width")?
            .clone_from(old_value);
    }
    anyhow::ensure!(
        saw_key,
        "UPDATE relation {}.{} has no replication key",
        relation.schema,
        relation.name
    );
    Ok(normalized)
}

/// A TRUNCATE has no tuple on the wire, but the raw Parquet schema is table-shaped. A full-width
/// NULL tuple carries the table-level `op='t'` boundary through that schema; the transformer filters its
/// values and orders the boundary solely by `(commit_lsn, lsn)`.
#[must_use]
pub(crate) fn truncate_values(relation: &common::PgRelation) -> Vec<TupleValue> {
    vec![TupleValue::Null; relation.columns.len()]
}

impl<C: Clock + Clone> BatchRouter<C> {
    /// A fresh router with no batchers. Pure — nothing is registered anywhere, so a discarded call
    /// builds and drops the routing table. `clippy::must_use_candidate` skips it (the `C` type
    /// parameter and the `impl Into<String>` argument both read to that lint as possibly
    /// side-effecting), which is why the attribute is written out; same for
    /// [`StreamDemux::new`](crate::stream_txn::StreamDemux::new).
    #[must_use]
    pub fn new(
        triggers: BatchTriggers,
        clock: C,
        epoch: EpochNo,
        extractor_instance: impl Into<String>,
    ) -> Self {
        let extractor_instance = extractor_instance.into();
        BatchRouter {
            batchers: HashMap::new(),
            bindings: HashMap::new(),
            pending_cuts: Vec::new(),
            triggers,
            clock,
            epoch,
            extractor_instance,
            txn_xid: 0,
        }
    }

    /// Bind subsequent ordinary changes for `oid` to the exact relation version announced by pgoutput.
    pub fn bind_relation(&mut self, oid: u32, version: common::SchemaVersionNo) {
        self.bindings.insert(oid, version);
    }

    /// Promote schema bindings only after the source transaction's atomic control receipt is
    /// durable. Streamed Relation messages are transaction-local while the transaction is open, so
    /// binding them globally at decode time could leak an aborting schema into ordinary rows. The
    /// commit path supplies only surviving, successfully published DDL identities here.
    fn commit_ddl_bindings(&mut self, bindings: &[(u32, common::SchemaVersionNo)]) {
        for &(oid, version) in bindings {
            self.bind_relation(oid, version);
        }
    }

    /// Route one decoded message. `Begin` sets the txn context; `I/U/D` buffer against the open txn;
    /// `Commit` promotes them and returns any batches that a trigger sealed. A key-changing update
    /// contributes two rows (delete the old key, then upsert the new image), and `Truncate`
    /// contributes one data-free boundary row per affected table. The outer decode loop sends
    /// streamed transactions through `StreamDemux`; logical `Message` frames do not create batches.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if a commit timestamp is invalid, a relation cannot create a batcher,
    /// or buffered rows cannot be promoted and sealed through [`crate::batch::BatchError`].
    pub fn route(
        &mut self,
        cache: &RelationCache,
        msg: &Message,
        frame_lsn: Lsn,
        _schema_version: common::SchemaVersionNo,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        match msg {
            Message::Begin { xid, .. } => {
                self.txn_xid = *xid;
                Ok(Vec::new())
            }
            Message::Insert {
                relation_oid,
                new,
                xid,
            } => {
                self.push(
                    cache,
                    RowSource {
                        oid: *relation_oid,
                        op: Op::Insert,
                        values: new,
                        frame_lsn,
                        xid: xid.unwrap_or(self.txn_xid),
                    },
                )?;
                Ok(Vec::new())
            }
            Message::Update {
                relation_oid,
                old,
                new,
                xid,
                ..
            } => {
                let xid = xid.unwrap_or(self.txn_xid);
                let cached = self.cached_relation(cache, *relation_oid)?;
                let normalized =
                    normalize_update_keys(&cached.relation, old.as_deref(), new.as_slice())?;
                if let Some(old) = old
                    && update_changes_key(&cached.relation, old, normalized.as_ref())?
                {
                    // pgoutput's UPDATE new image cannot remove the old mirror row after a key
                    // change: it addresses the new key. Preserve the old identity as an ordered
                    // delete in the same source transaction, then route the new image below.
                    self.push(
                        cache,
                        RowSource {
                            oid: *relation_oid,
                            op: Op::Delete,
                            values: old,
                            frame_lsn,
                            xid,
                        },
                    )?;
                }
                self.push(
                    cache,
                    RowSource {
                        oid: *relation_oid,
                        op: Op::Update,
                        values: normalized.as_ref(),
                        frame_lsn,
                        xid,
                    },
                )?;
                Ok(Vec::new())
            }
            Message::Delete {
                relation_oid,
                old,
                xid,
                ..
            } => {
                // The old-key tuple is full-width (non-key columns as NULL under DEFAULT identity).
                self.push(
                    cache,
                    RowSource {
                        oid: *relation_oid,
                        op: Op::Delete,
                        values: old,
                        frame_lsn,
                        xid: xid.unwrap_or(self.txn_xid),
                    },
                )?;
                Ok(Vec::new())
            }
            Message::Truncate { xid, relations, .. } => {
                let xid = xid.unwrap_or(self.txn_xid);
                anyhow::ensure!(
                    !relations.is_empty(),
                    "pgoutput TRUNCATE names no relations"
                );
                // Resolve every shape before mutating any batcher. If one relation is unexpectedly
                // absent, fail the whole frame loudly instead of retaining only a prefix of a
                // multi-table TRUNCATE and acknowledging a partial wipe.
                let targets = relations
                    .iter()
                    .map(|oid| self.cached_relation(cache, *oid))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                for cached in targets {
                    let values = truncate_values(&cached.relation);
                    self.push(
                        cache,
                        RowSource {
                            oid: cached.relation.oid,
                            op: Op::Truncate,
                            values: &values,
                            frame_lsn,
                            xid,
                        },
                    )?;
                }
                Ok(Vec::new())
            }
            Message::Commit {
                commit_lsn,
                commit_ts,
                ..
            } => self.commit(
                *commit_lsn,
                UtcTimestamp::from_pg_micros(*commit_ts)?,
                false,
            ),
            // Wildcard is deliberate: Message is #[non_exhaustive], and this dispatcher ignores other families.
            _ => Ok(Vec::new()),
        }
    }

    fn cached_relation(
        &self,
        cache: &RelationCache,
        oid: u32,
    ) -> anyhow::Result<Arc<crate::relcache::CachedRelation>> {
        // A transaction-local DDL can put a provisional newer shape in the shared cache. Route by the
        // Relation message that actually preceded this ordinary change, never by a global `latest`
        // read. An explicit binding whose exact cache entry vanished is corruption, not permission
        // to fall forward to a newer hydrated version.
        if let Some(version) = self.bindings.get(&oid) {
            return cache.get(oid, *version).with_context(|| {
                format!(
                    "bound ordinary change relation version is not cached: oid={oid} version={version}"
                )
            });
        }
        cache
            .latest_for(oid)
            .with_context(|| format!("ordinary change relation is not cached: oid={oid}"))
    }

    fn push(&mut self, cache: &RelationCache, row: RowSource<'_>) -> anyhow::Result<()> {
        let cached = self.cached_relation(cache, row.oid)?;
        let batcher = match self.batchers.entry(row.oid) {
            Entry::Occupied(e) => e.into_mut(),
            // The clock clone stays inside this arm. `C` is an `Arc` in production, so hoisting it
            // above the match would bump and drop the refcount on every row for the overwhelmingly
            // common occupied hit; `batchers`, `triggers`, and `clock` are disjoint fields, so the
            // borrow checker is content reading them under the entry's `&mut self.batchers`.
            Entry::Vacant(e) => e.insert(
                TableBatcher::new(Arc::clone(&cached), self.triggers, self.clock.clone())
                    .context("create table batcher")?,
            ),
        };
        let meta = ExtractorMeta {
            op: row.op,
            lsn: row.frame_lsn,
            commit_lsn: Lsn::ZERO, // patched at the batcher's on_commit
            commit_ts: UtcTimestamp::now(), // placeholder — patched at on_commit from Commit's ts
            xid: row.xid,
            epoch: self.epoch,
            batch_id: String::new(), // assigned by the batcher when the batch opens
            schema_version: cached.schema_version,
            source_schema: cached.relation.schema.clone(),
            source_table: cached.relation.name.clone(),
            kind: Kind::Stream,
            unchanged_toast: Box::default(),
            extractor_instance: self.extractor_instance.clone(),
            extractor_processed_at: UtcTimestamp::now(),
        };
        batcher.push(meta, row.values);
        Ok(())
    }

    /// Cut the current file for `schema.table` without discarding an open ordinary transaction.
    ///
    /// Before creating the schema cut, every earlier committed prefix is sealed across all tables
    /// while this transaction's speculative rows remain pending. This prevents an eventual atomic
    /// DDL receipt from absorbing older commits, whose presence can differ on lost-ACK replay. The
    /// target's speculative pre-DDL rows then move to `pending_cuts`; Commit promotes and force-seals
    /// that old-version segment. The next post-DDL change opens a fresh batcher at its
    /// Relation-bound version.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if committed rows cannot be sealed through
    /// [`crate::batch::BatchError`].
    pub fn cut_table(
        &mut self,
        cache: &RelationCache,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        let Some(oid) = cache.oid_for(schema, table) else {
            return Ok(Vec::new()); // never buffered this table yet — nothing to cut
        };
        // This is a global commit-order boundary, not merely a target-table cut. Other tables may
        // already contain committed rows plus pending rows from this same source transaction; if
        // those prefixes survive until Commit, force-sealing the touched batcher would make the
        // DDL receipt's semantic file shape depend on prior flush timing.
        let sealed = self
            .seal_stream_commit_boundary()
            .context("seal committed prefixes before DDL cut")?;
        if let Some(batcher) = self.batchers.remove(&oid)
            && batcher.has_open_txn()
        {
            self.pending_cuts.push(batcher);
        }
        Ok(sealed)
    }

    fn commit(
        &mut self,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
        force_current_transaction: bool,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for mut batcher in self.pending_cuts.drain(..) {
            batcher
                .on_commit(commit_lsn, commit_ts)
                .context("promote pre-DDL transaction segment")?;
            if batcher.committed_rows() > 0 {
                sealed.push(batcher.seal().context("seal pre-DDL transaction segment")?);
            }
        }
        for batcher in self.batchers.values_mut() {
            let had_open_transaction = batcher.has_open_txn();
            batcher
                .on_commit(commit_lsn, commit_ts)
                .context("promote committed rows")?;
            if batcher.should_flush() || (force_current_transaction && had_open_transaction) {
                sealed.push(batcher.seal().context("seal batch")?);
            }
        }
        Ok(sealed)
    }

    /// Whether the current ordinary transaction has routed user-table rows in either its live
    /// batchers or a pre-DDL cut. Must be sampled before [`Self::commit`] promotes those rows.
    #[must_use]
    fn has_open_ordinary_data(&self) -> bool {
        self.batchers.values().any(TableBatcher::has_open_txn)
            || self.pending_cuts.iter().any(TableBatcher::has_open_txn)
    }

    /// The earliest commit LSN of any committed-but-unsealed row across all tables, or `None` if
    /// nothing is buffered. An idle heartbeat must not advance `confirmed_flush` past this.
    pub fn undurable_floor(&self) -> Option<Lsn> {
        let live = self
            .batchers
            .values()
            .filter_map(TableBatcher::undurable_floor)
            .min();
        let pending = self
            .pending_cuts
            .iter()
            .filter_map(TableBatcher::undurable_floor)
            .min();
        match (live, pending) {
            (Some(live), Some(pending)) => Some(live.min(pending)),
            (live, pending) => live.or(pending),
        }
    }

    /// Force every committed segment for one source table into sealed batches.
    ///
    /// This is the in-memory half of an end-fence durability barrier.  The caller invokes it at
    /// the fence transaction's commit boundary, then durably writes every returned batch before
    /// publishing or resolving that fence.  Other tables remain open and continue to contribute
    /// to [`Self::undurable_floor`], so targeted fencing does not weaken normal slot-ACK ordering.
    ///
    /// Both the current batcher and any pre-DDL cut are considered, which makes "all committed
    /// rows for this table" true across a schema boundary.  No open transaction is ever discarded.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if any matching segment still has an open transaction (the caller
    /// is not at a commit boundary), or if its committed Arrow batch cannot be sealed.
    pub fn force_flush_table(
        &mut self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        let has_open_target = self
            .batchers
            .values()
            .any(|batcher| batcher.is_for_table(schema, table) && batcher.has_open_txn())
            || self
                .pending_cuts
                .iter()
                .any(|batcher| batcher.is_for_table(schema, table) && batcher.has_open_txn());
        if has_open_target {
            anyhow::bail!(
                "cannot force-flush durability fence for {schema}.{table} with an open transaction"
            );
        }

        let mut sealed = Vec::new();
        for batcher in self
            .pending_cuts
            .iter_mut()
            .chain(self.batchers.values_mut())
            .filter(|batcher| batcher.is_for_table(schema, table))
        {
            if let Some(batch) = batcher
                .force_seal_committed()
                .context("force-seal table durability fence")?
            {
                sealed.push(batch);
            }
        }
        Ok(sealed)
    }

    /// `StreamCommit` ordering fence: seal every already-committed ordinary row without discarding
    /// a concurrently open ordinary transaction. This is distinct from [`Self::drain_for_shutdown`],
    /// whose job is explicitly to discard speculative state before process exit.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if a committed Arrow batch cannot be sealed.
    pub fn seal_stream_commit_boundary(&mut self) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for batcher in &mut self.pending_cuts {
            if let Some(batch) = batcher
                .seal_committed_boundary()
                .context("seal committed pre-DDL segment at StreamCommit boundary")?
            {
                sealed.push(batch);
            }
        }
        for batcher in self.batchers.values_mut() {
            if let Some(batch) = batcher
                .seal_committed_boundary()
                .context("seal committed batch at StreamCommit boundary")?
            {
                sealed.push(batch);
            }
        }
        Ok(sealed)
    }

    /// Graceful-shutdown seal: seal every table's in-flight **committed** batch, dropping any
    /// open speculative buffers. The returned batches are flushed with the usual PUT → manifest → slot
    /// ordering before the final standby update.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if any table's committed Arrow batch cannot be sealed.
    pub fn drain_for_shutdown(&mut self) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for batcher in &mut self.pending_cuts {
            if let Some(batch) = batcher
                .drain_for_shutdown()
                .context("drain committed pre-DDL segment")?
            {
                sealed.push(batch);
            }
        }
        self.pending_cuts.clear();
        for batcher in self.batchers.values_mut() {
            if let Some(batch) = batcher
                .drain_for_shutdown()
                .context("drain committed batch")?
            {
                sealed.push(batch);
            }
        }
        Ok(sealed)
    }
}

/// On a `Relation` message: build the Arrow schema + descriptors, cache them under
/// `(oid, schema_version)`, and **persist** the `schema_registry` row (idempotent on
/// `(epoch, schema, table, version)`). Internal walrus tables are never registered. The persist is a
/// control-DB write, so this is `async`; the order is build → cache → persist.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if the relation cannot be mapped to Arrow, its snapshot cannot be
/// serialized, or [`control::ControlError`] prevents the registry upsert.
pub async fn on_relation(
    cache: &mut RelationCache,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: EpochNo,
    relation: common::PgRelation,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<()> {
    let Some(row) = cache_relation(cache, epoch, relation, schema_version)? else {
        return Ok(());
    };
    persist_registry(ex, &row).await
}

/// Build/cache one user relation and return the control row that may be persisted now or commit-gated
/// by [`crate::ddl::DdlConsumer`]. Internal walrus relations return `None`.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if the relation cannot be mapped to Arrow or serialized.
pub fn cache_relation(
    cache: &mut RelationCache,
    epoch: EpochNo,
    relation: common::PgRelation,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<Option<control::RegistryRow>> {
    if is_internal_table(&relation.schema, &relation.name) {
        return Ok(None);
    }
    let cached = cache
        .upsert_from_relation(relation, schema_version)
        .context("build Arrow schema for relation")?;
    let row = registry_row_from_cached(epoch, cached.as_ref())?;
    Ok(Some(row))
}

fn registry_row_from_cached(
    epoch: EpochNo,
    cached: &crate::relcache::CachedRelation,
) -> anyhow::Result<control::RegistryRow> {
    Ok(control::RegistryRow {
        epoch,
        source_schema: cached.relation.schema.clone(),
        source_table: cached.relation.name.clone(),
        schema_version: cached.schema_version,
        descriptors: cached.descriptors.clone(),
        columns: cached.registry_columns.clone(),
    })
}

/// Persist a previously cached registry row.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if control Postgres rejects the idempotent registry upsert.
pub async fn persist_registry(
    ex: impl sqlx::PgExecutor<'_>,
    row: &control::RegistryRow,
) -> anyhow::Result<()> {
    control::upsert_registry(ex, row)
        .await
        .context("upsert schema_registry")?;
    tracing::info!(
        source_table = %format_args!("{}.{}", row.source_schema, row.source_table),
        schema_version = %row.schema_version,
        "registered relation"
    );
    Ok(())
}

/// Route one live frame. A keepalive is a no-op here (its feedback is sent inside the stream); an
/// `XLogData` payload is decoded by the **existing** `pgoutput::parse_message`, which updates `ctx`
/// on `Stream Start`/`Stop`. A decode error on a real message is a bug — fail loud.
///
/// # Errors
///
/// Returns [`anyhow::Error`] wrapping [`crate::pgoutput::DecodeError`] when an `XLogData` payload is
/// malformed or violates pgoutput protocol state.
pub fn on_frame(ctx: &mut StreamCtx, frame: ReplicationMessage) -> anyhow::Result<Option<Message>> {
    match frame {
        ReplicationMessage::Keepalive { .. } => Ok(None),
        ReplicationMessage::XLogData { data, .. } => {
            let mut reader = Reader::new(&data);
            let msg = pgoutput::parse_message(&mut reader, ctx)
                .context("decode pgoutput XLogData payload")?;
            Ok(Some(msg))
        }
    }
}

/// Structured log for one decoded message — **fields, not string interpolation**, so logs stay
/// queryable (`op`, `source_table`, `commit_lsn`, `lsn`, `xid`).
///
/// Every arm is `trace!`, the level this function's name has always claimed. It fires once per
/// decoded WAL record — per *row* for the change families — so at `info` a single busy source
/// buries the events an operator actually reads: the durability line each flushed file writes, the
/// DDL bumps, the heartbeat round-trips. Per-record decode detail is what
/// `RUST_LOG=extractor=trace` is for; the loop's own lifecycle events around it stay at `info`.
fn trace_message(msg: &Message) {
    match msg {
        Message::Begin { final_lsn, xid, .. } => {
            tracing::trace!(op = "begin", xid, final_lsn = %final_lsn, "decoded");
        }
        Message::Commit {
            commit_lsn,
            end_lsn,
            ..
        } => {
            tracing::trace!(op = "commit", commit_lsn = %commit_lsn, end_lsn = %end_lsn, "decoded");
        }
        Message::Origin { commit_lsn, name } => {
            tracing::trace!(op = "origin", commit_lsn = %commit_lsn, name, "decoded");
        }
        Message::Relation { xid, relation } => tracing::trace!(
            op = "relation",
            xid = ?xid,
            source_table = %format_args!("{}.{}", relation.schema, relation.name),
            relation_oid = relation.oid,
            "decoded"
        ),
        Message::Type { xid, oid, name, .. } => {
            tracing::trace!(op = "type", xid = ?xid, type_oid = oid, name, "decoded");
        }
        Message::Insert {
            xid,
            relation_oid,
            new,
        } => tracing::trace!(op = "insert", xid = ?xid, relation_oid, cols = new.len(), "decoded"),
        Message::Update {
            xid, relation_oid, ..
        } => tracing::trace!(op = "update", xid = ?xid, relation_oid, "decoded"),
        Message::Delete {
            xid, relation_oid, ..
        } => tracing::trace!(op = "delete", xid = ?xid, relation_oid, "decoded"),
        Message::Truncate { xid, relations, .. } => {
            tracing::trace!(op = "truncate", xid = ?xid, relations = relations.len(), "decoded");
        }
        Message::Message {
            xid,
            transactional,
            lsn,
            prefix,
            ..
        } => tracing::trace!(
            op = "message",
            xid = ?xid,
            transactional,
            lsn = %lsn,
            prefix,
            "decoded"
        ),
        Message::StreamStart { xid, first_segment } => {
            tracing::trace!(op = "stream_start", xid, first_segment, "decoded");
        }
        Message::StreamStop => tracing::trace!(op = "stream_stop", "decoded"),
        Message::StreamCommit {
            xid, commit_lsn, ..
        } => tracing::trace!(op = "stream_commit", xid, commit_lsn = %commit_lsn, "decoded"),
        Message::StreamAbort { top_xid, sub_xid } => {
            tracing::trace!(op = "stream_abort", top_xid, sub_xid, "decoded");
        }
        // Two-phase (v3) frames never occur at v2; log opaquely rather than special-case.
        other => tracing::trace!(op = "other", detail = ?other, "decoded"),
    }
}

#[cfg(test)]
#[path = "consume_test.rs"]
mod tests;
