//! Large-transaction streaming + sub-transaction exclusion + memory-ceiling spill (§1.6, §1.3, proto
//! §8/§9). With `streaming='on'`, a transaction larger than `logical_decoding_work_mem` arrives
//! **before its commit**, chopped into interleaved `Stream Start … Stream Stop` blocks that finish with
//! `Stream Commit` or `Stream Abort`. This module makes the extractor correct under that:
//!
//! 1. **Demultiplex per top-level `xid`** — a [`StreamDemux`] of per-xid `StreamedTxn` buffers,
//!    reassembling non-contiguous segments via the `Stream Start` first-segment flag.
//! 2. **Commit-gate visibility** — a txn's rows become a `ready` manifest file **only on `Stream
//!    Commit`**; nothing is visible before it.
//! 3. **Hold the slot** — [`StreamDemux::open_floor`] is the oldest open txn's begin LSN; the checkpoint
//!    clamps `confirmed_flush_lsn` to it, so a crash always re-streams an incomplete txn.
//! 4. **Discard aborts** — a whole-txn `Stream Abort {sub == top}` drops the buffer entirely; a
//!    **sub-transaction** abort `Stream Abort {sub != top}` (the dangerous savepoint case, proto §9b)
//!    drops **exactly** that sub-xid's rows while the top-level continues to commit.
//! 5. **Bound memory** — when the aggregate [`InflightMeter`] crosses `max_inflight_bytes`, the largest
//!    open `(table, sub-xid)` buffer is **spilled speculatively** to S3 staging — **no
//!    manifest row, slot NOT advanced** (§1.5). Spilling is **per sub-xid** so an aborted sub-xid's
//!    already-spilled file can be dropped without contaminating survivors.
//!
//! **top vs sub xid (proto §7).** `Stream Start` carries the **top-level** xid; every streamed change
//! carries its **sub**-xid. The abort names the sub-xid — each buffered/spilled row is tagged with its
//! sub-xid so `iter_survivors` excludes exactly the aborted ones. *Freeing memory (the spill) is NOT
//! advancing the slot or making data visible (the `ready` row).*

use crate::batch::{BatchTriggers, Clock, SystemClock, TableBatcher};
use crate::memory::{InflightMeter, ProcessMemoryBudget, TableId};
use crate::pgoutput::Message;
use crate::relcache::RelationCache;
use crate::staging::{FileKind, ParquetStager, WrittenObject};
use anyhow::Context;
use common::{EpochNo, ExtractorMeta, Kind, Lsn, Op, TupleValue, UtcTimestamp};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// One streamed change, tagged with **its** sub-transaction xid (proto §7).
///
/// `values` is the decoded tuple, fixed at its source relation's width the moment the row is
/// buffered — nothing ever appends to it — so it is frozen into a `Box<[_]>`: the capacity word a
/// `Vec` would carry is dead weight repeated once per buffered row.
///
/// Deliberately **not** `Clone`. A buffered row is only ever moved (`push_change`, then
/// `take_stream`'s `extract_if`) or borrowed (`iter_survivors`), and [`InflightMeter`] charges its
/// bytes exactly once against the ceiling that decides when to spill. Deriving `Clone` would let a
/// second, unmetered deep copy of an open transaction's buffer — one `Box<[TupleValue]>` per row —
/// compile silently, so duplication here has to be a deliberate `impl`, not a derive nobody uses.
#[derive(Debug)]
struct StreamedChange {
    sub_xid: u32,
    oid: TableId,
    op: Op,
    values: Box<[TupleValue]>,
    lsn: Lsn,
    /// Exact relation version bound when this change was decoded. Never looked up as `latest` later.
    schema_version: common::SchemaVersionNo,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<StreamedChange>() == 48,
    "StreamedChange buffers every row of an open streamed transaction"
);

/// A speculatively-spilled S3 object for one `(sub_xid)` of an open txn — no manifest row until commit.
#[derive(Debug)]
struct StagedSpill {
    sub_xid: u32,
    written: WrittenObject,
}

/// One transaction-local Relation binding. Keeping the sub-xid provenance lets a savepoint abort
/// restore the prior parent/nested-savepoint shape even when the next segment omits Relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelationBinding {
    sub_xid: u32,
    version: common::SchemaVersionNo,
}

/// Per top-level xid buffer for an in-progress streamed transaction.
#[derive(Debug)]
struct StreamedTxn {
    /// The floor `confirmed_flush` must not pass while this txn is open (its first-segment LSN).
    begin_lsn: Lsn,
    /// Monotonic instant at which the first streamed segment was observed.
    opened_at: Instant,
    /// Buffered (not-yet-spilled) changes in commit order, each tagged with its sub-xid.
    changes: Vec<StreamedChange>,
    /// Exactly the distinct `(oid, sub_xid)` streams currently represented in `changes`.
    keys: HashSet<(TableId, u32)>,
    /// Speculatively-spilled files, each homogeneous in one sub-xid (droppable on that sub-xid's abort).
    staged: Vec<StagedSpill>,
    /// Sub-xids that rolled back (`Stream Abort {sub != top}`) — excluded from `iter_survivors`.
    aborted: HashSet<u32>,
}

/// A semantic violation of pgoutput's streamed-transaction state machine.
///
/// Wire-shape failures remain [`crate::pgoutput::DecodeError`]. These errors mean a well-formed
/// `StreamStart`/`Stop`/`Commit`/`Abort` sequence cannot be reconciled with the transaction state we
/// have retained, so acknowledging past it would risk loss. Every variant is therefore terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StreamProtocolError {
    /// A second segment began before the active one stopped.
    #[error(
        "stream start for top xid {incoming_top} while top xid {active_top} already has an active segment"
    )]
    SegmentAlreadyActive {
        /// Top-level transaction whose segment is still active.
        active_top: u32,
        /// Top-level transaction that attempted to start another segment.
        incoming_top: u32,
    },
    /// `first_segment=true` was repeated for an already-open transaction.
    #[error("first StreamStart repeated for already-open top xid {top_xid}")]
    DuplicateFirstSegment {
        /// Top-level transaction that already received its first segment.
        top_xid: u32,
    },
    /// A continuation cannot be reconstructed without its first segment.
    #[error("continuation StreamStart for unknown top xid {top_xid}")]
    UnknownContinuation {
        /// Top-level transaction missing retained first-segment state.
        top_xid: u32,
    },
    /// A stop has no segment to close.
    #[error("stream stop arrived without an active segment")]
    StopWithoutStart,
    /// Transaction outcome messages are top-level and must follow `StreamStop`.
    #[error(
        "{outcome} for top xid {outcome_top} arrived while top xid {active_top} has an active segment"
    )]
    OutcomeDuringSegment {
        /// Kind of transaction outcome that arrived early.
        outcome: &'static str,
        /// Top-level transaction named by the outcome.
        outcome_top: u32,
        /// Top-level transaction whose segment is still active.
        active_top: u32,
    },
    /// A commit for an unknown xid must never be treated as an empty transaction.
    #[error("stream commit for unknown top xid {top_xid}")]
    UnknownCommit {
        /// Top-level transaction missing retained stream state.
        top_xid: u32,
    },
    /// An abort for an unknown xid cannot safely alter any other open transaction.
    #[error("stream abort for unknown top xid {top_xid} (sub xid {sub_xid})")]
    UnknownAbort {
        /// Top-level transaction missing retained stream state.
        top_xid: u32,
        /// Subtransaction the abort attempted to discard.
        sub_xid: u32,
    },
}

impl StreamedTxn {
    fn new(begin_lsn: Lsn, opened_at: Instant) -> Self {
        StreamedTxn {
            begin_lsn,
            opened_at,
            changes: Vec::new(),
            keys: HashSet::new(),
            staged: Vec::new(),
            aborted: HashSet::new(),
        }
    }

    fn push_change(&mut self, change: StreamedChange) {
        self.keys.insert((change.oid, change.sub_xid));
        self.changes.push(change);
    }

    /// Move one stream out without cloning and preserve the relative order of rows and survivors.
    ///
    /// `extract_if` keeps the linear complexity of `mem::take` + `partition`
    /// and ordering while avoiding a fresh survivor buffer. The txn is still open and still
    /// buffering when the ceiling sheds one of its streams, so the survivors stay in `changes`'s
    /// own allocation — the very one the next `push_change` refills. Taking first handed that
    /// allocation to `IntoIter`, grew a second vector for the survivors, and freed the original on
    /// every spill. Only the returned rows allocate.
    fn take_stream(&mut self, oid: TableId, sub_xid: u32) -> Vec<StreamedChange> {
        let rows: Vec<StreamedChange> = self
            .changes
            .extract_if(.., |c| c.oid == oid && c.sub_xid == sub_xid)
            .collect();
        self.keys.remove(&(oid, sub_xid));
        rows
    }

    /// Drop an aborted savepoint's still-buffered rows immediately and return the stream keys whose
    /// shared-memory accounting the caller must release after this borrow ends. Keeping the xid in
    /// `aborted` still excludes malformed late frames and any already-staged spill entries.
    fn abort_subtxn(&mut self, sub_xid: u32) -> (usize, Vec<(TableId, u32)>) {
        self.aborted.insert(sub_xid);
        let before = self.changes.len();
        self.changes.retain(|change| change.sub_xid != sub_xid);
        let dropped = before.saturating_sub(self.changes.len());
        let keys = self
            .keys
            .iter()
            .copied()
            .filter(|(_, xid)| *xid == sub_xid)
            .collect::<Vec<_>>();
        for key in &keys {
            self.keys.remove(key);
        }
        (dropped, keys)
    }

    /// The buffered (in-memory) rows that survive to commit: every change **except** aborted sub-xids.
    /// The single definition of survivorship — `on_stream_commit` materialises exactly this iterator,
    /// and `survivor_count` counts it.
    ///
    /// `aborted` is bound to a local first: a `move` closure reading `self.aborted` would truncate
    /// the capture path at the `*self` deref and capture the whole `&StreamedTxn` (RFC 2229).
    ///
    /// No `#[must_use]` here: the `Iterator` trait carries one, so an `impl Iterator` return is
    /// already covered — unlike [`crate::relcache::Iter`], a named struct that needs its own.
    fn iter_survivors(&self) -> impl Iterator<Item = &StreamedChange> {
        let aborted = &self.aborted;
        self.changes
            .iter()
            .filter(move |c| !aborted.contains(&c.sub_xid))
    }
}

/// Demultiplexes interleaved streamed transactions, commit-gates visibility, and spills under memory
/// pressure. **DB-free** — `on_stream_commit` returns the complete object set for one atomic
/// control-plane publication.
#[derive(Debug)]
pub struct StreamDemux<C = std::sync::Arc<SystemClock>> {
    open: HashMap<u32, StreamedTxn>,
    /// `(relation_oid, sub_xid)` maps to the one top-level xid buffering that stream.
    ///
    /// Postgres xids are process-global, so one stream key has one owner. Invariant:
    /// `owner[key] == top` iff `open[top].keys` contains `key`; every owner key is metered.
    owner: HashMap<(TableId, u32), u32>,
    /// The top-level xid of the currently-open `Stream Start … Stream Stop` block; changes route here.
    current_top: Option<u32>,
    /// Relation-message history per open top-level transaction and relation OID. The last surviving
    /// binding is current; a subtransaction abort removes only entries introduced by that sub-xid.
    bindings: HashMap<(u32, TableId), Vec<RelationBinding>>,
    triggers: BatchTriggers,
    clock: C,
    epoch: EpochNo,
    extractor_instance: String,
    meter: InflightMeter,
    spill_count: u64,
}

/// Read-only protocol-v2 guard state. The LSN floor is the ACK clamp; the age makes a transaction
/// that began before or during a reload observable even if its next segment is temporarily idle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenStreamStats {
    /// Number of top-level streamed transactions that have not committed or aborted.
    pub count: usize,
    /// First-segment LSN of the numerically oldest open transaction.
    pub oldest_floor: Option<Lsn>,
    /// Monotonic age of the earliest-opened transaction.
    pub oldest_age: Option<Duration>,
}

impl<C: Clock + Clone> StreamDemux<C> {
    /// A fresh demux with no open streams. Pure, and invisible to `clippy::must_use_candidate` for
    /// the reason [`BatchRouter::new`](crate::consume::BatchRouter::new) is.
    #[must_use]
    pub fn new(
        triggers: BatchTriggers,
        clock: C,
        epoch: EpochNo,
        extractor_instance: impl Into<String>,
        max_inflight_bytes: NonZeroU64,
    ) -> Self {
        Self::with_process_memory_budget(
            triggers,
            clock,
            epoch,
            extractor_instance,
            max_inflight_bytes,
            Arc::new(ProcessMemoryBudget::new(max_inflight_bytes)),
        )
    }

    /// Build the production demux against the same process accounting reload workers reserve.
    #[must_use]
    pub(crate) fn with_process_memory_budget(
        triggers: BatchTriggers,
        clock: C,
        epoch: EpochNo,
        extractor_instance: impl Into<String>,
        max_inflight_bytes: NonZeroU64,
        process_budget: Arc<ProcessMemoryBudget>,
    ) -> Self {
        let extractor_instance = extractor_instance.into();
        StreamDemux {
            open: HashMap::new(),
            owner: HashMap::new(),
            current_top: None,
            bindings: HashMap::new(),
            triggers,
            clock,
            epoch,
            extractor_instance,
            meter: InflightMeter::with_process_budget(max_inflight_bytes, process_budget),
            spill_count: 0,
        }
    }

    /// Total speculative spills so far; the value is exported as a metric.
    #[must_use]
    pub const fn spill_count(&self) -> u64 {
        self.spill_count
    }

    /// `Stream Start`: open the first segment or resume an already-open transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StreamProtocolError`] for a nested segment or when `first_segment` disagrees with
    /// whether `top_xid` is already open. State is unchanged on error.
    pub fn on_stream_start(
        &mut self,
        top_xid: u32,
        first_segment: bool,
        lsn: Lsn,
    ) -> Result<(), StreamProtocolError> {
        if let Some(active_top) = self.current_top {
            return Err(StreamProtocolError::SegmentAlreadyActive {
                active_top,
                incoming_top: top_xid,
            });
        }
        match (self.open.entry(top_xid), first_segment) {
            (Entry::Occupied(_), true) => {
                return Err(StreamProtocolError::DuplicateFirstSegment { top_xid });
            }
            (Entry::Vacant(_), false) => {
                return Err(StreamProtocolError::UnknownContinuation { top_xid });
            }
            (Entry::Vacant(entry), true) => {
                entry.insert(StreamedTxn::new(lsn, self.clock.now()));
            }
            (Entry::Occupied(_), false) => {}
        }
        self.current_top = Some(top_xid);
        Ok(())
    }

    /// `Stream Stop`: the block ended (the txn may resume with a later segment).
    ///
    /// # Errors
    ///
    /// Returns [`StreamProtocolError::StopWithoutStart`] if no segment is active.
    pub const fn on_stream_stop(&mut self) -> Result<(), StreamProtocolError> {
        if self.current_top.is_none() {
            return Err(StreamProtocolError::StopWithoutStart);
        }
        self.current_top = None;
        Ok(())
    }

    /// Top-level xid of the currently open StreamStart..Stop block.
    #[must_use]
    pub const fn current_top(&self) -> Option<u32> {
        self.current_top
    }

    /// Bind subsequent changes in `top_xid` to the exact schema version from its Relation message.
    pub fn bind_relation(
        &mut self,
        top_xid: u32,
        sub_xid: u32,
        relation_oid: u32,
        version: common::SchemaVersionNo,
    ) {
        let history = self
            .bindings
            .entry((top_xid, TableId(relation_oid)))
            .or_default();
        let binding = RelationBinding { sub_xid, version };
        if history.last() != Some(&binding) {
            history.push(binding);
        }
    }

    /// Claim one buffered row's bytes and record or confirm the stream's unique owner.
    fn claim_stream(&mut self, key: (TableId, u32), top: u32, bytes: u64) {
        self.meter.add(key, bytes);
        let prior = self.owner.insert(key, top);
        debug_assert!(prior.is_none() || prior == Some(top));
    }

    /// Forget a stream in the owner index and meter as one operation.
    fn forget_stream(&mut self, key: (TableId, u32)) {
        self.owner.remove(&key);
        self.meter.release(key);
    }

    /// A streamed change: buffer it against the current top-level xid, tagged with its sub-xid, and
    /// meter its bytes. If the aggregate ceiling is crossed, spill the largest open `(table, sub-xid)`
    /// buffer speculatively (no manifest row, slot not advanced).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if no stream block is active, the relation is unknown, batching fails,
    /// or a speculative Parquet spill cannot be written.
    pub async fn on_change(
        &mut self,
        cache: &RelationCache,
        msg: &Message,
        stager: &ParquetStager,
        lsn: Lsn,
    ) -> anyhow::Result<()> {
        let top = self
            .current_top
            .context("streamed change arrived outside a Stream Start block")?;
        // Build the complete frame before mutating the transaction. In particular, a multi-table
        // TRUNCATE with one unknown relation must fail as a unit instead of buffering a partial wipe.
        // `clone().into_boxed_slice()` freezes each tuple at its decoded width; the clone already
        // allocates exactly `len`, so the conversion is a header change, not a second allocation.
        let mut changes = Vec::new();
        match msg {
            Message::Insert {
                relation_oid,
                new,
                xid,
            } => {
                let oid = TableId(*relation_oid);
                let cached = self.cached_relation(cache, top, oid)?;
                changes.push(StreamedChange {
                    sub_xid: xid.unwrap_or(top),
                    oid,
                    op: Op::Insert,
                    values: new.clone().into_boxed_slice(),
                    lsn,
                    schema_version: cached.schema_version,
                });
            }
            Message::Update {
                relation_oid,
                old,
                new,
                xid,
                ..
            } => {
                let oid = TableId(*relation_oid);
                let sub_xid = xid.unwrap_or(top);
                let cached = self.cached_relation(cache, top, oid)?;
                let normalized = crate::consume::normalize_update_keys(
                    &cached.relation,
                    old.as_deref(),
                    new.as_slice(),
                )?;
                if let Some(old) = old
                    && crate::consume::update_changes_key(
                        &cached.relation,
                        old,
                        normalized.as_ref(),
                    )?
                {
                    changes.push(StreamedChange {
                        sub_xid,
                        oid,
                        op: Op::Delete,
                        values: old.clone().into_boxed_slice(),
                        lsn,
                        schema_version: cached.schema_version,
                    });
                }
                changes.push(StreamedChange {
                    sub_xid,
                    oid,
                    op: Op::Update,
                    values: normalized.into_owned().into_boxed_slice(),
                    lsn,
                    schema_version: cached.schema_version,
                });
            }
            Message::Delete {
                relation_oid,
                old,
                xid,
                ..
            } => {
                let oid = TableId(*relation_oid);
                let cached = self.cached_relation(cache, top, oid)?;
                changes.push(StreamedChange {
                    sub_xid: xid.unwrap_or(top),
                    oid,
                    op: Op::Delete,
                    values: old.clone().into_boxed_slice(),
                    lsn,
                    schema_version: cached.schema_version,
                });
            }
            Message::Truncate { xid, relations, .. } => {
                anyhow::ensure!(
                    !relations.is_empty(),
                    "streamed pgoutput TRUNCATE names no relations"
                );
                let sub_xid = xid.unwrap_or(top);
                for relation_oid in relations {
                    let oid = TableId(*relation_oid);
                    let cached = self.cached_relation(cache, top, oid)?;
                    changes.push(StreamedChange {
                        sub_xid,
                        oid,
                        op: Op::Truncate,
                        values: crate::consume::truncate_values(&cached.relation)
                            .into_boxed_slice(),
                        lsn,
                        schema_version: cached.schema_version,
                    });
                }
            }
            // Wildcard is deliberate: Message is #[non_exhaustive], and this dispatcher ignores other families.
            _ => return Ok(()),
        }

        for change in changes {
            let key = (change.oid, change.sub_xid);
            let bytes = estimate_change_bytes(&change.values);
            let txn = self
                .open
                .get_mut(&top)
                .context("no open buffer for the current stream block")?;
            txn.push_change(change);
            self.claim_stream(key, top, bytes);
        }
        self.spill_if_over_ceiling(cache, stager).await
    }

    fn cached_relation(
        &self,
        cache: &RelationCache,
        top: u32,
        oid: TableId,
    ) -> anyhow::Result<Arc<crate::relcache::CachedRelation>> {
        // pgoutput deliberately sends a Relation before the first change for each relation in each
        // streamed top-level transaction. Do not fall back to the cache's highest hydrated version:
        // after a lost ACK, that maximum may belong to a later transaction whose control history was
        // already durable, while this replayed change still belongs to an older schema version.
        let binding = self
            .bindings
            .get(&(top, oid))
            .and_then(|history| history.last())
            .with_context(|| {
                format!(
                    "streamed change arrived before its Relation binding: oid={} top_xid={top}",
                    oid.0
                )
            })?;
        cache.get(oid.0, binding.version).with_context(|| {
            format!(
                "streamed change relation version is not cached: oid={} top_xid={top} version={}",
                oid.0, binding.version
            )
        })
    }

    /// While over the aggregate ceiling, spill the largest open `(table, sub-xid)` buffer to a
    /// speculative S3 object (frees memory; **not** a manifest row, **not** a slot advance).
    async fn spill_if_over_ceiling(
        &mut self,
        cache: &RelationCache,
        stager: &ParquetStager,
    ) -> anyhow::Result<()> {
        if !self.meter.is_over_ceiling() {
            return Ok(());
        }

        // Build one snapshot per shed episode. Inside this loop priorities only ever fall: each
        // successfully uploaded candidate is released before the next condition check, and no new
        // rows can enter while this awaited method owns the demux. Each popped tuple is only a hint
        // and its snapshot byte count is never treated as live accounting.
        let mut candidates = self.meter.to_spill_order();
        while self.meter.is_over_ceiling() {
            let Some((_bytes, oid, sub_xid)) = candidates.pop() else {
                break;
            };
            let key = (oid, sub_xid);
            let Some(top) = self.owner.get(&key).copied() else {
                self.forget_stream(key); // stale accounting; nothing buffered
                continue;
            };
            let (triggers, clock, epoch, instance) = (
                self.triggers,
                self.clock.clone(),
                self.epoch,
                self.extractor_instance.clone(),
            );
            let (begin, rows) = {
                let Some(txn) = self.open.get_mut(&top) else {
                    // A stale owner must be forgotten so the over-ceiling loop cannot spin.
                    self.forget_stream(key);
                    continue;
                };
                (txn.begin_lsn, txn.take_stream(oid, sub_xid))
            };
            let mut batchers = BTreeMap::new();
            for c in &rows {
                let cached = cache
                    .get(oid.0, c.schema_version)
                    .context("spill relation version is not cached")?;
                let meta = ExtractorMeta {
                    op: c.op,
                    lsn: c.lsn,
                    commit_lsn: begin, // placeholder until commit stamps the real one on the manifest
                    // Best-effort: the real commit_ts arrives only at Stream Commit, but this spill file
                    // is already durable in S3 by then (like commit_lsn, which the transformer overrides via
                    // the manifest lsn_end; commit_ts has no such override) — so spilled rows carry the
                    // spill-time instant, always within the transaction's lifetime.
                    commit_ts: UtcTimestamp::now(),
                    xid: c.sub_xid,
                    epoch,
                    batch_id: String::new(),
                    schema_version: cached.schema_version,
                    source_schema: cached.relation.schema.clone(),
                    source_table: cached.relation.name.clone(),
                    kind: Kind::Stream,
                    unchanged_toast: Box::default(),
                    extractor_instance: instance.clone(),
                    extractor_processed_at: UtcTimestamp::now(),
                };
                let batcher = match batchers.entry(c.schema_version) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        TableBatcher::new(Arc::clone(&cached), triggers, clock.clone())
                            .context("open spill batcher")?,
                    ),
                };
                batcher.push(meta, &c.values);
            }
            for mut batcher in batchers.into_values() {
                // The spill-time instant stands in for commit_ts here (see the meta comment above).
                batcher
                    .on_commit(begin, UtcTimestamp::now())
                    .context("promote spill rows")?;
                if batcher.committed_rows() == 0 {
                    continue;
                }
                // Tag as `Spill`: `begin` is a placeholder until StreamCommit supplies the real LSN.
                let written = stager
                    .put_with_kind(
                        batcher.seal().context("seal speculative spill batch")?,
                        FileKind::Spill,
                    )
                    .await
                    .context("speculative spill PUT")?;
                self.spill_count += 1;
                common::metrics::inc_spill();
                tracing::info!(
                    top_xid = top,
                    sub_xid,
                    oid = oid.0,
                    schema_version = %written.schema_version,
                    spill_count = self.spill_count,
                    uri = %written.s3_uri,
                    "spilled open-txn buffer speculatively (no manifest, slot held)"
                );
                let Some(txn) = self.open.get_mut(&top) else {
                    continue;
                };
                txn.staged.push(StagedSpill { sub_xid, written });
            }
            // `rows` retains the original tuple allocations while Arrow/Parquet is built. Do not
            // publish the meter reduction (which can wake a reload worker) until the remote write
            // has drained and those originals are actually freed.
            drop(rows);
            self.forget_stream(key);
        }
        Ok(())
    }

    /// `Stream Abort {top, sub}`. **sub == top** (whole-txn): drop the buffer AND delete its speculative
    /// files. **sub != top** (rolled-back savepoint): mark the sub-xid dead and delete only ITS
    /// speculative files; the top-level txn stays open and commits its survivors.
    ///
    /// # Errors
    ///
    /// Returns [`StreamProtocolError::OutcomeDuringSegment`] if the active segment has not stopped,
    /// or [`StreamProtocolError::UnknownAbort`] when `top_xid` is not open. State is unchanged for
    /// either protocol error; speculative object deletion remains best-effort after a valid abort.
    pub async fn on_stream_abort(
        &mut self,
        top_xid: u32,
        sub_xid: u32,
        stager: &ParquetStager,
    ) -> Result<(), StreamProtocolError> {
        if let Some(active_top) = self.current_top {
            return Err(StreamProtocolError::OutcomeDuringSegment {
                outcome: "StreamAbort",
                outcome_top: top_xid,
                active_top,
            });
        }
        if !self.open.contains_key(&top_xid) {
            return Err(StreamProtocolError::UnknownAbort { top_xid, sub_xid });
        }
        if top_xid == sub_xid {
            common::metrics::inc_aborted_txn(); // whole-txn abort
            self.bindings.retain(|(owner, _), _| *owner != top_xid);
            let Some(txn) = self.open.remove(&top_xid) else {
                return Err(StreamProtocolError::UnknownAbort { top_xid, sub_xid });
            };
            let rows = txn.changes.len();
            let meter_keys = txn.keys.iter().copied().collect::<Vec<_>>();
            let staged = txn.staged;
            // Free buffered tuples before publishing the lower shared-memory total. Remote staged
            // objects contain no in-memory row payload and may be cleaned up afterward.
            drop(txn.changes);
            for key in meter_keys {
                self.forget_stream(key);
            }
            for s in &staged {
                if let Err(error) = stager.delete(&s.written.key).await {
                    tracing::warn!(
                        key = %s.written.key,
                        error = ?error,
                        "failed to delete aborted speculative spill; object orphaned in staging"
                    );
                }
            }
            tracing::info!(top_xid, rows, staged = staged.len(), "whole-txn abort");
            return Ok(());
        }
        self.bindings.retain(|(owner, _), history| {
            if *owner != top_xid {
                return true;
            }
            history.retain(|binding| binding.sub_xid != sub_xid);
            !history.is_empty()
        });
        // Compact both in-memory rows and staged objects before publishing the lower shared-memory
        // total. The doomed spills move out so no transaction borrow crosses the awaited deletes.
        let (dropped_rows, meter_keys, doomed) = {
            let Some(txn) = self.open.get_mut(&top_xid) else {
                return Err(StreamProtocolError::UnknownAbort { top_xid, sub_xid });
            };
            let (dropped_rows, meter_keys) = txn.abort_subtxn(sub_xid);
            let doomed = txn
                .staged
                .extract_if(.., |s| s.sub_xid == sub_xid)
                .collect::<Vec<_>>();
            (dropped_rows, meter_keys, doomed)
        };
        for key in meter_keys {
            self.forget_stream(key);
        }
        for spill in &doomed {
            if let Err(error) = stager.delete(&spill.written.key).await {
                tracing::warn!(
                    key = %spill.written.key,
                    error = ?error,
                    "failed to delete rolled-back speculative spill; object orphaned in staging"
                );
            }
        }
        tracing::info!(
            top_xid,
            sub_xid,
            dropped_rows,
            dropped_spills = doomed.len(),
            "sub-txn abort: savepoint rows excluded"
        );
        Ok(())
    }

    /// Validate a `StreamCommit` before the caller performs its commit-order durability fence.
    ///
    /// The consume loop must seal older ordinary batches before publishing the streamed transaction,
    /// but a malformed outcome must be rejected before that seal mutates those batches. The commit
    /// path calls this again at its own boundary so direct callers receive the same guarantee.
    ///
    /// # Errors
    ///
    /// Returns [`StreamProtocolError::OutcomeDuringSegment`] when a stream segment has not stopped,
    /// or [`StreamProtocolError::UnknownCommit`] when `top_xid` was never opened (or already ended).
    pub fn validate_stream_commit(&self, top_xid: u32) -> Result<(), StreamProtocolError> {
        if let Some(active_top) = self.current_top {
            return Err(StreamProtocolError::OutcomeDuringSegment {
                outcome: "StreamCommit",
                outcome_top: top_xid,
                active_top,
            });
        }
        if !self.open.contains_key(&top_xid) {
            return Err(StreamProtocolError::UnknownCommit { top_xid });
        }
        Ok(())
    }

    /// `Stream Commit`: publish the (non-aborted) speculative spills stamped with the real `commit_lsn`,
    /// and materialise the in-memory survivors, returning every object for the caller's atomic
    /// streamed-transaction publication.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] wrapping [`StreamProtocolError`] for an invalid outcome, or when
    /// survivor batching, commit timestamp propagation, or durable Parquet publication of an
    /// in-memory group fails.
    pub async fn on_stream_commit(
        &mut self,
        top_xid: u32,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
        cache: &RelationCache,
        stager: &ParquetStager,
    ) -> anyhow::Result<Vec<WrittenObject>> {
        self.validate_stream_commit(top_xid)?;
        self.bindings.retain(|(owner, _), _| *owner != top_xid);
        let Some(mut txn) = self.open.remove(&top_xid) else {
            return Err(StreamProtocolError::UnknownCommit { top_xid }.into());
        };
        let meter_keys = txn.keys.iter().copied().collect::<Vec<_>>();
        let mut out = Vec::new();
        // Publish speculative spills whose sub-xid did NOT abort, stamped with the real commit LSN.
        // `take` moves the spill vector out without moving `txn`, so `txn.iter_survivors()` remains usable.
        for spill in std::mem::take(&mut txn.staged) {
            if txn.aborted.contains(&spill.sub_xid) {
                if let Err(error) = stager.delete(&spill.written.key).await {
                    tracing::warn!(
                        key = %spill.written.key,
                        error = ?error,
                        "failed to delete aborted speculative spill; object orphaned in staging"
                    );
                }
                continue;
            }
            let mut w = spill.written;
            w.lsn_end = commit_lsn;
            out.push(w);
        }
        // Materialise the still-in-memory survivors — the same predicate the accessor defines.
        let (triggers, clock, epoch, instance) = (
            self.triggers,
            self.clock.clone(),
            self.epoch,
            self.extractor_instance.clone(),
        );
        let mut batchers: HashMap<(TableId, common::SchemaVersionNo), TableBatcher<C>> =
            HashMap::new();
        for c in txn.iter_survivors() {
            let cached = cache.get(c.oid.0, c.schema_version).with_context(|| {
                format!(
                    "stream commit relation version is not cached: oid={} version={}",
                    c.oid.0, c.schema_version
                )
            })?;
            let meta = ExtractorMeta {
                op: c.op,
                lsn: c.lsn,
                commit_lsn,
                commit_ts, // the real Stream-Commit timestamp (also re-stamped by on_commit below)
                xid: c.sub_xid,
                epoch,
                batch_id: String::new(),
                schema_version: cached.schema_version,
                source_schema: cached.relation.schema.clone(),
                source_table: cached.relation.name.clone(),
                kind: Kind::Stream,
                unchanged_toast: Box::default(),
                extractor_instance: instance.clone(),
                extractor_processed_at: UtcTimestamp::now(),
            };
            let batcher = match batchers.entry((c.oid, c.schema_version)) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(
                    TableBatcher::new(Arc::clone(&cached), triggers, clock.clone())
                        .context("open streamed materialise batcher")?,
                ),
            };
            batcher.push(meta, &c.values);
            batcher
                .on_commit(commit_lsn, commit_ts)
                .context("promote streamed survivors")?;
            if batcher.should_flush() {
                out.push(
                    stager
                        .put(batcher.seal().context("seal streamed sub-batch")?)
                        .await
                        .context("materialise streamed sub-batch")?,
                );
            }
        }
        for batcher in batchers.values_mut() {
            if batcher.committed_rows() > 0 {
                out.push(
                    stager
                        .put(batcher.seal().context("seal final streamed batch")?)
                        .await
                        .context("materialise final streamed batch")?,
                );
            }
        }
        // The original streamed tuples coexist with their Arrow/Parquet materialization until this
        // point. Release shared process accounting only after those source allocations are gone.
        drop(txn);
        for key in meter_keys {
            self.forget_stream(key);
        }
        out.sort_by(|left, right| {
            (
                &left.source_schema,
                &left.source_table,
                left.schema_version,
                left.lsn_start,
            )
                .cmp(&(
                    &right.source_schema,
                    &right.source_table,
                    right.schema_version,
                    right.lsn_start,
                ))
        });
        Ok(out)
    }

    /// The oldest open txn's begin LSN — `confirmed_flush` must never pass this (§1.6). `None` when no
    /// streamed txn is open.
    #[must_use]
    pub fn open_floor(&self) -> Option<Lsn> {
        self.open.values().map(|t| t.begin_lsn).min()
    }

    /// Count, oldest first-segment LSN and monotonic age for open protocol-v2 transactions.
    /// Reading this state has no effect on the checkpoint or feedback socket.
    #[must_use]
    pub fn open_stats(&self) -> OpenStreamStats {
        let now = self.clock.now();
        OpenStreamStats {
            count: self.open.len(),
            oldest_floor: self.open_floor(),
            oldest_age: self
                .open
                .values()
                .map(|txn| now.saturating_duration_since(txn.opened_at))
                .max(),
        }
    }

    #[cfg(test)]
    fn owner_len(&self) -> usize {
        self.owner.len()
    }

    #[cfg(test)]
    fn survivor_count(&self, top_xid: u32) -> usize {
        self.open
            .get(&top_xid)
            .map(|t| t.iter_survivors().count())
            .unwrap_or(0)
    }
}

/// A rough per-change byte estimate (Arrow-buffered size, not serialized Parquet) for the meter.
///
/// Saturating to match the [`InflightMeter`] it feeds, whose own `add` clamps: a wrapped sum here
/// would hand the meter a *small* number in release and silently lift the memory ceiling, which is
/// the backstop against an OOM-kill.
fn estimate_change_bytes(values: &[TupleValue]) -> u64 {
    const META_OVERHEAD: u64 = 96;
    values
        .iter()
        .map(|v| match v {
            TupleValue::Text(s) => u64::try_from(s.len()).unwrap_or(u64::MAX),
            TupleValue::Binary(b) => u64::try_from(b.len()).unwrap_or(u64::MAX),
            TupleValue::Null | TupleValue::UnchangedToast => 1,
        })
        .fold(META_OVERHEAD, u64::saturating_add)
}

/// A streamed row change or table-level truncate carries its sub-xid; a non-streamed change never
/// enters the demux.
///
/// A pure classification of `msg`, so calling it for effect is meaningless. It escapes
/// `clippy::must_use_candidate` through the argument: a [`Message`] can hold a `TupleValue::Binary`,
/// whose `bytes::Bytes` is not `Freeze`, and that lint reads any non-`Freeze` reference as a
/// mutable — therefore side-effecting — argument.
#[must_use]
pub const fn is_streamed_change(msg: &Message) -> bool {
    matches!(
        msg,
        Message::Insert { xid: Some(_), .. }
            | Message::Update { xid: Some(_), .. }
            | Message::Delete { xid: Some(_), .. }
            | Message::Truncate { xid: Some(_), .. }
    )
}

#[cfg(test)]
#[path = "stream_txn_test.rs"]
mod tests;
