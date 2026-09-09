//! Micro-batching + cadence flush triggers (§1.3, §1.6).
//!
//! Accumulate decoded changes into a per-table Arrow builder and decide *when* to cut a file. A batch
//! flushes when **any** threshold trips — `max_fill` (cadence), `max_rows`, or `max_bytes` — but
//! **never in the middle of a committed transaction's tail**: rows buffer against the open txn and
//! become flush-eligible only at `Commit`, so a batch may span many small txns but never a fraction of
//! one (§1.6). The sealed in-memory [`RecordBatch`] is then written to Parquet and S3.
//!
//! `lsn_end` is the **commit LSN** of the batch's last transaction — the load-bearing manifest key
//! and the commit-domain input the checkpoint translates to pgoutput `end_lsn`; it is deliberately
//! *not* the max per-row LSN or the replication-feedback cursor.
//!
//! ## Dynamic dispatch in `extractor` — the deliberate list
//!
//! | Site | Dispatch | Why |
//! |---|---|---|
//! | [`Clock`] (this module) | **static** (`C: Clock`) | one production impl on a per-commit path |
//! | `Arc<dyn ObjectStore>` (`staging.rs`) | dynamic | `BufWriter::new` takes `Arc<dyn ObjectStore>` |
//! | `Box<dyn ArrayBuilder>` (`pg-to-arrow`) | dynamic | one heterogeneous builder per column |
//!
//! Only one concrete store (`AmazonS3`) is ever built, so the middle row is *not* "flexibility": it
//! is the shape `object_store::buffered::BufWriter::new` demands by value, which `put_with_kind`
//! calls per file. Where nothing upstream demands it — the transformer, which spends its store on one
//! `head` — the client stays concrete (`transformer::app::build_store`).

use crate::relcache::CachedRelation;
use arrow::record_batch::RecordBatch;
use common::{ExtractorMeta, Lsn, SchemaVersionNo, TupleValue, UtcTimestamp};
use pg_to_arrow::BatchBuilder;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod private {
    pub trait Sealed {}
}

/// Injectable clock so `max_fill` is testable without sleeping. Production has exactly one impl
/// ([`SystemClock`]); the trait exists **for that test seam**, not as dead generality. The seam is
/// **statically dispatched**: every owner is
/// generic over `C: Clock`, so the one production instantiation inlines and no vtable rides the
/// per-commit path. `Arc<SystemClock>` / `Arc<FakeClock>` satisfy the bound via the delegating impls
/// below.
///
/// This trait is sealed: it can be called and stored from anywhere, but only `extractor` can implement
/// it.
///
/// ```compile_fail
/// use std::time::Instant;
///
/// #[derive(Debug)]
/// struct WallClock;
///
/// impl extractor::batch::Clock for WallClock {
///     fn now(&self) -> Instant {
///         Instant::now()
///     }
/// }
/// ```
pub trait Clock: private::Sealed + Send + Sync + fmt::Debug {
    /// Return the clock's current monotonic instant.
    fn now(&self) -> Instant;

    /// Return the instant `after` from now, or `None` if it would overflow [`Instant`].
    ///
    /// This generic convenience method is available to concrete clocks. The `Self: Sized` gate
    /// excludes it from the vtable so [`Clock`] remains dyn-compatible.
    fn deadline<D: Into<Duration>>(&self, after: D) -> Option<Instant>
    where
        Self: Sized,
    {
        self.now().checked_add(after.into())
    }
}

/// Dyn-compatibility guard. Adding an ungated generic method, a bare-`Self` return, or an
/// associated const to [`Clock`] breaks compilation here, which is exactly the intent.
const _: fn(&dyn Clock) -> std::time::Instant = |c| c.now();

/// The wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl private::Sealed for SystemClock {}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Delegating wrapper impls preserve the private seal while allowing a shared or borrowed clock to
/// satisfy a `C: Clock` bound — the std pattern (`impl<R: Read + ?Sized> Read for &mut R`). Walrus
/// clocks are held behind an `Arc` because batchers share one, so without these impls the generic
/// form is unusable.
///
/// `?Sized` is load-bearing: it keeps a trait-object clock behind `Arc` covered. Both `Clock` impls
/// target wrapper types; a bare `impl<T: Clock> Clock for T` would collide with
/// `impl Clock for SystemClock` (E0119).
impl<T: Clock + ?Sized> private::Sealed for std::sync::Arc<T> {}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

impl<T: Clock + ?Sized> private::Sealed for &T {}

impl<T: Clock + ?Sized> Clock for &T {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

/// The three per-batch flush triggers. Whichever trips first (at a commit boundary) cuts the file.
#[derive(Clone, Copy, Debug)]
pub struct BatchTriggers {
    /// How long a batch may stay open before it is cut. This is the bound on end-to-end latency
    /// when traffic is too light to trip either size trigger.
    pub max_fill: Duration,
    /// Committed-row ceiling. Non-zero because a zero trigger would cut a file per row.
    pub max_rows: NonZeroU64,
    /// Buffered-byte ceiling, measured on the Arrow builders rather than the encoded Parquet.
    pub max_bytes: NonZeroU64,
}

/// A finished, ready-to-write batch. `lsn_end` = commit LSN of the last txn (NOT the max row LSN).
#[derive(Debug)]
pub struct SealedBatch {
    /// The finished Arrow batch, ready to encode as Parquet.
    pub record_batch: RecordBatch,
    /// Source schema of the rows.
    pub schema: String,
    /// Source table of the rows.
    pub table: String,
    /// The shape the rows were encoded against; carried onto the manifest row.
    pub schema_version: SchemaVersionNo,
    /// Commit LSN of the first transaction in the batch.
    pub lsn_start: Lsn,
    /// Commit LSN of the **last** transaction in the batch — not the highest per-row LSN. This is
    /// the value the transformer's claim order sorts on, so the distinction is load-bearing.
    pub lsn_end: Lsn,
    /// Rows in the batch, all of them committed.
    pub row_count: u64,
}

/// The batch's flush-eligible half: either nothing has committed into it yet, or a row count that
/// comes **with** the bounds every trigger and the sealed file read.
///
/// These five values are one state, so they are one value. Held apart — a `u64` count beside three
/// `Option`s — "N committed rows but no commit-LSN bounds" was representable, and [`TableBatcher::seal`]
/// had to catch that impossible combination at runtime; here the compiler does. `first_commit_lsn`
/// and `opened_at` belong to the FIRST promoted row, so a promotion that fails part-way through a
/// transaction can no longer leave bounds behind with no rows to match them.
#[derive(Debug)]
enum CommittedRows {
    /// No transaction has committed into this batch yet — nothing is flush-eligible, nothing to seal.
    None,
    Rows {
        count: NonZeroU64,
        /// A rough running Arrow-size estimate (see [`estimate_row_bytes`]) — the `max_bytes` trigger.
        bytes: u64,
        /// Commit LSN of the batch's first / last committed txn.
        first_commit_lsn: Lsn,
        last_commit_lsn: Lsn,
        /// When the first committed row landed (drives `max_fill`).
        opened_at: Instant,
    },
}

impl CommittedRows {
    /// Count one freshly-promoted row: the first opens the state (fixing the lower bound and starting
    /// the `max_fill` clock), each later one only extends the upper bound. Named apart from
    /// [`TableBatcher::push`], which buffers a row that has *not* committed yet.
    const fn record_row(&mut self, commit_lsn: Lsn, now: Instant) {
        match self {
            CommittedRows::None => {
                *self = CommittedRows::Rows {
                    count: NonZeroU64::MIN,
                    bytes: 0,
                    first_commit_lsn: commit_lsn,
                    last_commit_lsn: commit_lsn,
                    opened_at: now,
                };
            }
            CommittedRows::Rows {
                count,
                last_commit_lsn,
                ..
            } => {
                *count = count.saturating_add(1);
                *last_commit_lsn = commit_lsn;
            }
        }
    }

    /// Fold one promoted transaction's byte estimate in. Its only caller
    /// ([`TableBatcher::on_commit`]) promotes at least one row first, so the empty arm is a
    /// can't-happen no-op rather than a silent drop — and it needs no `unreachable!`.
    const fn add_bytes(&mut self, added: u64) {
        if let CommittedRows::Rows { bytes, .. } = self {
            *bytes = bytes.saturating_add(added);
        }
    }

    /// The committed row count — `0` before the first commit.
    const fn count(&self) -> u64 {
        match self {
            CommittedRows::None => 0,
            CommittedRows::Rows { count, .. } => count.get(),
        }
    }
}

/// Accumulates one table's committed changes into an Arrow builder until a trigger trips.
#[derive(Debug)]
pub struct TableBatcher<C> {
    rel: Arc<CachedRelation>,
    triggers: BatchTriggers,
    clock: C,
    /// Committed (flush-eligible) rows.
    builder: BatchBuilder,
    /// Rows of the currently-open transaction — not yet flush-eligible. The outer `Vec` is reused
    /// across transactions (see [`Self::on_commit`]), but each buffered tuple is frozen at its
    /// decoded width, so it carries no capacity word.
    pending: Vec<(ExtractorMeta, Box<[TupleValue]>)>,
    pending_bytes: u64,
    /// The flush-eligible half — count, bytes, and LSN bounds as one state (see [`CommittedRows`]).
    committed: CommittedRows,
    /// The file id shared by every row of this batch (assigned when it opens and used as the
    /// manifest key). `None` until the first row is pushed.
    batch_id: Option<String>,
}

impl<C: Clock> TableBatcher<C> {
    /// Create an empty batcher for one cached relation.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Arrow`] if the relation cannot produce a supported Arrow schema or
    /// typed builder set.
    pub fn new(
        rel: Arc<CachedRelation>,
        triggers: BatchTriggers,
        clock: C,
    ) -> Result<Self, BatchError> {
        let builder = BatchBuilder::new(&rel.relation)?;
        Ok(TableBatcher {
            rel,
            triggers,
            clock,
            builder,
            pending: Vec::new(),
            pending_bytes: 0,
            committed: CommittedRows::None,
            batch_id: None,
        })
    }

    /// Append one change to the OPEN txn buffer (not yet flush-eligible). Its `meta.commit_lsn` and
    /// `meta.batch_id` are patched at [`Self::on_commit`].
    pub fn push(&mut self, mut meta: ExtractorMeta, values: &[TupleValue]) {
        self.batch_id.get_or_insert_with(|| {
            format!("{}.{}-{}", meta.source_schema, meta.source_table, meta.lsn)
        });
        // `UnchangedToast` is represented as Arrow NULL in the typed data column; its column name
        // in row metadata is the only way the transformer can distinguish that sentinel from a real SQL
        // NULL and back-scan the prior value. Derive the list here, at the single choke point shared
        // by ordinary WAL, streamed in-memory rows, and speculative spills, so no producer path can
        // forget it. Reload rows contain no sentinels and retain the allocation-free empty
        // boxed slice.
        let mut unchanged_toast = Vec::new();
        for (column, value) in self.rel.relation.columns.iter().zip(values) {
            if matches!(value, TupleValue::UnchangedToast) {
                unchanged_toast.push(column.name.clone());
            }
        }
        meta.unchanged_toast = unchanged_toast.into_boxed_slice();
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(estimate_row_bytes(values));
        self.pending.push((meta, values.into()));
    }

    /// Append a row whose commit metadata is already final directly to the Arrow builder.
    ///
    /// Reload COPY rows are synthesized behind a durable fence, so their `commit_lsn` and
    /// `commit_ts` are known before conversion. Sending them through the ordinary open-transaction
    /// `pending` buffer would retain a second owned copy of every value until `on_commit`; this path
    /// makes them immediately flush-eligible and keeps the reload worker at router-sized memory.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Arrow`] if a value or its provenance cannot be appended to the
    /// relation's Arrow builders.
    pub fn push_committed(
        &mut self,
        mut meta: ExtractorMeta,
        values: &[TupleValue],
    ) -> Result<(), BatchError> {
        let batch_id = self.batch_id.get_or_insert_with(|| {
            format!("{}.{}-{}", meta.source_schema, meta.source_table, meta.lsn)
        });
        meta.batch_id.clone_from(batch_id);

        let mut unchanged_toast = Vec::new();
        for (column, value) in self.rel.relation.columns.iter().zip(values) {
            if matches!(value, TupleValue::UnchangedToast) {
                unchanged_toast.push(column.name.clone());
            }
        }
        meta.unchanged_toast = unchanged_toast.into_boxed_slice();

        self.builder.append_row(values, &meta)?;
        self.committed.record_row(meta.commit_lsn, self.clock.now());
        self.committed.add_bytes(estimate_row_bytes(values));
        Ok(())
    }

    /// Whether an open transaction's rows are buffered (not a commit boundary).
    #[must_use]
    pub const fn has_open_txn(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Promote the open txn's rows to the committed builder at `(commit_lsn, commit_ts)`; they are now
    /// flush-eligible. `commit_lsn` and `commit_ts` are known only at Commit, so per-row metas were
    /// pushed with placeholders and get the real transaction values stamped here. A commit
    /// with no rows for this table is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Unassigned`] if buffered rows have no assigned batch id, or
    /// [`BatchError::Arrow`] if a buffered value or its provenance cannot be appended to the
    /// relation's Arrow builders.
    pub fn on_commit(
        &mut self,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
    ) -> Result<(), BatchError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Borrowed, not cloned: every use below only *reads* the id (`clone_from`'s source), and
        // `batch_id` is a disjoint field from the `pending`/`builder`/`committed`/`pending_bytes`
        // this body mutates — so the shared borrow lives across the whole loop for free, where an
        // owned copy cost one `String` allocation per commit boundary per table.
        let batch_id = self.batch_id.as_ref().ok_or(BatchError::Unassigned)?;
        // One clock read per commit boundary (never per row); only the batch's first promoted row
        // keeps it, as the `max_fill` start.
        let now = self.clock.now();
        // Draining keeps the allocation so the next transaction refills the same pending buffer.
        for (mut meta, values) in self.pending.drain(..) {
            meta.commit_lsn = commit_lsn;
            meta.commit_ts = commit_ts;
            meta.batch_id.clone_from(batch_id);
            self.builder.append_row(&values, &meta)?;
            self.committed.record_row(commit_lsn, now);
        }
        self.committed
            .add_bytes(std::mem::take(&mut self.pending_bytes));
        Ok(())
    }

    /// True iff a trigger trips **and** we're at a commit boundary (no open txn, ≥1 committed row).
    #[must_use]
    pub fn should_flush(&self) -> bool {
        if self.has_open_txn() {
            return false;
        }
        let CommittedRows::Rows {
            count,
            bytes,
            opened_at,
            ..
        } = self.committed
        else {
            return false; // nothing committed is never flush-eligible
        };
        count.get() >= self.triggers.max_rows.get()
            || bytes >= self.triggers.max_bytes.get()
            || self.clock.now().saturating_duration_since(opened_at) >= self.triggers.max_fill
    }

    /// Finish the Arrow builders into a [`SealedBatch`] and reset. Errors if an open txn would be
    /// split.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::OpenTransaction`] if uncommitted rows remain,
    /// [`BatchError::Empty`] if no committed rows exist, [`BatchError::Unassigned`] if committed
    /// rows have no assigned batch id, or [`BatchError::Arrow`] if finishing or rebuilding the
    /// typed Arrow batch fails.
    pub fn seal(&mut self) -> Result<SealedBatch, BatchError> {
        if self.has_open_txn() {
            return Err(BatchError::OpenTransaction);
        }
        let CommittedRows::Rows {
            count,
            first_commit_lsn,
            last_commit_lsn,
            ..
        } = self.committed
        else {
            return Err(BatchError::Empty);
        };
        // The LSN bounds arrived WITH the rows above, so only the separately-assigned id can be missing.
        if self.batch_id.is_none() {
            return Err(BatchError::Unassigned);
        }
        let builder = std::mem::replace(&mut self.builder, BatchBuilder::new(&self.rel.relation)?);
        let record_batch = builder.into_record_batch()?;
        let sealed = SealedBatch {
            record_batch,
            schema: self.rel.relation.schema.clone(),
            table: self.rel.relation.name.clone(),
            schema_version: self.rel.schema_version,
            lsn_start: first_commit_lsn,
            lsn_end: last_commit_lsn,
            row_count: count.get(),
        };
        self.committed = CommittedRows::None;
        self.batch_id = None;
        Ok(sealed)
    }

    /// Force the currently committed rows into a file without waiting for a cadence/size trigger.
    ///
    /// Unlike [`Self::drain_for_shutdown`], this never drops an open transaction. A table durability
    /// fence calls this immediately after the fence transaction's `Commit`, where an open ordinary
    /// transaction is impossible; treating one as an error keeps an accidental mid-transaction
    /// caller from silently losing its speculative tail.
    ///
    /// `None` means the table has no committed rows waiting for durability.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::OpenTransaction`] if called away from a commit boundary, or the same
    /// assignment/Arrow errors as [`Self::seal`] when committed rows are present.
    pub fn force_seal_committed(&mut self) -> Result<Option<SealedBatch>, BatchError> {
        if self.has_open_txn() {
            return Err(BatchError::OpenTransaction);
        }
        if matches!(self.committed, CommittedRows::None) {
            return Ok(None);
        }
        self.seal().map(Some)
    }

    /// Seal committed rows at an interleaved `StreamCommit` boundary while preserving any ordinary
    /// transaction currently buffered in `pending`.
    ///
    /// The committed builder and speculative pending rows normally share one file identity. Cutting
    /// the committed prefix therefore gives the preserved transaction a fresh identity before it can
    /// later commit; its rows remain memory-only throughout this operation.
    ///
    /// # Errors
    ///
    /// Returns the assignment/Arrow errors from [`Self::seal`]. An open transaction is explicitly
    /// supported and restored on both success and error.
    pub fn seal_committed_boundary(&mut self) -> Result<Option<SealedBatch>, BatchError> {
        if matches!(self.committed, CommittedRows::None) {
            return Ok(None);
        }
        if !self.has_open_txn() {
            return self.seal().map(Some);
        }

        let pending = std::mem::take(&mut self.pending);
        let pending_bytes = std::mem::take(&mut self.pending_bytes);
        let sealed = self.seal();

        self.pending = pending;
        self.pending_bytes = pending_bytes;
        if sealed.is_ok() {
            let batch_id = self.pending.first().map(|(meta, _)| {
                format!("{}.{}-{}", meta.source_schema, meta.source_table, meta.lsn)
            });
            if let Some(batch_id) = &batch_id {
                for (meta, _) in &mut self.pending {
                    meta.batch_id.clone_from(batch_id);
                }
            }
            self.batch_id = batch_id;
        }
        sealed.map(Some)
    }

    /// Whether this batcher owns `schema.table`.
    ///
    /// Kept crate-visible for [`crate::consume::BatchRouter`]'s targeted durability fence; callers
    /// outside the extractor should identify tables through the registry rather than in-memory batches.
    #[must_use]
    pub(crate) fn is_for_table(&self, schema: &str, table: &str) -> bool {
        self.rel.relation.schema == schema && self.rel.relation.name == table
    }

    /// Rows that are committed and therefore publishable. Excludes anything buffered for a
    /// transaction still open, which is why this — not the builder length — drives the row trigger.
    #[must_use]
    pub const fn committed_rows(&self) -> u64 {
        self.committed.count()
    }

    /// The commit LSN of the earliest committed-but-unsealed row, or `None` if nothing is buffered.
    /// The durability floor an idle heartbeat must not advance `confirmed_flush` past: those
    /// rows are not yet in S3, so a slot advance beyond them would lose them on crash. Open-txn
    /// (uncommitted) rows do **not** count — their future commit LSN re-streams regardless.
    #[must_use]
    pub const fn undurable_floor(&self) -> Option<Lsn> {
        match self.committed {
            CommittedRows::None => None,
            CommittedRows::Rows {
                first_commit_lsn, ..
            } => Some(first_commit_lsn),
        }
    }

    /// **Drop** the open (uncommitted) transaction's speculative buffer — on a graceful drain
    /// these have no `Commit` yet, so forcing them out would orphan an S3 object with no way to resolve
    /// it; they simply re-stream on resume (at-least-once). Committed rows are untouched.
    pub fn drop_open_txn(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
    }

    /// Seal the in-flight **committed** batch on drain: drop any open speculative buffer first, then
    /// seal iff there are committed rows. `None` when nothing committed is in flight.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Unassigned`] if the committed rows carry no batch id, or the
    /// [`BatchError::Arrow`] produced while sealing committed rows. The open transaction is
    /// deliberately discarded first, so [`BatchError::OpenTransaction`] is not expected here.
    pub fn drain_for_shutdown(&mut self) -> Result<Option<SealedBatch>, BatchError> {
        self.drop_open_txn();
        if matches!(self.committed, CommittedRows::None) {
            return Ok(None);
        }
        self.seal().map(Some)
    }
}

/// A rough running byte estimate of the buffered Arrow size (not the compressed Parquet size, which
/// isn't known until write) — enough to drive the `max_bytes` trigger.
///
/// Saturating for the reason [`crate::memory::InflightMeter::add`] is: the only question asked of
/// this number is whether the batch has reached `max_bytes`, and `u64::MAX` preserves that answer.
/// The per-value clamps already state that intent; a `sum`/`+` would discard it by wrapping to a
/// *small* estimate in release — the one outcome that silently disables the trigger.
fn estimate_row_bytes(values: &[TupleValue]) -> u64 {
    const META_OVERHEAD: u64 = 96; // the walrus_extractor_meta JSON per row, roughly
    values
        .iter()
        .map(|v| match v {
            TupleValue::Text(s) => u64::try_from(s.len()).unwrap_or(u64::MAX),
            TupleValue::Binary(b) => u64::try_from(b.len()).unwrap_or(u64::MAX),
            TupleValue::Null | TupleValue::UnchangedToast => 1,
        })
        .fold(META_OVERHEAD, u64::saturating_add)
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BatchError {
    /// Sealing was attempted while a transaction was still open, which would split it across two
    /// files and break the all-or-nothing guarantee a commit boundary provides.
    #[error("cannot seal mid-transaction (would split a committed txn tail)")]
    OpenTransaction,
    /// Sealing was attempted with no committed rows, which would produce an empty Parquet file and
    /// a manifest row claiming zero rows.
    #[error("nothing to seal (empty batch)")]
    Empty,
    /// Committed rows exist but the batch id was never assigned. Its commit-LSN half is gone: the
    /// committed state now carries the LSN bounds with the rows, so that mismatch cannot occur.
    #[error("batch has committed rows but no assigned batch id")]
    Unassigned,
    /// Encoding the rows into their typed Arrow builders failed. `transparent` because
    /// [`pg_to_arrow::Error`] already names the column and the value.
    #[error(transparent)]
    Arrow(#[from] pg_to_arrow::Error),
}

#[cfg(test)]
#[path = "batch_test.rs"]
mod tests;
