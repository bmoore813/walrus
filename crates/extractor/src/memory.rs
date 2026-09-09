//! Aggregate, process-wide in-memory accounting + backpressure (§1.3). The per-batch `max_bytes`/
//! `max_rows` caps bound **one** batch; they do nothing to stop the *sum* of all in-flight
//! `(table, xid)` Arrow builders from OOM-killing the pod when a giant open transaction streams faster
//! than S3 drains. This module adds the aggregate `max_inflight_bytes` ceiling and the shed order.
//!
//! **`logical_decoding_work_mem` does NOT bound *our* memory** — it bounds the *source's* reorder
//! buffer (when it decides to stream), not the extractor's buffered Arrow. So the ceiling must sit **below
//! the pod memory limit** (with request = limit for Guaranteed QoS) so a graceful spill beats a cgroup
//! OOM-kill.
//!
//! **Shed order** (cheapest, correctness-free move first): **flush committed** batches (frees memory
//! *and* may advance the slot to the open-txn floor) → **spill open-txn buffers** speculatively to S3
//! (frees memory, slot NOT advanced past the floor) → **pause-poll** (stop requesting WAL) as the last
//! resort. Freeing memory and advancing the slot stay separable (§1.5).

use std::collections::{BinaryHeap, HashMap};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;
pub use walrus_config::{BandError, HysteresisBand, Ratio, RatioError};

/// Live accounting shared by streamed-WAL buffers and reload worker routes.
///
/// The ordinary streaming meter publishes its current estimate here. Reload workers reserve a
/// conservative fixed allowance before allocating their Arrow/Parquet/multipart pipeline. This is
/// accounting rather than an allocator: one source value can be larger than its estimate, but the
/// two ingestion paths cannot each independently spend the full configured ceiling.
#[derive(Debug)]
pub(crate) struct ProcessMemoryBudget {
    ceiling_bytes: NonZeroU64,
    // LOCK-CHOICE: every access is a short counter mutation and no guard crosses an await.
    state: Mutex<ProcessMemoryState>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct ProcessMemoryState {
    wal_bytes: u64,
    reload_bytes: u64,
}

impl ProcessMemoryBudget {
    #[must_use]
    pub(crate) fn new(ceiling_bytes: NonZeroU64) -> Self {
        Self {
            ceiling_bytes,
            state: Mutex::new(ProcessMemoryState::default()),
            changed: Notify::new(),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ProcessMemoryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set_wal_bytes(&self, bytes: u64) {
        let decreased = {
            let mut state = self.lock_state();
            let decreased = bytes < state.wal_bytes;
            state.wal_bytes = bytes;
            decreased
        };
        if decreased {
            self.changed.notify_one();
        }
    }

    fn reload_bytes(&self) -> u64 {
        self.lock_state().reload_bytes
    }

    /// Reserve a reload route. When the WAL side already occupies most of the ceiling, one route
    /// may exceed it so reload cannot deadlock; further routes wait. The WAL meter observes this
    /// reservation and spills on its next change, keeping the progress exception bounded to one
    /// route rather than admitting the entire configured fan-out.
    pub(crate) async fn reserve_reload(
        self: &Arc<Self>,
        bytes: NonZeroU64,
    ) -> ReloadMemoryReservation {
        loop {
            // Create the waiter before reading shared state. Producers use `notify_one`, which
            // retains a permit even if this future has not been polled yet, so a release between
            // the check and await cannot become a lost wake-up.
            let changed = self.changed.notified();
            let admitted = {
                // WAL publication and reload reservation share this short critical section. The
                // combined ceiling check is therefore linearizable rather than a cross-atomic
                // handshake whose two Acquire loads could both observe stale values.
                let mut state = self.lock_state();
                let within_ceiling = state
                    .wal_bytes
                    .saturating_add(state.reload_bytes)
                    .saturating_add(bytes.get())
                    <= self.ceiling_bytes.get();
                if within_ceiling || state.reload_bytes == 0 {
                    state.reload_bytes = state.reload_bytes.saturating_add(bytes.get());
                    true
                } else {
                    false
                }
            };
            if admitted {
                // Continue an admission chain when the remaining shared budget can fit more
                // routes. `notify_one` retains a permit if the next waiter has not registered yet.
                self.changed.notify_one();
                return ReloadMemoryReservation {
                    budget: Arc::clone(self),
                    bytes: bytes.get(),
                };
            }
            changed.await;
        }
    }
}

/// RAII release for one reload worker's conservative memory reservation.
#[derive(Debug)]
pub(crate) struct ReloadMemoryReservation {
    budget: Arc<ProcessMemoryBudget>,
    bytes: u64,
}

impl Drop for ReloadMemoryReservation {
    fn drop(&mut self) {
        {
            let mut state = self.budget.lock_state();
            state.reload_bytes = state.reload_bytes.saturating_sub(self.bytes);
        }
        self.budget.changed.notify_one();
    }
}

/// A pg relation OID (a stable table id).
///
/// A newtype, not an alias: every in-flight stream is keyed by `(TableId, xid)` — two `u32`s that an
/// alias lets a caller transpose silently, which is precisely the mix-up this accounting cannot
/// detect (both halves are small opaque integers, and a swapped key simply meters the wrong stream).
/// The transparent representation keeps it exactly one `u32` wide inside the per-row
/// `StreamedChange` — asserted directly below, as every other transparent walrus id asserts its own
/// layout, and again indirectly by that row's move-cost budget in `stream_txn.rs`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u32);

const _: () =
    assert!(size_of::<TableId>() == size_of::<u32>() && align_of::<TableId>() == align_of::<u32>());

/// Aggregate, process-wide accounting across all `(table, xid)` Arrow builders — distinct from any
/// single batch's `max_bytes`.
#[derive(Debug)]
pub struct InflightMeter {
    ceiling_bytes: NonZeroU64,
    total: u64,
    by_stream: HashMap<(TableId, u32), u64>,
    process_budget: Arc<ProcessMemoryBudget>,
}

impl Drop for InflightMeter {
    fn drop(&mut self) {
        self.process_budget.set_wal_bytes(0);
    }
}

impl InflightMeter {
    /// An empty meter with the ceiling shedding is measured against.
    ///
    /// `NonZeroU64` rather than `u64` because a zero ceiling would put the meter permanently over
    /// the limit before a single row arrived; the config layer rejects it, and the type keeps that
    /// rejection from having to be re-checked here.
    #[must_use]
    pub fn new(ceiling_bytes: NonZeroU64) -> Self {
        Self::with_process_budget(
            ceiling_bytes,
            Arc::new(ProcessMemoryBudget::new(ceiling_bytes)),
        )
    }

    /// Build a meter against the accounting object reload workers also reserve from.
    #[must_use]
    pub(crate) fn with_process_budget(
        ceiling_bytes: NonZeroU64,
        process_budget: Arc<ProcessMemoryBudget>,
    ) -> Self {
        debug_assert_eq!(ceiling_bytes, process_budget.ceiling_bytes);
        InflightMeter {
            ceiling_bytes,
            total: 0,
            by_stream: HashMap::new(),
            process_budget,
        }
    }

    /// Account `bytes` more buffered for `(table, xid)`.
    ///
    /// This gauge saturates because its consumer only needs to know whether memory is over the
    /// ceiling. At the integer bound, `u64::MAX` preserves that answer; returning an error would
    /// leave the shedding caller with no more accurate value to record.
    pub fn add(&mut self, key: (TableId, u32), bytes: u64) {
        let stream = self.by_stream.entry(key).or_insert(0);
        *stream = stream.saturating_add(bytes);
        self.total = self.total.saturating_add(bytes);
        self.process_budget.set_wal_bytes(self.total);
    }

    /// Drop all accounting for `(table, xid)` (its buffer was flushed or spilled).
    ///
    /// The normal path clamps at zero rather than wrapping. Once the aggregate reaches `u64::MAX`,
    /// it no longer records the overflow amount, so release recomputes the saturating sum of the
    /// remaining streams to keep the gauge conservative.
    pub fn release(&mut self, key: (TableId, u32)) {
        if let Some(bytes) = self.by_stream.remove(&key) {
            self.total = if self.total == u64::MAX {
                self.by_stream
                    .values()
                    .fold(0_u64, |total, &stream| total.saturating_add(stream))
            } else {
                self.total.saturating_sub(bytes)
            };
            self.process_budget.set_wal_bytes(self.total);
        }
    }

    /// Aggregate buffered bytes across every open stream. Saturating, so at `u64::MAX` this is a
    /// floor on the true figure rather than the figure itself — which is all the ceiling test needs.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// The configured ceiling this meter was built with. Constant for the meter's lifetime.
    #[must_use]
    pub const fn ceiling(&self) -> NonZeroU64 {
        self.ceiling_bytes
    }

    /// Whether the aggregate has passed the ceiling — the single question shedding is driven by.
    #[must_use = "the ceiling check drives shedding — ignoring it silently disables backpressure"]
    pub fn is_over_ceiling(&self) -> bool {
        self.total
            .saturating_add(self.process_budget.reload_bytes())
            > self.ceiling_bytes.get()
    }

    /// A one-shot snapshot of spill candidates, largest first: `(bytes, table_id, xid)`.
    ///
    /// `by_stream` remains the authoritative accounting store. This heap is built for one shed
    /// episode, drained by it, and dropped; it is never kept in sync with [`Self::add`] or
    /// [`Self::release`], because `BinaryHeap` supports neither priority updates nor arbitrary
    /// removal. Inside the drain loop priorities only ever fall: it calls `release`, never `add`.
    /// A popped candidate is therefore only a hint that the caller re-validates against the live
    /// meter and owner index.
    ///
    /// Tuple ordering provides a deterministic total order: bytes, then table id, then xid.
    ///
    /// Allocates and heapifies one entry per open stream — hence `to_`: build it once per shed
    /// episode, never per candidate.
    #[must_use = "the heap snapshot must be drained to select spill candidates"]
    pub fn to_spill_order(&self) -> BinaryHeap<(u64, TableId, u32)> {
        self.by_stream
            .iter()
            .map(|(&(table_id, xid), &bytes)| (bytes, table_id, xid))
            .collect()
    }

    /// The largest in-flight `(table, xid)` stream — the best spill candidate. Uses the same
    /// deterministic tie-break as [`Self::to_spill_order`].
    #[must_use]
    pub fn largest_open(&self) -> Option<(TableId, u32)> {
        self.by_stream
            .iter()
            .map(|(&(table_id, xid), &bytes)| (bytes, table_id, xid))
            .max()
            .map(|(_bytes, table_id, xid)| (table_id, xid))
    }
}

/// What to do when the ceiling is crossed — cheapest correctness-free move first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedAction {
    /// Normal path: frees memory AND may advance the slot (to the open-txn floor).
    FlushCommitted,
    /// Speculative S3 staging of an open txn's buffer — frees memory, slot NOT advanced.
    SpillOpenTxn(TableId, u32),
    /// Reactive backstop: stop requesting WAL until memory drains.
    PausePoll,
}

/// Decide the shed action when over the ceiling: committed first (if any), then spill the largest open
/// stream, then pause. `None` when under the ceiling.
#[must_use]
pub fn decide(meter: &InflightMeter, has_committed: bool) -> Option<ShedAction> {
    if !meter.is_over_ceiling() {
        return None;
    }
    if has_committed {
        return Some(ShedAction::FlushCommitted);
    }
    match meter.largest_open() {
        Some((t, x)) => Some(ShedAction::SpillOpenTxn(t, x)),
        None => Some(ShedAction::PausePoll),
    }
}

/// Hysteresis so the pause-poll backstop doesn't flap around the ceiling: pause at the high `activate`
/// ratio, resume only at the lower `resume` ratio.
#[allow(
    missing_copy_implementations,
    reason = "copying this mutable hysteresis state could silently detach pause transitions"
)]
#[derive(Debug)]
pub struct Backpressure {
    band: HysteresisBand,
    paused: bool,
}

impl Backpressure {
    /// A backpressure latch over `band`, starting un-paused.
    #[must_use]
    pub const fn new(band: HysteresisBand) -> Self {
        Backpressure {
            band,
            paused: false,
        }
    }

    /// Update from the current total vs ceiling; returns whether intake should be PAUSED afterwards.
    /// The non-zero ceiling makes the ratio total, so this path needs no divide-by-zero fallback.
    ///
    /// `const` for the same reason [`Ratio::new`] is: the float division and comparisons are what
    /// clippy's const model cannot see through, not something the compiler refuses to evaluate.
    pub const fn tick(&mut self, total: u64, ceiling: NonZeroU64) -> bool {
        let ratio = total as f64 / ceiling.get() as f64;
        if self.paused {
            if ratio <= self.band.resume().as_f64() {
                self.paused = false;
            }
        } else if ratio >= self.band.activate().as_f64() {
            self.paused = true;
        }
        self.paused
    }

    /// The latch's current state, without advancing it. Read this to decide whether to poll the
    /// replication stream; call [`tick`](Self::tick) to update it.
    #[must_use]
    pub const fn is_paused(&self) -> bool {
        self.paused
    }
}

#[cfg(test)]
#[path = "memory_test.rs"]
mod tests;
