//! Echo-wait watermark capture (reload H1).
//!
//! The exporter INSERTs a `public.walrus_reload_signal` row and blocks on a waiter; when the
//! extractor decodes its *own* insert coming back through the replication stream, the transaction's
//! **commit LSN** is the chunk's low watermark `L_i` — the value chunk rows are stamped with. Two
//! rules make the handoff race-free and are worth stating where the code can't show them:
//!
//! - **Subscribe-then-insert.** The exporter subscribes BEFORE writing the signal row: the echo
//!   round-trip can be faster than the exporter's next `await`, so the registry must already hold
//!   the sender when the INSERT commits. (The reverse order can miss the echo forever.)
//! - **Buffer at `Insert`, resolve at `Commit`.** The watermark is the transaction's commit LSN —
//!   a property of the `Commit` message, which arrives *after* the `Insert` — so the decoded
//!   insert is held as a [`PendingSignal`] until its transaction's fate is known.
//!
//! The row's embedded `wal_insert_lsn` is never the stamp — it is the free cross-check:
//! an insert's WAL position strictly precedes its commit record, so `embedded < commit` on every
//! echo, or the watermark model itself is broken (metric + error log, never a panic). This check
//! bounds the race between row visibility and commit-record visibility.

use common::{Lsn, ReloadId};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;

/// One resolved echo: the authoritative stamp and its cross-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Echo {
    /// The signal transaction's decoded COMMIT LSN — this IS the chunk's low watermark `L_i` (H1).
    pub commit_lsn: Lsn,
    /// The row's embedded `wal_insert_lsn` — strictly earlier than `commit_lsn`, or the model is
    /// wrong (the cross-check, never the stamp).
    pub embedded_lsn: Lsn,
}

type WaiterKey = (ReloadId, i64);
type WaiterEntry = (u64, oneshot::Sender<Echo>);

/// Registry of in-flight watermark waits, keyed by `(reload_id, chunk_no)`.
///
/// Shared (`Arc`) between the decode loop (which resolves) and the exporter tasks (which
/// subscribe). [`Self::subscribe`] returns a [`SubscribeGuard`] whose [`Drop`] removes its
/// entry, including when the exporter returns before sending the signal. [`Self::resolve`] also
/// removes the entry before delivering its echo. Each entry has a generation so dropping a stale
/// guard after a re-subscribe cannot evict the replacement waiter.
#[derive(Debug, Default)]
pub struct WatermarkWaiters {
    // Every production access writes once; the only reader is the test-facing `len()`.
    // LOCK-CHOICE: a Mutex fits exclusive one-operation access; an RwLock adds no useful concurrency.
    waiters: Mutex<HashMap<WaiterKey, WaiterEntry>>,
    next_generation: AtomicU64,
    /// Cross-check violations observed (mirrors the Prometheus counter so unit tests — which run
    /// without a recorder — can assert the count).
    crosscheck_violations: AtomicU64,
}

impl WatermarkWaiters {
    /// Register interest in chunk `(reload_id, chunk_no)`'s echo. Call BEFORE inserting the
    /// signal row (subscribe-then-insert). A duplicate subscribe replaces the stale sender —
    /// the previous receiver resolves as `Err(Closed)`, which the exporter treats as superseded.
    pub fn subscribe(&self, reload_id: ReloadId, chunk_no: i64) -> SubscribeGuard<'_> {
        let (tx, rx) = oneshot::channel();
        let key = (reload_id, chunk_no);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.waiters.lock().insert(key, (generation, tx));
        SubscribeGuard {
            waiters: self,
            key,
            generation,
            rx,
        }
    }

    /// Remove `key` only if it still holds `generation`; a stale guard must not evict the live
    /// waiter that replaced it.
    ///
    /// The occupied entry holds the slot the lookup found, so the generation check and the eviction
    /// share one hash — a `get`-then-`remove` pair would hash twice while holding the registry lock.
    fn unsubscribe(&self, key: WaiterKey, generation: u64) {
        let mut waiters = self.waiters.lock();
        if let Entry::Occupied(entry) = waiters.entry(key)
            && entry.get().0 == generation
        {
            entry.remove();
        }
    }

    /// Number of in-flight watermark waits.
    ///
    /// This read and [`Self::crosscheck_violations`] below both compute an answer and change
    /// nothing, so each carries `#[must_use]` explicitly: `clippy::must_use_candidate` skips them
    /// because `&self` on a type with interior mutability (the waiter mutex, the two atomics) reads
    /// to that lint as a mutable — therefore side-effecting — argument.
    #[must_use]
    pub fn waiter_count(&self) -> usize {
        self.waiters.lock().len()
    }

    /// Deliver an echo from the consume path (at the `Commit` of a transaction that carried a
    /// signal insert). Runs the cross-check first: `embedded < commit`, or the counter ticks and
    /// an error is logged — loud, not fatal, and the waiter still resolves (the commit LSN is
    /// still the only defensible stamp). An unsubscribed echo (e.g. redelivered WAL after an
    /// exporter crash — recovery is control-pg's job, never WAL replay) is dropped with a debug
    /// log.
    pub fn resolve(&self, reload_id: ReloadId, chunk_no: i64, echo: Echo) {
        if echo.embedded_lsn >= echo.commit_lsn {
            self.crosscheck_violations.fetch_add(1, Ordering::Relaxed);
            common::metrics::record_reload_crosscheck_violation();
            tracing::error!(
                reload_id = %reload_id,
                chunk_no,
                embedded_lsn = %echo.embedded_lsn,
                commit_lsn = %echo.commit_lsn,
                "reload echo cross-check VIOLATION: embedded wal_insert_lsn >= commit LSN — \
                 the watermark model is wrong; stop reloads and investigate"
            );
        }
        // A guard temporary in a `match` scrutinee lives through the whole match. Bind the
        // removal first so logging and sender notification never hold the registry lock.
        let waiter = self.waiters.lock().remove(&(reload_id, chunk_no));
        match waiter {
            Some((_, tx)) => {
                if tx.send(echo).is_err() {
                    // The exporter gave up (timeout) and dropped its receiver — fine.
                    tracing::debug!(reload_id = %reload_id, chunk_no, "echo resolved after waiter gave up");
                } else {
                    tracing::info!(
                        reload_id = %reload_id,
                        chunk_no,
                        commit_lsn = %echo.commit_lsn,
                        embedded_lsn = %echo.embedded_lsn,
                        "reload_signal echo"
                    );
                }
            }
            None => {
                tracing::debug!(reload_id = %reload_id, chunk_no, "echo with no subscriber; dropped");
            }
        }
    }

    /// Cross-check violations seen so far (the unit-testable mirror of the Prometheus counter).
    #[must_use]
    pub fn crosscheck_violations(&self) -> u64 {
        self.crosscheck_violations.load(Ordering::Relaxed)
    }
}

/// An in-flight watermark subscription. Awaiting the guard awaits its echo; dropping it removes
/// the matching registry entry.
///
/// The receiver stays private: the two ways to take an echo are `.await` (the
/// [`std::future::Future`] impl the exporter uses) and [`Self::try_recv`]. A `Deref` to the inner
/// [`oneshot::Receiver`] would additionally publish `close` and `blocking_recv` — a caller closing
/// the channel behind the registry's back, or blocking a runtime thread, is not part of what a
/// subscription offers (API guideline C-DEREF: a guard exposes the access it means to grant, not
/// its whole innards).
///
/// The attribute below sits on the TYPE rather than on [`WatermarkWaiters::subscribe`], so it covers
/// every construction path at once — and it is the RAII case, not a style preference: this guard IS
/// the subscription. `waiters.subscribe(id, chunk);` as a bare statement would register the waiter
/// and drop it in the same expression, unsubscribing before the exporter ever writes its signal row,
/// and the echo would then have no sender to resolve. `clippy::must_use_candidate` cannot reach
/// `subscribe` (it takes `&self` on a type with interior mutability, which that lint reads as a
/// side-effecting argument), so nothing but this attribute states the rule.
///
/// Dropping a subscription on the floor is therefore a compile error:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # let waiters = extractor::reload_signal::WatermarkWaiters::default();
/// waiters.subscribe(common::ReloadId(1), 0);
/// ```
#[must_use = "the guard IS the subscription and its future — dropping it unsubscribes immediately"]
#[derive(Debug)]
pub struct SubscribeGuard<'a> {
    waiters: &'a WatermarkWaiters,
    key: WaiterKey,
    generation: u64,
    rx: oneshot::Receiver<Echo>,
}

impl SubscribeGuard<'_> {
    /// Take the echo if it has already been resolved, without awaiting.
    ///
    /// # Errors
    ///
    /// [`oneshot::error::TryRecvError::Empty`] while the echo is still in flight;
    /// [`oneshot::error::TryRecvError::Closed`] once the sender is gone — the subscription was
    /// superseded by a later `subscribe` on the same key.
    pub fn try_recv(&mut self) -> Result<Echo, oneshot::error::TryRecvError> {
        self.rx.try_recv()
    }
}

impl std::future::Future for SubscribeGuard<'_> {
    type Output = Result<Echo, oneshot::error::RecvError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::future::Future::poll(std::pin::Pin::new(&mut self.get_mut().rx), cx)
    }
}

impl Drop for SubscribeGuard<'_> {
    fn drop(&mut self) {
        self.waiters.unsubscribe(self.key, self.generation);
    }
}

/// A decoded `reload_signal` insert held between its `Insert` message and its transaction's fate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSignal {
    /// Which reload attempt the signal belongs to.
    pub reload_id: ReloadId,
    /// Which chunk of that attempt. With `reload_id`, the key a waiter subscribes under.
    pub chunk_no: i64,
    /// The LSN the exporter wrote *into* the signal row. Used only to cross-check the decoded
    /// commit LSN — it is never the watermark itself, which the commit supplies.
    pub embedded_lsn: Lsn,
    /// The per-message xid — `Some` only inside a streamed transaction (which a single-row signal
    /// txn can never be; kept so the defensive stream paths stay precise).
    pub xid: Option<u32>,
    /// Top-level streamed transaction that owns this signal. `StreamCommit` names the top xid while
    /// the insert itself can carry a savepoint's sub-xid.
    pub top_xid: Option<u32>,
}

/// Why a decoded `public.walrus_reload_signal` tuple is not a [`PendingSignal`] — the column that failed.
///
/// A dropped signal costs the exporter its echo, and its only other symptom is a chunk wait that
/// times out much later, so naming the column is what turns "something is malformed" into the
/// shape drift an operator can act on. Mirrors [`crate::ddl::DdlError::MissingColumn`], the same
/// decode-an-internal-table's-tuple failure on the DDL side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("reload_signal tuple missing/invalid column: {0}")]
pub struct SignalTupleError(pub &'static str);

/// The value of `name` in a decoded signal tuple, located by the noted relation shape and parsed
/// into the id / chunk number / LSN that column carries. Every way it can fail — absent from the
/// shape, absent from the tuple, non-text (a Delete's old-key image carries NULLs for the non-key
/// columns), or unparseable — names the same column.
fn signal_field<T: std::str::FromStr>(
    rel: &common::PgRelation,
    new: &[common::TupleValue],
    name: &'static str,
) -> Result<T, SignalTupleError> {
    let idx = rel
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or(SignalTupleError(name))?;
    let common::TupleValue::Text(text) = new.get(idx).ok_or(SignalTupleError(name))? else {
        return Err(SignalTupleError(name));
    };
    text.parse().map_err(|_| SignalTupleError(name))
}

impl PendingSignal {
    /// Parse a decoded signal tuple by column NAME from the noted relation shape (internal tables
    /// are never in the [`RelationCache`](crate::relcache::RelationCache), so the shape comes from
    /// [`InternalTables`](crate::heartbeat::InternalTables)). The caller logs a malformed tuple and
    /// drops it; it can never wedge the loop.
    ///
    /// # Errors
    ///
    /// Returns [`SignalTupleError`] naming the first column that is absent, non-text, or does not
    /// parse as the value it carries.
    pub fn from_tuple(
        rel: &common::PgRelation,
        new: &[common::TupleValue],
        xid: Option<u32>,
        top_xid: Option<u32>,
    ) -> Result<Self, SignalTupleError> {
        let signal = PendingSignal {
            reload_id: signal_field(rel, new, "reload_id")?,
            chunk_no: signal_field(rel, new, "chunk_no")?,
            embedded_lsn: signal_field(rel, new, "wal_insert_lsn")?,
            xid,
            top_xid,
        };
        if signal.xid.is_some() != signal.top_xid.is_some() {
            return Err(SignalTupleError("xid/top_xid"));
        }
        Ok(signal)
    }
}

/// The decode loop's between-Insert-and-Commit buffer.
///
/// A signal transaction is a tiny single-row commit and can never be the "largest in-progress
/// transaction" streaming selects, so the `Stream*` paths below are can't-happen defenses in the
/// house style: a comment saying why, plus code that survives it anyway — including the one subtle
/// case that MUST stay correct, a subtransaction-aborted signal insert (its per-message xid is the
/// sub-xid `StreamAbort` names, so it is dropped precisely and never resolves a waiter).
#[derive(Debug, Default)]
pub struct PendingSignals {
    pending: Vec<PendingSignal>,
}

impl PendingSignals {
    /// Hold a decoded signal until its transaction commits.
    ///
    /// A signal cannot be resolved when it is decoded: its watermark *is* the commit LSN, which is
    /// not known until the COMMIT message arrives. So every signal waits here first.
    pub fn push(&mut self, signal: PendingSignal) {
        self.pending.push(signal);
    }

    /// An ordinary (non-streamed) transaction committed: every buffered non-streamed signal in it
    /// resolves with this commit LSN.
    pub fn on_commit(&mut self, commit_lsn: Lsn, waiters: &WatermarkWaiters) {
        for sig in extract(&mut self.pending, |s| s.xid.is_none()) {
            waiters.resolve(
                sig.reload_id,
                sig.chunk_no,
                Echo {
                    commit_lsn,
                    embedded_lsn: sig.embedded_lsn,
                },
            );
        }
    }

    /// Can't-happen defense: a signal insert arrived inside a streamed transaction. The surviving
    /// (non-aborted) buffered streamed signals resolve at its `Stream Commit` — with a warning,
    /// because a streamed signal txn means something upstream changed.
    pub fn on_stream_commit(&mut self, top_xid: u32, commit_lsn: Lsn, waiters: &WatermarkWaiters) {
        for sig in extract(&mut self.pending, |s| s.top_xid == Some(top_xid)) {
            tracing::warn!(
                reload_id = %sig.reload_id,
                chunk_no = sig.chunk_no,
                "reload_signal echo arrived inside a STREAMED transaction (single-row signal \
                 txns should never stream); resolving at Stream Commit"
            );
            waiters.resolve(
                sig.reload_id,
                sig.chunk_no,
                Echo {
                    commit_lsn,
                    embedded_lsn: sig.embedded_lsn,
                },
            );
        }
    }

    /// `Stream Abort`: `sub == top` aborts the whole transaction (drop every signal buffered under
    /// it); `sub != top` is a rolled-back savepoint — drop exactly the signals tagged with that
    /// sub-xid (the per-message xid IS the sub-xid), because the commit never carried them and
    /// resolving would stamp a chunk with a watermark for rows that don't exist.
    pub fn on_stream_abort(&mut self, top_xid: u32, sub_xid: u32) {
        let dropped = extract(&mut self.pending, |s| {
            s.top_xid == Some(top_xid) && (top_xid == sub_xid || s.xid == Some(sub_xid))
        });
        for sig in &dropped {
            tracing::warn!(
                reload_id = %sig.reload_id,
                chunk_no = sig.chunk_no,
                top_xid,
                sub_xid,
                "buffered reload_signal dropped by Stream Abort (never resolves a waiter)"
            );
        }
    }

    /// Whether no signal is awaiting a commit — the steady state, since a reload signal is rare.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Drain every element matching `pred` out of `v`, preserving both halves' relative order and
/// retaining `v`'s allocation for the next reload-signal transaction.
///
/// `extract_if` keeps the linear complexity of `mem::take` + `partition` and
/// ordering while avoiding a fresh survivor buffer. `Vec` remains correct because callers drain by
/// predicate, never from the front of a queue.
fn extract<T>(v: &mut Vec<T>, mut pred: impl FnMut(&T) -> bool) -> Vec<T> {
    v.extract_if(.., |t| pred(t)).collect()
}

#[cfg(test)]
#[path = "reload_signal_test.rs"]
mod tests;
