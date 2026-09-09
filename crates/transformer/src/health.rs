//! The transformer's K8s health endpoints (transformer §8.3) — **the catch-up-lag trap avoided**.
//!
//! - `/startup` — 200 once bootstrap completes (gates lease/fence acquisition + DuckLake attach).
//! - `/ready`   — 200 iff local bootstrap is done, the initial frozen all-table reconciliation has
//!   published, and the process is **not quarantined**. It is never gated on ordinary WAL backlog:
//!   a legitimately-behind streaming transformer is still ready. A **quarantined** table (a failed
//!   lossy DDL cast) degrades `/ready` — a loud, terminal signal, not a silent continue.
//! - `/healthz` — liveness = *progress*, read from an in-memory `last_poll_completed_at` stamped every
//!   cycle (even a no-op). It reflects **no** lag metric — an idle-but-healthy transformer must stay live.

use axum::{
    Router, extract::State, http::StatusCode, http::header, response::IntoResponse, routing::get,
};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// The transformer's health lifecycle — exactly one of three states.
///
/// [`Quarantined`](TransformerPhase::Quarantined) implies bootstrap finished: its producer is a failed
/// lossy DDL cast in the apply loop, which cannot run before bootstrap. That implication keeps
/// `/startup` satisfied while `/ready` degrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TransformerPhase {
    /// Leases/fences are not yet held and DuckLake connections are not yet open. Both `/startup` and `/ready`
    /// answer 503. The default, and byte `0` — which `AtomicPhase`'s zero default depends on.
    #[default]
    Bootstrapping = 0,
    /// Local bootstrap is complete. `/ready` additionally consults the generation-published latch.
    Ready = 1,
    /// Latched by a failed lossy DDL cast. A reload rebuild is its only exit.
    Quarantined = 2,
}

/// An out-of-range transformer phase byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPhase(pub u8);

impl TryFrom<u8> for TransformerPhase {
    type Error = InvalidPhase;

    /// Decode a phase byte read back out of the atomic the probes share.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPhase`] carrying `byte` for anything other than `0`
    /// ([`TransformerPhase::Bootstrapping`]), `1` ([`TransformerPhase::Ready`]), or `2`
    /// ([`TransformerPhase::Quarantined`]). Only this module's typed stores write that atomic, so an
    /// unknown byte is a bug here rather than a probe input.
    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        // AtomicU8::default(), the typed stores, the quarantine compare_exchange, and this decoder
        // all rely on these exact bytes.
        const {
            assert!(
                TransformerPhase::Bootstrapping as u8 == 0,
                "TransformerPhase::Bootstrapping must stay byte 0 because AtomicPhase defaults to zero"
            );
            assert!(
                TransformerPhase::Ready as u8 == 1,
                "TransformerPhase::Ready must stay byte 1 so AtomicPhase store and decode agree"
            );
            assert!(
                TransformerPhase::Quarantined as u8 == 2,
                "TransformerPhase::Quarantined must stay byte 2 or clear_quarantine's compare_exchange \
                 swaps the wrong phase"
            );
        }

        match byte {
            0 => Ok(Self::Bootstrapping),
            1 => Ok(Self::Ready),
            2 => Ok(Self::Quarantined),
            other => Err(InvalidPhase(other)),
        }
    }
}

/// A [`TransformerPhase`] stored atomically. Only enum values can be written through this wrapper.
#[derive(Debug, Default)]
struct AtomicPhase(AtomicU8);

impl AtomicPhase {
    fn store(&self, phase: TransformerPhase) {
        // Release: `Ready` publishes bootstrap (leases held, files open) and `Quarantined` publishes
        // the failed cast that latched it. SeqCst would only add a total order with other atomics,
        // and this is the sole atomic every probe reads.
        self.0.store(phase as u8, Ordering::Release);
    }

    fn load(&self) -> TransformerPhase {
        // Acquire: pairs with the Release store, so a probe that sees a phase sees what produced it.
        let byte = self.0.load(Ordering::Acquire);
        TransformerPhase::try_from(byte).unwrap_or_else(|InvalidPhase(invalid)| {
            tracing::error!(
                phase_byte = invalid,
                "invalid transformer health phase byte; defaulting to bootstrapping"
            );
            TransformerPhase::Bootstrapping
        })
    }

    fn transition(&self, from: TransformerPhase, to: TransformerPhase) -> bool {
        // AcqRel on success: the read half sees the store that latched `from` (the quarantining
        // cast), the write half publishes `to` like the plain store above. Relaxed on failure —
        // the losing byte is dropped on the floor, so nothing is read behind it.
        self.0
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
}

/// The state the three Kubernetes probes read, shared by every table worker.
///
/// One process holds exactly one of these behind an `Arc`: the phase latch is process-wide (a
/// single quarantined table degrades the whole pod's `/ready`), and the poll stamp is whichever
/// worker finished a cycle most recently.
#[derive(Debug, Default)]
pub struct TransformerState {
    phase: AtomicPhase,
    /// Idempotent table identities currently requiring a replacement generation. A set (rather
    /// than a boolean/counter) prevents one table's successful reload from clearing another
    /// table's independent quarantine and makes repeated polls harmless.
    // Mutations are brief, synchronous set operations.
    // LOCK-CHOICE: parking_lot::Mutex — the guard is never held across an await.
    quarantined_tables: Mutex<HashSet<(String, String)>>,
    /// Fresh all-table reconciliation gates external readiness independently of local startup.
    /// Keeping this separate from quarantine means a repaired table cannot accidentally advertise
    /// ready before the rest of its bootstrap group has published.
    generation_ready: AtomicBool,
    /// The end of the last poll cycle — liveness proof, NOT a lag metric. `None` until bootstrap ends.
    // LOCK-CHOICE: parking_lot::Mutex — poll-cycle writes dominate the one-expression kubelet read.
    last_poll_completed_at: Mutex<Option<Instant>>,
}

impl TransformerState {
    /// A fresh state, already wrapped in the `Arc` every caller needs.
    ///
    /// Hands back `Arc<Self>` rather than `Self` because there is no useful unshared owner: the
    /// probe router and the workers both hold it. That shape is why `clippy::new_ret_no_self` has a
    /// scoped allow on the extractor's equivalent.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(TransformerState::default())
    }

    /// Local bootstrap finished for an already-published generation: leases held + files open →
    /// `/startup` and `/ready` answer 200.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the quarantine lock fences phase publication against a concurrent table quarantine"
    )]
    pub fn mark_ready(&self) {
        self.generation_ready.store(true, Ordering::Release);
        let quarantined = self.quarantined_tables.lock();
        let phase = if quarantined.is_empty() {
            TransformerPhase::Ready
        } else {
            TransformerPhase::Quarantined
        };
        self.phase.store(phase);
    }

    /// Local bootstrap finished, but the control generation is still reconciling its frozen table
    /// group. `/startup` succeeds and liveness runs; `/ready` remains gated.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the quarantine lock fences phase publication against a concurrent table quarantine"
    )]
    pub fn mark_reconciling(&self) {
        self.generation_ready.store(false, Ordering::Release);
        let quarantined = self.quarantined_tables.lock();
        let phase = if quarantined.is_empty() {
            TransformerPhase::Ready
        } else {
            TransformerPhase::Quarantined
        };
        self.phase.store(phase);
    }

    /// The extractor promoted this generation after every table shadow was published.
    pub fn mark_generation_ready(&self) {
        self.generation_ready.store(true, Ordering::Release);
    }

    /// The extractor durably retired this generation before replacing its lost slot. Drop readiness
    /// immediately; the transformer process then drains and exits while the successor is established.
    pub fn mark_generation_retired(&self) {
        self.generation_ready.store(false, Ordering::Release);
    }

    // The four probe reads in this impl (`is_started`, `is_ready`, `is_quarantined`, `is_live`)
    // answer a question and change nothing, so discarding one is always a bug — hence the explicit
    // `#[must_use]` on each. `clippy::must_use_candidate` reaches none of them: `&self` on a struct
    // with interior mutability (the atomic phase, the poll-stamp mutex) reads to that lint as a
    // mutable — therefore side-effecting — argument. The mutators between them return `()` and
    // correctly carry nothing.
    /// `/startup` gate: bootstrap finished, including a later quarantine.
    #[must_use]
    pub fn is_started(&self) -> bool {
        matches!(
            self.phase.load(),
            TransformerPhase::Ready | TransformerPhase::Quarantined
        )
    }

    /// `/ready` answers 200 only after local startup and generation publication.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.phase.load(), TransformerPhase::Ready)
            && self.generation_ready.load(Ordering::Acquire)
    }

    /// Latch the quarantine flag — a failed lossy DDL cast. `/ready` degrades and stays
    /// degraded; the caller also logs an error-level alert and exits. The latch has exactly one
    /// exit: a single-table-reload rebuild, which REPLACES the data instead of
    /// retrying the cast on it ([`TransformerState::clear_quarantine`]).
    pub fn quarantine(&self) {
        self.quarantine_table("__walrus_internal", "legacy_process_quarantine");
    }

    /// Degrade readiness for one table. Repeating the same table identity is idempotent. Schema and
    /// table stay separate because joining legal quoted identifiers with `.` is not injective.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the quarantine lock must cover both set insertion and phase publication"
    )]
    pub fn quarantine_table(&self, schema: &str, table: &str) {
        let mut quarantined = self.quarantined_tables.lock();
        quarantined.insert((schema.to_string(), table.to_string()));
        self.phase.store(TransformerPhase::Quarantined);
    }

    /// The one legitimate quarantine exit: a reload rebuild just recreated the table at
    /// the attempt's schema_version, so the lossy cast the latch recorded no longer applies to
    /// anything — `/ready` recovers.
    pub fn clear_quarantine(&self) {
        self.clear_table_quarantine("__walrus_internal", "legacy_process_quarantine");
    }

    /// Clear one table's quarantine only. Readiness recovers after the final table is repaired.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the quarantine lock prevents a concurrent insertion from being cleared by this phase transition"
    )]
    pub fn clear_table_quarantine(&self, schema: &str, table: &str) {
        let mut quarantined = self.quarantined_tables.lock();
        quarantined.remove(&(schema.to_string(), table.to_string()));
        let empty = quarantined.is_empty();
        if empty {
            let _transitioned = self
                .phase
                .transition(TransformerPhase::Quarantined, TransformerPhase::Ready);
        }
    }

    /// Whether the quarantine latch is set — the state `/ready` reports 503 for while `/startup`
    /// stays satisfied.
    #[must_use]
    pub fn is_quarantined(&self) -> bool {
        matches!(self.phase.load(), TransformerPhase::Quarantined)
    }

    /// Stamp progress — called at the end of **every** poll cycle (and once at bootstrap end so an
    /// idle transformer stays live).
    pub fn stamp_poll(&self) {
        *self.last_poll_completed_at.lock() = Some(Instant::now());
    }

    /// Liveness = we have completed at least one cycle (progress stamped). Deliberately lag-free.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.last_poll_completed_at.lock().is_some()
    }
}

async fn startup(State(s): State<Arc<TransformerState>>) -> StatusCode {
    ok_or_unavailable(s.is_started())
}
async fn ready(State(s): State<Arc<TransformerState>>) -> StatusCode {
    ok_or_unavailable(s.is_ready())
}
async fn healthz(State(s): State<Arc<TransformerState>>) -> StatusCode {
    ok_or_unavailable(s.is_live())
}

const fn ok_or_unavailable(ok: bool) -> StatusCode {
    if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The Prometheus text exposition — stateless; reads the process-wide recorder.
async fn metrics() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        common::metrics::render(),
    )
}

/// The probe + metrics router, with the shared state injected.
///
/// Deliberately **not** `#[must_use]`, for the reason the extractor's `health::router` records: axum's
/// `Router` already carries the attribute, so a second bare one is `clippy::double_must_use`.
pub fn router(state: Arc<TransformerState>) -> Router {
    Router::new()
        .route("/startup", get(startup))
        .route("/ready", get(ready))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Serve transformer health and metrics routes until `shutdown` is cancelled.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if Axum fails while accepting or serving a connection on `listener`.
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    state: Arc<TransformerState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
