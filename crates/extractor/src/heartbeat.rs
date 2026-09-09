//! The idle heartbeat and its round-trip liveness signal (§1.9).
//!
//! An idle *published* set of tables cannot advance `confirmed_flush_lsn` — no commits flow, so
//! `restart_lsn` freezes and retained WAL grows unbounded until the source disk fills or the slot is
//! invalidated. The mitigation is an **extractor-driven, idle-triggered** beat: one `UPDATE` to the
//! published `public.walrus_heartbeat` table over a *separate* ordinary SQL connection. Because that table
//! rides `walrus_pub`, the beat decodes back through the replication stream — and *that return* is a
//! round-trip liveness signal, distinct from both the keepalive and the durability
//! checkpoint.
//!
//! **Three LSNs, kept apart:**
//! - *keepalive/received* (`write`) — moves **unconditionally** on every frame ([`crate::replication`]);
//! - the *beat's established point* — where an idle beat's commit lets the checkpoint advance;
//! - *`confirmed_flush_lsn`* (`flush`/`apply`) — moves **only after durability** ([`crate::checkpoint`]).
//!
//! **Never self-harm:** a stale round-trip during a legitimate catch-up sets a non-gating `degraded`
//! flag on `/ready`. It is *never* a `livenessProbe` kill and *never* a hard readiness gate — a
//! catching-up extractor is stale by design (same reason liveness is never slot-lag, §4.3).

use common::TupleValue;
use std::time::Duration;
use tokio::time::Instant;

/// Bounds-validated in `config.rs`: both `> 0` and `idle_after < roundtrip_deadline`.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    /// Fire a beat only after the published tables have been idle this long (monotonic).
    pub idle_after: Duration,
    /// A beat still un-returned after this long marks the extractor `degraded` (observability only).
    pub roundtrip_deadline: Duration,
}

/// OIDs (and layout) of walrus's own published tables — consumed for control, never materialised.
#[derive(Debug, Default, Clone)]
pub struct InternalTables {
    /// `public.walrus_heartbeat`'s OID, learned from its `Relation` message; `None` until one arrives.
    pub heartbeat_oid: Option<u32>,
    /// Column index of `public.walrus_heartbeat.beat_seq`, learned from its `Relation` message.
    heartbeat_beat_seq_col: Option<usize>,
    /// `public.walrus_ddl_audit`'s OID — its INSERTs are the extractor's DDL signal, consumed for control
    /// and NEVER materialised as a `<table>`/`<table>_raw` file.
    pub ddl_audit_oid: Option<u32>,
    /// The `ddl_audit` relation shape (its INSERTs are never cached), so [`crate::ddl::DdlEvent`] can
    /// parse a decoded tuple by column name.
    ddl_audit_rel: Option<common::PgRelation>,
    /// `public.walrus_reload_signal`'s OID — its INSERTs are chunk-watermark echoes, consumed to
    /// resolve [`crate::reload_signal::WatermarkWaiters`] and NEVER materialised.
    pub reload_signal_oid: Option<u32>,
    /// The `reload_signal` relation shape, so [`crate::reload_signal::PendingSignal`] can parse a
    /// decoded tuple by column name.
    reload_signal_rel: Option<common::PgRelation>,
    /// `public.walrus_reload_event`'s OID. Requests and fences are commit-gated control events, never data.
    pub reload_event_oid: Option<u32>,
    /// Relation shape used to parse append-only request/fence tuples by column name.
    reload_event_rel: Option<common::PgRelation>,
}

impl InternalTables {
    /// Is this relation OID one of walrus's own control tables (never staged as user data)?
    /// `public.walrus_heartbeat`, `public.walrus_ddl_audit`, and `public.walrus_reload_signal` are all consumed
    /// specially, never materialised.
    #[must_use]
    pub fn is_internal(&self, rel_oid: u32) -> bool {
        self.heartbeat_oid == Some(rel_oid)
            || self.ddl_audit_oid == Some(rel_oid)
            || self.reload_signal_oid == Some(rel_oid)
            || self.reload_event_oid == Some(rel_oid)
    }

    /// Whether this OID is the DDL-audit table (its INSERTs drive [`crate::ddl::DdlConsumer`]).
    #[must_use]
    pub fn is_ddl_audit(&self, rel_oid: u32) -> bool {
        self.ddl_audit_oid == Some(rel_oid)
    }

    /// Whether this OID is the heartbeat table (only its tuples carry a `beat_seq`).
    #[must_use]
    pub fn is_heartbeat(&self, rel_oid: u32) -> bool {
        self.heartbeat_oid == Some(rel_oid)
    }

    /// Whether this OID is the reload signal table (its INSERTs resolve watermark waiters).
    #[must_use]
    pub fn is_reload_signal(&self, rel_oid: u32) -> bool {
        self.reload_signal_oid == Some(rel_oid)
    }

    /// Whether this OID is the append-only reload request/fence relation.
    #[must_use]
    pub fn is_reload_event(&self, rel_oid: u32) -> bool {
        self.reload_event_oid == Some(rel_oid)
    }

    /// Learn a walrus-internal table's OID + relevant column offsets from its `Relation` message.
    /// The internal-table changes are never cached in the [`crate::relcache::RelationCache`], so any
    /// column offsets needed later (heartbeat `beat_seq`) are captured here.
    pub fn note_relation(&mut self, relation: &common::PgRelation) {
        if relation.schema == "public" && relation.name == "walrus_heartbeat" {
            self.heartbeat_oid = Some(relation.oid);
            self.heartbeat_beat_seq_col =
                relation.columns.iter().position(|c| c.name == "beat_seq");
        }
        if relation.schema == "public" && relation.name == "walrus_ddl_audit" {
            self.ddl_audit_oid = Some(relation.oid);
            self.ddl_audit_rel = Some(relation.clone());
        }
        if relation.schema == "public" && relation.name == "walrus_reload_signal" {
            self.reload_signal_oid = Some(relation.oid);
            self.reload_signal_rel = Some(relation.clone());
        }
        if relation.schema == "public" && relation.name == "walrus_reload_event" {
            self.reload_event_oid = Some(relation.oid);
            self.reload_event_rel = Some(relation.clone());
        }
    }

    /// The `ddl_audit` relation shape, once its `Relation` message has been seen.
    #[must_use]
    pub const fn ddl_audit_rel(&self) -> Option<&common::PgRelation> {
        self.ddl_audit_rel.as_ref()
    }

    /// The `reload_signal` relation shape, once its `Relation` message has been seen.
    #[must_use]
    pub const fn reload_signal_rel(&self) -> Option<&common::PgRelation> {
        self.reload_signal_rel.as_ref()
    }

    /// The `reload_event` relation shape, once its `Relation` message has been seen.
    #[must_use]
    pub const fn reload_event_rel(&self) -> Option<&common::PgRelation> {
        self.reload_event_rel.as_ref()
    }

    /// Extract the returned `beat_seq` from a decoded `public.walrus_heartbeat` new-tuple (text format).
    ///
    /// The non-text images are listed rather than absorbed by a wildcard: a new [`TupleValue`]
    /// variant must decide here whether it can carry a beat seq, instead of silently reading as
    /// "no beat".
    #[deny(clippy::wildcard_enum_match_arm)]
    pub fn beat_seq_of(&self, new: &[TupleValue]) -> Option<i64> {
        let idx = self.heartbeat_beat_seq_col?;
        match new.get(idx)? {
            TupleValue::Text(s) => s.parse::<i64>().ok(),
            TupleValue::Null | TupleValue::UnchangedToast | TupleValue::Binary(_) => None,
        }
    }
}

/// Pure idle/round-trip bookkeeping — no I/O, so it is unit-tested directly (the async
/// [`Heartbeat`] wraps it around a real SQL connection).
#[derive(Debug, Clone)]
struct BeatState {
    cfg: HeartbeatConfig,
    last_beat: Option<Instant>,
    /// A beat's seq written and awaiting its return through the stream; `None` once it returns.
    pending_seq: Option<i64>,
    /// The last time a beat completed its round-trip (observability / logs).
    last_roundtrip: Option<Instant>,
}

impl BeatState {
    const fn new(cfg: HeartbeatConfig) -> Self {
        BeatState {
            cfg,
            last_beat: None,
            pending_seq: None,
            last_roundtrip: None,
        }
    }

    /// Fire iff idle on **both** clocks: no user activity for `idle_after`, and no beat within
    /// `idle_after` (so beats never pile up under a still publication).
    fn should_beat(&self, now: Instant, last_activity: Instant) -> bool {
        let idle_activity = now.saturating_duration_since(last_activity) >= self.cfg.idle_after;
        let idle_beat = self
            .last_beat
            .is_none_or(|t| now.saturating_duration_since(t) >= self.cfg.idle_after);
        idle_activity && idle_beat
    }

    const fn on_beat_sent(&mut self, seq: i64, now: Instant) {
        self.last_beat = Some(now);
        self.pending_seq = Some(seq);
    }

    /// The beat decoded back: `beat_seq` is monotonic, so a returned seq **≥** pending closes the
    /// round-trip (a coalesced/late beat still counts — never require exact lock-step).
    fn observe_return(&mut self, beat_seq: i64, now: Instant) {
        if self.pending_seq.is_some_and(|p| beat_seq >= p) {
            self.pending_seq = None;
            self.last_roundtrip = Some(now);
        }
    }

    /// Non-gating: an outstanding beat that has not returned within `roundtrip_deadline`. Under steady
    /// traffic (no pending beat) this is always `false` — the stream itself proves liveness.
    fn is_degraded(&self, now: Instant) -> bool {
        matches!(
            (self.pending_seq, self.last_beat),
            (Some(_), Some(sent))
                if now.saturating_duration_since(sent) > self.cfg.roundtrip_deadline
        )
    }
}

/// Owns a *separate* ordinary SQL connection to the source (distinct from the read-only replication
/// connection) plus the pure `BeatState`. The beat **must** ride the published `public.walrus_heartbeat`
/// table or `pgoutput` filters it out and there is no round-trip.
#[derive(Debug)]
pub struct Heartbeat {
    sql: tokio_postgres::Client,
    instance: String,
    state: BeatState,
}

impl Heartbeat {
    /// Open the ordinary SQL connection and drive it in the background (`NoTls` — the dev/source auth
    /// is `trust`; TLS is the operator's network concern, not this control path).
    ///
    /// # Errors
    ///
    /// Returns [`HeartbeatError::Connect`] if the source SQL connection or startup handshake fails.
    pub async fn connect(
        dsn: &str,
        instance: impl Into<String>,
        cfg: HeartbeatConfig,
    ) -> Result<Self, HeartbeatError> {
        let instance = instance.into();
        let (sql, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(HeartbeatError::Connect)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = ?e, "heartbeat SQL connection closed");
            }
        });
        Ok(Heartbeat {
            sql,
            instance,
            state: BeatState::new(cfg),
        })
    }

    /// The idle window — the decode loop uses it to pace its beat check.
    #[must_use]
    pub const fn idle_after(&self) -> Duration {
        self.state.cfg.idle_after
    }

    /// Fire exactly one beat iff idle on both clocks; returns the seq written, or `None` (suppressed).
    /// The `UPDATE` rides the **published** `public.walrus_heartbeat` so it decodes back to us.
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe as a logical beat.** PostgreSQL may commit the update before a dropped query
    /// future returns, leaving the local sent-sequence state unchanged. The decode loop awaits this
    /// inside the selected heartbeat arm, so sibling frame readiness cannot cancel it; a retry would
    /// only advance the monotonic beat sequence again.
    ///
    /// # Errors
    ///
    /// Returns [`HeartbeatError::Beat`] if the heartbeat update or returned sequence read fails.
    pub async fn maybe_beat(
        &mut self,
        now: Instant,
        last_activity: Instant,
    ) -> Result<Option<i64>, HeartbeatError> {
        if !self.state.should_beat(now, last_activity) {
            return Ok(None);
        }
        let row = self
            .sql
            .query_one(
                "UPDATE public.walrus_heartbeat \
                 SET beat_seq = beat_seq + 1, ts = now(), extractor_instance = $1 \
                 WHERE id = 1 RETURNING beat_seq",
                &[&self.instance],
            )
            .await
            .map_err(HeartbeatError::Beat)?;
        let seq: i64 = row.get(0);
        self.state.on_beat_sent(seq, now);
        Ok(Some(seq))
    }

    /// The heartbeat relation decoded back through the stream: record the round-trip.
    pub fn observe_return(&mut self, beat_seq: i64, now: Instant) {
        self.state.observe_return(beat_seq, now);
    }

    /// Non-gating health signal (see `BeatState::is_degraded`).
    #[must_use]
    pub fn is_degraded(&self, now: Instant) -> bool {
        self.state.is_degraded(now)
    }
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HeartbeatError {
    /// The heartbeat's own SQL connection could not be opened. Separate from the replication
    /// connection on purpose: a beat is an ordinary write, not a stream operation.
    #[error("connect heartbeat SQL connection: {0}")]
    Connect(#[source] tokio_postgres::Error),
    /// The heartbeat UPDATE failed, or its returned sequence could not be read.
    #[error("fire heartbeat UPDATE: {0}")]
    Beat(#[source] tokio_postgres::Error),
}

impl From<HeartbeatError> for common::Error {
    fn from(e: HeartbeatError) -> Self {
        common::Error::SourceDb(e.to_string())
    }
}

#[cfg(test)]
#[path = "heartbeat_test.rs"]
mod tests;
