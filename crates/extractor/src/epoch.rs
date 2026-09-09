//! Slot-loss classification and the total-restart decision (§1.8). The single lifelong slot is resumed
//! forever; the **only** time walrus opens a new one is when — on a **successful** source connection —
//! the slot is authoritatively gone: **absent** (`pg_replication_slots` empty) or **invalidated**
//! (`wal_status = 'lost'` after `max_slot_wal_keep_size` was exceeded). Then the change history since
//! `confirmed_flush_lsn` is permanently lost and the only correct recovery is a whole-system re-sync
//! under a bumped epoch.
//!
//! The single most dangerous bug here is a **false positive** — treating a network blip as slot loss
//! would replace and re-reconcile the whole system on every hiccup. So classification is split from the
//! decision: [`classify_slot`] does the I/O (and maps a query failure to [`SlotStatus::Unreachable`]),
//! and the pure [`decide`] guarantees [`Unreachable`](SlotStatus::Unreachable) routes to a retry,
//! **never** a fresh slot. Only a catalog that authoritatively says "connected, slot gone" opens a
//! new generation.

use common::Lsn;

/// Postgres's `pg_replication_slots.wal_status` vocabulary, decoded once per catalog read.
///
/// Two decisions downstream turn on this single column — the categorical gauge and the invalidation
/// test — and as raw string compares each spelled `"lost"` for itself. A typo in the second is
/// precisely the bug this module exists to prevent, and it would be a silent one: the slot would
/// classify as healthy forever while the WAL it needs was already gone. One decode leaves one place
/// to get the word wrong, and the compiler checks every use of it after that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalStatus {
    /// The WAL the slot needs is within `max_wal_size` — the healthy steady state.
    Reserved,
    /// Retained past `max_wal_size`, still inside `max_slot_wal_keep_size`.
    Extended,
    /// Past `max_slot_wal_keep_size`: the next checkpoint may invalidate the slot.
    Unreserved,
    /// Invalidated — the WAL this slot needed has been removed. The one value that is slot loss.
    Lost,
}

impl WalStatus {
    /// Decode the catalog text; `None` for SQL NULL and for any word a later PostgreSQL adds.
    ///
    /// `None` has to stay exactly as harmless as `Reserved` at every call site: a value walrus does
    /// not recognise is no evidence the slot was invalidated, and a false positive here rebuilds
    /// the whole system (see this module's note).
    fn from_catalog(text: &str) -> Option<Self> {
        match text {
            "reserved" => Some(Self::Reserved),
            "extended" => Some(Self::Extended),
            "unreserved" => Some(Self::Unreserved),
            "lost" => Some(Self::Lost),
            _ => None,
        }
    }

    /// This status as the `walrus_extractor_wal_status` gauge code. `deploy/observability/` reads these
    /// exact numbers — the stat panel is titled `0 reserved · 1 unreserved · 2 lost` and
    /// `WalrusSlotWalStatusDegraded` pages at `>= 1` — so `extended`, which is retention working as
    /// designed rather than a problem, has to stay a healthy `0`.
    const fn gauge_code(self) -> u8 {
        match self {
            Self::Reserved | Self::Extended => 0,
            Self::Unreserved => 1,
            Self::Lost => 2,
        }
    }
}

/// Result of inspecting the slot on a source connection. Only [`Absent`](SlotStatus::Absent) /
/// [`Invalidated`](SlotStatus::Invalidated) — observed on a **successful** connection — are slot
/// loss; [`Unreachable`](SlotStatus::Unreachable) is a hiccup (retry, never total-restart).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    /// Present and usable — resume from `confirmed_flush`.
    Healthy {
        /// Last WAL position durably acknowledged by the slot.
        confirmed_flush: Lsn,
    },
    /// Connected, but `pg_replication_slots` has no row → the slot was dropped.
    Absent,
    /// Connected, but `wal_status = 'lost'` → the slot was invalidated (its WAL is gone).
    Invalidated,
    /// The classification query itself failed (connection lost) → transient, retry via backoff.
    Unreachable,
}

/// The bootstrap action a classified slot implies — the whole false-positive guard, as a pure function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAction {
    /// Slot healthy → resume streaming from `confirmed_flush`.
    Resume {
        /// Durable WAL position from which streaming resumes.
        confirmed_flush: Lsn,
    },
    /// Slot gone on a successful connection → open a fresh slot + reconcile every table. A **total-restart**
    /// (epoch bump, loud alert) when a prior epoch exists, or the very first bootstrap when none does —
    /// the caller distinguishes those only to decide whether to alert.
    FreshSlot,
    /// Could not classify (connection hiccup) → retry via the bootstrap backoff; **never** bump the epoch.
    Retry,
}

/// Decide what to do from a classified slot. Pure (no I/O) so the guard is unit-tested:
/// [`Unreachable`](SlotStatus::Unreachable) must map to [`Retry`](SlotAction::Retry), and only
/// [`Absent`](SlotStatus::Absent) / [`Invalidated`](SlotStatus::Invalidated) to
/// [`FreshSlot`](SlotAction::FreshSlot).
#[must_use]
pub const fn decide(status: SlotStatus) -> SlotAction {
    match status {
        SlotStatus::Healthy { confirmed_flush } => SlotAction::Resume { confirmed_flush },
        SlotStatus::Absent | SlotStatus::Invalidated => SlotAction::FreshSlot,
        SlotStatus::Unreachable => SlotAction::Retry,
    }
}

/// Classify the slot over a **live** source connection (post-preflight): a present row with
/// `wal_status <> 'lost'` is [`Healthy`](SlotStatus::Healthy); `wal_status = 'lost'` is
/// [`Invalidated`](SlotStatus::Invalidated); no row is [`Absent`](SlotStatus::Absent); a query error
/// is [`Unreachable`](SlotStatus::Unreachable) (the connection died — a hiccup, not slot loss).
/// `wal_status` is the PG14+ invalidation signal, distinct from an empty result (a dropped slot) —
/// both are handled.
pub async fn classify_slot(client: &tokio_postgres::Client, slot: &str) -> SlotStatus {
    let rows = match client
        .query(
            "SELECT wal_status, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = ?e, slot, "could not read pg_replication_slots (transient) → Unreachable");
            return SlotStatus::Unreachable;
        }
    };
    let Some(row) = rows.first() else {
        return SlotStatus::Absent;
    };
    let reported: Option<String> = row.get(0);
    let wal_status = reported.as_deref().and_then(WalStatus::from_catalog);
    // Expose the categorical slot health as a gauge from this existing read — no extra query.
    // NULL and any word this walrus does not know fall to 0, the reserved/extended code, exactly as
    // they fail the `Lost` test below: neither is evidence the slot was invalidated.
    common::metrics::set_wal_status(wal_status.map_or(0, WalStatus::gauge_code));
    if wal_status == Some(WalStatus::Lost) {
        return SlotStatus::Invalidated;
    }
    let confirmed: Option<String> = row.get(1);
    let confirmed_flush = confirmed
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(Lsn::ZERO);
    SlotStatus::Healthy { confirmed_flush }
}

#[cfg(test)]
#[path = "epoch_test.rs"]
mod tests;
