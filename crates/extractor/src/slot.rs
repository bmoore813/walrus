//! Replication-slot management (bootstrap step 4).
//!
//! Verify the slot exists and read its resume position (`confirmed_flush_lsn`), or create it. Slot
//! *management* is done over an ordinary SQL connection (`pg_replication_slots` /
//! `pg_create_logical_replication_slot`) — the `START_REPLICATION` streaming itself is the
//! hand-rolled connection in [`crate::replication`].
//!
//! The creation LSN retains WAL before the source-WAL-triggered all-table reconciliation establishes
//! each table's own `F`; bootstrap and later repair therefore use one reconciliation path.

use anyhow::Context as _;
use common::Lsn;

const DROP_INVALIDATED_SLOT_SQL: &str = "SELECT pg_drop_replication_slot(slot_name)
     FROM pg_replication_slots
     WHERE slot_name = $1 AND wal_status = 'lost'";

/// A pre-existing slot's resume position.
#[derive(Debug, Clone, Copy)]
pub struct SlotInfo {
    /// The oldest WAL the slot still pins on the source — what actually holds disk.
    pub restart_lsn: Lsn,
    /// The position the slot's consumer has confirmed. This, not `restart_lsn`, is where streaming
    /// resumes.
    pub confirmed_flush_lsn: Lsn,
}

/// Whether the slot already existed or we just created it.
#[derive(Debug, Clone, Copy)]
pub enum SlotResume {
    /// The slot was already there; resume from what it has confirmed.
    Existing(SlotInfo),
    /// The slot was created by this run, so there is no history to resume — only its creation point.
    Created {
        /// The LSN at which the new slot became consistent; where streaming starts.
        consistent_point: Lsn,
    },
}

impl SlotResume {
    /// The LSN to hand `START_REPLICATION`. Resuming an existing slot means its
    /// `confirmed_flush_lsn`; a fresh slot means its creation point. (The server clamps up to its own
    /// value regardless.)
    #[must_use]
    pub const fn start_lsn(&self) -> Lsn {
        match self {
            SlotResume::Existing(info) => info.confirmed_flush_lsn,
            SlotResume::Created {
                consistent_point, ..
            } => *consistent_point,
        }
    }
}

/// `field` names the catalog column the text came from, so a bad value says *which* one it was.
/// Attaching context (rather than formatting the cause into an `anyhow!` message) keeps the typed
/// `LsnParseError` — which already carries the offending text — in the chain, so `{:#}` reads
/// "parse restart_lsn as a Postgres LSN: invalid LSN …" and `downcast_ref` still finds it.
fn parse_lsn(s: &str, field: &'static str) -> anyhow::Result<Lsn> {
    s.parse()
        .with_context(|| format!("parse {field} as a Postgres LSN"))
}

/// Read a slot's resume position **without** creating it — `None` if it does not exist. The bootstrap
/// uses this to decide between resuming (`Some`) and first-time reconciliation (`None`).
///
/// # Errors
///
/// Returns [`anyhow::Error`] if `pg_replication_slots` cannot be queried or either stored LSN is
/// malformed.
pub async fn read_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<Option<SlotInfo>> {
    let rows = client
        .query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("read replication slot {slot:?} from pg_replication_slots"))?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let restart: Option<String> = row.get(0);
    let confirmed: Option<String> = row.get(1);
    Ok(Some(SlotInfo {
        restart_lsn: restart
            .as_deref()
            .map(|s| parse_lsn(s, "restart_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO),
        confirmed_flush_lsn: confirmed
            .as_deref()
            .map(|s| parse_lsn(s, "confirmed_flush_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO),
    }))
}

/// Verify the slot (reading `restart_lsn` / `confirmed_flush_lsn`), or create it via SQL.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if slot inspection/creation fails or a returned LSN cannot be parsed.
pub async fn verify_or_create_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<SlotResume> {
    let rows = client
        .query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("verify replication slot {slot:?} in pg_replication_slots"))?;

    if let Some(row) = rows.first() {
        // A freshly-created slot can have NULL LSNs until first use — treat NULL as ZERO.
        let restart: Option<String> = row.get(0);
        let confirmed: Option<String> = row.get(1);
        let restart_lsn = restart
            .as_deref()
            .map(|s| parse_lsn(s, "restart_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO);
        let confirmed_flush_lsn = confirmed
            .as_deref()
            .map(|s| parse_lsn(s, "confirmed_flush_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO);
        return Ok(SlotResume::Existing(SlotInfo {
            restart_lsn,
            confirmed_flush_lsn,
        }));
    }

    let row = match client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
    {
        Ok(row) => row,
        Err(error) if error.code() == Some(&tokio_postgres::error::SqlState::DUPLICATE_OBJECT) => {
            let info = read_slot(client, slot).await?.with_context(|| {
                format!("replication slot {slot:?} disappeared during concurrent creation")
            })?;
            return Ok(SlotResume::Existing(info));
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("create logical replication slot {slot:?} with pgoutput")
            });
        }
    };
    let lsn: String = row.get(0);
    Ok(SlotResume::Created {
        consistent_point: parse_lsn(&lsn, "consistent_point")?,
    })
}

/// Drop a slot only if the catalog still identifies that exact name as invalidated, then establish
/// a replacement. If another actor has already removed or replaced it, this never drops the new
/// healthy slot: [`verify_or_create_slot`] returns that slot as [`SlotResume::Existing`] and the
/// caller retries classification.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if the conditional drop, catalog read, or replacement creation fails.
pub async fn recreate_invalidated_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<SlotResume> {
    client
        .query("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&slot])
        .await
        .with_context(|| format!("lock invalidated slot replacement for {slot:?}"))?;
    let result = async {
        client
            .query(DROP_INVALIDATED_SLOT_SQL, &[&slot])
            .await
            .with_context(|| format!("drop invalidated replication slot {slot:?}"))?;
        verify_or_create_slot(client, slot).await
    }
    .await;
    let unlocked = client
        .query(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            &[&slot],
        )
        .await
        .with_context(|| format!("unlock invalidated slot replacement for {slot:?}"));
    match (result, unlocked) {
        (Ok(resume), Ok(_)) => Ok(resume),
        (Err(error), Ok(_)) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(unlock_error)) => {
            tracing::warn!(error = ?unlock_error, slot, "failed to release slot replacement advisory lock");
            Err(error)
        }
    }
}

#[cfg(test)]
#[path = "slot_test.rs"]
mod tests;
