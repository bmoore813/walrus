//! `transformer_checkpoint` models: the two per-table commit-LSN watermarks.
//!
//! The transformer tracks progress with **two watermarks per `(epoch, schema, table)`**, both
//! **commit-LSN valued** (never max-row LSN): `raw_appended_lsn` (Phase A — the `<table>_raw` CDC
//! log is durable up to here) and `transformed_lsn` (Phase B — the `<table>` mirror is derived up
//! to here). They advance **independently** — Phase A in the transformer's control txn alongside the
//! manifest delete, Phase B on its own commit — and the mirror is never ahead of
//! the log, an invariant the DB enforces via `CHECK (transformed_lsn <= raw_appended_lsn)`.

use crate::ControlError;
use common::{EpochNo, Lsn};
use sqlx::PgExecutor;

/// Per-table, per-epoch progress. **Invariant (DB-enforced):** `transformed_lsn <= raw_appended_lsn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Generation these watermarks belong to — they reset with a new one, they do not carry over.
    pub epoch: EpochNo,
    /// Schema of the table being tracked.
    pub source_schema: String,
    /// Table being tracked. With `epoch` and `source_schema`, the row's identity.
    pub source_table: String,
    /// Phase A frontier — the CDC log is durable up to this commit LSN.
    pub raw_appended_lsn: Lsn,
    /// Phase B frontier — the mirror is derived up to this commit LSN (`<= raw_appended_lsn`).
    pub transformed_lsn: Lsn,
}

/// Read the checkpoint for a table, if one exists yet.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the checkpoint query cannot reach or read control Postgres.
pub async fn read_checkpoint(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
) -> Result<Option<Checkpoint>, ControlError> {
    Ok(sqlx::query_file_as!(
        Checkpoint,
        "sql/postgres/queries/read_checkpoint.sql",
        epoch.0,
        schema,
        table,
    )
    .fetch_optional(ex)
    .await?)
}

/// Create the row at `(0/0, 0/0)` if missing; a no-op if present. Called once at transformer bootstrap
/// and kept separate from `advance_*` so a fresh table starts at zero without a spurious
/// "advance to zero".
///
/// # Errors
///
/// Returns [`ControlError::Connect`] for a database failure, or [`ControlError::CheckViolation`] if
/// the seed would violate a control-plane invariant.
pub async fn ensure_checkpoint(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
) -> Result<(), ControlError> {
    sqlx::query_file!(
        "sql/postgres/queries/ensure_checkpoint.sql",
        epoch.0,
        schema,
        table,
    )
    .execute(ex)
    .await?;
    Ok(())
}

/// Phase A: advance `raw_appended_lsn` (UPSERT). The caller passes the executor so this can share
/// the control-DB transaction that also deletes claimed manifest rows. `GREATEST` makes
/// the advance **monotonic** — a re-run after a crash never moves the frontier backward.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] for a database failure, or [`ControlError::CheckViolation`] if
/// the resulting checkpoint violates its ordering constraint.
pub async fn advance_raw_appended(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    lsn: Lsn,
) -> Result<(), ControlError> {
    sqlx::query_file!(
        "sql/postgres/queries/advance_raw_appended.sql",
        epoch.0,
        schema,
        table,
        lsn as Lsn,
    )
    .execute(ex)
    .await?;
    Ok(())
}

/// Phase B: advance `transformed_lsn` (UPSERT), monotonically. The `CHECK` guards it — advancing
/// above the current `raw_appended_lsn` fails as a terminal [`ControlError::CheckViolation`]. (The
/// INSERT fallback seeds `raw_appended_lsn` equal so the CHECK holds; in practice
/// `ensure_checkpoint` has already created the row.)
///
/// # Errors
///
/// Returns [`ControlError::CheckViolation`] when the requested frontier exceeds
/// `raw_appended_lsn`; other database failures become the transient [`ControlError::Connect`].
pub async fn advance_transformed(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    lsn: Lsn,
) -> Result<(), ControlError> {
    sqlx::query_file!(
        "sql/postgres/queries/advance_transformed.sql",
        epoch.0,
        schema,
        table,
        lsn as Lsn,
    )
    .execute(ex)
    .await?;
    Ok(())
}
