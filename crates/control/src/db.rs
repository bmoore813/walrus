//! Control-DB connection pool and migration runner.

use crate::parse::ParseEnumError;
use common::{EpochNo, FailureClass, Lsn, ReloadId};
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

/// Errors from the control-DB entrypoint, classified terminal-vs-transient like [`common::Error`].
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ControlError {
    /// Could not connect to / query the control Postgres. May be transient during a rollout.
    #[error("control database unavailable: {0}")]
    Connect(#[source] sqlx::Error),

    /// A migration failed to apply (bad SQL, checksum mismatch, …). Terminal — retrying won't help.
    #[error("control-plane migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// A DB CHECK constraint was violated (e.g. `transformed_lsn > raw_appended_lsn`). Terminal —
    /// it means a programming bug, never a transient condition.
    ///
    /// `message` is the constraint text an operator reads; `source` keeps the driver error that
    /// produced the verdict, so the SQLSTATE, constraint name and detail/hint behind it stay
    /// reachable rather than being reduced to that one line.
    #[error("control-plane invariant violated (check constraint): {message}")]
    CheckViolation {
        /// Operator-facing constraint failure detail.
        message: String,
        /// Typed database error carrying SQLSTATE and constraint metadata.
        #[source]
        source: sqlx::Error,
    },

    /// A direct/operator reload was requested for a table that already has a live attempt — the
    /// `walrus_table_reload_one_live` partial unique index fired. Terminal for THIS direct request:
    /// retrying is pointless until the active attempt reaches `complete`/`failed`. Source-WAL
    /// requests use their UUID for idempotency and queue instead of returning this error.
    #[error("a reload is already in progress for {schema}.{table}")]
    ReloadInProgress {
        /// Source schema with an active reload.
        schema: String,
        /// Source table with an active reload.
        table: String,
    },

    /// A source request UUID was replayed for the same target with a different immutable payload.
    /// Treating that as a fresh request would destroy idempotency; accepting the changed payload
    /// would make the UUID ambiguous, so it is a terminal caller/protocol error.
    #[error(
        "source reload request {request_id} was replayed with different data for {schema}.{table}"
    )]
    SourceRequestConflict {
        /// Replayed source request identity.
        request_id: Uuid,
        /// Source schema bound to the conflicting payload.
        schema: String,
        /// Source table bound to the conflicting payload.
        table: String,
    },

    /// A reload transition's guarded UPDATE matched zero rows — the row was not in the expected
    /// state (an illegal jump, a lost race, or a stale caller). Terminal: it means a bug or a
    /// superseded actor, never a cold dependency.
    #[error("illegal reload transition: reload {reload_id} is not in status {expected}")]
    ReloadTransition {
        /// Reload attempt whose guarded transition matched no row.
        reload_id: ReloadId,
        /// Status or ownership predicate required by the transition.
        expected: &'static str,
    },

    /// A replay of a streamed commit found a durable publication receipt with a conflicting
    /// identity. A publication is one control transaction, so partial/mismatched state is never a
    /// retryable condition.
    #[error("stream publication conflict for epoch {epoch}, xid {top_xid}, commit {commit_lsn}")]
    StreamPublicationConflict {
        /// Slot generation containing the conflicting receipt.
        epoch: EpochNo,
        /// Top-level streamed transaction identity.
        top_xid: u32,
        /// Authoritative source commit position.
        commit_lsn: Lsn,
    },

    /// An append-only DDL or schema-registry key was replayed with different content. Rewriting
    /// schema history would make already-published files mean something different, so this is a
    /// terminal source/protocol conflict rather than an upsert.
    #[error("immutable {entity} history conflict at {key}")]
    ImmutableHistoryConflict {
        /// Durable history relation whose key collided.
        entity: &'static str,
        /// Human-readable immutable primary key.
        key: String,
    },

    /// A manifest or group failed a Rust-side invariant before SQL was allowed to mutate state.
    #[error("manifest invariant violated: {message}")]
    ManifestInvariant {
        /// Violated manifest or group invariant.
        message: String,
    },

    /// A transformer tried to mutate table-scoped recovery/publication state after its ownership lease
    /// expired or its monotonic fencing token was replaced.
    #[error("table ownership fence lost for epoch {epoch} table {schema}.{table}")]
    TableOwnershipFenceLost {
        /// Slot generation containing the table work.
        epoch: EpochNo,
        /// Source schema whose ownership fence was lost.
        schema: String,
        /// Source table whose ownership fence was lost.
        table: String,
    },

    /// A text column held a value outside its enum's known set (e.g. an unrecognised `file_manifest`
    /// `kind`/`status`). The DB CHECK and the extractor's `as_str()` writer should make this impossible,
    /// so it is a data-integrity bug — terminal, never transient.
    #[error("control-plane decode: {0}")]
    Decode(#[from] ParseEnumError),
}

impl FailureClass for ControlError {
    /// True when retrying can never help — a broken migration or a violated invariant is a bug, not
    /// a cold dependency.
    fn is_terminal(&self) -> bool {
        match self {
            ControlError::Migrate(_)
            | ControlError::CheckViolation { .. }
            | ControlError::ReloadInProgress { .. }
            | ControlError::SourceRequestConflict { .. }
            | ControlError::ReloadTransition { .. }
            | ControlError::StreamPublicationConflict { .. }
            | ControlError::ImmutableHistoryConflict { .. }
            | ControlError::ManifestInvariant { .. }
            | ControlError::TableOwnershipFenceLost { .. }
            | ControlError::Decode(_) => true,
            ControlError::Connect(_) => false,
        }
    }

    // `is_transient` and `exit_code` take the defaults. A ControlError is always wrapped before it
    // reaches a `main`, so its own unclassified exit code is never surfaced by a running process.
}

impl ControlError {
    /// Classify a `sqlx::Error`: a CHECK violation (SQLSTATE `23514`) becomes the terminal
    /// [`ControlError::CheckViolation`]; everything else is a (possibly transient) [`Connect`].
    ///
    /// Private on purpose: conversion goes through `?` / [`From`], so call sites cannot skip the
    /// classification. The reload module's constraint-specific closure delegates here as well.
    fn from_sqlx(e: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &e
            && db.code().as_deref() == Some("23514")
        {
            // Read the message out while the borrow is live, then hand the driver error itself on
            // as the cause — the verdict summarises it rather than replacing it.
            let message = db.message().to_string();
            return ControlError::CheckViolation { message, source: e };
        }
        ControlError::Connect(e)
    }
}

/// Classify every propagated sqlx error rather than blindly treating invariant failures as
/// transient connection errors. This is hand-written because the conversion chooses a variant.
impl From<sqlx::Error> for ControlError {
    fn from(e: sqlx::Error) -> Self {
        ControlError::from_sqlx(e)
    }
}

/// The control-pool ceiling. A small fixed pool matches the low-volume coordination workload;
/// expose it as configuration only if pool contention becomes measurable.
const DEFAULT_MAX_CONNECTIONS: u32 = 5;

/// Connect to the control Postgres, returning a ready connection pool.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the DSN cannot be parsed or the initial connection fails.
/// This dependency failure is transient during startup and may be retried under the deadline.
pub async fn connect(dsn: &str) -> Result<PgPool, ControlError> {
    Ok(PgPoolOptions::new()
        .max_connections(DEFAULT_MAX_CONNECTIONS)
        .connect(dsn)
        .await?)
}

/// Apply every migration in `migrations/control/` idempotently — sqlx records applied versions in
/// `_sqlx_migrations`, so a second run is a no-op. The path is relative to this crate's `Cargo.toml`.
///
/// # Errors
///
/// Returns [`ControlError::Migrate`] when a migration cannot be read or applied, or its recorded
/// checksum differs. Migration failures are terminal.
pub async fn run_migrations(pool: &PgPool) -> Result<(), ControlError> {
    sqlx::migrate!("../../migrations/control").run(pool).await?;
    Ok(())
}

#[cfg(test)]
#[path = "db_test.rs"]
mod tests;
