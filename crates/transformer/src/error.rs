//! [`TransformerError`] — every terminal bootstrap failure, each mapped to a distinct [`common::ExitCode`]
//! so a broken deploy is greppable in `kubectl logs` (the "context in the loop, exit code at `main`"
//! idiom). Transient failures are retried to a deadline *before* becoming one of these.

use crate::config::ConfigError;
use common::{EpochNo, ExitCode, FailureClass};

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransformerError {
    /// Not `transparent`: [`ConfigError`]'s variants name the offending knob, and this is where the
    /// "which configuration" framing belongs.
    #[error("invalid transformer configuration: {0}")]
    Config(#[from] ConfigError),
    /// A control-plane call failed. `transparent` because [`control::ControlError`] already names
    /// the operation, and it is the one variant here that can be transient.
    #[error(transparent)]
    Control(#[from] control::ControlError),
    /// A DuckDB engine call failed. `op` names the operation while `source` keeps the typed engine
    /// failure available to error-chain walkers.
    #[error("DuckDB: {op}")]
    Duck {
        /// Operation that was attempted.
        op: String,
        /// Typed DuckDB failure.
        #[source]
        source: duckdb::Error,
    },
    /// The dedicated DuckLake PostgreSQL catalog failed or its advisory-lock session was lost.
    #[error("ducklake catalog: {op}")]
    Catalog {
        /// Catalog operation that was attempted.
        op: &'static str,
        /// Typed PostgreSQL catalog failure.
        #[source]
        source: sqlx::Error,
    },
    /// An object-store call failed. `op` names the call while `source` keeps the store's own typed
    /// failure — its path, its status, its nested transport cause — reachable by
    /// [`source()`](std::error::Error::source)/`downcast_ref` instead of collapsed into a sentence.
    /// Boxed like [`TransformerError::Health`]: `Result<_, TransformerError>` is threaded through the whole
    /// transformer, so the store's wide error enum stays behind a pointer.
    ///
    /// Unlike [`TransformerError::Duck`], `Display` still inlines the cause: this message already read
    /// `object store: <op>: <store error>` before the store failure was typed, and typing it must
    /// not shorten what an operator sees.
    #[error("object store: {op}: {source}")]
    ObjectStore {
        /// Object-store operation that was attempted.
        op: &'static str,
        /// Typed object-store failure.
        #[source]
        source: Box<object_store::Error>,
    },
    /// Bytes downloaded for a manifest did not match the control-plane immutable-object receipt.
    #[error("staged object integrity failure for {uri}: {reason}")]
    ObjectIntegrity {
        /// URI of the object whose receipt did not match.
        uri: String,
        /// Observed integrity mismatch.
        reason: String,
    },
    /// A manifest unit or durable ingest receipt violated an atomicity/idempotency invariant.
    #[error("manifest invariant: {message}")]
    ManifestInvariant {
        /// Violated manifest or receipt invariant.
        message: String,
    },
    /// A *live* owner already holds the lease — a second writer must NOT proceed.
    #[error("lease for {table} is held by a live owner ({owner})")]
    LeaseContended {
        /// Qualified table whose ownership was requested.
        table: String,
        /// Identity of the current live owner.
        owner: String,
    },
    /// `transformed_lsn > raw_appended_lsn` — the checkpoint is corrupt (should be impossible: the DB
    /// enforces `CHECK (transformed_lsn <= raw_appended_lsn)`), so this is terminal.
    #[error("corrupt checkpoint for {table}: transformed_lsn > raw_appended_lsn")]
    CorruptCheckpoint {
        /// Qualified table with inconsistent watermarks.
        table: String,
    },
    /// A lossy/incompatible `ALTER COLUMN TYPE` failed the in-place mirror cast. The table is
    /// quarantined and processing STOPS — an accepted, alerting v1 outcome (never silent data loss).
    #[error("table {table} quarantined: {reason}")]
    Quarantine {
        /// Qualified table removed from normal processing.
        table: String,
        /// Operator-facing reason for quarantine.
        reason: String,
    },
    /// The control plane opened a NEW generation (§1.8 total-restart) while this transformer was running the
    /// old one. Exit **loudly** so the orchestrator restarts us into a rebuild under the new epoch —
    /// never rebuild a running generation in place.
    #[error(
        "epoch bumped {from} → {to}: control-plane opened a new generation (total-restart) — restarting to rebuild"
    )]
    EpochBumped {
        /// Generation the running process bootstrapped against.
        from: EpochNo,
        /// New generation published by the control plane.
        to: EpochNo,
    },
    /// A schema-registry column snapshot did not decode into the relation shape the extractor wrote.
    #[error("decode registry columns for {table} v{version}")]
    RegistryDecode {
        /// Qualified source table whose registry row was read.
        table: String,
        /// Schema version containing the malformed snapshot.
        version: i64,
        /// Typed JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// A Parquet column name is not a bare lowercase snake_case SQL column, so the append's explicit
    /// column list could not be built. `source` keeps *which* rule it broke, so a caller
    /// can branch on it instead of matching on the message. `Display` inlines the cause: the text
    /// was already `parquet column name from <uri>: <rule>` while this was a
    /// [`TransformerError::Internal`] string.
    #[error("parquet column name from {uri}: {source}")]
    Ident {
        /// Staged object containing the invalid column name.
        uri: String,
        /// Column-name policy violation.
        #[source]
        source: common::sql::ColumnNameError,
    },
    /// A stored watermark string failed to parse as a Postgres LSN.
    #[error("parse {field} as an LSN")]
    LsnParse {
        /// Watermark field containing the malformed value.
        field: &'static str,
        /// Typed LSN parse failure.
        #[source]
        source: common::lsn::LsnParseError,
    },
    /// A control-DB transaction could not be begun or committed.
    #[error("control transaction: {op}")]
    ControlTxn {
        /// Transaction boundary that failed.
        op: &'static str,
        /// Typed PostgreSQL transaction failure.
        #[source]
        source: sqlx::Error,
    },
    /// The health/metrics server failed to bind, join, or serve.
    #[error("health server: {op}")]
    Health {
        /// Health-server operation that failed.
        op: &'static str,
        /// Underlying bind, serve, or task failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A local `.duckdb` file operation failed. `op` names it and `path` locates it, while `source`
    /// keeps the OS error — so "permission denied" and "read-only file system", one sentence apart
    /// to a log reader but two different operator actions, stay distinguishable by
    /// [`std::io::ErrorKind`]. `Display` inlines the cause for the same reason
    /// [`TransformerError::ObjectStore`] does: the text was already `retire <path>: <os error>` while
    /// this was a [`TransformerError::Internal`] string.
    #[error("{op} {path}: {source}")]
    File {
        /// Filesystem operation that failed.
        op: &'static str,
        /// Local DuckDB path involved in the operation.
        path: String,
        /// Typed operating-system failure.
        #[source]
        source: std::io::Error,
    },
    /// An asserted invariant does not hold and no more specific typed cause exists.
    #[error("{0}")]
    Internal(String),
}

/// The classified terminal error `main` surfaces as an exit code.
///
/// Takes `&TransformerError` because the caller keeps its error for logging; the standard blanket impl
/// then also provides `Into<common::Error>` for `&TransformerError`.
impl From<&TransformerError> for common::Error {
    fn from(e: &TransformerError) -> Self {
        match e {
            TransformerError::Config(e) => common::Error::Config(e.to_string()),
            TransformerError::Control(e) => common::Error::ControlDb(e.to_string()),
            TransformerError::Duck { op, source } => {
                common::Error::Internal(format!("duckdb: {op}: {source}"))
            }
            TransformerError::Catalog { op, source } => {
                common::Error::ControlDb(format!("ducklake catalog {op}: {source}"))
            }
            TransformerError::ObjectStore { op, source } => {
                common::Error::ObjectStore(format!("{op}: {source}"))
            }
            TransformerError::ObjectIntegrity { uri, reason } => common::Error::Internal(format!(
                "staged object integrity failure for {uri}: {reason}"
            )),
            TransformerError::ManifestInvariant { message } => {
                common::Error::Internal(format!("manifest invariant: {message}"))
            }
            TransformerError::LeaseContended { table, owner } => {
                common::Error::LeaseContended(format!("{table} held by {owner}"))
            }
            TransformerError::CorruptCheckpoint { table } => {
                common::Error::Internal(format!("corrupt checkpoint for {table}"))
            }
            TransformerError::Quarantine { table, reason } => {
                common::Error::Quarantine(format!("{table}: {reason}"))
            }
            TransformerError::EpochBumped { from, to } => {
                common::Error::Internal(format!("epoch bumped {from} → {to} (total-restart)"))
            }
            TransformerError::RegistryDecode {
                table,
                version,
                source,
            } => common::Error::Internal(format!(
                "decode registry columns for {table} v{version}: {source}"
            )),
            TransformerError::Ident { uri, source } => {
                common::Error::Internal(format!("parquet column name from {uri}: {source}"))
            }
            TransformerError::LsnParse { field, source } => {
                common::Error::Internal(format!("parse {field} as an LSN: {source}"))
            }
            // Deliberate remap: a control-pg failure is ExitCode::ControlDb (11), not Internal (70).
            TransformerError::ControlTxn { op, source } => {
                common::Error::ControlDb(format!("control transaction {op}: {source}"))
            }
            TransformerError::Health { op, source } => {
                common::Error::Internal(format!("health server {op}: {source}"))
            }
            TransformerError::File { op, path, source } => {
                common::Error::Internal(format!("{op} {path}: {source}"))
            }
            TransformerError::Internal(m) => common::Error::Internal(m.clone()),
        }
    }
}

impl FailureClass for TransformerError {
    /// Exhaustive, no `_` arm. Only a wrapped [`control::ControlError`] can be transient; every
    /// other variant is a terminal bootstrap failure by construction.
    fn is_terminal(&self) -> bool {
        match self {
            TransformerError::Control(e) => e.is_terminal(),
            TransformerError::Config(_)
            | TransformerError::Duck { .. }
            | TransformerError::Catalog { .. }
            | TransformerError::ObjectStore { .. }
            | TransformerError::ObjectIntegrity { .. }
            | TransformerError::ManifestInvariant { .. }
            | TransformerError::LeaseContended { .. }
            | TransformerError::CorruptCheckpoint { .. }
            | TransformerError::Quarantine { .. }
            | TransformerError::EpochBumped { .. }
            | TransformerError::RegistryDecode { .. }
            | TransformerError::Ident { .. }
            | TransformerError::LsnParse { .. }
            | TransformerError::ControlTxn { .. }
            | TransformerError::Health { .. }
            | TransformerError::File { .. }
            | TransformerError::Internal(_) => true,
        }
    }

    /// OVERRIDE of the default: preserve the existing per-variant codes by routing through the
    /// exhaustive `From<&TransformerError>` mapping.
    fn exit_code(&self) -> ExitCode {
        common::Error::from(self).exit_code()
    }
}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
