//! The one place an `anyhow::Error` becomes a process exit status.

use common::{ExitCode, FailureClass};

/// Recover the distinct exit code for a failure that reached `main`.
///
/// Tries the classified [`common::Error`] first, then each typed extractor error that can escape the
/// run loop, and finally the control crate's error. Anything unrecognised keeps the historical
/// [`ExitCode::Internal`] fallback.
///
/// Each typed error is matched **exhaustively** — no `_` arm — because the numbers are a runbook
/// contract (`common::error`): a new variant must pick its code here instead of inheriting one.
#[deny(clippy::wildcard_enum_match_arm)]
#[must_use]
pub fn code_for(err: &anyhow::Error) -> ExitCode {
    if let Some(e) = err.downcast_ref::<common::Error>() {
        return e.exit_code();
    }
    if let Some(e) = err.downcast_ref::<crate::staging::StagingError>() {
        return match e {
            crate::staging::StagingError::Encode(_)
            | crate::staging::StagingError::SchemaMismatch
            | crate::staging::StagingError::EmptyStream
            | crate::staging::StagingError::ClosedStream => ExitCode::Internal,
            crate::staging::StagingError::Store(_) => ExitCode::ObjectStore,
        };
    }
    if err
        .downcast_ref::<crate::manifest::ManifestError>()
        .is_some()
    {
        return ExitCode::ControlDb;
    }
    if err.downcast_ref::<crate::config::ConfigError>().is_some() {
        return ExitCode::Config;
    }
    if let Some(e) = err.downcast_ref::<crate::preflight::PreflightError>() {
        return match e {
            crate::preflight::PreflightError::NoPrimaryKey { .. } => ExitCode::KeylessTable,
            // The rest of the taxonomy shares exit 13, listed rather than absorbed by a wildcard.
            crate::preflight::PreflightError::WalLevel { .. }
            | crate::preflight::PreflightError::ServerTooOld { .. }
            | crate::preflight::PreflightError::UnsupportedGeneratedColumnPublication { .. }
            | crate::preflight::PreflightError::NoHeadroom { .. }
            | crate::preflight::PreflightError::SlotNameDrift { .. }
            | crate::preflight::PreflightError::PublicationMissing { .. }
            | crate::preflight::PreflightError::PublicationGap { .. }
            | crate::preflight::PreflightError::PublicationCoverage(_)
            | crate::preflight::PreflightError::NoReplicationPriv
            | crate::preflight::PreflightError::NoSchemaUsagePrivilege { .. }
            | crate::preflight::PreflightError::NoTableLockPrivilege { .. }
            | crate::preflight::PreflightError::NoTableSelectPrivilege { .. }
            | crate::preflight::PreflightError::DdlCaptureMissing { .. }
            | crate::preflight::PreflightError::ReloadSignalMissing { .. }
            | crate::preflight::PreflightError::ReloadEventMissing { .. }
            | crate::preflight::PreflightError::Query(_)
            | crate::preflight::PreflightError::UnusableResult(_)
            | crate::preflight::PreflightError::Ident(_) => ExitCode::Preflight,
        };
    }
    if err
        .downcast_ref::<crate::heartbeat::HeartbeatError>()
        .is_some()
    {
        return ExitCode::SourceDb;
    }
    if err.downcast_ref::<control::ControlError>().is_some() {
        return ExitCode::ControlDb;
    }
    ExitCode::Internal
}

#[cfg(test)]
#[path = "exit_test.rs"]
mod tests;
