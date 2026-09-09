//! Durability step (b): after the Parquet PUT is durable in S3, **commit** a `file_manifest`
//! `ready` row — the transformer's work-queue entry (§1.5).
//!
//! This is a thin adapter: map a [`WrittenObject`] → [`control::NewManifestFile`] and delegate to
//! the control plane's table-fenced ordinary publisher. **`lsn_end` is the commit LSN** carried from
//! the [`SealedBatch`](crate::batch::SealedBatch) — never `max(row.lsn)`, which would silently drop a
//! late-committing large txn.
//!
//! **Ordering & at-least-once:** the row is committed *only after* the PUT returns durable. A crash
//! *between* the PUT and this commit leaves no `ready` row, so the batch re-streams and re-writes — no
//! loss. Before a reload cutover, a duplicated INSERT after such a retry just produces a second
//! `ready` row. Once a durable reload seal covers the replayed commit, the control plane instead
//! returns [`control::PublishManifestOutcome::CoveredBySeal`] and the extractor discards the redundant
//! object without recreating work below the cutover.

use crate::staging::WrittenObject;
use common::{EpochNo, ReloadId};

/// Publish a durable ordinary object against its table's reload seal. **Call ONLY after the PUT is
/// durable.** This compatibility spelling delegates to [`publish_ordinary`]; callers must inspect
/// [`control::PublishManifestOutcome::CoveredBySeal`] if they own object cleanup.
///
/// # Errors
///
/// Returns [`ManifestError::Control`] if control Postgres cannot insert the ready manifest row.
pub async fn record_ready<'a>(
    acquire: impl sqlx::Acquire<'a, Database = sqlx::Postgres>,
    epoch: EpochNo,
    obj: &WrittenObject,
) -> Result<control::PublishManifestOutcome, ManifestError> {
    publish_ordinary(acquire, epoch, obj).await
}

/// Publish one durable ordinary object, returning coverage instead of recreating queue work when a
/// committed reload seal already owns its source prefix.
///
/// # Errors
///
/// Returns [`ManifestError::Control`] if the transactional table fence/manifest operation fails.
pub async fn publish_ordinary<'a>(
    acquire: impl sqlx::Acquire<'a, Database = sqlx::Postgres>,
    epoch: EpochNo,
    obj: &WrittenObject,
) -> Result<control::PublishManifestOutcome, ManifestError> {
    Ok(control::publish_ready_manifest(acquire, &to_ready_row(epoch, obj, None)).await?)
}

/// Record one reload object with the exact reload attempt that owns it. This strict insert is kept
/// crate-private so ordinary Stream/Snapshot callers cannot bypass [`publish_ordinary`].
///
/// # Errors
///
/// Returns [`ManifestError::Control`] if the ready row violates a control-plane invariant or cannot
/// be committed.
pub(crate) async fn record_reload_ready(
    ex: impl sqlx::PgExecutor<'_>,
    epoch: EpochNo,
    obj: &WrittenObject,
    reload_id: ReloadId,
) -> Result<common::ManifestId, ManifestError> {
    if obj.kind != control::ManifestKind::Reload {
        return Err(control::ControlError::ManifestInvariant {
            message: format!(
                "reload manifest {} has non-reload kind {}",
                obj.s3_uri,
                obj.kind.as_str()
            ),
        }
        .into());
    }
    Ok(control::insert_ready(ex, &to_ready_row(epoch, obj, Some(reload_id))).await?)
}

/// `WrittenObject` → the `ready` row (`kind` from the object, `lsn_end` = commit LSN).
#[must_use]
pub(crate) fn to_ready_row(
    epoch: EpochNo,
    obj: &WrittenObject,
    reload_id: Option<ReloadId>,
) -> control::NewManifestFile {
    control::NewManifestFile {
        epoch,
        source_schema: obj.source_schema.clone(),
        source_table: obj.source_table.clone(),
        s3_uri: obj.s3_uri.clone(),
        kind: obj.kind,
        row_count: i64::try_from(obj.row_count).unwrap_or(i64::MAX),
        object_size: i64::try_from(obj.object_size).unwrap_or(i64::MAX),
        sha256: obj.sha256.to_vec(),
        lsn_start: obj.lsn_start,
        lsn_end: obj.lsn_end,
        schema_version: obj.schema_version,
        reload_id,
    }
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ManifestError {
    /// The manifest row could not be written. `transparent` because [`control::ControlError`]
    /// already names the operation, and it is the only way this step fails.
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

impl From<ManifestError> for common::Error {
    fn from(e: ManifestError) -> Self {
        common::Error::ControlDb(e.to_string())
    }
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
