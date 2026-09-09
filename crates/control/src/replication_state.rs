//! `replication_state` models: the epoch generation that namespaces all control-plane state.

use crate::{ControlError, parse::ParseEnumError};
use common::string_enum;
use common::{EpochNo, Lsn};
use sqlx::{PgExecutor, Row};
use uuid::Uuid;

/// Catalog-fence protocol understood by this binary. Version zero denotes generations created
/// before the source-table lock/LSN fence was introduced and is deliberately not resumable by the
/// extractor: startup opens a reconciled successor instead.
pub const CURRENT_CATALOG_FENCE_VERSION: i32 = 1;

string_enum! {
    /// Where a generation stands — the canonical enum for the `status` text column.
    ///
    /// The migration carries the same three-value `CHECK`, while this type keeps invalid database
    /// values from crossing the Rust boundary. A fresh generation is born `Bootstrapping` and moves
    /// to `Streaming` only after its bound
    /// all-table reconciliation publishes every child. `TotalRestart` is the durable intent written
    /// before replacing a lost source slot; a process that finds that intent must open a successor
    /// generation instead of resuming this one.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReplicationStatus {
        error = ParseEnumError;
        column = "replication_state.status";
        Bootstrapping => "bootstrapping",
        Streaming => "streaming",
        TotalRestart => "total_restart",
    }
}

/// One row per slot lifetime; a new slot = a new epoch (architecture §1.8). The `epoch` namespaces
/// **all** other state (manifest, checkpoints, registry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationState {
    /// The generation counter itself — the value every other control-plane table is keyed by.
    pub epoch: EpochNo,
    /// The replication slot whose lifetime *is* this generation.
    pub slot_name: String,
    /// The generation's writer-drained catalog/start LSN (no exported snapshot is required).
    pub created_lsn: Lsn,
    /// Provenance for the catalog/LSN boundary used to open this generation. Zero is legacy/racy.
    pub catalog_fence_version: i32,
    /// Where the generation stands; see [`ReplicationStatus`].
    pub status: ReplicationStatus,
}

/// Durable progress of the all-table request that builds a new epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapProgress {
    /// Source request UUID bound to the epoch when it was created.
    pub request_id: Uuid,
    /// Frozen publication inventory size registered before the request was emitted.
    pub expected_tables: i64,
    /// Exact ordered source targets, retained so a crash can re-emit identical request data.
    pub targets: serde_json::Value,
    /// Concrete request children durably created from the decoded source event.
    pub children: i64,
    /// Children whose shadow generation has been atomically published.
    pub complete: i64,
    /// Terminal children; any non-zero value prevents generation promotion.
    pub failed: i64,
}

impl BootstrapProgress {
    /// Whether every expected child exists and has completed successfully.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.children == self.expected_tables
            && self.complete == self.expected_tables
            && self.failed == 0
    }
}

/// The highest-epoch (current) generation, if bootstrap has run.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the current generation cannot be queried, or
/// [`ControlError::Decode`] if the stored status is outside the enum's set.
pub async fn read_current_epoch(
    ex: impl PgExecutor<'_>,
) -> Result<Option<ReplicationState>, ControlError> {
    // The `status` text column decodes to `String` here, then parses into the typed enum: a value
    // outside the known set is a data-integrity bug (the extractor only ever writes `as_str()`) that
    // belongs in the terminal `Decode`.
    let current = sqlx::query_file!("sql/postgres/queries/read_current_epoch.sql")
        .fetch_optional(ex)
        .await?;
    let Some(current) = current else {
        return Ok(None);
    };
    let state = ReplicationState {
        epoch: current.epoch.into(),
        slot_name: current.slot_name,
        created_lsn: current.created_lsn,
        catalog_fence_version: current.catalog_fence_version,
        status: current.status.parse()?,
    };
    Ok(Some(state))
}

/// Durably arm a total restart for `expected_current` before the source slot is created or replaced.
///
/// The highest-epoch predicate is part of the same statement as the status transition. Returning
/// `false` means another actor opened a newer generation after the caller read
/// [`read_current_epoch`], so the caller must not touch the source slot. Repeating the call for an
/// already-armed current epoch returns `true`, which lets a restart recover after a crash on either
/// side of the source slot mutation.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded status update cannot be executed.
pub async fn mark_total_restart(
    ex: impl PgExecutor<'_>,
    expected_current: EpochNo,
) -> Result<bool, ControlError> {
    let result = sqlx::query(
        "UPDATE public.walrus_replication_state AS target
         SET status = 'total_restart'
         WHERE target.epoch = $1
           AND target.epoch = (
             SELECT MAX(current_state.epoch)
             FROM public.walrus_replication_state AS current_state
           )",
    )
    .bind(expected_current.0)
    .execute(ex)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Insert a legacy, provenance-zero generation row.
///
/// This helper exists for historical/test callers that do not own the source catalog-fence
/// protocol. It deliberately cannot assert [`CURRENT_CATALOG_FENCE_VERSION`]; production opens a
/// resumable generation only through [`bump_bootstrap_epoch`].
///
/// # Errors
///
/// Returns [`ControlError::Connect`] for an insert failure, or [`ControlError::CheckViolation`] if
/// the proposed generation violates a database invariant.
pub async fn insert_epoch(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    slot_name: &str,
    created_lsn: Lsn,
    status: ReplicationStatus,
) -> Result<(), ControlError> {
    sqlx::query_file!(
        "sql/postgres/queries/insert_epoch.sql",
        epoch.0,
        slot_name,
        created_lsn as Lsn,
        status.as_str(),
    )
    .execute(ex)
    .await?;
    Ok(())
}

/// Open a legacy, provenance-zero generation at `MAX(epoch) + 1`.
///
/// This compatibility helper cannot assert catalog-fence provenance. Production total restart
/// uses [`bump_bootstrap_epoch`] after capturing the source fence. The insert is atomic (the
/// `SELECT MAX … RETURNING` is one statement),
/// and monotonic by construction — the new epoch strictly exceeds every prior one, so it cleanly
/// re-namespaces all state (S3 prefix, manifest, checkpoints, registry). On an empty table it yields
/// `1`, so first-bootstrap and total-restart share this one path; the caller distinguishes them (a prior
/// epoch present ⇒ a *loud* total-restart) only to decide whether to alert.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the atomic generation insert fails, or
/// [`ControlError::CheckViolation`] if the new row violates a control-plane invariant.
pub async fn bump_epoch(
    ex: impl PgExecutor<'_>,
    slot_name: &str,
    created_lsn: Lsn,
    status: ReplicationStatus,
) -> Result<EpochNo, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/bump_epoch.sql",
        slot_name,
        created_lsn as Lsn,
        status.as_str(),
    )
    .fetch_one(ex)
    .await?;
    Ok(EpochNo(rec.epoch))
}

/// Compare-and-set the current generation, then atomically bind a successor bootstrapping
/// generation to one complete publication inventory and source-WAL request UUID.
///
/// `expected_current = None` succeeds only when no generation exists. `Some(epoch)` succeeds only
/// when that exact epoch is still the maximum. A racing caller for the same predecessor cannot open
/// an additional generation: the epoch primary-key conflict is a no-op, and this function returns
/// `None`. The caller must retry startup so it resumes the winner's bootstrapping generation.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the generation insert fails.
pub async fn bump_bootstrap_epoch(
    ex: impl PgExecutor<'_>,
    expected_current: Option<EpochNo>,
    slot_name: &str,
    created_lsn: Lsn,
    request_id: Uuid,
    expected_tables: i64,
    targets: &serde_json::Value,
) -> Result<Option<EpochNo>, ControlError> {
    let row = sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT pg_catalog.set_config('walrus.catalog_fence_protocol', '1', true) AS protocol
         ), current_state AS (
           SELECT MAX(epoch) AS epoch
           FROM public.walrus_replication_state
         ), inserted AS MATERIALIZED (
           INSERT INTO public.walrus_replication_state
           (epoch, slot_name, created_lsn, status, bootstrap_request_id,
            bootstrap_expected_tables, bootstrap_targets, catalog_fence_version)
           SELECT COALESCE(current_state.epoch, 0) + 1,
                  $1, $2, 'bootstrapping', $3, $4, $5, $7
           FROM current_state
           CROSS JOIN authorized
           WHERE current_state.epoch IS NOT DISTINCT FROM $6
             AND authorized.protocol = '1'
           ON CONFLICT (epoch) DO NOTHING
           RETURNING epoch
         ), deauthorized AS MATERIALIZED (
           SELECT pg_catalog.set_config('walrus.catalog_fence_protocol', '', true) AS protocol
           FROM (SELECT count(*) AS inserted_count FROM inserted) AS consumed
           WHERE consumed.inserted_count >= 0
         )
         SELECT inserted.epoch
         FROM deauthorized
         LEFT JOIN inserted ON true
         WHERE deauthorized.protocol = ''",
    )
    .bind(slot_name)
    .bind(created_lsn)
    .bind(request_id)
    .bind(expected_tables)
    .bind(targets)
    .bind(expected_current.map(|epoch| epoch.0))
    .bind(CURRENT_CATALOG_FENCE_VERSION)
    .fetch_one(ex)
    .await?;
    let epoch = row.try_get::<Option<i64>, _>("epoch")?;
    Ok(epoch.map(EpochNo))
}

/// Read progress for an epoch that is still waiting on its bound all-table reconciliation.
/// Streaming/legacy epochs return `None` and cost the caller no follow-up query.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the progress query fails.
pub async fn read_bootstrap_progress(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Option<BootstrapProgress>, ControlError> {
    let row = sqlx::query(
        "WITH latest_child AS (
           SELECT DISTINCT ON (tr.source_schema, tr.source_table)
                  tr.reload_id, tr.epoch, tr.parent_request_id, tr.request_scope, tr.status
           FROM public.walrus_table_reload tr
           JOIN public.walrus_replication_state parent
             ON parent.epoch = tr.epoch
            AND parent.bootstrap_request_id = tr.parent_request_id
           WHERE tr.epoch = $1
           ORDER BY tr.source_schema, tr.source_table, tr.reload_id DESC
         )
         SELECT rs.bootstrap_request_id,
                rs.bootstrap_expected_tables,
                rs.bootstrap_targets,
                count(tr.reload_id) AS children,
                count(tr.reload_id) FILTER (WHERE tr.status = 'complete') AS complete,
                count(tr.reload_id) FILTER (WHERE tr.status = 'failed') AS failed
         FROM public.walrus_replication_state rs
         LEFT JOIN latest_child tr
           ON tr.epoch = rs.epoch
          AND tr.parent_request_id = rs.bootstrap_request_id
          AND tr.request_scope = 'all_published'
         WHERE rs.epoch = $1
           AND rs.status = 'bootstrapping'
           AND rs.bootstrap_request_id IS NOT NULL
         GROUP BY rs.bootstrap_request_id, rs.bootstrap_expected_tables, rs.bootstrap_targets",
    )
    .bind(epoch.0)
    .fetch_optional(ex)
    .await?;
    row.map(|row| {
        Ok::<BootstrapProgress, sqlx::Error>(BootstrapProgress {
            request_id: row.try_get("bootstrap_request_id")?,
            expected_tables: row.try_get("bootstrap_expected_tables")?,
            targets: row.try_get("bootstrap_targets")?,
            children: row.try_get("children")?,
            complete: row.try_get("complete")?,
            failed: row.try_get("failed")?,
        })
    })
    .transpose()
    .map_err(Into::into)
}

/// Promote a bootstrapping epoch only if every exact frozen target's latest child is complete.
///
/// The migration-owned readiness function validates the target JSON, rejects duplicate identities,
/// selects the latest DDL-restart child for each table, and requires exact bidirectional equality
/// for both the child and schema-registry table sets. The row trigger calls the same function, so a
/// direct status update cannot bypass this guarded path. Returns `false` when the group is
/// incomplete or another actor already promoted it.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded update fails.
pub async fn complete_bootstrap(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    request_id: Uuid,
) -> Result<bool, ControlError> {
    let done = sqlx::query(
        "UPDATE public.walrus_replication_state rs
         SET status = 'streaming'
         WHERE rs.epoch = $1
           AND rs.status = 'bootstrapping'
           AND rs.bootstrap_request_id = $2
           AND public.walrus_bootstrap_generation_ready(
             rs.epoch,
             rs.bootstrap_request_id,
             rs.bootstrap_expected_tables,
             rs.bootstrap_targets,
             rs.catalog_fence_version
           )",
    )
    .bind(epoch.0)
    .bind(request_id)
    .execute(ex)
    .await?;
    Ok(done.rows_affected() > 0)
}

#[cfg(test)]
#[path = "replication_state_test.rs"]
mod tests;
