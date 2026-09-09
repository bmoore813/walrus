//! Durable, bounded recovery from an immutable staged-object integrity failure.
//!
//! A corrupt manifest is never skipped. The exact singleton or complete StreamCommit group becomes
//! failed, generic claims pause at the table level, and one fresh full-table reload is requested.
//! A second failure before that replacement publishes exhausts the default transformer budget and leaves
//! an explicit quarantine row. All of those changes share one control-Postgres transaction.

use crate::reload::{ReloadPublication, ReloadStatus};
use crate::{ControlError, ManifestGroupId};
use common::{EpochNo, ManifestId, ReloadId};
use sqlx::{PgExecutor, PgPool, Row};
use uuid::Uuid;

/// Durable state of the current/last table integrity-recovery cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityRecoveryStatus {
    /// A bounded replacement reload is in progress.
    Retrying,
    /// The retry budget was exhausted and table claims remain blocked.
    Quarantined,
    /// A replacement reload published successfully.
    Recovered,
}

impl IntegrityRecoveryStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Retrying => "retrying",
            Self::Quarantined => "quarantined",
            Self::Recovered => "recovered",
        }
    }

    fn parse(raw: &str) -> Result<Self, ControlError> {
        match raw {
            "retrying" => Ok(Self::Retrying),
            "quarantined" => Ok(Self::Quarantined),
            "recovered" => Ok(Self::Recovered),
            other => Err(ControlError::ManifestInvariant {
                message: format!("unknown table_integrity_recovery status {other:?}"),
            }),
        }
    }
}

/// The persisted table-level recovery receipt, including the current reload's state when present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityRecoveryRow {
    /// Replication-slot generation containing the failure.
    pub epoch: EpochNo,
    /// Source schema whose transformer is fenced.
    pub source_schema: String,
    /// Source table whose transformer is fenced.
    pub source_table: String,
    /// Whether a replacement is pending, the budget is exhausted, or publication recovered.
    pub status: IntegrityRecoveryStatus,
    /// One-based count of integrity incidents in this recovery cycle.
    pub attempt_count: i32,
    /// Maximum replacement snapshots permitted for this cycle.
    pub max_attempts: i32,
    /// Replacement reload currently responsible for recovery, when one was scheduled.
    pub recovery_reload_id: Option<ReloadId>,
    /// Current status of `recovery_reload_id`, joined for operator/startup decisions.
    pub recovery_reload_status: Option<ReloadStatus>,
    /// Manifest whose verification triggered the latest incident.
    pub failed_manifest_id: ManifestId,
    /// Complete protocol-v2 group fenced with that manifest, when grouped.
    pub failed_group_id: Option<ManifestGroupId>,
    /// Exact verification/export failure retained for diagnosis.
    pub last_error: String,
}

/// Exact transformer-publication identity when corruption was detected while rebuilding a hidden
/// generation. It prevents a stale transformer from aborting a successor's publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrityPublicationFence {
    /// Reload whose hidden generation was receiving the corrupt object.
    pub reload_id: ReloadId,
    /// Exact publication attempt nonce owned by the reporting transformer.
    pub publication_nonce: Uuid,
}

impl From<&ReloadPublication> for IntegrityPublicationFence {
    fn from(publication: &ReloadPublication) -> Self {
        Self {
            reload_id: publication.reload_id,
            publication_nonce: publication.publication_nonce,
        }
    }
}

/// One verified immutable-object failure reported by the current table owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityFailure<'a> {
    /// Replication-slot generation containing the manifest.
    pub epoch: EpochNo,
    /// Source schema of the poisoned manifest unit.
    pub source_schema: &'a str,
    /// Source table of the poisoned manifest unit.
    pub source_table: &'a str,
    /// Exact manifest whose immutable bytes failed verification.
    pub manifest_id: ManifestId,
    /// Human-readable expected/actual mismatch retained in control Postgres.
    pub reason: &'a str,
    /// Current table-lease owner reporting the failure.
    pub owner_pod: &'a str,
    /// Current monotonic table-ownership fencing token.
    pub fencing_token: i64,
    /// Hidden-generation publication identity, required when one has already started.
    pub publication: Option<IntegrityPublicationFence>,
    /// Number of fresh full-table snapshots permitted in one recovery cycle. Zero quarantines the
    /// first incident; one schedules exactly one replacement and quarantines its first bad input.
    pub max_resnapshots: u32,
}

/// Result of atomically fencing the poison input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityFailureOutcome {
    /// A new source snapshot may replace every effect covered by the poison input.
    RecoveryScheduled {
        /// Fresh or still-unstarted reload selected for the recovery snapshot.
        reload_id: ReloadId,
        /// One-based incident number in this recovery cycle.
        attempt: i32,
    },
    /// The configured replacement budget was exhausted; claims remain durably stopped.
    Quarantined {
        /// One-based incident number that exceeded the budget.
        attempt: i32,
    },
}

fn next_attempt(
    status: Option<IntegrityRecoveryStatus>,
    previous: Option<i32>,
) -> Result<i32, ControlError> {
    if status.is_none() || status == Some(IntegrityRecoveryStatus::Recovered) {
        return Ok(1);
    }
    previous
        .ok_or_else(|| ControlError::ManifestInvariant {
            message: "active integrity recovery has no attempt count".to_string(),
        })?
        .checked_add(1)
        .ok_or_else(|| ControlError::ManifestInvariant {
            message: "integrity recovery attempt counter overflow".to_string(),
        })
}

/// Atomically fail the complete manifest unit, fence any already-started reload that could have
/// consumed it, and either schedule a fresh direct reload or quarantine the table.
///
/// # Errors
///
/// Returns a terminal ownership/invariant error for a stale transformer, an incomplete group, a changed
/// publication identity, or a manifest that is no longer ready. Database failures retain their
/// normal control-plane classification.
pub async fn handle_integrity_failure(
    pool: &PgPool,
    failure: &IntegrityFailure<'_>,
) -> Result<IntegrityFailureOutcome, ControlError> {
    let configured_max_attempts =
        i32::try_from(failure.max_resnapshots).map_err(|_| ControlError::ManifestInvariant {
            message: format!(
                "max integrity resnapshots {} exceeds PostgreSQL int",
                failure.max_resnapshots
            ),
        })?;
    let mut tx = pool.begin().await?;

    let ownership = sqlx::query_scalar::<_, bool>(
        "SELECT true FROM public.walrus_table_ownership
         WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
           AND owner_pod = $4 AND fencing_token = $5
           AND lease_expiry > statement_timestamp()
         FOR UPDATE",
    )
    .bind(failure.epoch.0)
    .bind(failure.source_schema)
    .bind(failure.source_table)
    .bind(failure.owner_pod)
    .bind(failure.fencing_token)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(false);
    if !ownership {
        return Err(ControlError::TableOwnershipFenceLost {
            epoch: failure.epoch,
            schema: failure.source_schema.to_string(),
            table: failure.source_table.to_string(),
        });
    }

    // Lock every live attempt in the same reload-id order as claim_requested before taking any
    // manifest-group lock. This both matches publication's reload-before-manifest ordering and
    // closes the quarantine/claim race: a claimant either owns a requested row first and is
    // observed below as exporting after we wait, or its SKIP LOCKED scan cannot promote a row held
    // by this transaction.
    sqlx::query(
        "SELECT reload_id FROM public.walrus_table_reload
         WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
           AND status IN ('requested', 'exporting', 'export_complete', 'publishing')
         ORDER BY reload_id FOR UPDATE",
    )
    .bind(failure.epoch.0)
    .bind(failure.source_schema)
    .bind(failure.source_table)
    .fetch_all(&mut *tx)
    .await?;

    let target = sqlx::query(
        "SELECT stream_group_id FROM public.walrus_file_manifest
         WHERE id = $1 AND epoch = $2 AND source_schema = $3 AND source_table = $4
         ",
    )
    .bind(failure.manifest_id.0)
    .bind(failure.epoch.0)
    .bind(failure.source_schema)
    .bind(failure.source_table)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ControlError::ManifestInvariant {
        message: format!(
            "integrity failure target manifest {} is absent, changed table, or no longer ready",
            failure.manifest_id
        ),
    })?;
    let group_id = target
        .try_get::<Option<i64>, _>("stream_group_id")?
        .map(ManifestGroupId);

    if let Some(group_id) = group_id {
        // Discovering the immutable group id above takes no row lock. Every grouped mutation then
        // follows the workspace-wide parent-before-children order used by delete_claimed and
        // delete_superseded, preventing a corruption fence and a retirement from deadlocking each
        // other as parent->child versus child->parent.
        let group = sqlx::query(
            "SELECT expected_files, status FROM public.walrus_stream_manifest_group
             WHERE id = $1 AND epoch = $2 AND source_schema = $3 AND source_table = $4
             FOR UPDATE",
        )
        .bind(group_id.0)
        .bind(failure.epoch.0)
        .bind(failure.source_schema)
        .bind(failure.source_table)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ControlError::ManifestInvariant {
            message: format!(
                "manifest {} references missing stream group {}",
                failure.manifest_id, group_id.0
            ),
        })?;
        let expected: i64 = group.try_get("expected_files")?;
        let group_status: String = group.try_get("status")?;
        let children = sqlx::query(
            "SELECT id, status FROM public.walrus_file_manifest
             WHERE stream_group_id = $1 ORDER BY id FOR UPDATE",
        )
        .bind(group_id.0)
        .fetch_all(&mut *tx)
        .await?;
        let child_states = children
            .iter()
            .map(|row| {
                Ok((
                    row.try_get::<i64, _>("id")?,
                    row.try_get::<String, _>("status")?,
                ))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        let target_is_locked = child_states
            .iter()
            .any(|(id, _)| *id == failure.manifest_id.0);
        if group_status != "ready"
            || usize::try_from(expected).ok() != Some(child_states.len())
            || !target_is_locked
            || child_states.iter().any(|(_, status)| status != "ready")
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "cannot fail incomplete/non-ready stream group {} for manifest {}",
                    group_id.0, failure.manifest_id
                ),
            });
        }
        let failed_group = sqlx::query(
            "UPDATE public.walrus_stream_manifest_group
             SET status = 'failed' WHERE id = $1 AND status = 'ready'",
        )
        .bind(group_id.0)
        .execute(&mut *tx)
        .await?;
        let failed_children = sqlx::query(
            "UPDATE public.walrus_file_manifest SET status = 'failed'
             WHERE stream_group_id = $1 AND status = 'ready'",
        )
        .bind(group_id.0)
        .execute(&mut *tx)
        .await?;
        if failed_group.rows_affected() != 1
            || i64::try_from(failed_children.rows_affected()).ok() != Some(expected)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {} changed while fencing corruption",
                    group_id.0
                ),
            });
        }
    } else {
        let locked = sqlx::query_scalar::<_, bool>(
            "SELECT true FROM public.walrus_file_manifest
             WHERE id = $1 AND epoch = $2 AND source_schema = $3 AND source_table = $4
               AND stream_group_id IS NULL AND status = 'ready'
             FOR UPDATE",
        )
        .bind(failure.manifest_id.0)
        .bind(failure.epoch.0)
        .bind(failure.source_schema)
        .bind(failure.source_table)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);
        if !locked {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "integrity failure target manifest {} changed group or is no longer ready",
                    failure.manifest_id
                ),
            });
        }
        let changed = sqlx::query(
            "UPDATE public.walrus_file_manifest SET status = 'failed'
             WHERE id = $1 AND status = 'ready'",
        )
        .bind(failure.manifest_id.0)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "manifest {} changed while fencing corruption",
                    failure.manifest_id
                ),
            });
        }
    }

    // A reload whose F was chosen before this report cannot be trusted to supersede the poison
    // object. Abort every already-started stage under the row lock; its exporter/publication sees
    // the status fence on its next guarded write. A merely requested attempt is safe because its F
    // will be emitted after this transaction commits.
    let active = sqlx::query(
        "SELECT reload_id, status, publication_nonce, publisher_owner_pod,
                publisher_fencing_token
         FROM public.walrus_table_reload
         WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
           AND status IN ('exporting', 'export_complete', 'publishing')
         ORDER BY reload_id DESC LIMIT 1 FOR UPDATE",
    )
    .bind(failure.epoch.0)
    .bind(failure.source_schema)
    .bind(failure.source_table)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(active) = active {
        let reload_id = ReloadId(active.try_get("reload_id")?);
        let status: String = active.try_get("status")?;
        if status == "publishing" {
            let publication_nonce: Option<Uuid> = active.try_get("publication_nonce")?;
            let publisher_owner_pod: Option<String> = active.try_get("publisher_owner_pod")?;
            let publisher_fencing_token: Option<i64> = active.try_get("publisher_fencing_token")?;
            let exact = failure.publication.is_some_and(|publication| {
                publication.reload_id == reload_id
                    && publication_nonce == Some(publication.publication_nonce)
                    && publisher_owner_pod.as_deref() == Some(failure.owner_pod)
                    && publisher_fencing_token == Some(failure.fencing_token)
            });
            if !exact {
                return Err(ControlError::ReloadTransition {
                    reload_id,
                    expected: "the reporting transformer's exact publishing nonce and ownership fence",
                });
            }
        }
        // The row lock above serializes this purge with reload-manifest insertion. Purge before
        // the terminal flip so the database's immediate terminal-with-no-children invariant is
        // independently true; a later failure rolls the entire transaction back.
        sqlx::query(
            "WITH authorized AS MATERIALIZED (
               SELECT set_config('walrus.manifest_delete_protocol', '2', true) AS protocol
             )
             DELETE FROM public.walrus_file_manifest
             WHERE reload_id = $1
               AND (SELECT protocol = '2' FROM authorized)",
        )
        .bind(reload_id.0)
        .execute(&mut *tx)
        .await?;
        sqlx::query("SELECT pg_catalog.set_config('walrus.manifest_delete_protocol', '', true)")
            .execute(&mut *tx)
            .await?;
        let aborted = sqlx::query(
            "UPDATE public.walrus_table_reload
             SET status = 'failed', error = $2,
                 publication_error = CASE WHEN status = 'publishing' THEN $2 ELSE publication_error END,
                 publication_error_at = CASE WHEN status = 'publishing' THEN now() ELSE publication_error_at END,
                 updated_at = now()
             WHERE reload_id = $1 AND status IN ('exporting', 'export_complete', 'publishing')",
        )
        .bind(reload_id.0)
        .bind(failure.reason)
        .execute(&mut *tx)
        .await?;
        if aborted.rows_affected() != 1 {
            return Err(ControlError::ReloadTransition {
                reload_id,
                expected: "the row-locked active reload being aborted for integrity recovery",
            });
        }
    } else if let Some(publication) = failure.publication {
        return Err(ControlError::ReloadTransition {
            reload_id: publication.reload_id,
            expected: "a live publishing attempt for the reported corruption",
        });
    }

    let previous = sqlx::query(
        "SELECT status, attempt_count, max_attempts, recovery_reload_id
         FROM public.walrus_table_integrity_recovery
         WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 FOR UPDATE",
    )
    .bind(failure.epoch.0)
    .bind(failure.source_schema)
    .bind(failure.source_table)
    .fetch_optional(&mut *tx)
    .await?;
    let (previous_status, previous_attempt, previous_max_attempts, previous_recovery_reload) =
        match previous {
            Some(row) => (
                Some(IntegrityRecoveryStatus::parse(
                    &row.try_get::<String, _>("status")?,
                )?),
                Some(row.try_get::<i32, _>("attempt_count")?),
                Some(row.try_get::<i32, _>("max_attempts")?),
                row.try_get::<Option<i64>, _>("recovery_reload_id")?
                    .map(ReloadId),
            ),
            None => (None, None, None, None),
        };
    // Pin the budget when a cycle begins. Mixed-version transformer pods must not expand or shrink an
    // in-flight recovery depending on which one reports the next corrupt replacement object.
    let max_attempts = match previous_status {
        Some(IntegrityRecoveryStatus::Retrying | IntegrityRecoveryStatus::Quarantined) => {
            previous_max_attempts.ok_or_else(|| ControlError::ManifestInvariant {
                message: "active integrity recovery has no pinned retry budget".to_string(),
            })?
        }
        Some(IntegrityRecoveryStatus::Recovered) | None => configured_max_attempts,
    };
    let attempt = next_attempt(previous_status, previous_attempt)?;

    let outcome = if attempt > max_attempts {
        // If the previous replacement has not started yet, retire it now. Leaving a requested row
        // behind a durable quarantine would create an immortal, unclaimable "live" request and
        // could unexpectedly run if an operator later repaired the quarantine row by hand.
        if let Some(reload_id) = previous_recovery_reload {
            sqlx::query(
                "UPDATE public.walrus_table_reload
                 SET status = 'failed', error = $2, updated_at = now()
                 WHERE reload_id = $1 AND status = 'requested'",
            )
            .bind(reload_id.0)
            .bind(failure.reason)
            .execute(&mut *tx)
            .await?;
        }
        IntegrityFailureOutcome::Quarantined { attempt }
    } else {
        let requested = sqlx::query_scalar::<_, i64>(
            "SELECT reload_id FROM public.walrus_table_reload
             WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
               AND status = 'requested'
             ORDER BY reload_id LIMIT 1 FOR UPDATE",
        )
        .bind(failure.epoch.0)
        .bind(failure.source_schema)
        .bind(failure.source_table)
        .fetch_optional(&mut *tx)
        .await?;
        let reload_id = match requested {
            Some(id) => ReloadId(id),
            None => ReloadId(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO public.walrus_table_reload
                       (epoch, source_schema, source_table, flavor, status, parent_request_id)
                     VALUES ($1, $2, $3, 'reload', 'requested', $4)
                     RETURNING reload_id",
                )
                .bind(failure.epoch.0)
                .bind(failure.source_schema)
                .bind(failure.source_table)
                .bind(Uuid::new_v4())
                .fetch_one(&mut *tx)
                .await?,
            ),
        };
        IntegrityFailureOutcome::RecoveryScheduled { reload_id, attempt }
    };
    let (status, recovery_reload_id) = match outcome {
        IntegrityFailureOutcome::RecoveryScheduled { reload_id, .. } => {
            (IntegrityRecoveryStatus::Retrying, Some(reload_id.0))
        }
        IntegrityFailureOutcome::Quarantined { .. } => (
            IntegrityRecoveryStatus::Quarantined,
            previous_status
                .filter(|status| *status != IntegrityRecoveryStatus::Recovered)
                .and(previous_recovery_reload)
                .map(|id| id.0),
        ),
    };
    sqlx::query(
        "INSERT INTO public.walrus_table_integrity_recovery
           (epoch, source_schema, source_table, status, attempt_count, max_attempts,
            recovery_reload_id, failed_manifest_id, failed_group_id, last_error)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (epoch, source_schema, source_table) DO UPDATE SET
           status = EXCLUDED.status,
           attempt_count = EXCLUDED.attempt_count,
           max_attempts = EXCLUDED.max_attempts,
           recovery_reload_id = EXCLUDED.recovery_reload_id,
           failed_manifest_id = EXCLUDED.failed_manifest_id,
           failed_group_id = EXCLUDED.failed_group_id,
           last_error = EXCLUDED.last_error,
           first_failed_at = CASE
             WHEN public.walrus_table_integrity_recovery.status = 'recovered' THEN now()
             ELSE public.walrus_table_integrity_recovery.first_failed_at
           END,
           updated_at = now()",
    )
    .bind(failure.epoch.0)
    .bind(failure.source_schema)
    .bind(failure.source_table)
    .bind(status.as_str())
    .bind(attempt)
    .bind(max_attempts)
    .bind(recovery_reload_id)
    .bind(failure.manifest_id.0)
    .bind(group_id.map(|id| id.0))
    .bind(failure.reason)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(outcome)
}

/// Read the current/last integrity recovery state for one table.
///
/// # Errors
///
/// Returns a database/decode error if the control row cannot be read exactly.
pub async fn read_integrity_recovery(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<IntegrityRecoveryRow>, ControlError> {
    let row = sqlx::query(
        "SELECT recovery.epoch, recovery.source_schema, recovery.source_table,
                recovery.status, recovery.attempt_count, recovery.max_attempts,
                recovery.recovery_reload_id, recovery.failed_manifest_id,
                recovery.failed_group_id, recovery.last_error,
                reload.status AS recovery_reload_status
         FROM public.walrus_table_integrity_recovery recovery
         LEFT JOIN public.walrus_table_reload reload
           ON reload.reload_id = recovery.recovery_reload_id
         WHERE recovery.epoch = $1 AND recovery.source_schema = $2
           AND recovery.source_table = $3",
    )
    .bind(epoch.0)
    .bind(source_schema)
    .bind(source_table)
    .fetch_optional(ex)
    .await?;
    row.map(|row| {
        let reload_status = row
            .try_get::<Option<String>, _>("recovery_reload_status")?
            .map(|raw| raw.parse())
            .transpose()?;
        Ok(IntegrityRecoveryRow {
            epoch: EpochNo(row.try_get("epoch")?),
            source_schema: row.try_get("source_schema")?,
            source_table: row.try_get("source_table")?,
            status: IntegrityRecoveryStatus::parse(&row.try_get::<String, _>("status")?)?,
            attempt_count: row.try_get("attempt_count")?,
            max_attempts: row.try_get("max_attempts")?,
            recovery_reload_id: row
                .try_get::<Option<i64>, _>("recovery_reload_id")?
                .map(ReloadId),
            recovery_reload_status: reload_status,
            failed_manifest_id: ManifestId(row.try_get("failed_manifest_id")?),
            failed_group_id: row
                .try_get::<Option<i64>, _>("failed_group_id")?
                .map(ManifestGroupId),
            last_error: row.try_get("last_error")?,
        })
    })
    .transpose()
}

/// A recovery reload that failed before publication is terminal for the current bounded cycle.
pub(crate) async fn note_recovery_reload_failed(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    reason: &str,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE public.walrus_table_integrity_recovery
         SET status = 'quarantined', last_error = $2, updated_at = now()
         WHERE recovery_reload_id = $1 AND status = 'retrying'",
    )
    .bind(reload_id.0)
    .bind(reason)
    .execute(ex)
    .await?;
    Ok(())
}

/// Carry an integrity-recovery identity across a safe exporter restart with a new source snapshot.
pub(crate) async fn relink_recovery_reload(
    ex: impl PgExecutor<'_>,
    predecessor: ReloadId,
    successor: ReloadId,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE public.walrus_table_integrity_recovery
         SET recovery_reload_id = $2, status = 'retrying', updated_at = now()
         WHERE recovery_reload_id = $1 AND status IN ('retrying', 'quarantined')",
    )
    .bind(predecessor.0)
    .bind(successor.0)
    .execute(ex)
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "integrity_test.rs"]
mod tests;
