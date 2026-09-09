//! `table_ownership` — the transformer's cooperative single-writer lease (transformer §8.1).
//!
//! The FIRST fence: a control-plane row per owned `(epoch, schema, table)` with a monotonic
//! `fencing_token`, acquired **before** the transformer takes DuckDB's read-write file lock (the second
//! fence). A live owner keeps `lease_expiry` in the future by renewing well under the TTL; a dead PID's
//! lease simply expires and is reclaimable. All time comparisons happen against PostgreSQL's
//! per-statement clock, so even a caller inside a long transaction cannot resurrect an expired
//! lease and the Rust side never needs a timestamp type.

use crate::ControlError;
use common::EpochNo;
use sqlx::PgExecutor;

/// A held lease. Every successful explicit acquisition mints a new `fencing_token`, including a
/// reacquisition by the same configured instance name. Renewal never changes the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// Monotonic token, bumped by 1 on every successful explicit acquisition. A holder that stalls
    /// past its TTL, restarts under the same instance name, or otherwise reacquires cannot reuse a
    /// stale renewal/release capability.
    pub fencing_token: i64,
    /// The instance identity holding the lease — the transformer's configured `instance`.
    pub owner_pod: String,
}

/// Conditionally acquire the lease for `ttl_secs`: succeeds iff the lease is **free** (expired) or
/// **already ours**. Returns `Ok(None)` when a *live* different owner holds it — the caller maps that
/// to the terminal [`common::ExitCode::LeaseContended`]. Every successful explicit acquisition
/// bumps the token by 1; callers that only want to extend a held lease must use [`renew_lease`].
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the conditional lease upsert cannot execute, or
/// [`ControlError::CheckViolation`] if a lease invariant is violated.
pub async fn acquire_lease(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    self_pod: &str,
    ttl_secs: i64,
) -> Result<Option<Lease>, ControlError> {
    let row = sqlx::query_as::<_, (i64, String)>(
        r#"
        INSERT INTO public.walrus_table_ownership
            (epoch, source_schema, source_table, owner_pod, fencing_token, lease_expiry, updated_at)
        VALUES (
            $1, $2, $3, $4, 1,
            statement_timestamp() + make_interval(secs => $5),
            statement_timestamp()
        )
        ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
        SET owner_pod = EXCLUDED.owner_pod,
            fencing_token = public.walrus_table_ownership.fencing_token + 1,
            lease_expiry = EXCLUDED.lease_expiry,
            updated_at = statement_timestamp()
        WHERE public.walrus_table_ownership.lease_expiry <= statement_timestamp()
           OR public.walrus_table_ownership.owner_pod = EXCLUDED.owner_pod
        RETURNING fencing_token, owner_pod
        "#,
    )
    .bind(epoch)
    .bind(schema)
    .bind(table)
    .bind(self_pod)
    .bind(ttl_secs as f64)
    .fetch_optional(ex)
    .await?;
    Ok(row.map(|(fencing_token, owner_pod)| Lease {
        fencing_token,
        owner_pod,
    }))
}

/// Renew our lease (extend `lease_expiry`), off the apply-loop thread and well under the TTL. Fails
/// to affect any row if the owner/token capability is stale or the lease has already expired. An
/// expired holder must explicitly reacquire and receive a new token; renewal can never resurrect it.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded renewal update cannot execute.
pub async fn renew_lease(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    self_pod: &str,
    fencing_token: i64,
    ttl_secs: i64,
) -> Result<bool, ControlError> {
    let done = sqlx::query(
        r#"
        UPDATE public.walrus_table_ownership
        SET lease_expiry = statement_timestamp() + make_interval(secs => $6),
            updated_at = statement_timestamp()
        WHERE epoch = $1
          AND source_schema = $2
          AND source_table = $3
          AND owner_pod = $4
          AND fencing_token = $5
          AND lease_expiry > statement_timestamp()
        "#,
    )
    .bind(epoch)
    .bind(schema)
    .bind(table)
    .bind(self_pod)
    .bind(fencing_token)
    .bind(ttl_secs as f64)
    .execute(ex)
    .await?;
    Ok(done.rows_affected() > 0)
}

/// Release our lease on graceful shutdown (expire it immediately) so a replacement pod need not
/// wait out the TTL. A no-op unless both the owner identity and fencing token still match. Release
/// intentionally accepts an already-expired matching lease; expiring it again is harmless and
/// keeps graceful cleanup idempotent.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded expiry update cannot execute.
pub async fn release_lease(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    self_pod: &str,
    fencing_token: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        r#"
        UPDATE public.walrus_table_ownership
        SET lease_expiry = statement_timestamp() - make_interval(secs => 1),
            updated_at = statement_timestamp()
        WHERE epoch = $1
          AND source_schema = $2
          AND source_table = $3
          AND owner_pod = $4
          AND fencing_token = $5
        "#,
    )
    .bind(epoch)
    .bind(schema)
    .bind(table)
    .bind(self_pod)
    .bind(fencing_token)
    .execute(ex)
    .await?;
    Ok(())
}
