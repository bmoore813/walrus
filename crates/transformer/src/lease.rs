//! The ownership lease (transformer §8.1) — the **first** fence, acquired before DuckDB's file lock. Wraps
//! the control plane's `table_ownership` row: a live owner → terminal
//! [`TransformerError::LeaseContended`]; an expired
//! lease → reclaim. Renewal runs on a background task well under the TTL so a busy apply loop can never
//! let the lease lapse and admit a phantom second writer.

use crate::config::{LeaseTtl, MIN_LEASE_TTL};
use crate::error::TransformerError;
use common::EpochNo;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Floor for the renew cadence: never tick sub-second, however small the admitted TTL.
pub(crate) const MIN_RENEW_INTERVAL: Duration = Duration::from_secs(1);

/// Exact per-table ownership capability held by this transformer process. The token is part of the
/// identity: two processes configured with the same instance name must still fence one another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldLease {
    /// Source schema.
    pub schema: String,
    /// Source table.
    pub table: String,
    /// Token minted by this process's successful explicit acquisition.
    pub fencing_token: i64,
}

// `renew_interval`'s `clamp` bounds invert — and panic — if its floor exceeds the TTL. A `LeaseTtl`
// admits nothing below `MIN_LEASE_TTL`, so keeping that floor at or above this one is what turns the
// ordering into a compile-time fact rather than a runtime hope.
const _: () = assert!(
    MIN_LEASE_TTL.as_nanos() >= MIN_RENEW_INTERVAL.as_nanos(),
    "MIN_LEASE_TTL must stay >= MIN_RENEW_INTERVAL or renew_interval's clamp bounds invert"
);

/// Return one third of `ttl`, bounded into `[MIN_RENEW_INTERVAL, ttl]`.
///
/// The upper bound is a correctness fence: renewal at or after expiry could admit a second writer.
/// The lower bound needs `MIN_RENEW_INTERVAL <= ttl`; the [`LeaseTtl`] parameter *is* that proof (see
/// the const assertion above), so no caller can reach this `clamp` with an unchecked duration.
fn renew_interval(ttl: LeaseTtl) -> Duration {
    let ttl = ttl.get();
    (ttl / 3).clamp(MIN_RENEW_INTERVAL, ttl)
}

/// Convert a lease TTL to the control plane's signed seconds, saturating oversized durations.
fn ttl_secs(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX)
}

/// Acquire (or reclaim) the lease for one table. `Ok` only when the lease is free or already ours;
/// a live owner is terminal.
///
/// # Errors
///
/// Returns [`TransformerError::Control`] if the lease query fails, or
/// [`TransformerError::LeaseContended`] when another live pod owns the table.
pub async fn acquire(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    self_pod: &str,
    ttl: Duration,
) -> Result<control::Lease, TransformerError> {
    control::acquire_lease(pool, epoch, schema, table, self_pod, ttl_secs(ttl))
        .await?
        .ok_or_else(|| TransformerError::LeaseContended {
            table: format!("{schema}.{table}"),
            owner: "another live pod".to_string(),
        })
}

/// Renew every owned table's lease every `ttl/3`, off the apply-loop thread, until cancelled. A
/// stale token or expired lease cancels the shared service token immediately so the process drops
/// its Duck catalog lock instead of continuing as an unfenced writer.
///
/// Takes a parsed [`LeaseTtl`] rather than a bare `Duration`: the renew cadence is only well-defined
/// for a TTL the renewal can land inside, and that check belongs at the config edge, once.
#[must_use]
pub fn spawn_renewer(
    pool: sqlx::PgPool,
    epoch: EpochNo,
    leases: Vec<HeldLease>,
    self_pod: String,
    ttl: LeaseTtl,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(renew_interval(ttl));
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => return,
                _ = tick.tick() => {
                    for lease in &leases {
                        match control::renew_lease(
                            &pool,
                            epoch,
                            &lease.schema,
                            &lease.table,
                            &self_pod,
                            lease.fencing_token,
                            ttl_secs(ttl.get()),
                        ).await {
                            Ok(true) => {}
                            Ok(false) => {
                                tracing::error!(
                                    table = %format_args!("{}.{}", lease.schema, lease.table),
                                    fencing_token = lease.fencing_token,
                                    "lease lost — owner/token is stale or the lease expired; draining transformer"
                                );
                                // Continuing under the Duck catalog lock would block the rightful
                                // successor while this process no longer has a control-plane write
                                // capability. Drain all workers and release the hard lock instead.
                                token.cancel();
                                return;
                            }
                            Err(e) => tracing::warn!(error = ?e, "lease renew failed (will retry)"),
                        }
                    }
                }
            }
        }
    })
}

/// Release every owned table's lease on graceful shutdown (best-effort).
pub async fn release_all(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    leases: &[HeldLease],
    self_pod: &str,
) {
    for lease in leases {
        if let Err(e) = control::release_lease(
            pool,
            epoch,
            &lease.schema,
            &lease.table,
            self_pod,
            lease.fencing_token,
        )
        .await
        {
            tracing::warn!(error = ?e, "lease release failed");
        }
    }
}

#[cfg(test)]
#[path = "lease_test.rs"]
mod tests;
