//! Total-restart on the transformer side (§1.8). When the control plane opens a new generation (the extractor
//! bumped `replication_state.epoch` after the single lifelong slot was lost/invalidated), every
//! DuckLake namespace built for the retired generation holds stale `<table>`/`<table>_raw` data. The
//! fix is a whole-table **rebuild**: wipe the mirror + CDC log so the new generation's fenced full
//! exports repopulate every table. **Both watermarks reset for free** — the new epoch's
//! `transformer_checkpoint` row is a fresh `(0/0, 0/0)`, since checkpoints are epoch-keyed.
//!
//! Detection is at **bootstrap** (compare each file's `_walrus_meta['epoch']` to the control epoch) and,
//! for a *running* transformer, through one shared epoch poller ([`apply_loop`](crate::apply_loop) exits loudly
//! on a bump so the orchestrator restarts it into a rebuild). A rebuild is **whole-system** by
//! construction — every table shares the epoch and is rebuilt together. The same fenced protocol also
//! handles later per-table reloads without an epoch change.

use crate::duck::TableDb;
use crate::error::TransformerError;
use crate::health::TransformerState;
use common::EpochNo;
use control::ReplicationStatus;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// If `db` was built for an older generation than `control_epoch`, wipe its mirror + raw so the caller's
/// subsequent `ensure_tables*` recreates them empty and the new generation's reconciliation rebuilds
/// the file. Returns `true` iff a rebuild happened. A no-op (returns `false`) when the file is brand-new (never stamped) or
/// already at `control_epoch` — so first-bootstrap and steady resume are untouched.
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if the stored epoch cannot be read or a retired generation cannot
/// be wiped.
pub fn rebuild_for_new_epoch(
    db: &TableDb,
    table: &str,
    control_epoch: EpochNo,
) -> Result<bool, TransformerError> {
    match db.built_epoch()? {
        Some(built) if built < control_epoch => {
            tracing::error!(
                table,
                old_epoch = %built,
                new_epoch = %control_epoch,
                "TOTAL-RESTART: table was built for a retired generation — wiping mirror + raw to \
                 rebuild under the new epoch (both watermarks reset from the fresh checkpoint)"
            );
            db.wipe_generation(table)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Poll the control plane's current epoch once per `period` and publish its latest value to every
/// apply loop. A failed read is only a delayed guard observation, so it is logged and retried on the
/// next tick. The task stops on shutdown or when its final receiver is dropped.
///
/// # Panics
///
/// The spawned task panics if `period` is zero — [`tokio::time::interval`] rejects a zero period.
/// The failure surfaces on the returned [`JoinHandle`](tokio::task::JoinHandle), not at this call,
/// because the interval is built on the task's first poll.
#[must_use]
pub fn spawn_epoch_watch(
    pool: sqlx::PgPool,
    baseline: EpochNo,
    period: Duration,
    state: Arc<TransformerState>,
    token: CancellationToken,
) -> (watch::Receiver<EpochNo>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = watch::channel(baseline);
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // `baseline` was just read; skip the interval's immediate tick.

        loop {
            if tx.is_closed() {
                return;
            }
            tokio::select! {
                biased;
                () = token.cancelled() => return,
                () = tx.closed() => return,
                _ = tick.tick() => {}
            }
            if tx.is_closed() {
                return;
            }

            let observed = tokio::select! {
                biased;
                () = token.cancelled() => return,
                () = tx.closed() => return,
                observed = control::read_current_epoch(&pool) => observed,
            };
            match observed {
                Ok(Some(observed))
                    if generation_is_retired(observed.epoch, observed.status, baseline) =>
                {
                    // `total_restart` is written before the source slot changes. Waiting for its
                    // successor epoch would leave this provably stale generation ready forever if
                    // the extractor crashes in that seam, so gate readiness and terminate now.
                    state.mark_generation_retired();
                    tracing::error!(
                        epoch = %observed.epoch,
                        "TOTAL-RESTART: current transformer generation was retired before slot replacement; draining and exiting"
                    );
                    token.cancel();
                    return;
                }
                Ok(observed) => {
                    advance(&tx, observed.map(|state| state.epoch));
                }
                Err(error) => {
                    tracing::warn!(error = ?error, "epoch watch read failed; retrying next tick");
                }
            }
        }
    });
    (rx, handle)
}

pub(crate) fn generation_is_retired(
    observed_epoch: EpochNo,
    observed_status: ReplicationStatus,
    baseline: EpochNo,
) -> bool {
    observed_epoch >= baseline && observed_status == ReplicationStatus::TotalRestart
}

/// Keep a freshly-created generation out of service until the extractor has observed every frozen
/// all-table child publish and atomically promoted `replication_state` to `streaming`.
///
/// This watcher is deliberately independent of table workers: a transformer shard can own zero tables,
/// but it still must not advertise the generation before the global reconciliation is complete.
/// Failed control reads delay readiness and are retried; they never fail open.
#[must_use]
pub fn spawn_generation_ready_watch(
    pool: sqlx::PgPool,
    epoch: EpochNo,
    period: Duration,
    state: Arc<TransformerState>,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = token.cancelled() => return,
                _ = tick.tick() => {}
            }

            // A pool wait or network read must not hold the app's ordered shutdown join after the
            // token is cancelled. Dropping this query future is safe and leaves the next process to
            // retry the read from scratch.
            let observed = tokio::select! {
                biased;
                () = token.cancelled() => return,
                observed = control::read_current_epoch(&pool) => observed,
            };
            match observed {
                Ok(Some(observed))
                    if observed.epoch == epoch
                        && observed.status == ReplicationStatus::Streaming =>
                {
                    state.mark_generation_ready();
                    tracing::info!(%epoch, "initial all-table reconciliation published; transformer ready");
                    return;
                }
                Ok(Some(observed)) if observed.epoch > epoch => {
                    // The ordinary epoch guard makes workers exit. Staying unready here is the
                    // conservative behavior for an empty shard while its process winds down.
                    tracing::warn!(
                        baseline = %epoch,
                        current = %observed.epoch,
                        "generation changed before initial reconciliation published"
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(error = ?error, "generation readiness read failed; retrying next tick");
                }
            }
        }
    })
}

/// Publish only a genuine forward epoch move, returning whether receivers were notified.
pub(crate) fn advance(tx: &watch::Sender<EpochNo>, observed: Option<EpochNo>) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    tx.send_if_modified(|current| {
        if observed > *current {
            *current = observed;
            true
        } else {
            false
        }
    })
}

/// Return a receiver pinned to `epoch`; borrowing retains the final value after its sender is gone.
#[must_use]
pub fn fixed_epoch_watch(epoch: EpochNo) -> watch::Receiver<EpochNo> {
    watch::channel(epoch).1
}

#[cfg(test)]
#[path = "epoch_test.rs"]
mod tests;
