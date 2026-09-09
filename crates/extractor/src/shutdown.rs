//! One cancellation source fanned out to every task (§4.5), plus the ordered extractor SIGTERM
//! **drain** (§4.5).
//!
//! SIGTERM (Kubernetes' graceful-stop signal) and SIGINT both trip a single [`CancellationToken`];
//! `clone()` it into each task, which selects on `token.cancelled()`. The process must be able to be
//! PID 1 with an exec-form entrypoint so the signal is delivered to *us*, not swallowed by a shell
//! (the Dockerfile uses an exec-form entrypoint: direct signal handling, no wrapper).
//!
//! **The K8s termination sequence** is preStop-*then*-SIGTERM, sharing one `terminationGracePeriod`
//! budget. walrus skips preStop: the process handles SIGTERM directly as PID 1, so
//! there is nothing a preStop hook would add. On SIGTERM the decode loop's cancellation branch fires
//! and calls [`drain`] — which runs **after** the `select!` loop exits, so a slow S3 PUT is never
//! aborted mid-flight; the grace period bounds it externally, not cancellation.
//!
//! **Why the drain never drops the slot:** the physical WAL stream remains resumable from the slot's
//! `confirmed_flush_lsn`; the drain only minimises the tail that can replay. Because a session guard
//! cannot prove publication continuity across process lifetimes, the replacement service opens a
//! reconciled successor epoch at that retained floor and rebuilds every frozen target while it
//! streams the tail. An ungraceful `SIGKILL` is therefore still correct without trusting the old
//! generation's catalog inventory.
//!
//! The signal task also selects on the token so it exits if cancellation comes from elsewhere — it
//! never outlives the token as a leaked handle.

use crate::batch::Clock;
use crate::checkpoint::DurabilityCheckpoint;
use crate::consume::{BatchRouter, flush_batch};
use crate::replication::ReplicationStream;
use crate::staging::ParquetStager;
use anyhow::Context;
use common::{EpochNo, Lsn};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

/// Run `body` while holding a [`CancellationToken`] drop guard. Whether the body succeeds,
/// returns early with an error, or unwinds, the token is cancelled before this function returns.
/// This scope must end before callers join tasks whose shutdown waits on the token.
pub async fn cancel_on_exit<F>(token: &CancellationToken, body: F) -> F::Output
where
    F: std::future::Future,
{
    let _guard = token.clone().drop_guard();
    body.await
}

/// Outcome of a drain attempt (the caller maps this to a [`common::ExitCode`] — a completed drain is
/// [`Success`](common::ExitCode::Success)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Committed batch(es) flushed + manifested, final feedback sent, connection closed — slot left
    /// in place. `confirmed_flush` is the durable LSN a replacement pod resumes from.
    Drained {
        /// Highest WAL position made durable before disconnecting.
        confirmed_flush: Lsn,
    },
    /// Nothing committed was in flight; final feedback + clean close only.
    Empty,
}

/// The ordered SIGTERM drain (§4.5). Runs to completion (**not** cancellable); the caller bounds it by
/// the K8s grace period. Steps: **(1)** stop consuming (the `select!` loop already exited) → **(2)**
/// flush + COMMIT the in-flight **committed** batch (PUT → manifest → advance checkpoint), dropping any
/// open uncommitted speculative buffers → **(3)** send a **final** standby update advancing
/// `confirmed_flush_lsn` (the checkpoint clamps it to any open-txn floor) →
/// **(4)** `CopyDone` + clean close → **(5)** return, leaving the slot in place. **Never** issues
/// `DROP_REPLICATION_SLOT`.
///
/// ## Cancel safety
///
/// **Not cancel-safe.** Dropping the drain can interrupt its ordered durability and wire-close
/// steps. The decode loop awaits it inside the already-selected cancellation branch, which Tokio
/// runs to completion; only the process's external grace-period deadline can end it early.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if sealing a committed batch, S3/manifest durability, the final standby
/// status, or `CopyDone` fails. Open uncommitted rows are intentionally discarded, not errors.
pub async fn drain<C: Clock + Clone>(
    stream: &mut ReplicationStream,
    router: &mut BatchRouter<C>,
    stager: &ParquetStager,
    checkpoint: &mut DurabilityCheckpoint,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> anyhow::Result<DrainOutcome> {
    // (2) Seal + flush the committed batches; open speculative buffers are dropped (they re-stream).
    let sealed = router
        .drain_for_shutdown()
        .context("seal committed batches on drain")?;
    let mut drained_any = false;
    let mut max_durable = None;
    for batch in sealed {
        let written = flush_batch(stager, pool, epoch, batch).await?; // (a) PUT then (b) manifest
        max_durable =
            Some(max_durable.map_or(written.lsn_end, |lsn: Lsn| lsn.max(written.lsn_end)));
        drained_any = true;
        tracing::info!(
            uri = %written.s3_uri,
            lsn_end = %written.lsn_end,
            "drain: flushed in-flight committed batch"
        );
    }
    if let Some(frontier) = max_durable {
        // (c) only after every sibling batch has a durable object + manifest. A crash during the
        // loop therefore replays the whole unacknowledged tail instead of losing later siblings.
        checkpoint.on_commit_durable(frontier)?;
    }
    // (3) The final standby update carries the durable confirmed_flush (clamped to the open-txn floor).
    checkpoint
        .send(stream, false)
        .await
        .context("send final drain standby status")?;
    // (4) CopyDone + clean close. (5) The slot persists — no DROP_REPLICATION_SLOT, ever.
    stream.copy_done().await.context("CopyDone on drain")?;
    if drained_any {
        Ok(DrainOutcome::Drained {
            confirmed_flush: checkpoint.confirmed_flush(),
        })
    } else {
        Ok(DrainOutcome::Empty)
    }
}

/// Install SIGTERM/SIGINT handlers and return the token they cancel. Also returns early (without
/// cancelling) if the token is cancelled by another source, so the task can't leak.
#[must_use]
pub fn install_signal_handlers() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = ?e, "failed to install SIGTERM handler");
                child.cancel();
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = ?e, "failed to install SIGINT handler");
                child.cancel();
                return;
            }
        };
        tokio::select! {
            biased;
            // Cancelled elsewhere (for example, a bootstrap failure): stop without claiming a
            // signal arrived or leaking the task.
            _ = child.cancelled() => {}
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received; cancelling");
                child.cancel();
            }
            _ = sigint.recv() => {
                tracing::info!("SIGINT received; cancelling");
                child.cancel();
            }
        }
    });
    token
}

#[cfg(test)]
#[path = "shutdown_test.rs"]
mod tests;
