//! Worker-to-supervisor failure reporting for the transformer's local apply tasks.

use crate::error::TransformerError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One apply worker's terminal failure.
#[derive(Debug)]
pub struct WorkerFailure {
    /// Schema of the table whose worker died.
    pub schema: String,
    /// Table whose worker died.
    pub table: String,
    /// The terminal error, moved rather than stringified so the supervisor can still classify it.
    pub error: TransformerError,
}

impl WorkerFailure {
    /// Return the operator-facing `schema.table` key. Formats a fresh `String` per call — hence
    /// `to_`; the two names themselves are borrowable fields.
    #[must_use]
    pub fn to_table_key(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// Create a bounded failure channel with one slot per worker.
#[must_use]
pub fn failure_channel(
    workers: usize,
) -> (mpsc::Sender<WorkerFailure>, mpsc::Receiver<WorkerFailure>) {
    mpsc::channel(workers.max(1))
}

/// Report a failure without ever parking the worker.
///
/// Both unhappy arms *discard* the failure, so each log below is that error's only record — hence
/// `?` (which carries the `#[source]` chain [`TransformerError`]'s `Display` omits) and `warn!` rather
/// than `error!`, signalling it was absorbed here.
pub fn report(tx: &mpsc::Sender<WorkerFailure>, failure: WorkerFailure) {
    match tx.try_send(failure) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(failure)) => {
            tracing::warn!(
                table = %failure.to_table_key(),
                error = ?failure.error,
                "worker failure channel full; dropping additional failure"
            );
        }
        Err(mpsc::error::TrySendError::Closed(failure)) => {
            tracing::warn!(
                table = %failure.to_table_key(),
                error = ?failure.error,
                "worker failure channel closed; supervisor already exited"
            );
        }
    }
}

/// Race worker failure reports against the sequential worker drain.
///
/// The first failure cancels the shared token so healthy workers finish draining, then returns that
/// original typed error once all workers have stopped. Additional failures are logged without
/// replacing the first failure.
///
/// The first failure is **returned, not logged**: `main` records it once, with its cause chain, as
/// the process's terminal error, so this layer only names the table `main` cannot see. The
/// additional failures go nowhere else, so those are logged in full.
pub async fn supervise<D>(
    mut rx: mpsc::Receiver<WorkerFailure>,
    token: &CancellationToken,
    drain: D,
) -> Option<WorkerFailure>
where
    D: std::future::Future<Output = ()>,
{
    tokio::pin!(drain);
    let mut first = None;
    loop {
        tokio::select! {
            biased;
            Some(failure) = rx.recv() => {
                if first.is_none() {
                    // Attribution only: this failure is propagated to `main`, which logs it when it
                    // maps it to an exit code. Repeating the error here would record one failure
                    // twice at ERROR with different amounts of context.
                    tracing::error!(
                        table = %failure.to_table_key(),
                        "apply worker failed; cancelling transformer"
                    );
                    token.cancel();
                    first = Some(failure);
                } else {
                    // These are absorbed here — nothing propagates them — so this is their one
                    // record, and `?` keeps the `#[source]` chain `Display` leaves behind.
                    tracing::error!(
                        table = %failure.to_table_key(),
                        error = ?failure.error,
                        "additional apply worker failed during drain"
                    );
                }
            }
            () = &mut drain => return first,
        }
    }
}

#[cfg(test)]
#[path = "supervisor_test.rs"]
mod tests;
