//! Runtime sizing policy shared by both walrus binaries.
//!
//! Runtime construction stays in each binary. This module shares only the bounds, automatic
//! default, and the typed bound violation so the transformer and extractor cannot drift apart.

/// Upper bound on Tokio worker threads.
pub const MAX_WORKER_THREADS: usize = 64;

/// Ceiling for Tokio's blocking pool.
pub const MAX_BLOCKING_THREADS: usize = 16;

/// Return the configured worker count, or the process's usable parallelism, or one.
#[must_use]
pub fn resolve_worker_threads(configured: Option<usize>) -> usize {
    configured.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
    })
}

/// Why a configured worker count is outside the inclusive `1..=64` bound.
///
/// Concrete rather than a detail string so each binary can branch on the violation — and recover
/// the offending count — instead of re-reading the rendered message. The two variants are the
/// whole taxonomy of a two-sided bound, so this enum is deliberately exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WorkerThreadsError {
    /// Tokio's builder panics on a zero worker count, so the bound rejects it first.
    #[error("must be >= 1 (omit for automatic sizing)")]
    Zero,
    /// Above [`MAX_WORKER_THREADS`] — a misconfiguration on a pod, not an intent.
    #[error(
        "must be <= {} (got {configured}; omit for automatic sizing)",
        MAX_WORKER_THREADS
    )]
    TooMany {
        /// Rejected worker count above [`MAX_WORKER_THREADS`].
        configured: usize,
    },
}

/// Validate an optional worker count against the inclusive `1..=64` bound.
///
/// # Errors
///
/// Returns [`WorkerThreadsError::Zero`] for a configured zero, or
/// [`WorkerThreadsError::TooMany`] above [`MAX_WORKER_THREADS`]. `None` selects automatic sizing
/// and is always accepted.
///
/// # Examples
///
/// ```
/// use common::runtime::{WorkerThreadsError, validate_worker_threads};
///
/// assert_eq!(validate_worker_threads(None), Ok(()));
/// assert_eq!(validate_worker_threads(Some(8)), Ok(()));
/// assert_eq!(validate_worker_threads(Some(0)), Err(WorkerThreadsError::Zero));
/// assert_eq!(
///     validate_worker_threads(Some(65)),
///     Err(WorkerThreadsError::TooMany { configured: 65 })
/// );
/// ```
pub const fn validate_worker_threads(configured: Option<usize>) -> Result<(), WorkerThreadsError> {
    let Some(threads) = configured else {
        return Ok(());
    };
    if threads == 0 {
        return Err(WorkerThreadsError::Zero);
    }
    if threads > MAX_WORKER_THREADS {
        return Err(WorkerThreadsError::TooMany {
            configured: threads,
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;
