use super::*;

#[test]
fn automatic_worker_count_tracks_available_parallelism_and_is_nonzero() {
    let expected = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let resolved = resolve_worker_threads(None);
    assert_eq!(resolved, expected);
    assert!(resolved >= 1);
}

#[test]
fn configured_worker_count_wins() {
    assert_eq!(resolve_worker_threads(Some(3)), 3);
}

#[test]
fn zero_and_values_above_the_ceiling_are_rejected() {
    assert_eq!(
        validate_worker_threads(Some(0)),
        Err(WorkerThreadsError::Zero)
    );
    assert_eq!(
        validate_worker_threads(Some(MAX_WORKER_THREADS + 1)),
        Err(WorkerThreadsError::TooMany {
            configured: MAX_WORKER_THREADS + 1
        })
    );
}

#[test]
fn rejection_messages_name_the_bound_and_the_escape_hatch() {
    assert_eq!(
        WorkerThreadsError::Zero.to_string(),
        "must be >= 1 (omit for automatic sizing)"
    );
    assert_eq!(
        WorkerThreadsError::TooMany {
            configured: MAX_WORKER_THREADS + 1
        }
        .to_string(),
        format!(
            "must be <= {MAX_WORKER_THREADS} (got {}; omit for automatic sizing)",
            MAX_WORKER_THREADS + 1
        )
    );
}

#[test]
fn worker_count_boundaries_and_automatic_sizing_are_accepted() {
    for workers in [Some(1), Some(MAX_WORKER_THREADS), None] {
        assert!(
            validate_worker_threads(workers).is_ok(),
            "{workers:?} must be accepted"
        );
    }
}
