use super::*;
use common::FailureClass;

#[tokio::test(start_paused = true)]
async fn joined_dependency_checks_cost_the_max_not_the_sum() {
    async fn fake_dependency(delay: Duration) -> Result<u8, Error> {
        tokio::time::sleep(delay).await;
        Ok(7)
    }

    let start = Instant::now();
    let delay = Duration::from_millis(200);
    let (control, object_store) =
        tokio::try_join!(fake_dependency(delay), fake_dependency(delay)).unwrap();
    let elapsed = start.elapsed();

    assert_eq!((control, object_store), (7, 7));
    assert!(
        elapsed < Duration::from_millis(350),
        "sequential awaits would take about 400ms; got {elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn try_join_fails_fast_on_a_terminal_error() {
    let start = Instant::now();
    let out: Result<(u8, u8), Error> =
        tokio::try_join!(async { Err(Error::Config("bad bucket".into())) }, async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(1)
        },);

    assert!(matches!(out, Err(Error::Config(_))));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "the slow branch must be dropped, not awaited"
    );
}

#[tokio::test]
async fn retry_returns_immediately_on_terminal() {
    let deadline = Instant::now() + Duration::from_secs(3600);
    let mut calls = 0;
    let out: Result<(), Error> = retry_transient(deadline, "x", || {
        calls += 1;
        async { Err(Error::Config("bad".into())) }
    })
    .await;
    assert!(matches!(out, Err(Error::Config(_))));
    assert_eq!(calls, 1, "terminal errors are not retried");
}

#[tokio::test]
async fn retry_gives_up_at_deadline_with_the_transient_error() {
    // Deadline already elapsed → one attempt, then surface the transient error.
    let deadline = Instant::now();
    let out: Result<(), Error> = retry_transient(deadline, "control database", || async {
        Err(Error::ControlDb("connection refused".into()))
    })
    .await;
    match out {
        Err(e) => {
            assert!(e.is_transient());
            assert_eq!(e.exit_code(), common::ExitCode::ControlDb);
        }
        Ok(()) => panic!("expected the transient error to be surfaced"),
    }
}

#[tokio::test]
async fn retry_succeeds_after_a_transient_blip() {
    let deadline = Instant::now() + Duration::from_secs(3600);
    let mut attempts = 0;
    let out: Result<u8, Error> = retry_transient(deadline, "object store", || {
        attempts += 1;
        async move {
            if attempts < 2 {
                Err(Error::ObjectStore("503".into()))
            } else {
                Ok(7)
            }
        }
    })
    .await;
    assert_eq!(out.unwrap(), 7);
    assert_eq!(attempts, 2);
}

/// The old `F: FnMut() -> Fut` bound could not express this: the attempt future borrows `label`
/// from the enclosing scope while the closure also mutates `attempts`.
#[tokio::test]
async fn retry_accepts_a_borrowing_async_closure() {
    let deadline = Instant::now() + Duration::from_secs(3600);
    let label = String::from("canary");
    let mut attempts = 0_u32;
    let out: Result<usize, Error> = retry_transient(deadline, "borrowing", async || {
        attempts += 1;
        if attempts < 2 {
            Err(Error::ObjectStore("503".into()))
        } else {
            Ok(label.len())
        }
    })
    .await;
    assert_eq!(out.unwrap(), 6);
    assert_eq!(attempts, 2);
}

#[test]
fn backoff_doubles_then_saturates_at_the_ceiling() {
    assert_eq!(
        next_backoff(Duration::from_millis(200)),
        Duration::from_millis(400)
    );
    assert_eq!(next_backoff(Duration::from_secs(4)), MAX_BACKOFF);
    assert_eq!(next_backoff(MAX_BACKOFF), MAX_BACKOFF);
}

#[test]
fn backoff_from_duration_max_does_not_panic() {
    assert_eq!(next_backoff(Duration::MAX), MAX_BACKOFF);
}

#[test]
fn backoff_never_drops_below_the_floor() {
    assert_eq!(next_backoff(Duration::ZERO), INITIAL_BACKOFF);
}
