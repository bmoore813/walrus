use super::cancel_on_exit;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// When cancellation and periodic work are both ready, shutdown must win every selection.
#[tokio::test]
async fn biased_select_prefers_cancellation() {
    let token = CancellationToken::new();
    token.cancel();
    let mut tick = tokio::time::interval(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut cancels = 0_u32;
    for _ in 0..100 {
        tokio::select! {
            biased;
            () = token.cancelled() => cancels += 1,
            _ = tick.tick() => {}
        }
    }
    assert_eq!(cancels, 100, "shutdown must always win a ready race");
}

#[tokio::test]
async fn cancel_on_exit_cancels_when_the_body_fails() {
    let token = CancellationToken::new();
    let observer = token.clone();
    let out: Result<(), &str> =
        cancel_on_exit(&token, async { Err("post-spawn step failed") }).await;
    assert!(out.is_err());
    assert!(
        observer.is_cancelled(),
        "an early return from a post-spawn step must cancel the token"
    );
}

#[tokio::test]
async fn cancel_on_exit_cancels_on_success_so_the_join_can_complete() {
    let token = CancellationToken::new();
    let observer = token.clone();
    let out: Result<u8, &str> = cancel_on_exit(&token, async { Ok(7) }).await;
    assert_eq!(out.unwrap(), 7);
    assert!(
        observer.is_cancelled(),
        "the health-server join needs cancellation after a successful pipeline"
    );
}
