use super::*;

#[tokio::test]
async fn token_is_live_until_cancelled() {
    let token = install_signal_handlers();
    assert!(!token.is_cancelled());
    // A cancel from another source trips the same token and unwinds the signal task.
    token.cancel();
    token.cancelled().await;
    assert!(token.is_cancelled());
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
