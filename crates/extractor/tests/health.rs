#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! The health server, end to end: `/startup` gates `/ready` until bootstrap completes, and a
//! cancelled token drives `with_graceful_shutdown` to return — the same path SIGTERM trips.

use extractor::health::{HealthState, serve_on};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Raw HTTP/1.1 GET → the numeric status code. `Connection: close` lets `read_to_end` finish.
async fn get_status(addr: SocketAddr, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    text.lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

async fn spawn_server() -> (
    SocketAddr,
    Arc<HealthState>,
    CancellationToken,
    JoinHandle<anyhow::Result<()>>,
) {
    let state = HealthState::new();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = CancellationToken::new();
    let handle = tokio::spawn(serve_on(listener, Arc::clone(&state), token.clone()));
    (addr, state, token, handle)
}

#[tokio::test]
async fn startup_gates_ready_until_bootstrap_completes() {
    let (addr, state, token, handle) = spawn_server().await;

    // Bootstrapping: /startup and /ready are 503; /healthz is already 200 (liveness = deadlock only).
    assert_eq!(get_status(addr, "/startup").await, 503);
    assert_eq!(get_status(addr, "/ready").await, 503);
    assert_eq!(get_status(addr, "/healthz").await, 200);

    state.mark_ready();
    assert_eq!(get_status(addr, "/startup").await, 200);
    assert_eq!(get_status(addr, "/ready").await, 200);

    // Terminating drops readiness but never liveness.
    state.mark_terminating();
    assert_eq!(get_status(addr, "/ready").await, 503);
    assert_eq!(get_status(addr, "/healthz").await, 200);

    token.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sigterm_cancels_and_server_returns() {
    // A real SIGTERM would kill the test runner; SIGTERM's only effect is to cancel the token, so we
    // assert that path drives the server to return via `with_graceful_shutdown`.
    let (_addr, _state, token, handle) = spawn_server().await;
    token.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("server returns promptly after cancel");
    joined.unwrap().unwrap();
}
