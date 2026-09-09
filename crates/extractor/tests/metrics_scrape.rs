#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! `/metrics` exposes every extractor series the design enumerates. Guards against a metric-name
//! rename silently breaking the committed Grafana dashboard / Prometheus alerts: the endpoint's
//! exposition must contain every `common::metrics::names::EXTRACTOR_*` constant.

use common::metrics::names;
use extractor::health::{HealthState, serve_on};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// Raw HTTP/1.1 GET → the full response text. `Connection: close` lets `read_to_end` finish.
async fn get_body(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test]
async fn metrics_endpoint_exposes_all_extractor_series() {
    // init() installs the recorder and zero-inits every global extractor series, so the whole catalogue is
    // present on a fresh scrape (before any real batch has moved a needle).
    common::metrics::init();

    let state = HealthState::new();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = CancellationToken::new();
    let server = tokio::spawn(serve_on(listener, state, token.clone()));

    let body = get_body(addr, "/metrics").await;
    for name in names::EXTRACTOR_ALL {
        assert!(
            body.contains(name),
            "extractor series `{name}` missing from /metrics"
        );
    }

    token.cancel();
    server.await.unwrap().unwrap();
}
