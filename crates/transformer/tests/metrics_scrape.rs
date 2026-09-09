#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! `/metrics` exposes every transformer series the design enumerates. The transformer's series are
//! per-table, so `init_table_series` is how they first appear (the transformer calls it per owned table at
//! bootstrap); here we register a demo table and assert the exposition lists every
//! `common::metrics::names::TRANSFORMER_*` constant, labelled by table.

use common::metrics::names;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use transformer::health::{TransformerState, serve_on};

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
async fn metrics_endpoint_exposes_all_transformer_series() {
    common::metrics::init();
    // Per-table series appear once emitted for a table — the transformer does this per owned table at
    // bootstrap; the test stands in a demo table.
    common::metrics::init_table_series("public.demo");

    let state = TransformerState::new();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = CancellationToken::new();
    let server = tokio::spawn(serve_on(listener, state, token.clone()));

    let body = get_body(addr, "/metrics").await;
    for name in names::TRANSFORMER_ALL {
        assert!(
            body.contains(name),
            "transformer series `{name}` missing from /metrics"
        );
    }
    // The per-table label must be present (bounded cardinality: labelled by table, never per-row).
    assert!(
        body.contains("table=\"public.demo\""),
        "per-table label missing from /metrics"
    );

    token.cancel();
    server.await.unwrap().unwrap();
}
