#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "unit test assertions use unwrap/expect for impossible fixture failures"
)]

use super::*;
use crate::config::{CommonConfig, ObjectStoreConfig};

/// A config differing from the default only in the two object-store knobs `duck_s3_access` reads,
/// spelled as `WALRUS_OBJECT_STORE__ENDPOINT` / `__REGION` would deserialize them.
fn cfg(endpoint: Option<&str>) -> TransformerConfig {
    TransformerConfig {
        common: CommonConfig {
            object_store: ObjectStoreConfig {
                bucket: "walrus-staging".to_string(),
                endpoint: endpoint.map(String::from),
                region: "eu-west-2".to_string(),
            },
            ..CommonConfig::default()
        },
        ..TransformerConfig::default()
    }
}

/// DuckDB's httpfs wants a scheme-less `host:port`; the scheme is what selects TLS, and MinIO in
/// compose is served over plain HTTP.
#[test]
fn an_http_endpoint_loses_its_scheme_and_leaves_tls_off() {
    let access = duck_s3_access(&cfg(Some("http://minio:9000")));

    assert_eq!(access.endpoint, "minio:9000");
    assert!(!access.use_ssl);
}

#[test]
fn an_https_endpoint_loses_its_scheme_and_turns_tls_on() {
    let access = duck_s3_access(&cfg(Some("https://s3.eu-west-2.amazonaws.com")));

    assert_eq!(access.endpoint, "s3.eu-west-2.amazonaws.com");
    assert!(access.use_ssl);
}

/// A scheme-less endpoint is already in DuckDB's spelling, so it passes through untouched — and
/// stays plain HTTP, because only `https://` asks for TLS.
#[test]
fn a_scheme_less_endpoint_passes_through_verbatim() {
    let access = duck_s3_access(&cfg(Some("localhost:9000")));

    assert_eq!(access.endpoint, "localhost:9000");
    assert!(!access.use_ssl);
}

/// `endpoint: None` means real AWS, where DuckDB derives the host from the region itself.
#[test]
fn no_endpoint_yields_an_empty_host_and_the_configured_region() {
    let access = duck_s3_access(&cfg(None));

    assert!(access.endpoint.is_empty());
    assert!(!access.use_ssl);
    assert_eq!(access.region, "eu-west-2");
}

#[tokio::test]
async fn zero_table_shard_reports_an_epoch_bump_through_the_supervisor() {
    let baseline = common::EpochNo(7);
    let (epoch_tx, epoch_rx) = tokio::sync::watch::channel(baseline);
    let token = tokio_util::sync::CancellationToken::new();
    let (failure_tx, failure_rx) = crate::supervisor::failure_channel(0);
    epoch_tx.send(common::EpochNo(8)).unwrap();

    let failure = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        crate::supervisor::supervise(
            failure_rx,
            &token,
            guard_idle_epoch(epoch_rx, baseline, token.clone(), failure_tx),
        ),
    )
    .await
    .expect("an idle shard must not hang after its epoch retires")
    .expect("the forward epoch move is a supervised failure");

    assert!(matches!(
        failure.error,
        TransformerError::EpochBumped { from, to }
            if from == baseline && to == common::EpochNo(8)
    ));
    assert!(
        token.is_cancelled(),
        "the supervisor cancels every side task before pipeline joins them"
    );
}

#[tokio::test]
async fn zero_table_epoch_guard_joins_cleanly_on_shutdown() {
    let baseline = common::EpochNo(11);
    let (_epoch_tx, epoch_rx) = tokio::sync::watch::channel(baseline);
    let token = tokio_util::sync::CancellationToken::new();
    let (failure_tx, failure_rx) = crate::supervisor::failure_channel(0);
    token.cancel();

    let failure = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        crate::supervisor::supervise(
            failure_rx,
            &token,
            guard_idle_epoch(epoch_rx, baseline, token.clone(), failure_tx),
        ),
    )
    .await
    .expect("shutdown must promptly join an idle shard's epoch guard");
    assert!(
        failure.is_none(),
        "operator shutdown is not a worker failure"
    );
}
