use super::*;

fn raw(
    current: &str,
    restart: Option<&str>,
    confirmed: Option<&str>,
    status: Option<&str>,
    safe: Option<i64>,
) -> RawSlotSample {
    RawSlotSample {
        current_lsn: current.to_owned(),
        restart_lsn: restart.map(str::to_owned),
        confirmed_flush_lsn: confirmed.map(str::to_owned),
        wal_status: status.map(str::to_owned),
        safe_wal_size: safe,
    }
}

#[test]
fn capability_query_never_mentions_an_unsupported_optional_column() {
    for (wal_status, safe_wal_size) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let sql = SlotCatalogCapabilities {
            wal_status,
            safe_wal_size,
        }
        .sample_sql();
        assert_eq!(
            sql.contains("wal_status::text"),
            wal_status,
            "query must reference wal_status iff the catalog exposes it"
        );
        assert_eq!(
            sql.contains("       safe_wal_size\n"),
            safe_wal_size,
            "query must reference safe_wal_size iff the catalog exposes it"
        );
        assert!(sql.contains("NULL::text") ^ wal_status);
        assert!(sql.contains("NULL::bigint") ^ safe_wal_size);
    }
}

#[test]
fn sample_mapping_computes_lag_and_retention_from_the_exact_catalog_lsns() {
    let sample = SlotSample::try_from_raw(raw(
        "0/1000",
        Some("0/0400"),
        Some("0/0C00"),
        Some("extended"),
        Some(65_536),
    ))
    .unwrap();

    assert_eq!(sample.replication_lag_bytes(), 0x400);
    assert_eq!(sample.retained_wal_bytes(), 0xC00);
    assert_eq!(sample.safe_wal_size, Some(65_536));
    assert_eq!(sample.wal_status, Some(SlotWalStatus::Extended));
}

#[test]
fn nullable_new_slot_lsns_do_not_manufacture_lag_from_zero() {
    let sample =
        SlotSample::try_from_raw(raw("1/1000", None, None, Some("reserved"), None)).unwrap();

    assert_eq!(sample.replication_lag_bytes(), 0);
    assert_eq!(sample.retained_wal_bytes(), 0);
}

#[test]
fn catalog_positions_ahead_of_current_saturate_instead_of_wrapping() {
    let sample = SlotSample::try_from_raw(raw(
        "0/0100",
        Some("0/0200"),
        Some("0/0300"),
        Some("reserved"),
        Some(0),
    ))
    .unwrap();

    assert_eq!(sample.replication_lag_bytes(), 0);
    assert_eq!(sample.retained_wal_bytes(), 0);
}

#[test]
fn malformed_lsn_and_negative_safe_size_are_rejected() {
    assert!(
        SlotSample::try_from_raw(raw("not-an-lsn", None, None, None, None))
            .unwrap_err()
            .to_string()
            .contains("current_lsn")
    );
    assert!(
        SlotSample::try_from_raw(raw("0/1", None, None, None, Some(-1)))
            .unwrap_err()
            .to_string()
            .contains("negative")
    );
}

#[test]
fn unknown_status_is_visible_and_conservatively_degraded() {
    let sample = SlotSample::try_from_raw(raw(
        "0/100",
        Some("0/080"),
        Some("0/090"),
        Some("future_status"),
        None,
    ))
    .unwrap();
    let status = sample.wal_status.unwrap();

    assert_eq!(status, SlotWalStatus::Unknown("future_status".to_owned()));
    assert_eq!(status.gauge_code(), 1);
}

#[test]
fn absent_and_present_states_are_distinct_even_without_wal_status_support() {
    assert_ne!(ObservedState::Absent, ObservedState::Present(None));
}

#[test]
fn absent_or_lost_slot_is_terminal_but_unreserved_is_observable() {
    let capabilities = SlotCatalogCapabilities {
        wal_status: true,
        safe_wal_size: true,
    };
    let mut state = None;
    assert!(observe_sample("slot", capabilities, None, &mut state).is_err());

    let mut state = None;
    let lost = SlotSample::try_from_raw(raw(
        "0/100",
        Some("0/080"),
        Some("0/090"),
        Some("lost"),
        Some(0),
    ))
    .unwrap();
    assert!(observe_sample("slot", capabilities, Some(lost), &mut state).is_err());

    let mut state = None;
    let unreserved = SlotSample::try_from_raw(raw(
        "0/100",
        Some("0/080"),
        Some("0/090"),
        Some("unreserved"),
        Some(0),
    ))
    .unwrap();
    observe_sample("slot", capabilities, Some(unreserved), &mut state).unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_preempts_a_hung_catalog_query() {
    let token = CancellationToken::new();
    token.cancel();
    let result = cancellable_query(&token, std::future::pending::<anyhow::Result<()>>()).await;

    assert!(
        result.is_none(),
        "shutdown must not await a hung source query"
    );
}

#[tokio::test(start_paused = true)]
async fn a_hung_catalog_query_becomes_an_explicit_timeout() {
    let token = CancellationToken::new();
    let result = cancellable_query(&token, std::future::pending::<anyhow::Result<()>>())
        .await
        .unwrap()
        .unwrap_err();

    assert!(result.to_string().contains("timed out"));
}
