use super::*;

#[test]
fn transformer_phase_decoding_is_exhaustive() {
    assert_eq!(
        TransformerPhase::try_from(0),
        Ok(TransformerPhase::Bootstrapping)
    );
    assert_eq!(TransformerPhase::try_from(1), Ok(TransformerPhase::Ready));
    assert_eq!(
        TransformerPhase::try_from(2),
        Ok(TransformerPhase::Quarantined)
    );
    assert_eq!(TransformerPhase::try_from(7), Err(InvalidPhase(7)));
}

#[test]
fn clearing_quarantine_does_not_promote_bootstrapping() {
    let s = TransformerState::new();
    s.clear_quarantine();

    assert!(!s.is_started());
    assert!(!s.is_ready());
    assert!(!s.is_quarantined());
}

#[test]
fn quarantine_from_bootstrapping_does_not_publish_the_generation() {
    let s = TransformerState::new();
    s.quarantine();

    assert!(s.is_started());
    assert!(!s.is_ready());
    assert!(s.is_quarantined());

    s.clear_quarantine();
    assert!(
        !s.is_ready(),
        "repairing a table cannot infer that the global generation published"
    );
    assert!(!s.is_quarantined());

    s.mark_generation_ready();
    assert!(s.is_ready());
}

#[test]
fn ready_and_live_are_independent() {
    let s = TransformerState::new();
    assert!(!s.is_ready(), "not ready until bootstrap");
    assert!(!s.is_live(), "not live until the first poll stamp");
    s.stamp_poll();
    assert!(s.is_live(), "a stamped cycle → live");
    assert!(!s.is_ready(), "live does not imply ready");
    s.mark_ready();
    assert!(s.is_ready());
}

#[test]
fn reconciling_generation_is_started_but_not_ready_until_published() {
    let s = TransformerState::new();
    s.mark_reconciling();

    assert!(s.is_started(), "local bootstrap completed");
    assert!(!s.is_ready(), "the frozen table group has not published");

    s.mark_generation_ready();
    assert!(s.is_ready(), "generation publication opens readiness");
}

#[test]
fn retiring_a_published_generation_immediately_drops_readiness() {
    let s = TransformerState::new();
    s.mark_ready();
    assert!(s.is_ready());

    s.mark_generation_retired();
    assert!(!s.is_ready());
    assert!(
        s.is_started(),
        "local bootstrap remains complete while the process drains"
    );
}

#[test]
fn clearing_quarantine_does_not_bypass_generation_gate() {
    let s = TransformerState::new();
    s.mark_reconciling();
    s.quarantine();
    s.clear_quarantine();

    assert!(!s.is_quarantined());
    assert!(
        !s.is_ready(),
        "a table repair cannot publish the whole generation"
    );

    s.mark_generation_ready();
    assert!(s.is_ready());
}

#[test]
fn quarantine_degrades_ready_but_not_startup() {
    let s = TransformerState::new();
    s.mark_ready();
    assert!(s.is_ready() && s.is_started(), "ready after bootstrap");

    s.quarantine();
    assert!(s.is_quarantined(), "quarantine latched");
    assert!(!s.is_ready(), "/ready degrades on quarantine");
    assert!(
        s.is_started(),
        "/startup stays satisfied — bootstrap did complete"
    );

    // The one exit: a reload rebuild replaced the data — /ready recovers.
    s.clear_quarantine();
    assert!(!s.is_quarantined(), "the rebuild clears the latch");
    assert!(s.is_ready(), "/ready recovers after the rebuild");
}

#[test]
fn repairing_one_table_does_not_clear_another_tables_quarantine() {
    let s = TransformerState::new();
    s.mark_ready();
    s.quarantine_table("public", "accounts");
    s.quarantine_table("public", "orders");
    s.quarantine_table("public", "orders");

    s.clear_table_quarantine("public", "accounts");
    assert!(s.is_quarantined());
    assert!(!s.is_ready());

    s.clear_table_quarantine("public", "orders");
    assert!(!s.is_quarantined());
    assert!(s.is_ready());
}

#[test]
fn quoted_names_with_dots_are_distinct_quarantine_keys() {
    let s = TransformerState::new();
    s.mark_ready();
    s.quarantine_table("a.b", "c");
    s.quarantine_table("a", "b.c");

    s.clear_table_quarantine("a.b", "c");
    assert!(s.is_quarantined());

    s.clear_table_quarantine("a", "b.c");
    assert!(!s.is_quarantined());
}

#[test]
fn bootstrap_ready_transition_cannot_override_a_durable_table_quarantine() {
    let s = TransformerState::new();
    s.quarantine_table("public", "orders");

    s.mark_ready();
    assert!(s.is_started());
    assert!(s.is_quarantined());
    assert!(!s.is_ready());

    s.clear_table_quarantine("public", "orders");
    assert!(s.is_ready());
}
