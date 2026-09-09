use super::*;

#[test]
fn unreachable_never_triggers_total_restart() {
    // A connection hiccup must route to Retry — never FreshSlot (which would replace + rebuild the
    // whole system on every transient blip). This is the load-bearing false-positive guard.
    assert_eq!(decide(SlotStatus::Unreachable), SlotAction::Retry);
    assert_ne!(decide(SlotStatus::Unreachable), SlotAction::FreshSlot);
}

#[test]
fn absent_or_invalidated_on_success_triggers_total_restart() {
    // Both authoritative "connected, slot gone" states open a fresh slot (→ epoch bump when a prior
    // generation exists).
    assert_eq!(decide(SlotStatus::Absent), SlotAction::FreshSlot);
    assert_eq!(decide(SlotStatus::Invalidated), SlotAction::FreshSlot);
}

#[test]
fn only_lost_decodes_to_the_invalidating_wal_status() {
    // The whole catalog vocabulary, plus the two shapes that must decode to nothing: a word this
    // walrus does not know, and the right word in the wrong case. Neither one is slot loss, so
    // neither may compare equal to `Lost` at the classification site.
    for (text, decoded) in [
        ("reserved", Some(WalStatus::Reserved)),
        ("extended", Some(WalStatus::Extended)),
        ("unreserved", Some(WalStatus::Unreserved)),
        ("lost", Some(WalStatus::Lost)),
        ("Lost", None),
        ("", None),
    ] {
        assert_eq!(
            WalStatus::from_catalog(text),
            decoded,
            "wal_status {text:?}"
        );
    }
}

#[test]
fn gauge_codes_match_the_dashboard_and_alert_contract() {
    // `deploy/observability/` pins these numbers: the stat panel is titled
    // `0 reserved · 1 unreserved · 2 lost` and `WalrusSlotWalStatusDegraded` pages at `>= 1`. So
    // `extended` — retention doing its job — must stay a healthy 0, and only the last two page.
    assert_eq!(WalStatus::Reserved.gauge_code(), 0);
    assert_eq!(WalStatus::Extended.gauge_code(), 0);
    assert_eq!(WalStatus::Unreserved.gauge_code(), 1);
    assert_eq!(WalStatus::Lost.gauge_code(), 2);
}

#[test]
fn healthy_resumes_from_confirmed_flush() {
    let cf: Lsn = "0/1234".parse().unwrap();
    assert_eq!(
        decide(SlotStatus::Healthy {
            confirmed_flush: cf
        }),
        SlotAction::Resume {
            confirmed_flush: cf
        }
    );
}
