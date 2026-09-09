use super::*;

#[test]
fn recovered_state_starts_a_fresh_bounded_cycle() {
    assert!(matches!(
        next_attempt(Some(IntegrityRecoveryStatus::Recovered), Some(9)),
        Ok(1)
    ));
}

#[test]
fn active_state_increments_without_wrapping() {
    assert!(matches!(
        next_attempt(Some(IntegrityRecoveryStatus::Retrying), Some(1)),
        Ok(2)
    ));
    assert!(next_attempt(Some(IntegrityRecoveryStatus::Quarantined), Some(i32::MAX)).is_err());
}
