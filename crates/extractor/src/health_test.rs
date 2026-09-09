use super::*;

#[test]
fn phase_decoding_is_exhaustive() {
    assert_eq!(Phase::try_from(0), Ok(Phase::Bootstrapping));
    assert_eq!(Phase::try_from(1), Ok(Phase::Ready));
    assert_eq!(Phase::try_from(7), Err(InvalidPhase(7)));
}

#[test]
fn phase_gates_readiness() {
    let s = HealthState::new();
    assert_eq!(s.phase(), Phase::Bootstrapping);
    assert!(!s.is_ready());
    assert!(s.is_live(), "liveness is up from the start (deadlock-only)");

    s.mark_ready();
    assert_eq!(s.phase(), Phase::Ready);
    assert!(s.is_ready());

    // Terminating drops readiness but NOT liveness (§4.3).
    s.mark_terminating();
    assert!(!s.is_ready());
    assert!(s.is_live());
}

#[test]
fn default_matches_the_shared_constructor() {
    let owned = HealthState::default();
    let shared = HealthState::new();

    // `live` is the one field a bare `#[derive(Default)]` would silently flip to `false`.
    assert!(owned.is_live(), "a fresh state is alive (deadlock-only)");
    assert_eq!(owned.phase(), shared.phase());
    assert_eq!(owned.is_live(), shared.is_live());
    assert_eq!(owned.is_ready(), shared.is_ready());
    assert_eq!(owned.is_degraded(), shared.is_degraded());
}

#[test]
fn degraded_does_not_affect_liveness() {
    let s = HealthState::new();
    s.mark_ready();
    s.set_degraded(true);
    assert!(s.is_degraded());
    assert!(s.is_live(), "high lag must never fail liveness");
}
