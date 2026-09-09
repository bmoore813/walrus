use super::*;
use common::{EpochNo, FailureClass};

#[test]
fn a_watch_bump_trips_the_total_restart_guard() {
    let (tx, rx) = tokio::sync::watch::channel(EpochNo(7));
    assert!(epoch_guard(*rx.borrow(), EpochNo(7), EpochNo(7)).is_ok());
    assert!(crate::epoch::advance(&tx, Some(EpochNo(8))));

    let err = epoch_guard(*rx.borrow(), EpochNo(7), EpochNo(7))
        .expect_err("a bumped epoch must exit loudly");
    assert!(matches!(
        &err,
        TransformerError::EpochBumped {
            from: EpochNo(7),
            to: EpochNo(8)
        }
    ));
    assert_eq!(err.exit_code(), common::ExitCode::Internal);
}

#[test]
fn a_stale_lower_reading_never_disarms_the_guard() {
    let (tx, rx) = tokio::sync::watch::channel(EpochNo(9));

    assert!(!crate::epoch::advance(&tx, Some(EpochNo(4))));
    assert_eq!(*rx.borrow(), EpochNo(9));
    assert!(!crate::epoch::advance(&tx, None));
    assert_eq!(*rx.borrow(), EpochNo(9));
    assert!(epoch_guard(*rx.borrow(), EpochNo(8), EpochNo(8)).is_err());
}
