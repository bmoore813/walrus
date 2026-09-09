use super::*;

#[derive(Debug)]
enum Fake {
    Cold,
    Broken,
}

impl FailureClass for Fake {
    fn is_terminal(&self) -> bool {
        matches!(self, Fake::Broken)
    }
}

#[test]
fn defaulted_is_transient_is_the_exact_complement() {
    assert!(Fake::Cold.is_transient());
    assert!(!Fake::Cold.is_terminal());
    assert!(!Fake::Broken.is_transient());
    assert!(Fake::Broken.is_terminal());
}

#[test]
fn defaulted_exit_code_is_the_unclassified_fallback() {
    assert_eq!(Fake::Broken.exit_code(), ExitCode::Internal);
}

#[test]
fn overriding_exit_code_keeps_the_defaulted_is_transient() {
    let error = crate::Error::ControlDb("still starting".to_owned());
    assert_eq!(error.exit_code(), ExitCode::ControlDb);
    assert!(error.is_transient());
}
