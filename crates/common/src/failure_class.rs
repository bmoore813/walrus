//! [`FailureClass`] — the terminal-vs-transient contract every walrus error enum implements.
//!
//! **Required:** [`FailureClass::is_terminal`] — the one thing an implementor must decide, and
//! deliberately the only one: an exhaustive `match` with no `_` arm makes an unclassified new
//! variant a compile error.
//!
//! **Defaulted:** [`FailureClass::is_transient`] is the exact complement of `is_terminal` and should
//! never be overridden. [`FailureClass::exit_code`] answers [`Internal`](ExitCode::Internal) (70)
//! for an unclassified failure; every error type whose values reach a `main` overrides it with the
//! documented per-variant code.
//!
//! **Deliberately NOT sealed.** Cross-crate implementation *is* the contract: `control::ControlError`
//! and `transformer::TransformerError` classify themselves from their own crates, which a private supertrait
//! here would forbid. Sealing is for single-implementor seams like `extractor::batch::Clock`.
//!
//! # Examples
//!
//! An implementor decides `is_terminal` and inherits the other two answers:
//!
//! ```
//! use common::{ExitCode, FailureClass};
//!
//! #[derive(Debug)]
//! enum BootstrapError {
//!     BadConfig,
//!     ControlDbStillComingUp,
//! }
//!
//! impl FailureClass for BootstrapError {
//!     // The exhaustive match with no `_` arm is the point: a new variant will not compile
//!     // until it is classified here.
//!     fn is_terminal(&self) -> bool {
//!         match self {
//!             BootstrapError::BadConfig => true,
//!             BootstrapError::ControlDbStillComingUp => false,
//!         }
//!     }
//! }
//!
//! assert!(BootstrapError::BadConfig.is_terminal());
//! assert!(BootstrapError::ControlDbStillComingUp.is_transient());
//! // The defaulted exit code stays `Internal` until an implementor overrides it.
//! assert_eq!(BootstrapError::BadConfig.exit_code(), ExitCode::Internal);
//! ```

use crate::ExitCode;

/// The terminal-vs-transient contract every walrus error enum implements.
pub trait FailureClass {
    /// REQUIRED. True when retrying under the startup deadline can never help — die now, non-zero.
    #[must_use = "classification is the whole point — calling this for effect does nothing"]
    fn is_terminal(&self) -> bool;

    /// DEFAULTED. The exact complement of [`Self::is_terminal`]: a dependency that may still be
    /// coming up, so the bootstrap retries it with backoff. Do not override.
    #[must_use = "classification is the whole point — calling this for effect does nothing"]
    fn is_transient(&self) -> bool {
        !self.is_terminal()
    }

    /// DEFAULTED. The process exit status for this failure. The default is the unclassified
    /// fallback; override it wherever the values can reach `std::process::ExitCode`.
    #[must_use = "the exit code must reach main — dropping it loses the failure class"]
    fn exit_code(&self) -> ExitCode {
        ExitCode::Internal
    }
}

#[cfg(test)]
#[path = "failure_class_test.rs"]
mod tests;
