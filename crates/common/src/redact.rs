//! [`Redacted`] — a value that keeps itself out of every formatter.
//!
//! Logs leave the process. They are shipped to an aggregator, retained for weeks, and read by more
//! people than the secret store is — so a credential that reaches `tracing` has moved into a system
//! with weaker access control than the one it came from, and cannot be recalled. Walrus holds three
//! kinds of value that must never make that trip: a libpq DSN, which carries its password inline; an
//! S3 secret access key, which *is* the credential; and a source-table cell, which is whatever the
//! customer stored there.
//!
//! The transformer's one `#[instrument]` attribute uses `skip_all` and records only its precomputed
//! table identity; the extractor's manual reload span likewise records only a table name. Neither
//! auto-captures a database pool, object-store handle, or secret-bearing configuration. Structs that
//! *hold* those values still derive `Debug`, though, which puts every field one `?cfg` away from a
//! log line. [`Redacted`] moves the safety promise into the type: `Debug` and `Display` both print
//! [`REDACTED`], and the value comes back only through the named [`Redacted::expose`], so the audit
//! is a grep for that one method rather than a review of every formatter in the tree.
//!
//! There is deliberately no `Serialize`: `Deserialize` is what config loading needs, and the
//! reverse direction is exactly how a secret would reach a JSON log layer. The `secrecy` crate
//! offers this same shape behind `expose_secret`; walrus writes the wrapper rather than take a
//! dependency for two formatter impls.

use std::fmt;

/// What every formatter prints in place of a secret.
///
/// Public so a test can assert on the redaction by naming it, instead of restating the literal and
/// drifting from it.
pub const REDACTED: &str = "[redacted]";

/// A secret that renders as [`REDACTED`] under both `{:?}` and `{}`.
///
/// Wrapping is a one-way door for formatting: a struct that derives `Debug` can hold one of these
/// and still be safe to log whole, and an accidental `%secret` in a `tracing` macro prints the
/// placeholder rather than the value. [`Redacted::expose`] is the single, greppable way back out.
///
/// `Deserialize` is `transparent`, so a `Redacted<String>` config field loads from exactly the YAML
/// or environment value a bare `String` did — wrapping a field changes no operator-facing key.
#[derive(Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(transparent)]
pub struct Redacted<T>(T);

impl<T> Redacted<T> {
    /// Wrap a secret so it stops rendering itself.
    #[must_use]
    pub const fn new(secret: T) -> Self {
        Self(secret)
    }

    /// Borrow the secret. Every call site that reveals the value names this method.
    #[must_use]
    pub const fn expose(&self) -> &T {
        &self.0
    }
}

/// Prints [`REDACTED`], ignoring width and precision: a formatter flag must not become a way to
/// read the value one character at a time.
impl<T> fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// Identical to the `Debug` impl above, because `%secret` and `?secret` are the same mistake.
impl<T> fmt::Display for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// Spelled for `String` rather than blanket over `T`, so `Redacted::new` stays the conversion for
/// every other secret type and nothing is wrapped by inference alone.
impl From<String> for Redacted<String> {
    fn from(secret: String) -> Self {
        Self(secret)
    }
}

/// The borrowed half of the pair above: config fixtures and the reload controller both wrap a
/// `&str` they do not own.
impl From<&str> for Redacted<String> {
    fn from(secret: &str) -> Self {
        Self(secret.to_owned())
    }
}

#[cfg(test)]
#[path = "redact_test.rs"]
mod tests;
