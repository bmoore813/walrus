//! Context helpers for DuckDB results.
//!
//! Every DuckDB failure in this crate is terminal and must identify its operation while preserving
//! the engine error as a typed source. This module keeps that construction in one place.
//!
//! **Required:** [`DuckResultExt::duck_with`] — the lazy form is the orthogonal one. It decides both
//! the operation text and the fact that the text is built only on the error path, and nothing else
//! here can be expressed without it.
//!
//! **Defaulted:** [`DuckResultExt::duck`] is `duck_with` over a constant operation, so a receiver
//! cannot make the two disagree. Override it only where a receiver can attach a `&str` more cheaply
//! than through a closure; the observable [`TransformerError::Duck`] must stay identical.

use crate::error::TransformerError;

mod private {
    pub trait Sealed {}
}

/// Build the transformer's typed DuckDB error for the direct-error path in full-rebuild handling.
pub(crate) fn duck_err(op: impl Into<String>, source: duckdb::Error) -> TransformerError {
    TransformerError::Duck {
        op: op.into(),
        source,
    }
}

/// Attach operation context to a DuckDB result while preserving its typed source error.
///
/// This trait is sealed: `.duck()` / `.duck_with()` can be called from anywhere the lib is a
/// dependency (integration tests, benches, a future crate), but only `transformer` can implement it.
/// The whole point of the extension is that [`TransformerError::Duck`] is the *single* shape a DuckDB
/// failure takes — `op` plus the preserved engine source. A foreign impl over some other receiver
/// would reintroduce the hand-rolled mapping this module replaced, and it would make every method
/// added here later a breaking change instead of an internal one.
///
/// The example below is also the *whole* impl surface — one method, with `duck` defaulted on top of
/// it — and it still does not compile, because the seal is what rejects the receiver:
///
/// ```compile_fail
/// use transformer::duck_ext::DuckResultExt;
/// use transformer::error::TransformerError;
///
/// struct Outcome<T>(T);
///
/// impl<T> DuckResultExt for Outcome<T> {
///     type Ok = T;
///
///     fn duck_with(self, _op: impl FnOnce() -> String) -> Result<T, TransformerError> {
///         Ok(self.0)
///     }
/// }
/// ```
pub trait DuckResultExt: private::Sealed {
    /// The success payload the receiver carries through. It is *associated*, not a trait parameter:
    /// a receiver determines exactly one payload — `Result<T, duckdb::Error>` can only ever hand
    /// back its own `T` — so there is no second impl for the same receiver to disambiguate, and a
    /// bound reads `R: DuckResultExt` instead of dragging a free `T` through every signature.
    type Ok;

    /// REQUIRED. Lazily build a formatted operation description only on the error path.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] with the generated operation and original DuckDB error when the
    /// result is an error.
    fn duck_with(self, op: impl FnOnce() -> String) -> Result<Self::Ok, TransformerError>;

    /// DEFAULTED. Attach a constant operation description.
    ///
    /// The default hands [`Self::duck_with`] a closure that renders `op` only once the result is
    /// known to be an error — the same single allocation, on the same path, as a hand-written impl.
    /// `Self: Sized` is what lets the body move `self` into that call; it excludes nothing, since a
    /// receiver of this trait is always a `Result`.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] with `op` and the original DuckDB error when the result is an
    /// error.
    fn duck(self, op: &str) -> Result<Self::Ok, TransformerError>
    where
        Self: Sized,
    {
        self.duck_with(|| op.to_owned())
    }
}

/// The one receiver, for every payload `T`: attaching this context is only meaningful where the
/// failure already *is* a `duckdb::Error`. The seal impl sits with the blanket impl so the pair is
/// added or removed together.
impl<T> private::Sealed for Result<T, duckdb::Error> {}

impl<T> DuckResultExt for Result<T, duckdb::Error> {
    type Ok = T;

    /// The one required method; `duck` arrives from the trait's default and is not overridden — the
    /// default is already `map_err` over the same constructor, so an override could only re-say it.
    fn duck_with(self, op: impl FnOnce() -> String) -> Result<T, TransformerError> {
        self.map_err(|source| duck_err(op(), source))
    }
}

#[cfg(test)]
#[path = "duck_ext_test.rs"]
mod tests;
