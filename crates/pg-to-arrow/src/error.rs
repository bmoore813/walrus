//! `pg-to-arrow` error taxonomy.

use common::Redacted;

/// Cold parse-error detail boxed inside [`Error`] so successful per-cell conversions stay compact.
///
/// [`Error`] itself cannot be `Clone`/`PartialEq` (it carries opaque arrow/parquet sources), but this
/// payload is three owned strings — one of them wrapped — so a caller that destructures
/// [`Error::ValueParse`] can compare or keep the detail, and is the only thing that can read the
/// offending value back. Derives do not change the boxed layout the size assertion below pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueParseDetail {
    /// Name of the column whose value failed to parse.
    pub column: String,
    /// The offending wire text, as received — a **verbatim cell of a source table**, so it is
    /// whatever the customer stored there. This error rides `?` out of the per-row append, through
    /// `extractor::batch::BatchError` and `anyhow`, to the extractor's one `tracing::error!`: rendering it
    /// there would ship a customer's row to the log aggregator. [`Redacted`] keeps the text
    /// reachable by a caller that destructures the detail while removing it from every formatter —
    /// this struct's own derived `Debug` included.
    pub value: Redacted<String>,
    /// The Arrow type it was being parsed as, rendered for the message.
    pub data_type: String,
}

/// Everything that can go wrong mapping a Postgres relation to Arrow.
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A caller requested the Tier-1 mapping for a type handled by another tier or not supported at
    /// all. We fail loudly rather than emit a wrong-but-compiling field; the conformance tests guard
    /// that boundary.
    #[error("type oid {oid} (typmod {typmod}) is not a Tier-1 type")]
    NotTier1 {
        /// PostgreSQL type OID outside the Tier-1 mapping.
        oid: u32,
        /// PostgreSQL type modifier supplied with the OID.
        typmod: i32,
    },
    /// A relation arrived with no columns at all. Arrow tolerates an empty schema; walrus does not,
    /// because a file with no columns can never be reconciled against a later version of the table.
    #[error("relation {relation} has no columns")]
    EmptyRelation {
        /// Schema-qualified relation name.
        relation: String,
    },
    /// One cell's wire text did not parse as its planned type — a bad range literal, a malformed
    /// point. The detail is boxed so the success path stays cheap; see [`ValueParseDetail`].
    /// Names the column and the target type; the offending value renders as `[redacted]`, because
    /// it is a source-table cell — see [`ValueParseDetail::value`].
    #[error("column {}: cannot parse {} as {}", .0.column, .0.value, .0.data_type)]
    ValueParse(Box<ValueParseDetail>),
    /// A tuple's value count disagrees with the relation's column count, which means the row is
    /// being read against the wrong schema version.
    #[error("row has {got} values, relation has {expected} columns")]
    RowLenMismatch {
        /// Column count required by the relation shape.
        expected: usize,
        /// Value count present in the decoded row.
        got: usize,
    },
    /// A planned Arrow builder did not have the concrete type the plan recorded for it. This is a
    /// walrus bug rather than bad input.
    #[error("internal: builder downcast failed for column {column}")]
    Downcast {
        /// Column whose builder disagreed with the plan.
        column: String,
    },
    /// Arrow itself rejected the operation. Boxed to keep this enum small.
    #[error("arrow error: {0}")]
    Arrow(#[source] Box<arrow::error::ArrowError>),
    /// Parquet encoding or footer writing failed. Boxed for the same reason as `Arrow`.
    #[error("parquet error: {0}")]
    Parquet(#[source] Box<parquet::errors::ParquetError>),
}

impl From<arrow::error::ArrowError> for Error {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::Arrow(Box::new(error))
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(Box::new(error))
    }
}

impl Error {
    /// Build a boxed parse error without exposing its storage choice to call sites. `value` is
    /// wrapped on the way in, so no call site can forget the redaction.
    ///
    /// Cold because a well-formed cell never constructs this diagnostic payload.
    ///
    /// Building the diagnostic is the entire call: an `Error` that is not returned reports nothing.
    /// The `impl Into<String>` parameters are what hide that from `clippy::must_use_candidate`,
    /// which treats a generic argument as possibly side-effecting and skips the function — the same
    /// reason `common::__private::unknown_variant` states the attribute by hand. The attribute
    /// belongs here rather than on [`Error`] itself: a bare `#[must_use]` on a function whose return
    /// type already carries one is `clippy::double_must_use`.
    #[cold]
    #[must_use]
    pub fn value_parse(
        column: impl Into<String>,
        value: impl Into<String>,
        data_type: impl Into<String>,
    ) -> Self {
        Self::ValueParse(Box::new(ValueParseDetail {
            column: column.into(),
            value: Redacted::new(value.into()),
            data_type: data_type.into(),
        }))
    }
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<Error>() == 32,
    "pg_to_arrow::Error rides the per-row append_row Result; keep the cold payload boxed"
);
