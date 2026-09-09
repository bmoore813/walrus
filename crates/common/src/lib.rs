//! Shared primitives for walrus: errors + exit codes, [`Lsn`], telemetry,
//! [`ExtractorMeta`], [`Redacted`], and the neutral Postgres shape types shared across the workspace.
//!
//! # Features
//!
//! - **`sqlx`** *(off by default)* — SQLx `Type`/`Encode`/`Decode` for [`Lsn`] (Postgres `pg_lsn`)
//!   and for the [`ids`] newtypes (`int8`). `control` and `transformer` enable it; a consumer that needs
//!   only the domain types never links SQLx.
//! - **`tokio-console`** *(off by default, diagnostic only)* — adds the console layer used by the
//!   two binaries' own `tokio-console` features. It still leaves the one subscriber install here.
//!
//! Serde is deliberately *not* a feature. [`ExtractorMeta`], [`TypeDescriptor`], and [`Lsn`] are
//! wire contracts, while `serde_json` and `tracing-subscriber`'s `json` layer depend on serde
//! unconditionally. Reconsider a feature only if a supported consumer can compile `common`
//! without those contracts.
//!
//! # Stability
//!
//! `common::__private` is **not** part of this crate's API. It exists only because expansions of
//! `common`'s exported macros — currently `string_enum!` — must name their helpers from the
//! caller's crate, which forces those helpers to be `pub` somewhere. Its contents are exempt from
//! every stability guarantee the rest of this crate makes and may be renamed or removed in any
//! change; walrus has no `CHANGELOG`, so this is the note that says so. Write `string_enum!` and
//! let it reach `__private` for you; never name the module yourself.

#![cfg_attr(
    test,
    allow(
        clippy::let_underscore_must_use,
        reason = "unit tests intentionally discard cleanup results"
    )
)]

pub mod error;
pub mod extractor_meta;
pub mod failure_class;
pub mod ids;
pub mod lsn;
pub mod metrics;
pub mod oids;
pub mod pg_shape;
pub mod redact;
pub mod runtime;
pub mod sql;
pub mod telemetry;
pub mod type_descriptor;

// `string_enum!` is published from the crate root by its export attribute, so this module stays
// private: `use common::string_enum;` resolves the macro, not the module.
mod string_enum;

// The flat block below is this crate's entry point, and deliberately not a `prelude` to glob.
// Every consumer reaches the root through exactly one brace-grouped `use common::{…}`, spelling
// its own one-to-ten-item subset; a glob would swap those explicit lists for an over-import in
// which a `Result` shadows the `std` prelude's and an `Error` the crate-local one. The namespace
// modules (`oids`, `metrics`, `sql`, `runtime`) stay qualified too: `oids::INT4` says what a
// bare `INT4` would not. Reopen if a consumer ever needs a second `use` statement for this root.
pub use error::{Error, ExitCode, Result};
pub use extractor_meta::{
    ExtractorMeta, Kind, Op, PG_EPOCH_UNIX_MICROS, PG_EPOCH_UNIX_SECS, UtcTimestamp,
};
pub use failure_class::FailureClass;
pub use ids::{DdlId, EpochNo, ManifestId, ReloadId, SchemaVersionNo};
pub use lsn::Lsn;
pub use pg_shape::{PgColumn, PgRelation, ReplicaIdentity, TupleValue};
pub use redact::{REDACTED, Redacted};
#[cfg(feature = "tokio-console")]
pub use telemetry::init_tracing_with_console;
pub use telemetry::{TelemetryConfig, init_tracing};
pub use type_descriptor::{Tier, TypeDescriptor, TypeMeta};

/// Implementation details reachable from `#[macro_export]`ed macros. **Not API.**
///
/// Everything here must be `pub` so an expansion in another crate can name it as
/// `$crate::__private::…`; the hidden-doc attribute keeps it out of rendered documentation.
/// Nothing outside walrus's own macros may depend on these items; they change without notice.
#[doc(hidden)]
pub mod __private {
    pub use crate::string_enum::unknown_variant;
}

#[cfg(test)]
#[path = "lib_test.rs"]
mod tests;
