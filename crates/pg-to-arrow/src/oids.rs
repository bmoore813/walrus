//! Canonical `pg_catalog` base-type OIDs — moved down to `common` so `common::pg_shape`
//! and `transformer` can share the single source of truth. Re-exported here to keep `pg_to_arrow::oids::*`
//! resolving for this crate's existing call sites.

#[allow(
    clippy::wildcard_imports,
    reason = "forwarding shim — this module IS common::oids under pg_to_arrow's path, so naming \
              its consts here would be a second list to keep in step with the first"
)]
pub use common::oids::*;
