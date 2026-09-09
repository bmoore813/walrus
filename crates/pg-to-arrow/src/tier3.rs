//! Tier-3 **canonical-text carriers** (walrus-extractor.md §2.5).
//!
//! Some Postgres types have no lossless structural target in Parquet/DuckDB, so we carry their
//! canonical text verbatim in one `VARCHAR` column — always exact as a string — and defer the lost
//! *type metadata* (bit length, enum labels, …) to the descriptor. The value is carried
//! **verbatim**: the wire text is already canonical, and re-formatting risks a lossy round-trip.
//!
//! The headline case is `numeric`: `p ≤ 38` stays a Tier-1 `Decimal128`, but **unconstrained
//! or `p > 38` must become `VARCHAR`** — DuckDB's `DECIMAL` caps at precision 38 and its Parquet
//! reader downcasts any decimal with precision > 38 to `DOUBLE` (verified in `parquet_reader.cpp`), so
//! a `Decimal256` would silently lose digits. `VARCHAR` is exact.

use crate::oids;
use arrow::datatypes::{DataType, Field};

/// True if this OID+typmod is carried as canonical `VARCHAR`.
///
/// **NOTE:** `numeric` with `p ≤ 38` is *not* Tier-3 — it stays Tier-1 `Decimal128`. Only unconstrained
/// (`numeric_precision_scale` → `None`) or `p > 38` numeric is Tier-3; `p == 38` is the Tier-1 boundary.
#[must_use]
pub fn is_tier3_text(type_oid: u32, atttypmod: i32) -> bool {
    match type_oid {
        oids::NUMERIC => matches!(
            crate::schema::numeric_precision_scale(atttypmod),
            None | Some((39..=255, _))
        ),
        oids::BIT
        | oids::VARBIT
        | oids::INET
        | oids::CIDR
        | oids::MACADDR
        | oids::MACADDR8
        | oids::TSVECTOR
        | oids::TSQUERY
        | oids::PG_LSN
        | oids::XID
        | oids::XID8
        | oids::TXID_SNAPSHOT
        | oids::XML => true,
        _ => false,
    }
}

/// A single nullable `Utf8` field carrying the canonical text.
#[must_use]
pub fn tier3_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

#[cfg(test)]
#[path = "tier3_test.rs"]
mod tests;
