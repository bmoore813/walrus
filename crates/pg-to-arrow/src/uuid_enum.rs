//! `uuid` (native DuckDB `UUID`) and `enum` (VARCHAR + ordered labels) — two types that each hinge on
//! one subtlety (walrus-extractor.md §2.4 uuid, §2.5 enum).
//!
//! **uuid.** DuckDB reads a native `UUID` back from Parquet *only* when arrow-rs annotates the
//! `FixedSizeBinary(16)` with the `arrow.uuid` **canonical extension** (`ARROW:extension:name`) — a
//! *plain* FSB(16) writes un-annotated and reads back as a 16-byte `BLOB`. So this mapping is guarded
//! by a CI `write → read_parquet → typeof == UUID` conformance test and a pinned arrow-rs (Cargo.toml),
//! with a `VARCHAR + CAST(x AS UUID)` fallback if a bump ever drops the annotation.
//!
//! **enum.** Values are lossless as `VARCHAR`; the **ordered label set** is lost on the wire and is
//! carried by the descriptor, from which the transformer recreates the DuckDB `ENUM`. Enum OIDs
//! are dynamic (≥ [`oids::FIRST_NORMAL_OID`]). Because [`PgColumn`](common::PgColumn) has no
//! type-kind marker, [`is_enum_oid`] conservatively treats every non-builtin OID as an enum carrier.

use crate::error::Error;
use crate::oids;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;

/// The Arrow canonical-extension name that makes arrow-rs emit the Parquet UUID logical type.
pub const ARROW_UUID_EXTENSION: &str = "arrow.uuid";

/// `FixedSizeBinary(16)` carrying the `arrow.uuid` canonical extension → Parquet UUID → DuckDB `UUID`.
/// The extension metadata is the *only* thing that makes DuckDB see `UUID` rather than `BLOB`.
#[must_use]
pub fn uuid_field(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), true).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_string(),
        ARROW_UUID_EXTENSION.to_string(),
    )]))
}

/// Fallback if a pinned arrow-rs release ever drops the UUID annotation on the normal column path:
/// carry the canonical text as `Utf8` and `CAST(x AS UUID)` on load.
#[must_use]
pub fn uuid_as_varchar(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

/// `enum` → nullable `Utf8`; the ordered label set is carried by the descriptor, not here.
#[must_use]
pub fn enum_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

/// Conservative enum detection: a non-builtin OID (≥ [`oids::FIRST_NORMAL_OID`]) is treated as an
/// enum carrier because [`PgColumn`](common::PgColumn) does not expose a type-kind marker. Callers
/// can attach catalog-derived labels through the descriptor API.
#[must_use]
pub const fn is_enum_oid(type_oid: u32) -> bool {
    type_oid >= oids::FIRST_NORMAL_OID
}

/// Parse canonical UUID text (`"550e8400-e29b-41d4-a716-446655440000"`) into 16 bytes. Rejects
/// malformed input with [`Error::ValueParse`] (no silent zero-padding).
///
/// # Errors
///
/// Returns [`Error::ValueParse`] when `text` is not a canonical UUID accepted by `uuid::Uuid`.
///
/// # Examples
///
/// ```
/// use pg_to_arrow::uuid_enum::parse_uuid_bytes;
///
/// let bytes = parse_uuid_bytes("550e8400-e29b-41d4-a716-446655440000")?;
/// assert_eq!(
///     bytes,
///     [
///         0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
///         0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
///     ]
/// );
/// # Ok::<(), pg_to_arrow::Error>(())
/// ```
pub fn parse_uuid_bytes(text: &str) -> Result<[u8; 16], Error> {
    uuid::Uuid::parse_str(text)
        .map(uuid::Uuid::into_bytes)
        .map_err(|_| Error::value_parse("uuid", text, "uuid"))
}

#[cfg(test)]
#[path = "uuid_enum_test.rs"]
mod tests;
