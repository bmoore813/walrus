//! [`TypeDescriptor`] — the per-column type-mapping descriptor (walrus-extractor.md §2.6).
//!
//! Part of the same decoupling seam as [`crate::pg_shape`]: the extractor writes one descriptor per
//! source column into `schema_registry` (keyed by `schema_version`); the transformer reads it back to
//! recreate enum types / bit lengths / char lengths / interval structs and `CAST` the carried
//! columns into place. That is what makes "reconcile to the exact source shape" a **mechanical**
//! operation rather than a guess.

use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

/// The three-tier mapping model (walrus-extractor.md §2.2). Serializes as the **integer** `1 | 2 | 3`
/// to match the `"tier": 2` form in the §2.6 descriptor JSON (not the string `"2"`).
///
/// The `1|2|3` validation lives in [`TryFrom<u8>`], so it is reachable from ordinary code
/// (`Tier::try_from(n)`) and not only from a deserializer.
/// Wire form locked by `crates/common/tests/enum_wire_form.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum Tier {
    /// Native 1:1 (Parquet-native logical type survives unchanged).
    One,
    /// Structural decomposition (one source column → several emitted columns / a nested type).
    Two,
    /// Canonical-text carrier (carried as VARCHAR, cast + metadata re-applied on load).
    Three,
}

impl TryFrom<u8> for Tier {
    type Error = crate::Error;

    /// Validate a descriptor's `"tier"` number — the same check the `try_from = "u8"` deserializer
    /// runs on §2.6 JSON.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Internal`] if `n` is not `1`, `2`, or `3`; a tier outside that set
    /// means the registry row does not describe a mapping this build knows how to replay.
    fn try_from(n: u8) -> std::result::Result<Self, Self::Error> {
        match n {
            1 => Ok(Tier::One),
            2 => Ok(Tier::Two),
            3 => Ok(Tier::Three),
            other => Err(crate::Error::Internal(format!(
                "invalid tier {other}, expected 1, 2, or 3"
            ))),
        }
    }
}

impl From<Tier> for u8 {
    fn from(t: Tier) -> u8 {
        match t {
            Tier::One => 1,
            Tier::Two => 2,
            Tier::Three => 3,
        }
    }
}

/// Metadata that Parquet/DuckDB lose on read; the transformer re-applies it (§2.6). Each field is
/// `None` unless the column's type needs it.
///
/// Every key is optional on the wire, so the whole container defaults. `schema_registry` is history
/// and is **never pruned** (see `control::schema_registry`), so a descriptor written by an older
/// extractor must still load into a newer build: a metadata key that row predates fills from
/// [`Default`] instead of failing the read. `#[serde(default)]` affects deserialization only — the
/// §2.6 document still carries every key on the way out.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TypeMeta {
    /// Ordered label set for an `enum`.
    pub enum_labels: Option<Vec<String>>,
    /// `n` for `bit(n)` / `varbit(n)`; zero is invalid and becomes the `None` niche.
    pub bit_length: Option<NonZeroU32>,
    /// `n` (+ bpchar padding) for `char(n)` / `varchar(n)`; zero is likewise invalid.
    pub char_length: Option<NonZeroU32>,
    /// `lc_monetary` fractional digits for `money`. Zero is legal (for example, JPY), so this
    /// intentionally remains `Option<u32>`.
    pub money_fraction_digits: Option<u32>,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<TypeMeta>() == 40,
    "TypeMeta: one per source column"
);

/// Per-column mapping descriptor written to `schema_registry` (§2.6).
///
/// Only [`meta`](Self::meta) defaults on read; every identity and mapping key stays required, so a
/// truncated registry row is still a loud decode failure rather than a silently wrong plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDescriptor {
    /// Source column name — the join key back to the relation this descriptor came from.
    pub column: String,
    /// The source column's `pg_catalog` type OID (see [`crate::oids`]).
    pub pg_type_oid: u32,
    /// That OID's type name as text, so a registry row stays readable without an OID table.
    pub pg_type: String,
    /// Which mapping strategy the type takes; see [`Tier`].
    pub tier: Tier,
    /// How the value is shaped in Arrow, e.g. `"Struct/Decomposed"`.
    pub arrow: String,
    /// The DuckDB target type, e.g. `"INTERVAL"`.
    pub duckdb: String,
    /// The flat columns this type expands to, e.g. `["duration_months:INT32", …]`.
    pub emit: Vec<String>,
    /// The transformer-side recombine expression; `None` for a tier-1 scalar that needs none.
    pub recombine: Option<String>,
    /// The metadata the transformer re-applies. A default [`TypeMeta`] — every key `None` — means
    /// "nothing to re-apply", which is exactly what a tier-1 scalar carries, so a registry row that
    /// omits the key loads as "no metadata" instead of failing the transformer's decode.
    #[serde(default)]
    pub meta: TypeMeta,
}

/// Size budget for the whole per-column descriptor — the [`TypeMeta`] assertion above pins only its
/// tail. The extractor caches a `Vec<TypeDescriptor>` per relation *per schema version*, and the transformer
/// rebuilds one from `schema_registry` on every plan build, so a new owned field here is paid once
/// per source column of every cached shape.
///
/// A ceiling rather than an equality because the `Option`/`Vec` niches are a layout detail; the
/// slack is under a word, so any added pointer- or word-sized field still breaches it. If this
/// trips, shrink or box the growing field, or raise the budget deliberately in review.
const TYPE_DESCRIPTOR_MAX_BYTES: usize = 192;
const _: () = assert!(std::mem::size_of::<TypeDescriptor>() <= TYPE_DESCRIPTOR_MAX_BYTES);

#[cfg(test)]
#[path = "type_descriptor_test.rs"]
mod tests;
