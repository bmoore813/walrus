//! The neutral Postgres shape types — the decoupling seam between the decoder and everything
//! downstream.
//!
//! These plain value types live in `common` on purpose: the pgoutput decoder (in `extractor`)
//! **produces** them, `pg-to-arrow` **consumes** them, `control` **persists** the descriptor, and
//! `transformer` **reads it back** to rebuild types. That one decision is why `pg-to-arrow` is fully
//! unit-testable without the decoder, and why no crate ever has to depend on a binary.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Error;

/// Postgres `relreplident` — governs which old-image columns Update/Delete carry (proto §6).
///
/// The pgoutput byte parsed below — which the catalog's `relreplident::text` cast shares, through
/// the [`FromStr`](std::str::FromStr) that delegates to it — and this enum's serde string are
/// distinct wire forms. The serde form is a persisted control-plane contract inside
/// `schema_registry.columns`. Rows written before the lowercase form was introduced contain
/// PascalCase names, so the per-variant aliases are permanent compatibility for those historical
/// rows; no data migration is needed.
///
/// Roll out readers before writers: deploy `walrus-transformer` before `walrus-extractor`. The upgraded
/// reader accepts both spellings, while an old reader cannot parse newly written lowercase names.
/// Wire form locked by `crates/common/tests/enum_wire_form.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicaIdentity {
    /// `'d'` — the default: Update/Delete carry key columns only (`'K'`).
    #[serde(alias = "Default")]
    Default,
    /// `'n'` — nothing: Update/Delete carry no old image (unusable as a key source).
    #[serde(alias = "Nothing")]
    Nothing,
    /// `'f'` — full: Update/Delete carry the whole old row (`'O'`).
    #[serde(alias = "Full")]
    Full,
    /// `'i'` — a nominated unique index supplies the identity.
    #[serde(alias = "Index")]
    Index,
}

impl TryFrom<u8> for ReplicaIdentity {
    type Error = crate::Error;

    /// Parse the Relation message's `relreplident` byte; error on any other value.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if `c` is not one of `b'd'`, `b'n'`, `b'f'`, or `b'i'`, the
    /// four bytes PostgreSQL emits for `pg_class.relreplident`. This protocol mismatch is terminal.
    fn try_from(c: u8) -> Result<Self, Self::Error> {
        match c {
            b'd' => Ok(Self::Default),
            b'n' => Ok(Self::Nothing),
            b'f' => Ok(Self::Full),
            b'i' => Ok(Self::Index),
            other => Err(Error::Internal(format!(
                "unknown relreplident byte {:?}",
                char::from(other)
            ))),
        }
    }
}

impl std::str::FromStr for ReplicaIdentity {
    type Err = crate::Error;

    /// Parse the catalog code as `relreplident::text` renders it — one character, from the same
    /// vocabulary the `TryFrom<u8>` above owns and which this delegates to, so a catalog read and
    /// a Relation message can never drift into two tables. Deliberately **not** the serde word
    /// form: `"d"` parses here, `"default"` does not.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] unless `s` is exactly one of `"d"`, `"n"`, `"f"`, or `"i"`. A
    /// longer string is not a catalog code at all, so it is rejected before the byte table is
    /// consulted.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.as_bytes() {
            [code] => Self::try_from(*code),
            _ => Err(Error::Internal(format!("unknown relreplident text {s:?}"))),
        }
    }
}

/// One column of a relation, as seen in a Relation `'R'` message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgColumn {
    /// Column name as the source spells it — unquoted, exactly as it arrives on the wire.
    pub name: String,
    /// The column's `pg_catalog` type OID (see [`crate::oids`]); with `type_modifier`, this is what
    /// selects the Arrow mapping.
    pub type_oid: u32,
    /// `atttypmod`; `-1` = no modifier. For `numeric` it packs `(precision, scale)`.
    pub type_modifier: i32,
    /// The Relation flags bit 1 — this column is part of the replica-identity key.
    pub is_key: bool,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<PgColumn>() == 40,
    "PgColumn is stored once per source column"
);

impl PgColumn {
    /// Decode `numeric(p, s)` from `type_modifier` when this column is a `numeric`; `None` for a
    /// non-numeric column or an unconstrained `numeric` (`type_modifier == -1`).
    ///
    /// The exact Postgres packing is `precision = ((mod - 4) >> 16) & 0xFFFF`,
    /// `scale = (mod - 4) & 0xFFFF`.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::{PgColumn, oids};
    ///
    /// let amount = PgColumn {
    ///     name: "amount".to_string(),
    ///     type_oid: oids::NUMERIC,
    ///     type_modifier: 655_366,
    ///     is_key: false,
    /// };
    /// assert_eq!(amount.numeric_precision_scale(), Some((10, 2)));
    ///
    /// // An unconstrained `numeric` carries no modifier to decode.
    /// let unconstrained = PgColumn { type_modifier: -1, ..amount };
    /// assert_eq!(unconstrained.numeric_precision_scale(), None);
    /// ```
    #[must_use]
    pub fn numeric_precision_scale(&self) -> Option<(u16, u16)> {
        if self.type_oid != crate::oids::NUMERIC || self.type_modifier < 4 {
            return None;
        }
        let packed = u32::try_from(self.type_modifier - 4).ok()?;
        let precision = u16::try_from((packed >> 16) & 0xFFFF).ok()?;
        let scale = u16::try_from(packed & 0xFFFF).ok()?;
        Some((precision, scale))
    }
}

/// The shape of a source table at one `schema_version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgRelation {
    /// The relation's OID — the identity pgoutput uses in every later message, so it is the
    /// relation-cache key rather than the `(schema, name)` pair below.
    pub oid: u32,
    /// Source schema name.
    pub schema: String,
    /// Source table name.
    pub name: String,
    /// What the source publishes in the *old* tuple of an UPDATE/DELETE, which is what decides
    /// whether a change can be keyed at all; see [`ReplicaIdentity`].
    pub replica_identity: ReplicaIdentity,
    /// The columns in wire order. Position is significant: a tuple's values arrive in this order,
    /// so this vector is the only thing that pairs a value with its column.
    pub columns: Vec<PgColumn>,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<PgRelation>() == 80,
    "PgRelation is cached for every source table"
);

impl PgRelation {
    /// The key-column names (`is_key`) **in relation order** — the transformer's MERGE/dedup key list.
    /// Order matters for composite PKs, so this preserves column order rather than sorting.
    ///
    /// `to_`, not `as_`: this allocates a fresh `Vec` on every call. The `&str`s inside borrow
    /// from `self`, but the container does not. Bind it once; do not call it in a loop.
    ///
    /// # Examples
    ///
    /// ```
    /// # use common::{PgColumn, PgRelation, ReplicaIdentity};
    /// # let col = |name: &str, is_key: bool| PgColumn {
    /// #     name: name.to_string(),
    /// #     type_oid: 23,
    /// #     type_modifier: -1,
    /// #     is_key,
    /// # };
    /// # let customers = PgRelation {
    /// #     oid: 42,
    /// #     schema: "public".to_string(),
    /// #     name: "customers".to_string(),
    /// #     replica_identity: ReplicaIdentity::Default,
    /// #     columns: vec![col("region", true), col("id", true), col("email", false)],
    /// # };
    /// // `customers` is declared `region` (key), `id` (key), `email` (non-key): non-key columns
    /// // drop out, and the composite key keeps relation order, not sorted order.
    /// assert_eq!(customers.to_key_columns(), vec!["region", "id"]);
    /// ```
    #[must_use]
    pub fn to_key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }
}

/// One column value inside a TupleData (proto §5).
///
/// **[`Null`](TupleValue::Null) (`'n'`) and [`UnchangedToast`](TupleValue::UnchangedToast) (`'u'`)
/// are DISTINCT** — a whole transformer-correctness story depends on the difference surviving
/// from wire to `<table>_raw`, where the transformer resolves an unchanged-TOAST placeholder by
/// back-scanning. It must never be collapsed to [`Null`](TupleValue::Null).
///
/// Not `Serialize`: this is an in-memory wire value, not a persisted document with a stable JSON
/// contract. Add serde only if the type gains a supported persistence boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    /// `'n'` — a real SQL NULL.
    Null,
    /// `'u'` — an unchanged out-of-line TOAST value, absent from the wire.
    UnchangedToast,
    /// `'t'` — the textual representation of the value.
    Text(String),
    /// `'b'` — the binary representation (zero-copy via `bytes::Bytes`).
    Binary(Bytes),
}

/// Move-cost budget for the per-column decode hot path (`own-move-large`).
///
/// The compile-time budget reflects a measured 40-byte value. If it trips, shrink or box the
/// offending variant, or raise the budget deliberately with a new measurement.
const TUPLE_VALUE_MAX_BYTES: usize = 40;
const _: () = assert!(size_of::<TupleValue>() <= TUPLE_VALUE_MAX_BYTES);

#[cfg(test)]
#[path = "pg_shape_test.rs"]
mod tests;
