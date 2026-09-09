//! Build the **Tier-1** Arrow schema for a source relation.
//!
//! The one rule that governs everything (walrus-extractor.md §2.1): DuckDB infers column types from
//! Parquet's **native logical types**, so every distinction must be a real logical type. That means
//! **MICROS for every temporal** (NANOS+tz silently downgrades in DuckDB; MILLIS truncates) and
//! `Decimal128` for `numeric(p ≤ 38)`. Anything not (yet) Tier-1 returns `None`/`NotTier1` rather
//! than a fallback field — a wrong-but-compiling mapping is the bug the conformance tests catch.

use crate::error::Error;
use crate::oids;
use crate::range::RangeFamily;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::{PgColumn, PgRelation};

/// The trailing provenance column every walrus Parquet file carries (JSON text).
pub const EXTRACTOR_META_COLUMN: &str = "walrus_extractor_meta";

/// Full Arrow schema for `rel`: one field per Tier-1 column, then `walrus_extractor_meta` (Utf8,
/// non-null). Data fields are `nullable`; the meta column is not.
///
/// # Errors
///
/// Returns [`Error::EmptyRelation`] when `rel` has no columns, or [`Error::NotTier1`] when a column
/// has no supported native, expanded, or canonical-text representation.
pub fn build_schema(rel: &PgRelation) -> Result<Schema, Error> {
    if rel.columns.is_empty() {
        return Err(Error::EmptyRelation {
            relation: format!("{}.{}", rel.schema, rel.name),
        });
    }
    // One source column may fan out to several Tier-2 fields, so we `extend` rather than `push`.
    let mut fields: Vec<Field> = Vec::with_capacity(rel.columns.len() + 1);
    for col in &rel.columns {
        fields.extend(emit_fields(col)?);
    }
    // Provenance rides in one JSON-text column, always present → non-nullable.
    fields.push(Field::new(EXTRACTOR_META_COLUMN, DataType::Utf8, false));
    Ok(Schema::new(fields))
}

/// One source column → the Arrow field(s) the extractor emits for it. **Tier-1** maps 1:1 (one field);
/// **Tier-2** (`interval`, `timetz`, …) decomposes into several sibling fields the transformer recombines
/// (§2.4). **Data fields stay `nullable(true)`**: delete old-images and unchanged-TOAST placeholders
/// arrive partial, so even a PK column can be absent on the wire; the mirror's PK-not-null is enforced
/// downstream in the transformer, not here.
///
/// # Errors
///
/// Returns [`Error::NotTier1`] when the column OID and typmod do not map to any supported tier.
pub fn emit_fields(col: &PgColumn) -> Result<Vec<Field>, Error> {
    if let Some(dt) = tier1_data_type(col.type_oid, col.type_modifier) {
        return Ok(vec![Field::new(col.name.clone(), dt, true)]);
    }
    // Tier-3 canonical-text carriers → one Utf8 field (including unconstrained/p>38 numeric, §2.5).
    if crate::tier3::is_tier3_text(col.type_oid, col.type_modifier) {
        return Ok(vec![crate::tier3::tier3_field(&col.name)]);
    }
    match col.type_oid {
        oids::INTERVAL => return Ok(crate::tier2::interval_fields(&col.name)),
        oids::TIMETZ => return Ok(crate::tier2::timetz_fields(&col.name)),
        _ => {}
    }
    // Range → 5 flat sibling columns; multirange → one LIST<STRUCT> (§2.4).
    if let Some(fam) = RangeFamily::from_range_oid(col.type_oid) {
        return Ok(crate::tier2::range_fields(
            &col.name,
            fam,
            col.type_modifier,
        ));
    }
    if let Some(fam) = RangeFamily::from_multirange_oid(col.type_oid) {
        return Ok(vec![crate::tier2::multirange_field(
            &col.name,
            fam,
            col.type_modifier,
        )]);
    }
    // Geometric → one nested STRUCT/LIST-of-doubles field (§2.4).
    if let Some(field) = crate::geometric::geometric_field(&col.name, col.type_oid) {
        return Ok(vec![field]);
    }
    // uuid → native FixedSizeBinary(16)+arrow.uuid; enum (non-builtin OID) → Utf8 (§2.4/§2.5).
    if col.type_oid == oids::UUID {
        return Ok(vec![crate::uuid_enum::uuid_field(&col.name)]);
    }
    if crate::uuid_enum::is_enum_oid(col.type_oid) {
        return Ok(vec![crate::uuid_enum::enum_field(&col.name)]);
    }
    Err(Error::NotTier1 {
        oid: col.type_oid,
        typmod: col.type_modifier,
    })
}

/// Arrow `DataType` for a Tier-1 OID+typmod, or `None` if the type is not (yet) Tier-1.
#[must_use]
pub fn tier1_data_type(type_oid: u32, atttypmod: i32) -> Option<DataType> {
    Some(match type_oid {
        oids::BOOL => DataType::Boolean,
        oids::INT2 => DataType::Int16,
        oids::INT4 => DataType::Int32,
        oids::INT8 => DataType::Int64,
        oids::FLOAT4 => DataType::Float32,
        oids::FLOAT8 => DataType::Float64,
        // Two numeric cases, kept strictly apart (§2.3): p ≤ 38 is a lossless Decimal128;
        // unconstrained (typmod -1) or p > 38 is a Tier-3 VARCHAR carrier — NOT here.
        oids::NUMERIC => match numeric_precision_scale(atttypmod) {
            Some((p @ 1..=38, s)) => DataType::Decimal128(p, s),
            _ => return None,
        },
        oids::TEXT | oids::VARCHAR | oids::BPCHAR | oids::CHAR => DataType::Utf8,
        oids::BYTEA => DataType::Binary,
        // json / jsonb ride as UTF-8 text (DuckDB infers JSON from the string).
        oids::JSON | oids::JSONB => DataType::Utf8,
        oids::DATE => DataType::Date32,
        oids::TIME => DataType::Time64(TimeUnit::Microsecond),
        oids::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        // Some("UTC") is the marker DuckDB reads as isAdjustedToUTC=true.
        oids::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ => return None,
    })
}

/// Decode a `numeric` `atttypmod` into `(precision, scale)`, or `None` for unconstrained (`-1`).
/// `numeric(p,s)` packs `((p << 16) | s) + VARHDRSZ`; subtract `VARHDRSZ` (4) before unpacking.
#[must_use]
pub fn numeric_precision_scale(atttypmod: i32) -> Option<(u8, i8)> {
    if atttypmod < 4 {
        return None; // -1 (unconstrained) or an invalid value
    }
    let packed = u32::try_from(atttypmod - 4).ok()?;
    let precision = u8::try_from((packed >> 16) & 0xFFFF).ok()?;
    let scale = i8::try_from(packed & 0xFFFF).ok()?;
    Some((precision, scale))
}

#[cfg(test)]
#[path = "schema_test.rs"]
mod tests;
