//! Build the per-column [`TypeDescriptor`] (walrus-extractor.md §2.6).
//!
//! Every type-mapping decision becomes one serializable record: tier, emitted columns, recombine
//! expression, and the metadata (`enum_labels`,
//! `bit_length`, `char_length`) the transformer must re-apply. It is what turns "reconcile to the exact
//! source shape" from a guess into a mechanical operation.
//!
//! **Single source of truth.** `emit_of` is derived by *calling* [`schema::emit_fields`] — the same
//! dispatch the schema/batch builders use — so the descriptor's `emit[]` can never drift from the
//! Parquet the extractor actually writes. `tier_of` mirrors that dispatch's precedence branch-for-branch.

use crate::range::RangeFamily;
use crate::{Error, geometric, oids, schema, tier3, uuid_enum};
use arrow::datatypes::DataType;
use common::{PgColumn, PgRelation, Tier, TypeDescriptor, TypeMeta};
use std::borrow::Cow;
use std::num::NonZeroU32;

/// Derive the per-column mapping descriptor (§2.6). Enum `enum_labels` are caller-supplied; use
/// [`describe_column_with_labels`] when catalog metadata is available.
///
/// # Errors
///
/// Returns [`Error::NotTier1`] carrying the column's OID and typmod when it maps to no supported
/// tier. Every descriptor field is derived from [`schema::emit_fields`], so that is the only
/// failure reachable here.
#[must_use = "the descriptor error must be handled"]
pub fn describe_column(col: &PgColumn) -> Result<TypeDescriptor, Error> {
    describe_column_with_labels(col, None)
}

/// [`describe_column`] plus the ordered enum label set (only used when the column is an enum).
///
/// # Errors
///
/// Returns [`Error::NotTier1`] carrying `col`'s OID and typmod when it maps to no supported tier.
/// The `arrow`, `duckdb`, and `emit` fields all route through [`schema::emit_fields`], and the
/// remaining fields are infallible, so no other variant can surface. `enum_labels` is never
/// validated here — a label set for a non-enum column is ignored, not rejected.
#[must_use = "the descriptor error must be handled"]
pub fn describe_column_with_labels(
    col: &PgColumn,
    enum_labels: Option<Vec<String>>,
) -> Result<TypeDescriptor, Error> {
    let tier = tier_of(col);
    Ok(TypeDescriptor {
        column: col.name.clone(),
        pg_type_oid: col.type_oid,
        pg_type: pg_type_name(col.type_oid).to_string(),
        tier,
        arrow: arrow_of(col, tier)?,
        duckdb: duckdb_of(col, tier)?,
        emit: emit_of(col)?,
        recombine: recombine_of(col),
        meta: meta_of(col, enum_labels),
    })
}

/// Descriptors for every column of a relation, in column order.
///
/// # Errors
///
/// Returns the [`Error::NotTier1`] of the **first** column that maps to no supported tier: the
/// walk short-circuits there, so a relation with several unmappable columns reports only the
/// earliest. An empty relation is `Ok(vec![])` — [`schema::build_schema`], not this function, is
/// what rejects it with [`Error::EmptyRelation`].
pub fn describe_relation(rel: &PgRelation) -> Result<Vec<TypeDescriptor>, Error> {
    rel.columns.iter().map(describe_column).collect()
}

/// Tier for a column, mirroring [`schema::emit_fields`]'s dispatch precedence branch-for-branch.
fn tier_of(col: &PgColumn) -> Tier {
    if schema::tier1_data_type(col.type_oid, col.type_modifier).is_some() {
        return Tier::One;
    }
    if tier3::is_tier3_text(col.type_oid, col.type_modifier) {
        return Tier::Three;
    }
    if col.type_oid == oids::INTERVAL || col.type_oid == oids::TIMETZ {
        return Tier::Two;
    }
    if RangeFamily::from_range_oid(col.type_oid).is_some()
        || RangeFamily::from_multirange_oid(col.type_oid).is_some()
        || geometric::geo_kind(col.type_oid).is_some()
    {
        return Tier::Two;
    }
    if col.type_oid == oids::UUID {
        return Tier::One; // native DuckDB UUID is 1:1 (§2.4 uuid)
    }
    // enum (non-builtin OID) — and any unsupported column, which `describe_*` is not called for.
    Tier::Three
}

/// The emitted `name:ARROW_TYPE` list, taken **directly** from [`schema::emit_fields`] so it lists the
/// sibling columns in exactly the order (and with the names) the extractor writes them. The transformer
/// positional-binds against this list.
fn emit_of(col: &PgColumn) -> Result<Vec<String>, Error> {
    Ok(schema::emit_fields(col)?
        .iter()
        .map(|f| format!("{}:{}", f.name(), arrow_emit_name(f.data_type())))
        .collect())
}

/// The transformer-side recombine expression (a hint the transformer phase finalizes). Only the types that
/// collapse to a *single* DuckDB scalar carry one — `interval`/`timetz`. `range` stays flat sibling
/// columns, `multirange`/geometric stay nested, Tier-1/Tier-3 need none.
fn recombine_of(col: &PgColumn) -> Option<String> {
    match col.type_oid {
        oids::INTERVAL => Some("to_months(m)+to_days(d)+to_microseconds(us)".to_string()),
        oids::TIMETZ => Some("make_timetz(us, off)".to_string()),
        _ => None,
    }
}

/// Metadata Parquet/DuckDB lose on read, re-applied by the transformer (§2.6). Each field populated only
/// when it applies; `money_fraction_digits` is deferred (always `None` here).
fn meta_of(col: &PgColumn, enum_labels: Option<Vec<String>>) -> TypeMeta {
    let mut meta = TypeMeta::default();
    if uuid_enum::is_enum_oid(col.type_oid) {
        meta.enum_labels = enum_labels;
    }
    // bit(n)/varbit(n): atttypmod IS the bit count (no VARHDRSZ header); zero is invalid.
    if (col.type_oid == oids::BIT || col.type_oid == oids::VARBIT) && col.type_modifier > 0 {
        meta.bit_length = u32::try_from(col.type_modifier)
            .ok()
            .and_then(NonZeroU32::new);
    }
    // char(n)/varchar(n): atttypmod is n + VARHDRSZ (4); zero is invalid.
    if (col.type_oid == oids::BPCHAR || col.type_oid == oids::VARCHAR) && col.type_modifier > 4 {
        meta.char_length = u32::try_from(col.type_modifier - 4)
            .ok()
            .and_then(NonZeroU32::new);
    }
    meta
}

/// The `arrow` descriptor string: Tier-2 is a decomposition; Tier-1/Tier-3 report the single Arrow type.
#[deny(clippy::wildcard_enum_match_arm)]
fn arrow_of(col: &PgColumn, tier: Tier) -> Result<String, Error> {
    Ok(match tier {
        Tier::Two => "Struct/Decomposed".to_string(),
        Tier::One | Tier::Three => first_field_type(col)?
            .map(|dt| format!("{dt:?}"))
            .unwrap_or_else(|| "Utf8".to_string()),
    })
}

/// The `duckdb` target type string.
#[deny(clippy::wildcard_enum_match_arm)]
fn duckdb_of(col: &PgColumn, tier: Tier) -> Result<String, Error> {
    Ok(match tier {
        Tier::Two => match col.type_oid {
            oids::INTERVAL => "INTERVAL".to_string(),
            oids::TIMETZ => "TIMETZ".to_string(),
            _ if RangeFamily::from_range_oid(col.type_oid).is_some() => {
                "(flat sibling columns)".to_string()
            }
            _ if RangeFamily::from_multirange_oid(col.type_oid).is_some() => {
                "LIST(STRUCT)".to_string()
            }
            _ => "STRUCT".to_string(), // geometric
        },
        Tier::One | Tier::Three => first_field_type(col)?
            .map(|dt| duckdb_scalar_name(&dt).to_string())
            .unwrap_or_else(|| "VARCHAR".to_string()),
    })
}

fn first_field_type(col: &PgColumn) -> Result<Option<DataType>, Error> {
    Ok(schema::emit_fields(col)?
        .first()
        .map(|f| f.data_type().clone()))
}

/// Parquet-ish emit-suffix name (matches the §2.6 interval example `INT32`/`INT64`).
///
/// Every fixed name stays [`Cow::Borrowed`] — the sibling [`duckdb_scalar_name`] is a plain
/// `&'static str` for exactly that reason. Only the three parameterised forms (`DECIMAL(p,s)`,
/// `FIXEDBINARY(n)`, and the `{other:?}` fallback) have to build a string.
fn arrow_emit_name(dt: &DataType) -> Cow<'static, str> {
    match dt {
        DataType::Boolean => Cow::Borrowed("BOOLEAN"),
        DataType::Int16 => Cow::Borrowed("INT16"),
        DataType::Int32 => Cow::Borrowed("INT32"),
        DataType::Int64 => Cow::Borrowed("INT64"),
        DataType::Float32 => Cow::Borrowed("FLOAT"),
        DataType::Float64 => Cow::Borrowed("DOUBLE"),
        DataType::Decimal128(p, s) => Cow::Owned(format!("DECIMAL({p},{s})")),
        DataType::Utf8 => Cow::Borrowed("VARCHAR"),
        DataType::Binary => Cow::Borrowed("BLOB"),
        DataType::FixedSizeBinary(n) => Cow::Owned(format!("FIXEDBINARY({n})")),
        DataType::Date32 => Cow::Borrowed("DATE"),
        DataType::Time64(_) => Cow::Borrowed("TIME"),
        DataType::Timestamp(_, Some(_)) => Cow::Borrowed("TIMESTAMPTZ"),
        DataType::Timestamp(_, None) => Cow::Borrowed("TIMESTAMP"),
        DataType::Struct(_) => Cow::Borrowed("STRUCT"),
        DataType::List(_) => Cow::Borrowed("LIST"),
        other => Cow::Owned(format!("{other:?}")),
    }
}

/// DuckDB scalar type name for a Tier-1/Tier-3 single-field column.
const fn duckdb_scalar_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Decimal128(..) => "DECIMAL",
        DataType::Utf8 => "VARCHAR",
        DataType::Binary => "BLOB",
        DataType::FixedSizeBinary(_) => "UUID",
        DataType::Date32 => "DATE",
        DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE",
        DataType::Timestamp(_, None) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

/// The Postgres type name for an OID (informational descriptor field). Non-builtin → `enum`.
const fn pg_type_name(oid: u32) -> &'static str {
    match oid {
        oids::BOOL => "bool",
        oids::BYTEA => "bytea",
        oids::CHAR => "char",
        oids::INT8 => "int8",
        oids::INT2 => "int2",
        oids::INT4 => "int4",
        oids::TEXT => "text",
        oids::JSON => "json",
        oids::FLOAT4 => "float4",
        oids::FLOAT8 => "float8",
        oids::BPCHAR => "bpchar",
        oids::VARCHAR => "varchar",
        oids::DATE => "date",
        oids::TIME => "time",
        oids::TIMESTAMP => "timestamp",
        oids::TIMESTAMPTZ => "timestamptz",
        oids::NUMERIC => "numeric",
        oids::JSONB => "jsonb",
        oids::INTERVAL => "interval",
        oids::TIMETZ => "timetz",
        oids::INT4RANGE => "int4range",
        oids::NUMRANGE => "numrange",
        oids::TSRANGE => "tsrange",
        oids::TSTZRANGE => "tstzrange",
        oids::DATERANGE => "daterange",
        oids::INT8RANGE => "int8range",
        oids::INT4MULTIRANGE => "int4multirange",
        oids::NUMMULTIRANGE => "nummultirange",
        oids::TSMULTIRANGE => "tsmultirange",
        oids::TSTZMULTIRANGE => "tstzmultirange",
        oids::DATEMULTIRANGE => "datemultirange",
        oids::INT8MULTIRANGE => "int8multirange",
        oids::POINT => "point",
        oids::LSEG => "lseg",
        oids::PATH => "path",
        oids::BOX => "box",
        oids::POLYGON => "polygon",
        oids::LINE => "line",
        oids::CIRCLE => "circle",
        oids::XML => "xml",
        oids::XID => "xid",
        oids::CIDR => "cidr",
        oids::MACADDR8 => "macaddr8",
        oids::MACADDR => "macaddr",
        oids::INET => "inet",
        oids::BIT => "bit",
        oids::VARBIT => "varbit",
        oids::TXID_SNAPSHOT => "txid_snapshot",
        oids::PG_LSN => "pg_lsn",
        oids::TSVECTOR => "tsvector",
        oids::TSQUERY => "tsquery",
        oids::XID8 => "xid8",
        oids::UUID => "uuid",
        o if o >= oids::FIRST_NORMAL_OID => "enum",
        _ => "unknown",
    }
}

#[cfg(test)]
#[path = "descriptor_test.rs"]
mod tests;
