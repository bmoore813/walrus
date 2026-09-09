//! [`BatchBuilder`] — decoded [`TupleValue`]s + a [`ExtractorMeta`] → an Arrow [`RecordBatch`].
//!
//! pgoutput ships values as canonical **text**; this builder parses each into its Tier-1 Arrow
//! representation, maps [`TupleValue::Null`] and [`TupleValue::UnchangedToast`] onto the validity
//! bitmap (a null in both cases — the TOAST placeholder's column name is recorded in
//! [`ExtractorMeta::unchanged_toast`] upstream and echoed into the meta JSON; *resolving* it is the
//! transformer's back-scan), and serializes the provenance into the trailing
//! `walrus_extractor_meta` column. All column builders (including meta)
//! move in lockstep — every `append_row` pushes exactly one slot to every column.

#![deny(clippy::indexing_slicing)] // the 460 ns/row append path carries width proofs.

use crate::error::Error;
use crate::geometric::GeoKind;
use crate::oids;
use crate::range::RangeFamily;
use crate::schema::{EXTRACTOR_META_COLUMN, build_schema, tier1_data_type};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, ListBuilder, PrimitiveBuilder, RecordBatch, StringBuilder, StructBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, make_builder,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, FieldRef, SchemaRef, TimeUnit};
use common::{ExtractorMeta, PgColumn, PgRelation, TupleValue};
use std::fmt;
use std::sync::Arc;

/// How one source column's `TupleValue` fans out onto the flat builder list. Tier-1 consumes one
/// builder (the existing `append_value` path); Tier-2 spreads a single value across several sibling
/// builders. Ordering here MUST match `emit_fields` / `build_schema`; the descriptor's `emit[]`
/// list records the same suffixes in the same order (§2.4).
/// The batch plan stores one compact `Emit` per source column, independent of its row count.
#[derive(Debug)]
enum Emit {
    Scalar,             // 1 builder
    Interval,           // 3 builders: _months(i32), _days(i32), _micros(i64)
    Timetz,             // 2 builders: _micros(i64), _offset_seconds(i32)
    Range,              // 5 builders: _lower, _upper, _lower_inc, _upper_inc, _empty
    Multirange,         // 1 builder: ListBuilder<StructBuilder>
    Geometric(GeoKind), // 1 builder: a nested STRUCT / LIST<STRUCT> of doubles
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<Emit>() == 1,
    "Emit is stored once per source column"
);

impl Emit {
    /// Number of flat builders consumed by this source-column shape.
    const fn width(&self) -> usize {
        match self {
            Self::Scalar | Self::Multirange | Self::Geometric(_) => 1,
            Self::Interval => 3,
            Self::Timetz => 2,
            Self::Range => 5,
        }
    }
}

/// Arrow's erased builders do not implement [`Debug`](fmt::Debug); report only their arity so the
/// public [`BatchBuilder`] can derive `Debug` without inspecting foreign trait-object contents.
///
/// `Box<[_]>`, not `Vec`: the flat builder list is fixed by the schema in [`BatchBuilder::new`], and
/// `append_row`'s width proofs hold only because nothing can add or remove a builder afterwards.
struct Builders(Box<[Box<dyn ArrayBuilder>]>);

impl fmt::Debug for Builders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} column builders]", self.0.len())
    }
}

/// Classify a source column into its fan-out shape. MUST stay in lockstep with `emit_fields` — the
/// widths (1/3/2/5/1) are how `append_row` advances the flat builder cursor.
fn emit_kind(col: &PgColumn) -> Result<Emit, Error> {
    if tier1_data_type(col.type_oid, col.type_modifier).is_some() {
        return Ok(Emit::Scalar);
    }
    // Tier-3 carriers are a single Utf8 column → the same Scalar/append_value path.
    if crate::tier3::is_tier3_text(col.type_oid, col.type_modifier) {
        return Ok(Emit::Scalar);
    }
    // uuid (FixedSizeBinary) and enum (Utf8) are both single columns → Scalar/append_value.
    if col.type_oid == oids::UUID || crate::uuid_enum::is_enum_oid(col.type_oid) {
        return Ok(Emit::Scalar);
    }
    match col.type_oid {
        oids::INTERVAL => return Ok(Emit::Interval),
        oids::TIMETZ => return Ok(Emit::Timetz),
        _ => {}
    }
    if RangeFamily::from_range_oid(col.type_oid).is_some() {
        return Ok(Emit::Range);
    }
    if RangeFamily::from_multirange_oid(col.type_oid).is_some() {
        return Ok(Emit::Multirange);
    }
    if let Some(kind) = crate::geometric::geo_kind(col.type_oid) {
        return Ok(Emit::Geometric(kind));
    }
    Err(Error::NotTier1 {
        oid: col.type_oid,
        typmod: col.type_modifier,
    })
}

/// Accumulates decoded rows for ONE relation into a single Arrow [`RecordBatch`].
#[derive(Debug)]
pub struct BatchBuilder {
    schema: SchemaRef,
    builders: Builders,  // one per EMITTED field, flat, in schema order
    plan: Box<[Emit]>,   // one per SOURCE column: how its value fans out
    meta: StringBuilder, // the trailing walrus_extractor_meta column
    rows: usize,
    /// The batch-constant meta JSON fragment, serialized once from the first row.
    meta_const: Option<String>,
    /// Reused scratch for assembling each row's `{const,row}` meta JSON (avoids a per-row alloc).
    meta_buf: String,
    /// Reused scratch for RFC-3339 candidates handed to `jiff`, cleared and refilled per cell.
    ts_buf: String,
}

impl BatchBuilder {
    /// Build empty typed builders from the relation's Arrow schema, including Tier-2 fan-out.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EmptyRelation`] for a relation without columns, [`Error::NotTier1`] for an
    /// unsupported source type, or [`Error::Arrow`] if Arrow rejects a typed builder configuration.
    pub fn new(rel: &PgRelation) -> Result<Self, Error> {
        let schema = Arc::new(build_schema(rel)?);
        // One flat builder per data field (every field except the trailing meta column).
        let data_field_count = schema.fields().len() - 1;
        let mut builders = Vec::with_capacity(data_field_count);
        for field in schema.fields().iter().take(data_field_count) {
            builders.push(column_builder(field)?);
        }
        // One routing entry per source column; its widths sum to `data_field_count`.
        let mut plan = Vec::with_capacity(rel.columns.len());
        for col in &rel.columns {
            plan.push(emit_kind(col)?);
        }
        Ok(BatchBuilder {
            schema,
            builders: Builders(builders.into_boxed_slice()),
            plan: plan.into_boxed_slice(),
            meta: StringBuilder::new(),
            rows: 0,
            meta_const: None,
            meta_buf: String::new(),
            ts_buf: String::new(),
        })
    }

    /// Append one decoded tuple + its provenance. `values.len()` must equal the source column count
    /// (one [`TupleValue`] per source column — Tier-2 values fan out to several builders internally).
    ///
    /// # Errors
    ///
    /// Returns [`Error::RowLenMismatch`] when the tuple width differs from the relation, and
    /// [`Error::ValueParse`], [`Error::Downcast`], or [`Error::Arrow`] when a value cannot be
    /// converted into its planned Arrow builder or its provenance cannot be serialized.
    pub fn append_row(&mut self, values: &[TupleValue], meta: &ExtractorMeta) -> Result<(), Error> {
        if values.len() != self.plan.len() {
            return Err(row_len_error(self.plan.len(), values.len()));
        }
        // Read the field types straight out of the schema — `schema`, `builders`, and `ts_buf` are
        // disjoint fields, so the shared borrow coexists with the mutable ones (as `plan` already
        // does below). Cloning `Fields` here would be an Arc refcount round-trip on every row.
        let mut builders: &mut [Box<dyn ArrayBuilder>] = &mut self.builders.0;
        let mut remaining_fields: &[FieldRef] = self.schema.fields();
        for (emit, value) in self.plan.iter().zip(values) {
            let width = emit.width();
            let column = remaining_fields
                .first()
                .map_or("<unknown>", |field| field.name());
            let (current_builders, rest) = std::mem::take(&mut builders)
                .split_at_mut_checked(width)
                .ok_or_else(|| downcast_error(column))?;
            let (current_fields, fields_rest) = remaining_fields
                .split_at_checked(width)
                .ok_or_else(|| downcast_error(column))?;
            match emit {
                Emit::Scalar => {
                    let ([builder], [field]) = (current_builders, current_fields) else {
                        return Err(downcast_error(column));
                    };
                    append_value(builder.as_mut(), field, value, &mut self.ts_buf)?;
                }
                Emit::Interval => {
                    append_interval(current_builders, column, value)?;
                }
                Emit::Timetz => {
                    append_timetz(current_builders, column, value)?;
                }
                Emit::Range => {
                    append_range(current_builders, current_fields, value, &mut self.ts_buf)?;
                }
                Emit::Multirange => {
                    let ([builder], [field]) = (current_builders, current_fields) else {
                        return Err(downcast_error(column));
                    };
                    append_multirange(builder.as_mut(), field, value, &mut self.ts_buf)?;
                }
                Emit::Geometric(kind) => {
                    let ([builder], [field]) = (current_builders, current_fields) else {
                        return Err(downcast_error(column));
                    };
                    append_geometric(builder.as_mut(), field, value, *kind)?;
                }
            }
            builders = rest;
            remaining_fields = fields_rest;
        }
        self.append_meta(meta)?;
        self.rows += 1;
        Ok(())
    }

    /// Append the row's `walrus_extractor_meta` JSON, amortizing the batch-constant fields:
    /// serialize them once (from the first row), then per row splice `{const,row}` into a reused
    /// buffer. Byte-equivalent to `serde_json::to_string(meta)` (key order aside) — see
    /// `common::extractor_meta`'s `amortized_meta_matches_full` test.
    fn append_meta(&mut self, meta: &ExtractorMeta) -> Result<(), Error> {
        // The meta column is walrus's own JSON, never a source cell, so the serde reason is safe to
        // print — but `value_parse` withholds its value slot from every formatter for every caller
        // (that is the point), so the reason rides `data_type`, the slot `Display` still renders.
        let meta_err = |e: serde_json::Error| {
            let target = format!("json ({e})");
            Error::value_parse(EXTRACTOR_META_COLUMN, "", target)
        };
        if self.meta_const.is_none() {
            self.meta_const = Some(meta.to_const_json_inner().map_err(meta_err)?);
        }
        self.meta_buf.clear();
        self.meta_buf.push('{');
        // `meta_const` is `Some` here — the block above sets it on the first row and it is never
        // cleared — so this always appends; the `if let` just avoids an infallible unwrap.
        if let Some(mc) = self.meta_const.as_deref() {
            self.meta_buf.push_str(mc);
        }
        self.meta_buf.push(',');
        meta.write_row_json_inner(&mut self.meta_buf)
            .map_err(meta_err)?;
        self.meta_buf.push('}');
        self.meta.append_value(&self.meta_buf);
        Ok(())
    }

    /// Rows appended so far. This is the batch's *fill*, which the extractor's size triggers read; it is
    /// not the length of any one column builder.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows
    }

    /// Whether no row has been appended yet — so sealing would produce an empty file.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Consume the builder, finishing all column builders into arrays and assembling the
    /// schema-checked [`RecordBatch`].
    ///
    /// `into_`, not `finish`: arrow-rs's [`ArrayBuilder::finish`] takes `&mut self` and leaves the
    /// builder reusable, but this takes `self` by value and spends it — one [`BatchBuilder`] per
    /// sealed micro-batch. The names differ because the ownership does. Callers that hold the
    /// builder behind a `&mut` (see `extractor::batch::TableBatcher::seal`) must `mem::replace` it out
    /// first; the name is what tells them so.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Arrow`] if the finished arrays do not match the planned schema or row count.
    pub fn into_record_batch(mut self) -> Result<RecordBatch, Error> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.builders.0.len() + 1);
        for builder in self.builders.0.iter_mut() {
            arrays.push(builder.finish());
        }
        arrays.push(Arc::new(self.meta.finish()));
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?)
    }
}

/// A typed builder matching `field`'s Arrow type. `make_builder` covers most types; Decimal128 and
/// Timestamp are built explicitly so `finish()` preserves the precision/scale and timezone that the
/// schema and DuckDB read-back require.
fn column_builder(field: &Field) -> Result<Box<dyn ArrayBuilder>, Error> {
    Ok(match field.data_type() {
        DataType::Decimal128(p, s) => {
            Box::new(Decimal128Builder::new().with_precision_and_scale(*p, *s)?)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Box::new(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
        }
        // uuid: FixedSizeBinary(16). The `arrow.uuid` extension lives on the schema Field (not the
        // builder); it rides through `write_parquet`'s `batch.schema()` into the Parquet UUID type.
        DataType::FixedSizeBinary(width) => {
            Box::new(FixedSizeBinaryBuilder::with_capacity(0, *width))
        }
        // Multirange: LIST<STRUCT>. Build the struct's child builders via `column_builder` too, so a
        // Decimal/Timestamp member bound keeps its precision/scale/tz (`make_builder` would drop them
        // and the finished array would mismatch the schema). ListBuilder's default item field is
        // `("item", …, nullable=true)`, matching `multirange_field`'s `Field::new_list_field`.
        DataType::List(item) => match item.data_type() {
            DataType::Struct(struct_fields) => {
                let sb = StructBuilder::new(
                    struct_fields.clone(),
                    struct_child_builders(struct_fields)?,
                );
                Box::new(ListBuilder::new(sb))
            }
            _ => make_builder(field.data_type(), 0),
        },
        // Geometric STRUCTs (point/box/circle/line/path). Recurse via `column_builder` so a
        // nested Struct/List/Decimal/Timestamp child keeps its exact type (not `make_builder`'s default).
        DataType::Struct(struct_fields) => Box::new(StructBuilder::new(
            struct_fields.clone(),
            struct_child_builders(struct_fields)?,
        )),
        other => make_builder(other, 0),
    })
}

/// One `column_builder` per struct child, preserving each child's exact (possibly nested) type.
fn struct_child_builders(
    fields: &arrow::datatypes::Fields,
) -> Result<Vec<Box<dyn ArrayBuilder>>, Error> {
    fields.iter().map(|f| column_builder(f)).collect()
}

/// Append one `TupleValue` to one typed builder. `Null`/`UnchangedToast` → `append_null`.
fn append_value(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: &TupleValue,
    scratch: &mut String,
) -> Result<(), Error> {
    let col: &str = field.name();
    let dt = field.data_type();
    // NULL and unchanged-TOAST both append a null on the validity bitmap. (The TOAST placeholder's
    // column name lives in ExtractorMeta.unchanged_toast, echoed into the meta JSON, not resolved here.)
    let is_null = matches!(value, TupleValue::Null | TupleValue::UnchangedToast);

    match dt {
        DataType::Boolean => {
            let b = downcast::<BooleanBuilder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                b.append_value(parse_bool(text(value, col, dt)?, col)?);
            }
        }
        DataType::Int16 => append_num::<Int16Builder>(builder, value, col, dt, is_null)?,
        DataType::Int32 => append_num::<Int32Builder>(builder, value, col, dt, is_null)?,
        DataType::Int64 => append_num::<Int64Builder>(builder, value, col, dt, is_null)?,
        DataType::Float32 => append_num::<Float32Builder>(builder, value, col, dt, is_null)?,
        DataType::Float64 => append_num::<Float64Builder>(builder, value, col, dt, is_null)?,
        DataType::Decimal128(_, scale) => {
            let b = downcast::<Decimal128Builder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                b.append_value(parse_decimal(text(value, col, dt)?, *scale, col)?);
            }
        }
        DataType::Utf8 => {
            let b = downcast::<StringBuilder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                b.append_value(text(value, col, dt)?);
            }
        }
        DataType::Binary => {
            let b = downcast::<BinaryBuilder>(builder, col)?;
            match value {
                TupleValue::Null | TupleValue::UnchangedToast => b.append_null(),
                TupleValue::Binary(bytes) => b.append_value(bytes),
                // bytea text is `\x…` hex under text mode.
                TupleValue::Text(s) => b.append_value(&parse_bytea(s, col)?),
            }
        }
        DataType::Date32 => {
            let b = downcast::<Date32Builder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                b.append_value(parse_date_days(text(value, col, dt)?, col, scratch)?);
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let b = downcast::<Time64MicrosecondBuilder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                b.append_value(parse_time_micros(text(value, col, dt)?, col, scratch)?);
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let b = downcast::<TimestampMicrosecondBuilder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                let s = text(value, col, dt)?;
                let micros = if tz.is_some() {
                    parse_timestamptz_micros(s, col, scratch)?
                } else {
                    parse_timestamp_micros(s, col, scratch)?
                };
                b.append_value(micros);
            }
        }
        // uuid: parse canonical text → 16 bytes, append as fixed-width binary.
        DataType::FixedSizeBinary(_) => {
            let b = downcast::<FixedSizeBinaryBuilder>(builder, col)?;
            if is_null {
                b.append_null();
            } else {
                let bytes = crate::uuid_enum::parse_uuid_bytes(text(value, col, dt)?)?;
                b.append_value(bytes)?;
            }
        }
        // `append_value` runs for `Emit::Scalar` (Tier-1 + Tier-3 Utf8 + uuid) columns; the Tier-2
        // fan-out shapes have their own `append_*`. So no other Arrow type reaches this arm.
        _ => {
            return Err(downcast_error(col));
        }
    }
    Ok(())
}

/// Fan a single `interval` value across its three sibling builders (`_months` i32, `_days` i32,
/// `_micros` i64). NULL / unchanged-TOAST sets all three null in lockstep — the one shared logical
/// NULL that keeps a real zero interval `(0,0,0)` distinguishable from absence (§2.4).
#[deny(clippy::wildcard_enum_match_arm)]
fn append_interval(
    builders: &mut [Box<dyn ArrayBuilder>],
    col: &str,
    value: &TupleValue,
) -> Result<(), Error> {
    let [months_builder, days_builder, micros_builder] = builders else {
        return Err(downcast_error(col));
    };
    let parts =
        match value {
            TupleValue::Null | TupleValue::UnchangedToast => None,
            TupleValue::Text(_) | TupleValue::Binary(_) => Some(crate::tier2::parse_interval(
                text(value, col, &DataType::Int64)?,
            )?),
        };
    let months = downcast::<Int32Builder>(months_builder.as_mut(), col)?;
    match parts {
        Some((m, _, _)) => months.append_value(m),
        None => months.append_null(),
    }
    let days = downcast::<Int32Builder>(days_builder.as_mut(), col)?;
    match parts {
        Some((_, d, _)) => days.append_value(d),
        None => days.append_null(),
    }
    let micros = downcast::<Int64Builder>(micros_builder.as_mut(), col)?;
    match parts {
        Some((_, _, us)) => micros.append_value(us),
        None => micros.append_null(),
    }
    Ok(())
}

/// Fan a single `timetz` value across `_micros` (i64) and `_offset_seconds` (i32); NULL sets both.
#[deny(clippy::wildcard_enum_match_arm)]
fn append_timetz(
    builders: &mut [Box<dyn ArrayBuilder>],
    col: &str,
    value: &TupleValue,
) -> Result<(), Error> {
    let [micros_builder, offset_builder] = builders else {
        return Err(downcast_error(col));
    };
    let parts = match value {
        TupleValue::Null | TupleValue::UnchangedToast => None,
        TupleValue::Text(_) | TupleValue::Binary(_) => Some(crate::tier2::parse_timetz(text(
            value,
            col,
            &DataType::Int64,
        )?)?),
    };
    let micros = downcast::<Int64Builder>(micros_builder.as_mut(), col)?;
    match parts {
        Some((us, _)) => micros.append_value(us),
        None => micros.append_null(),
    }
    let offset = downcast::<Int32Builder>(offset_builder.as_mut(), col)?;
    match parts {
        Some((_, off)) => offset.append_value(off),
        None => offset.append_null(),
    }
    Ok(())
}

/// Fan a single `range` value across its five sibling builders (`_lower`, `_upper`, `_lower_inc`,
/// `_upper_inc`, `_empty`). The three states stay distinct: a whole SQL NULL nulls all five; `empty`
/// sets `_empty=true` with NULL bounds; an unbounded side is a NULL bound with `_empty=false`.
fn append_range(
    builders: &mut [Box<dyn ArrayBuilder>],
    fields: &[FieldRef],
    value: &TupleValue,
    scratch: &mut String,
) -> Result<(), Error> {
    let col = fields.first().map_or("<unknown>", |field| field.name());
    let [
        lower_builder,
        upper_builder,
        lower_inc_builder,
        upper_inc_builder,
        empty_builder,
    ] = builders
    else {
        return Err(downcast_error(col));
    };
    let [lower_field, upper_field, _, _, _] = fields else {
        return Err(downcast_error(col));
    };
    if matches!(value, TupleValue::Null | TupleValue::UnchangedToast) {
        // Whole-column NULL → every sibling null (bounds via append_value, flags via BooleanBuilder).
        append_value(
            lower_builder.as_mut(),
            lower_field,
            &TupleValue::Null,
            scratch,
        )?;
        append_value(
            upper_builder.as_mut(),
            upper_field,
            &TupleValue::Null,
            scratch,
        )?;
        for builder in [lower_inc_builder, upper_inc_builder, empty_builder] {
            downcast::<BooleanBuilder>(builder.as_mut(), col)?.append_null();
        }
        return Ok(());
    }
    let r = crate::range::parse_range(text(value, col, lower_field.data_type())?)?;
    // Bounds reuse the Tier-1 text parsing; a `None` (unbounded) bound appends null.
    append_value(
        lower_builder.as_mut(),
        lower_field,
        &opt_text_value(r.lower.as_deref()),
        scratch,
    )?;
    append_value(
        upper_builder.as_mut(),
        upper_field,
        &opt_text_value(r.upper.as_deref()),
        scratch,
    )?;
    downcast::<BooleanBuilder>(lower_inc_builder.as_mut(), col)?.append_value(r.lower_inc);
    downcast::<BooleanBuilder>(upper_inc_builder.as_mut(), col)?.append_value(r.upper_inc);
    downcast::<BooleanBuilder>(empty_builder.as_mut(), col)?.append_value(r.empty);
    Ok(())
}

/// Fan a single `multirange` value onto one `ListBuilder<StructBuilder>`: one struct per member, then
/// `append(true)`. Empty multirange = zero members + `append(true)` (empty list); NULL = `append_null`
/// (NULL list) — the two are kept distinct.
fn append_multirange(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: &TupleValue,
    scratch: &mut String,
) -> Result<(), Error> {
    let col = field.name();
    let lb = downcast::<ListBuilder<StructBuilder>>(builder, col)?;
    if matches!(value, TupleValue::Null | TupleValue::UnchangedToast) {
        lb.append_null();
        return Ok(());
    }
    let members = crate::range::parse_multirange(text(value, col, field.data_type())?)?;
    let elem = multirange_elem_type(field)?;
    {
        let sb = lb.values();
        for m in &members {
            append_struct_bound(sb, 0, &elem, m.lower.as_deref(), col, scratch)?;
            append_struct_bound(sb, 1, &elem, m.upper.as_deref(), col, scratch)?;
            struct_field::<BooleanBuilder>(sb, 2, col)?.append_value(m.lower_inc);
            struct_field::<BooleanBuilder>(sb, 3, col)?.append_value(m.upper_inc);
            sb.append(true);
        }
    }
    lb.append(true);
    Ok(())
}

/// The element (`_lower`/`_upper`) Arrow type carried inside a multirange's `LIST<STRUCT>` field.
fn multirange_elem_type(field: &Field) -> Result<DataType, Error> {
    if let DataType::List(item) = field.data_type()
        && let DataType::Struct(fs) = item.data_type()
        && let Some(bound) = fs.first()
    {
        return Ok(bound.data_type().clone());
    }
    Err(downcast_error(field.name()))
}

/// Append one multirange member bound (parsed text, or `None` = unbounded → null) to struct child `idx`.
fn append_struct_bound(
    sb: &mut StructBuilder,
    idx: usize,
    dt: &DataType,
    bound: Option<&str>,
    col: &str,
    scratch: &mut String,
) -> Result<(), Error> {
    match dt {
        DataType::Int32 => {
            let b = struct_field::<Int32Builder>(sb, idx, col)?;
            match bound {
                Some(s) => b.append_value(
                    s.parse::<i32>()
                        .map_err(|_| Error::value_parse(col, s, "Int32"))?,
                ),
                None => b.append_null(),
            }
        }
        DataType::Int64 => {
            let b = struct_field::<Int64Builder>(sb, idx, col)?;
            match bound {
                Some(s) => b.append_value(
                    s.parse::<i64>()
                        .map_err(|_| Error::value_parse(col, s, "Int64"))?,
                ),
                None => b.append_null(),
            }
        }
        DataType::Utf8 => {
            let b = struct_field::<StringBuilder>(sb, idx, col)?;
            match bound {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        DataType::Date32 => {
            let b = struct_field::<Date32Builder>(sb, idx, col)?;
            match bound {
                Some(s) => b.append_value(parse_date_days(s, col, scratch)?),
                None => b.append_null(),
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let micros = match bound {
                Some(s) if tz.is_some() => Some(parse_timestamptz_micros(s, col, scratch)?),
                Some(s) => Some(parse_timestamp_micros(s, col, scratch)?),
                None => None,
            };
            let b = struct_field::<TimestampMicrosecondBuilder>(sb, idx, col)?;
            match micros {
                Some(us) => b.append_value(us),
                None => b.append_null(),
            }
        }
        DataType::Decimal128(_, scale) => {
            let parsed = match bound {
                Some(s) => Some(parse_decimal(s, *scale, col)?),
                None => None,
            };
            let b = struct_field::<Decimal128Builder>(sb, idx, col)?;
            match parsed {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        _ => {
            return Err(downcast_error(col));
        }
    }
    Ok(())
}

/// Typed accessor for struct child `idx`, attributing a downcast failure to the column.
fn struct_field<'a, T: ArrayBuilder>(
    sb: &'a mut StructBuilder,
    idx: usize,
    col: &str,
) -> Result<&'a mut T, Error> {
    sb.field_builder::<T>(idx)
        .ok_or_else(|| downcast_error(col))
}

/// Typed accessor for a `dyn` column builder, attributing a downcast failure to the column.
///
/// The generic sibling of [`struct_field`]: that one reaches into a `StructBuilder` child, this one
/// re-types a whole column builder.
fn downcast<'a, T: ArrayBuilder>(
    builder: &'a mut dyn ArrayBuilder,
    col: &str,
) -> Result<&'a mut T, Error> {
    builder
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| downcast_error(col))
}

/// Build the "builder does not match the plan" error for a column.
///
/// A downcast failure is a `plan`/`builders` disagreement — an invariant break, never a per-row data
/// outcome — so `#[cold]` marks every branch reaching it as unlikely and `#[inline(never)]` keeps the
/// owned-`String` construction out of the 460 ns/row append path. It matters most for [`downcast`]
/// and [`struct_field`], which are generic: without this the allocation is re-emitted into every
/// builder-type instantiation *and* every call site. `batch_test.rs` pins the column name payload.
#[cold]
#[inline(never)]
fn downcast_error(col: &str) -> Error {
    Error::Downcast {
        column: col.to_string(),
    }
}

/// Build the "tuple width does not match the relation" error for [`BatchBuilder::append_row`].
///
/// The sibling of [`downcast_error`], and cold for the same reason: a width mismatch is a caller
/// invariant break — every row of one relation has the same width — never a per-row data outcome. It
/// guards the *entry* of the 460 ns/row append path, so `#[cold]` is what keeps that first branch
/// from being laid out alongside the per-column loop it precedes. The payload is two `usize` moves
/// with no allocation, so `#[inline(never)]` would add nothing on top. `batch_test.rs` pins the
/// expected/got payload.
#[cold]
const fn row_len_error(expected: usize, got: usize) -> Error {
    Error::RowLenMismatch { expected, got }
}

/// A range bound as a `TupleValue`: `Some(text)` → `Text`, `None` (unbounded) → `Null` (→ append_null).
fn opt_text_value(bound: Option<&str>) -> TupleValue {
    match bound {
        Some(s) => TupleValue::Text(s.to_owned()),
        None => TupleValue::Null,
    }
}

/// Append one geometric value onto its single nested builder. Each shape appends to *every* leaf for
/// every row (a NULL appends nulls to all leaves + closes the struct/list null), keeping the nested
/// child arrays length-locked — the invariant `StructBuilder` requires.
#[deny(clippy::wildcard_enum_match_arm)]
fn append_geometric(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: &TupleValue,
    kind: GeoKind,
) -> Result<(), Error> {
    use crate::geometric as geo;
    let col = field.name();
    let s = match value {
        TupleValue::Null | TupleValue::UnchangedToast => None,
        TupleValue::Text(_) | TupleValue::Binary(_) => Some(text(value, col, field.data_type())?),
    };
    match kind {
        GeoKind::Point => {
            let sb = downcast::<StructBuilder>(builder, col)?;
            let pt = s.map(geo::parse_point).transpose()?;
            push_doubles(sb, &[pt.map(|p| p.x), pt.map(|p| p.y)], col)?;
        }
        GeoKind::Line => {
            let sb = downcast::<StructBuilder>(builder, col)?;
            let abc = s.map(geo::parse_line).transpose()?;
            push_doubles(
                sb,
                &[abc.map(|v| v.0), abc.map(|v| v.1), abc.map(|v| v.2)],
                col,
            )?;
        }
        GeoKind::Circle => {
            let sb = downcast::<StructBuilder>(builder, col)?;
            let xyr = s.map(geo::parse_circle).transpose()?;
            push_doubles(
                sb,
                &[xyr.map(|v| v.0.x), xyr.map(|v| v.0.y), xyr.map(|v| v.1)],
                col,
            )?;
        }
        GeoKind::Lseg | GeoKind::Box => {
            let sb = downcast::<StructBuilder>(builder, col)?;
            let pts = s.map(geo::parse_box).transpose()?;
            push_point_child(sb, 0, pts.map(|(a, _)| a), col)?;
            push_point_child(sb, 1, pts.map(|(_, b)| b), col)?;
            sb.append(pts.is_some());
        }
        GeoKind::Path => {
            let sb = downcast::<StructBuilder>(builder, col)?;
            let parsed = s.map(geo::parse_path).transpose()?;
            match &parsed {
                Some((closed, _)) => {
                    struct_field::<BooleanBuilder>(sb, 0, col)?.append_value(*closed);
                }
                None => struct_field::<BooleanBuilder>(sb, 0, col)?.append_null(),
            }
            {
                let lb = struct_field::<ListBuilder<StructBuilder>>(sb, 1, col)?;
                match &parsed {
                    Some((_, pts)) => push_points_list(lb, pts, col)?,
                    None => lb.append_null(),
                }
            }
            sb.append(parsed.is_some());
        }
        GeoKind::Polygon => {
            let lb = downcast::<ListBuilder<StructBuilder>>(builder, col)?;
            match s {
                Some(t) => push_points_list(lb, &geo::parse_polygon(t)?, col)?,
                None => lb.append_null(),
            }
        }
    }
    Ok(())
}

/// Append `vals` to a struct's Float64 children by index, then close the struct (valid ⇔ all Some).
fn push_doubles(sb: &mut StructBuilder, vals: &[Option<f64>], col: &str) -> Result<(), Error> {
    for (i, v) in vals.iter().enumerate() {
        let b = struct_field::<Float64Builder>(sb, i, col)?;
        match v {
            Some(x) => b.append_value(*x),
            None => b.append_null(),
        }
    }
    sb.append(vals.iter().all(Option::is_some));
    Ok(())
}

/// Append a `Pt` (or null) into the `STRUCT(x,y)` child at struct index `idx`.
fn push_point_child(
    sb: &mut StructBuilder,
    idx: usize,
    pt: Option<crate::geometric::Pt>,
    col: &str,
) -> Result<(), Error> {
    let child = struct_field::<StructBuilder>(sb, idx, col)?;
    push_doubles(child, &[pt.map(|p| p.x), pt.map(|p| p.y)], col)
}

/// Append a run of points as `STRUCT(x,y)` members to a list builder, then close the (non-null) list.
fn push_points_list(
    lb: &mut ListBuilder<StructBuilder>,
    pts: &[crate::geometric::Pt],
    col: &str,
) -> Result<(), Error> {
    {
        let psb = lb.values();
        for p in pts {
            push_doubles(psb, &[Some(p.x), Some(p.y)], col)?;
        }
    }
    lb.append(true);
    Ok(())
}

/// Append a parsed number to a `FromStr` builder, attributing a failure to the column.
fn append_num<B>(
    builder: &mut dyn ArrayBuilder,
    value: &TupleValue,
    col: &str,
    dt: &DataType,
    is_null: bool,
) -> Result<(), Error>
where
    B: ArrayBuilder + ArrowNumBuilder,
{
    let b = downcast::<B>(builder, col)?;
    if is_null {
        b.append_null_val();
    } else {
        let s = text(value, col, dt)?;
        let parsed = s
            .parse::<B::Val>()
            .map_err(|_| Error::value_parse(col, s, dt.to_string()))?;
        b.append_val(parsed);
    }
    Ok(())
}

/// Tiny bridge so `append_num` can be generic over the numeric builders. Each Arrow numeric
/// builder accepts exactly one Rust scalar, so the value type is associated with the builder.
trait ArrowNumBuilder {
    /// The one scalar this builder appends.
    type Val: std::str::FromStr;

    fn append_val(&mut self, v: Self::Val);
    fn append_null_val(&mut self);
}

/// One impl for every Arrow primitive builder whose native scalar parses from text.
///
/// `Int16Builder` … `Float64Builder` are not five unrelated types needing five hand-written (or
/// macro-expanded) impls: each is an alias for `PrimitiveBuilder<T>`, whose inherent
/// `append_value`/`append_null` are themselves generic over `T: ArrowPrimitiveType`. The bound is
/// therefore all the bodies below need, and a numeric Arrow type added to [`append_value`]'s
/// dispatch gets its impl for free.
///
/// `T::Native: FromStr` is the gate: it excludes the builders whose native scalar has no text
/// parse (`i256`, the interval structs) and it is what lets `Val` satisfy the trait's own bound.
/// The impl does cover more builders than [`append_num`] is called with — `Date32Builder` is also
/// `PrimitiveBuilder<_>` over an `i32` — but which builder a column reaches is decided by the
/// `DataType` arm in [`append_value`], and the temporal/decimal arms bind their own converters
/// instead of `str::parse`. Being blanket, this also forecloses a per-builder override (E0119),
/// which is the intent: every numeric builder appends through the same two forwarding calls.
impl<T> ArrowNumBuilder for PrimitiveBuilder<T>
where
    T: ArrowPrimitiveType,
    T::Native: std::str::FromStr,
{
    type Val = T::Native;

    fn append_val(&mut self, v: Self::Val) {
        self.append_value(v);
    }
    fn append_null_val(&mut self) {
        self.append_null();
    }
}

/// Extract the text of a value (for the text-format Tier-1 types). The non-text images are listed
/// rather than absorbed by a wildcard, so a new `TupleValue` variant is a compile error here — the
/// same rule its callers above already follow.
///
/// The image *kind* is walrus's own wire vocabulary and is the whole diagnostic here, so it rides
/// `data_type` next to the target type; `{value:?}` no longer names it, because the payload behind
/// `Binary` is a verbatim source cell and the value slot is withheld from every formatter.
#[deny(clippy::wildcard_enum_match_arm)]
fn text<'a>(value: &'a TupleValue, col: &str, dt: &DataType) -> Result<&'a str, Error> {
    let kind = match value {
        TupleValue::Text(s) => return Ok(s),
        TupleValue::Null => "null",
        TupleValue::UnchangedToast => "unchanged-toast",
        TupleValue::Binary(_) => "binary",
    };
    let target = format!("{dt} (from a {kind} image)");
    Err(Error::value_parse(col, "", target))
}

fn parse_bool(s: &str, col: &str) -> Result<bool, Error> {
    match s {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(Error::value_parse(col, s, "Boolean")),
    }
}

/// Parse `"19.99"` at the field's scale into the unscaled `i128`. Rejects a value carrying more
/// fractional digits than the declared scale (rounding is out of scope).
fn parse_decimal(s: &str, scale: i8, col: &str) -> Result<i128, Error> {
    let err = || Error::value_parse(col, s, format!("Decimal128(scale {scale})"));
    if scale < 0 {
        return Err(err());
    }
    let scale = usize::try_from(scale).map_err(|_| err())?;
    let (sign, rest) = match s.strip_prefix('-') {
        Some(r) => (-1i128, r),
        None => (1, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = rest.split_once('.').unwrap_or((rest, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(err());
    }
    if frac_part.len() > scale {
        return Err(err());
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    digits.extend(std::iter::repeat_n('0', scale - frac_part.len()));
    let magnitude: i128 = digits.parse().map_err(|_| err())?;
    Ok(sign * magnitude)
}

fn parse_bytea(s: &str, col: &str) -> Result<Vec<u8>, Error> {
    let hex = s
        .strip_prefix("\\x")
        .ok_or_else(|| Error::value_parse(col, s, "Binary"))?;
    hex::decode(hex).map_err(|_| Error::value_parse(col, s, "Binary"))
}

/// Micros since the Unix epoch for an RFC-3339 string.
fn rfc3339_micros(s: &str) -> Option<i64> {
    s.parse::<jiff::Timestamp>()
        .ok()
        .map(jiff::Timestamp::as_microsecond)
}

/// `"2024-01-02"` → days since 1970-01-01, using a cleared and reused RFC-3339 scratch buffer.
fn parse_date_days(s: &str, col: &str, scratch: &mut String) -> Result<i32, Error> {
    scratch.clear();
    scratch.push_str(s);
    scratch.push_str("T00:00:00Z");
    let micros = rfc3339_micros(scratch).ok_or_else(|| Error::value_parse(col, s, "Date32"))?;
    i32::try_from(micros / 86_400_000_000).map_err(|_| Error::value_parse(col, s, "Date32"))
}

/// `"03:04:05.678901"` → micros since midnight.
fn parse_time_micros(s: &str, col: &str, scratch: &mut String) -> Result<i64, Error> {
    scratch.clear();
    scratch.push_str("1970-01-01T");
    scratch.push_str(s);
    scratch.push('Z');
    rfc3339_micros(scratch).ok_or_else(|| Error::value_parse(col, s, "Time64"))
}

/// `"2024-01-02 03:04:05.678901"` (no offset) → micros since epoch, treated as UTC.
fn parse_timestamp_micros(s: &str, col: &str, scratch: &mut String) -> Result<i64, Error> {
    scratch.clear();
    scratch.push_str(s);
    if let Some(i) = scratch.find(' ') {
        scratch.replace_range(i..i + 1, "T");
    }
    scratch.push('Z');
    rfc3339_micros(scratch).ok_or_else(|| Error::value_parse(col, s, "Timestamp"))
}

/// Canonical Postgres `timestamptz` (`"…+00"`, already UTC upstream) → micros since epoch, through
/// the same cleared and reused scratch its offset-less sibling uses — a `replacen` here would mean a
/// fresh `String` per timestamptz cell, i.e. once per row per column.
fn parse_timestamptz_micros(s: &str, col: &str, scratch: &mut String) -> Result<i64, Error> {
    scratch.clear();
    scratch.push_str(s);
    if let Some(i) = scratch.find(' ') {
        scratch.replace_range(i..i + 1, "T");
    }
    // Postgres prints whole-hour offsets as `+HH`; jiff wants `+HH:MM`.
    if let Some(t) = scratch.find('T')
        && let Some(sign) = scratch.get(t..).and_then(|suffix| suffix.rfind(['+', '-']))
        && t.checked_add(sign)
            .and_then(|start| scratch.get(start..))
            .is_some_and(|offset| offset.len() == 3)
    {
        scratch.push_str(":00");
    }
    rfc3339_micros(scratch).ok_or_else(|| Error::value_parse(col, s, "TimestampTz"))
}

#[cfg(test)]
#[path = "batch_test.rs"]
mod tests;
