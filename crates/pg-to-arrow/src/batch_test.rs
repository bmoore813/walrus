use super::*;
use crate::approx::assert_approx_eq;
use crate::oids;
use arrow::array::{
    Array, AsArray, BinaryArray, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use common::{ExtractorMeta, Kind, Op, PgColumn, PgRelation, ReplicaIdentity, UtcTimestamp};

fn col(name: &str, oid: u32, typmod: i32) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    }
}

fn orders() -> PgRelation {
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", oids::INT4, -1),
            col("amount", oids::NUMERIC, 655366), // numeric(10,2)
            col("created_at", oids::TIMESTAMPTZ, -1),
            col("note", oids::TEXT, -1),
        ],
    }
}

fn meta(unchanged_toast: Vec<String>) -> ExtractorMeta {
    ExtractorMeta {
        op: Op::Insert,
        lsn: "0/10".parse().unwrap(),
        commit_lsn: "0/20".parse().unwrap(),
        commit_ts: "2026-07-04T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        xid: 1,
        epoch: common::EpochNo(7),
        batch_id: "b1".to_string(),
        schema_version: common::SchemaVersionNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: unchanged_toast.into_boxed_slice(),
        extractor_instance: "walrus-extractor-0".to_string(),
        extractor_processed_at: "2026-07-04T12:00:00.123Z".parse::<UtcTimestamp>().unwrap(),
    }
}

fn text_vals(vals: &[&str]) -> Vec<TupleValue> {
    vals.iter()
        .map(|s| TupleValue::Text(s.to_string()))
        .collect()
}

#[test]
fn downcast_mismatch_names_the_column() {
    let mut b = BooleanBuilder::new();
    let err = downcast::<Int64Builder>(&mut b, "total_cents").expect_err("type mismatch");
    assert!(
        matches!(err, Error::Downcast { column } if column == "total_cents"),
        "a builder mismatch must name the offending column"
    );
}

#[test]
fn float8_nan_and_infinities_survive_the_text_parse_path() {
    let relation = one_col_rel("d", oids::FLOAT8, -1);
    let mut builder = BatchBuilder::new(&relation).unwrap();
    for raw in ["NaN", "Infinity", "-Infinity"] {
        builder
            .append_row(&[TupleValue::Text(raw.to_string())], &meta(vec![]))
            .unwrap();
    }

    let batch = builder.into_record_batch().unwrap();
    let values = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Float64Type>();
    assert!(values.value(0).is_nan(), "NaN must not become zero");
    assert!(values.value(1).is_infinite() && values.value(1).is_sign_positive());
    assert!(values.value(2).is_infinite() && values.value(2).is_sign_negative());
}

#[test]
fn builds_a_batch_from_an_orders_insert() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    b.append_row(
        &text_vals(&["42", "19.99", "2024-01-02 03:04:05.678901+00", "hi"]),
        &meta(vec![]),
    )
    .unwrap();
    assert_eq!(b.len(), 1);
    let batch = b.into_record_batch().unwrap();

    assert_eq!(batch.num_columns(), 5); // 4 data + meta
    assert_eq!(*batch.schema(), super::build_schema(&orders()).unwrap());

    let ids = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(ids.value(0), 42);
    let amt = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), 1999); // 19.99 at scale 2
    let ts = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(ts.value(0) > 0);
    let note = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(note.value(0), "hi");
}

/// The `timestamptz` parse borrows the SAME RFC-3339 scratch every other temporal parse uses, so it
/// must refill it rather than append to it: whole-hour offsets still gain their `:00`, an explicit
/// `+HH:MM` is left alone, and a short value parsed after a longer one is not contaminated by the
/// previous contents.
#[test]
fn timestamptz_offsets_parse_through_the_reused_scratch() {
    let mut scratch = String::new();
    for (text, expected) in [
        ("2024-01-02 03:04:05.678901+05:30", 1_704_144_845_678_901),
        ("2024-01-02 03:04:05.678901+00", 1_704_164_645_678_901),
        ("2024-01-02 03:04:05-05", 1_704_182_645_000_000),
        ("2024-01-02 03:04:05+00", 1_704_164_645_000_000),
    ] {
        assert_eq!(
            parse_timestamptz_micros(text, "created_at", &mut scratch).unwrap(),
            expected,
            "{text} must parse the same however the scratch was left"
        );
    }
}

#[test]
fn null_value_sets_validity_false() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    // note is NULL
    let vals = vec![
        TupleValue::Text("42".to_string()),
        TupleValue::Text("1.00".to_string()),
        TupleValue::Text("2024-01-02 03:04:05+00".to_string()),
        TupleValue::Null,
    ];
    b.append_row(&vals, &meta(vec![])).unwrap();
    let batch = b.into_record_batch().unwrap();
    let note = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(note.is_null(0));
}

#[test]
fn binary_column_accepts_binary_text_and_null() {
    let relation = one_col_rel("payload", oids::BYTEA, -1);
    let mut builder = BatchBuilder::new(&relation).unwrap();
    builder
        .append_row(
            &[TupleValue::Binary(vec![0xca, 0xfe].into())],
            &meta(vec![]),
        )
        .unwrap();
    builder
        .append_row(
            &[TupleValue::Text("\\xdeadbeef".to_string())],
            &meta(vec![]),
        )
        .unwrap();
    builder
        .append_row(&[TupleValue::Null], &meta(vec![]))
        .unwrap();

    let batch = builder.into_record_batch().unwrap();
    let payload = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(payload.value(0), &[0xca, 0xfe]);
    assert_eq!(payload.value(1), &[0xde, 0xad, 0xbe, 0xef]);
    assert!(payload.is_null(2));
}

#[test]
fn binary_arm_has_no_runtime_unreachable() {
    assert!(
        !include_str!("batch.rs").contains("unreachable!("),
        "the Binary arm must be exhaustive over TupleValue"
    );
}

#[test]
fn unchanged_toast_appends_null_and_is_listed_in_meta() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    let vals = vec![
        TupleValue::Text("42".to_string()),
        TupleValue::Text("1.00".to_string()),
        TupleValue::Text("2024-01-02 03:04:05+00".to_string()),
        TupleValue::UnchangedToast,
    ];
    b.append_row(&vals, &meta(vec!["note".to_string()]))
        .unwrap();
    let batch = b.into_record_batch().unwrap();
    let note = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(note.is_null(0), "unchanged-TOAST appends a null");
    // and the column name is carried in the meta JSON.
    let meta_col = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(meta_col.value(0).contains("\"unchanged_toast\":[\"note\"]"));
}

#[test]
fn wrong_arity_row_is_rejected() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    let err = b
        .append_row(&text_vals(&["42", "1.00"]), &meta(vec![]))
        .unwrap_err();
    assert!(matches!(
        err,
        Error::RowLenMismatch {
            expected: 4,
            got: 2
        }
    ));
}

#[test]
fn bad_int_text_reports_the_column_name() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    let err = b
        .append_row(
            &text_vals(&["abc", "1.00", "2024-01-02 03:04:05+00", "hi"]),
            &meta(vec![]),
        )
        .unwrap_err();
    match err {
        Error::ValueParse(detail) => assert_eq!(detail.column, "id"),
        other => panic!("expected ValueParse, got {other:?}"),
    }
}

/// The offending cell is customer data, and this error rides `?` to the extractor's one
/// `tracing::error!`. Both formatters must name the column and the target type and nothing else —
/// `Debug` included, because the services log their own errors with `?e` as well as `{e:#}`. The
/// value stays reachable for a caller that destructures the detail, which no log line does.
#[test]
fn a_bad_cell_is_diagnosed_without_reproducing_its_value() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    let cell = "alice@example.com";

    let err = b
        .append_row(
            &text_vals(&[cell, "1.00", "2024-01-02 03:04:05+00", "hi"]),
            &meta(vec![]),
        )
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "column id: cannot parse [redacted] as Int32"
    );
    assert!(!format!("{err:?}").contains(cell), "{err:?}");
    match err {
        Error::ValueParse(detail) => assert_eq!(detail.value.expose(), cell),
        other => panic!("expected ValueParse, got {other:?}"),
    }
}

/// A non-text image fails the same conversion, and *which* image it was is walrus's own wire
/// vocabulary — so that much stays in the message even though the payload behind it does not.
#[test]
fn a_non_text_image_names_its_kind_but_not_its_payload() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    let mut vals = text_vals(&["1", "1.00", "2024-01-02 03:04:05+00"]);
    vals.push(TupleValue::Binary(bytes::Bytes::from_static(
        b"\xde\xad\xbe\xef",
    )));

    let err = b.append_row(&vals, &meta(vec![])).unwrap_err();

    let rendered = err.to_string();

    assert!(
        rendered.starts_with("column note: cannot parse "),
        "{rendered}"
    );
    assert!(
        rendered.ends_with(" as Utf8 (from a binary image)"),
        "{rendered}"
    );
    assert!(rendered.contains(common::REDACTED), "{rendered}");
}

#[test]
fn meta_column_holds_serialized_extractor_meta_json() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    let m = meta(vec![]);
    b.append_row(
        &text_vals(&["42", "1.00", "2024-01-02 03:04:05+00", "hi"]),
        &m,
    )
    .unwrap();
    let batch = b.into_record_batch().unwrap();
    let meta_col = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Order-independent: the amortized serialization splices batch-constant + per-row
    // fragments, so the key ORDER may differ from `to_string(ExtractorMeta)`; the transformer parses by key.
    let got: serde_json::Value = serde_json::from_str(meta_col.value(0)).unwrap();
    let want: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
    assert_eq!(got, want);
}

#[test]
fn decimal_rejects_too_many_fractional_digits() {
    assert!(parse_decimal("1.999", 2, "amount").is_err());
    assert_eq!(parse_decimal("19.99", 2, "amount").unwrap(), 1999);
    assert_eq!(parse_decimal("-0.05", 2, "amount").unwrap(), -5);
    assert_eq!(parse_decimal("7", 2, "amount").unwrap(), 700);
}

/// A one-column relation of `oid` (used for the Tier-2 fan-out tests).
fn one_col_rel(name: &str, oid: u32, typmod: i32) -> PgRelation {
    PgRelation {
        oid: 3,
        schema: "public".to_string(),
        name: "t".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col(name, oid, typmod)],
    }
}

#[test]
fn interval_fans_out_to_three_builders() {
    let rel = one_col_rel("dur", oids::INTERVAL, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("1 mon 2 days 03:04:05".to_string())],
        &meta(vec![]),
    )
    .unwrap();
    let batch = b.into_record_batch().unwrap();
    assert_eq!(batch.num_columns(), 4); // months + days + micros + meta
    let months = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let days = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let micros = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(months.value(0), 1);
    assert_eq!(days.value(0), 2);
    assert_eq!(micros.value(0), (3 * 3600 + 4 * 60 + 5) * 1_000_000);
}

#[test]
fn timetz_fans_out_to_two_builders() {
    let rel = one_col_rel("t", oids::TIMETZ, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("12:34:56+05:30".to_string())],
        &meta(vec![]),
    )
    .unwrap();
    let batch = b.into_record_batch().unwrap();
    assert_eq!(batch.num_columns(), 3); // micros + offset + meta
    let micros = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let offset = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(micros.value(0), (12 * 3600 + 34 * 60 + 56) * 1_000_000);
    assert_eq!(offset.value(0), 19_800);
}

#[test]
fn interval_null_maps_all_three_columns_null() {
    let rel = one_col_rel("dur", oids::INTERVAL, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap();
    let batch = b.into_record_batch().unwrap();
    let months = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let days = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let micros = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(
        months.is_null(0) && days.is_null(0) && micros.is_null(0),
        "all three interval siblings share one logical NULL"
    );
}

#[test]
fn range_fans_out_to_five_builders() {
    let rel = one_col_rel("span", oids::INT4RANGE, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Text("[1,10)".to_string())], &meta(vec![]))
        .unwrap();
    let batch = b.into_record_batch().unwrap();
    assert_eq!(batch.num_columns(), 6); // 5 range cols + meta
    let lower = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    let upper = batch
        .column(1)
        .as_primitive::<arrow::datatypes::Int32Type>();
    let lower_inc = batch.column(2).as_boolean();
    let upper_inc = batch.column(3).as_boolean();
    let empty = batch.column(4).as_boolean();
    assert_eq!(lower.value(0), 1);
    assert_eq!(upper.value(0), 10);
    assert!(lower_inc.value(0));
    assert!(!upper_inc.value(0));
    assert!(!empty.value(0));
}

#[test]
fn range_empty_unbounded_and_null_are_three_distinct_states() {
    let rel = one_col_rel("span", oids::INT4RANGE, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Text("empty".to_string())], &meta(vec![]))
        .unwrap(); // row 0: empty
    b.append_row(&[TupleValue::Text("(,10)".to_string())], &meta(vec![]))
        .unwrap(); // row 1: unbounded lower
    b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap(); // row 2: whole NULL
    let batch = b.into_record_batch().unwrap();
    let lower = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    let empty = batch.column(4).as_boolean();
    // empty: _empty=true, bounds NULL.
    assert!(empty.value(0));
    assert!(lower.is_null(0));
    // unbounded-lower: _lower NULL but _empty=false (distinct from empty).
    assert!(lower.is_null(1));
    assert!(!empty.value(1));
    // whole NULL: both _lower and _empty NULL (distinct from empty and unbounded).
    assert!(lower.is_null(2));
    assert!(empty.is_null(2));
}

#[test]
fn multirange_builds_list_of_structs_empty_vs_null() {
    let rel = one_col_rel("spans", oids::INT4MULTIRANGE, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("{[1,4),[7,9)}".to_string())],
        &meta(vec![]),
    )
    .unwrap(); // row 0: two members
    b.append_row(&[TupleValue::Text("{}".to_string())], &meta(vec![]))
        .unwrap(); // row 1: empty list
    b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap(); // row 2: NULL list
    let batch = b.into_record_batch().unwrap();
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), 2);
    assert!(!list.is_null(0));
    assert_eq!(list.value_length(1), 0, "empty multirange = empty list");
    assert!(!list.is_null(1), "empty list is distinct from NULL");
    assert!(list.is_null(2), "NULL column = NULL list");
    // Member bounds round-trip in order.
    let members = list.value(0);
    let s = members.as_any().downcast_ref::<StructArray>().unwrap();
    let lo = s.column(0).as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(lo.value(0), 1);
    assert_eq!(lo.value(1), 7);
}

#[test]
fn geometric_point_round_trips_and_nulls() {
    let rel = one_col_rel("loc", oids::POINT, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Text("(1,2)".to_string())], &meta(vec![]))
        .unwrap();
    b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap();
    let batch = b.into_record_batch().unwrap();
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let x = s.column(0).as_primitive::<arrow::datatypes::Float64Type>();
    let y = s.column(1).as_primitive::<arrow::datatypes::Float64Type>();
    assert_approx_eq(x.value(0), 1.0);
    assert_approx_eq(y.value(0), 2.0);
    assert!(s.is_null(1), "a NULL point is a null struct row");
}

#[test]
fn geometric_box_nests_two_points() {
    let rel = one_col_rel("bx", oids::BOX, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("(2,3),(0,1)".to_string())],
        &meta(vec![]),
    )
    .unwrap();
    let batch = b.into_record_batch().unwrap();
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let p1 = s.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let p2 = s.column(1).as_any().downcast_ref::<StructArray>().unwrap();
    assert_approx_eq(
        p1.column(0)
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0),
        2.0,
    );
    assert_approx_eq(
        p2.column(1)
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0),
        1.0,
    );
}

#[test]
fn geometric_path_open_vs_closed_only_differs_by_is_closed() {
    let rel = one_col_rel("pth", oids::PATH, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("[(0,0),(1,1)]".to_string())],
        &meta(vec![]),
    )
    .unwrap(); // open
    b.append_row(
        &[TupleValue::Text("((0,0),(1,1))".to_string())],
        &meta(vec![]),
    )
    .unwrap(); // closed
    let batch = b.into_record_batch().unwrap();
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let is_closed = s.column(0).as_boolean();
    assert!(!is_closed.value(0), "brackets → open");
    assert!(is_closed.value(1), "double parens → closed");
    // Same points either way (the only difference is the flag).
    let pts = s.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(pts.value_length(0), 2);
    assert_eq!(pts.value_length(1), 2);
}

#[test]
fn geometric_polygon_is_list_of_points() {
    let rel = one_col_rel("poly", oids::POLYGON, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("((0,0),(1,0),(1,1))".to_string())],
        &meta(vec![]),
    )
    .unwrap();
    let batch = b.into_record_batch().unwrap();
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), 3);
    let members = list.value(0);
    let s = members.as_any().downcast_ref::<StructArray>().unwrap();
    let y = s.column(1).as_primitive::<arrow::datatypes::Float64Type>();
    assert_approx_eq(y.value(2), 1.0);
}
