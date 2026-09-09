#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! DuckDB read-back conformance harness.
//!
//! The harness writes a `RecordBatch` to Parquet, then reads it back through in-process
//! DuckDB and assert **both** the inferred DuckDB `typeof(col)` *and* the value — the only proof
//! that the byte we wrote is the DuckDB type we intended (DuckDB ignores arrow-rs's `ARROW:schema`
//! metadata and reads native Parquet logical types; §2.1). Gated behind `--features conformance`
//! so the bundled DuckDB compile stays out of the default build. Each supported Tier-2/3 mapping
//! has a focused test here.
#![cfg(feature = "conformance")]

use common::{
    ExtractorMeta, Kind, Op, PgColumn, PgRelation, ReplicaIdentity, TupleValue, UtcTimestamp,
};
use duckdb::Connection;
use pg_to_arrow::{BatchBuilder, oids, write_parquet_bytes};
use std::io::Write;

fn meta() -> ExtractorMeta {
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
        source_table: "t".to_string(),
        kind: Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "walrus-extractor-0".to_string(),
        extractor_processed_at: "2026-07-04T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
    }
}

/// One-column ("c") RecordBatch of a given pg type from one text value.
fn one_col_batch(oid: u32, typmod: i32, value: &str) -> arrow::array::RecordBatch {
    let rel = PgRelation {
        oid: 1,
        schema: "public".to_string(),
        name: "t".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "c".to_string(),
            type_oid: oid,
            type_modifier: typmod,
            is_key: false,
        }],
    };
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Text(value.to_string())], &meta())
        .unwrap();
    b.into_record_batch().unwrap()
}

/// Write `bytes` to a temp `.parquet`, then run `sql` (with `{p}` = `read_parquet('<path>')`) in
/// in-process DuckDB. Returns each row as (duckdb_typeof, value-as-varchar).
fn read_parquet_rows(bytes: &[u8], sql: &str) -> Vec<(String, String)> {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(bytes).unwrap();
    tmp.flush().unwrap();
    let path = tmp.path().to_string_lossy().replace('\'', "''");
    let full = sql.replace("{p}", &format!("read_parquet('{path}')"));

    let conn = Connection::open_in_memory().unwrap();
    let mut stmt = conn.prepare(&full).unwrap();
    let rows = stmt
        .query_map([], |row| {
            let t: String = row.get(0)?;
            let v: Option<String> = row.get(1)?;
            Ok((t, v.unwrap_or_else(|| "NULL".to_string())))
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

/// `typeof(c)` and `CAST(c AS VARCHAR)` for the single `c` column of a one-row batch.
fn typeof_and_value(oid: u32, typmod: i32, value: &str) -> (String, String) {
    let bytes = write_parquet_bytes(&one_col_batch(oid, typmod, value)).unwrap();
    let rows = read_parquet_rows(&bytes, "SELECT typeof(c), CAST(c AS VARCHAR) FROM {p}");
    assert_eq!(rows.len(), 1, "expected one row");
    rows.into_iter().next().unwrap()
}

#[test]
fn smallint_reads_back_as_int16_smallint() {
    let (t, v) = typeof_and_value(oids::INT2, -1, "7");
    assert_eq!(t, "SMALLINT");
    assert_eq!(v, "7");
}

#[test]
fn bool_int_float_roundtrip_type_and_value() {
    assert_eq!(
        typeof_and_value(oids::BOOL, -1, "t"),
        ("BOOLEAN".into(), "true".into())
    );
    assert_eq!(
        typeof_and_value(oids::INT4, -1, "42"),
        ("INTEGER".into(), "42".into())
    );
    assert_eq!(
        typeof_and_value(oids::INT8, -1, "9000000000"),
        ("BIGINT".into(), "9000000000".into())
    );
    assert_eq!(
        typeof_and_value(oids::FLOAT4, -1, "1.5"),
        ("FLOAT".into(), "1.5".into())
    );
    assert_eq!(
        typeof_and_value(oids::FLOAT8, -1, "2.25"),
        ("DOUBLE".into(), "2.25".into())
    );
}

#[test]
fn decimal_10_2_reads_back_as_decimal_10_2() {
    let (t, v) = typeof_and_value(oids::NUMERIC, 655366, "19.99"); // numeric(10,2)
    assert_eq!(t, "DECIMAL(10,2)");
    assert_eq!(v, "19.99");
}

#[test]
fn timestamptz_is_adjusted_to_utc() {
    let (t, v) = typeof_and_value(oids::TIMESTAMPTZ, -1, "2024-01-02 03:04:05+00");
    assert_eq!(t, "TIMESTAMP WITH TIME ZONE");
    assert!(v.contains("2024-01-02"), "value was {v}");
}

#[test]
fn timestamp_is_not_adjusted() {
    let (t, v) = typeof_and_value(oids::TIMESTAMP, -1, "2024-01-02 03:04:05.678901");
    assert_eq!(t, "TIMESTAMP");
    assert!(v.contains("2024-01-02 03:04:05.678901"), "value was {v}");
}

#[test]
fn time_is_micros() {
    let (t, v) = typeof_and_value(oids::TIME, -1, "03:04:05.678901");
    assert_eq!(t, "TIME");
    assert!(v.contains("03:04:05.678901"), "value was {v}");
}

#[test]
fn date_reads_back_as_date() {
    let (t, v) = typeof_and_value(oids::DATE, -1, "2024-01-02");
    assert_eq!(t, "DATE");
    assert_eq!(v, "2024-01-02");
}

#[test]
fn bytea_reads_back_as_blob() {
    let (t, _v) = typeof_and_value(oids::BYTEA, -1, "\\x6869"); // "hi"
    assert_eq!(t, "BLOB");
}

#[test]
fn json_is_verbatim() {
    let (t, v) = typeof_and_value(oids::JSON, -1, "{\"a\": 1}");
    assert_eq!(t, "VARCHAR");
    assert_eq!(v, "{\"a\": 1}");
}

// ---- Tier-2 fan-out -----------------------------------------------------------------------------
// One source column becomes several: interval → c_months/c_days/c_micros, timetz → c_micros/
// c_offset_seconds. We assert the sibling column *types* and that DuckDB rebuilds them back into the
// intended `INTERVAL` (via `to_months + to_days + to_microseconds`, §2.4) / pinned timetz offset.

/// A one-column ("c") relation of a given Tier-2 type.
fn one_col_rel(oid: u32, typmod: i32) -> PgRelation {
    PgRelation {
        oid: 1,
        schema: "public".to_string(),
        name: "t".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "c".to_string(),
            type_oid: oid,
            type_modifier: typmod,
            is_key: false,
        }],
    }
}

#[test]
fn interval_three_columns_rebuild_to_duckdb_interval() {
    // '1 mon 2 days 03:04:05' → c_months=1, c_days=2, c_micros=11_045_000_000.
    let bytes =
        write_parquet_bytes(&one_col_batch(oids::INTERVAL, -1, "1 mon 2 days 03:04:05")).unwrap();

    // Sibling column types: months/days are INTEGER, micros is BIGINT.
    let types = read_parquet_rows(&bytes, "SELECT typeof(c_months), typeof(c_micros) FROM {p}");
    assert_eq!(types[0].0, "INTEGER");
    assert_eq!(types[0].1, "BIGINT");
    let days_ty = read_parquet_rows(&bytes, "SELECT typeof(c_days), 'x' FROM {p}");
    assert_eq!(days_ty[0].0, "INTEGER");

    // The transformer's rebuild recovers a genuine DuckDB INTERVAL.
    let rebuild = "to_months(c_months) + to_days(c_days) + to_microseconds(c_micros)";
    let rows = read_parquet_rows(
        &bytes,
        &format!("SELECT typeof({rebuild}), CAST({rebuild} AS VARCHAR) FROM {{p}}"),
    );
    assert_eq!(rows[0].0, "INTERVAL");
    let v = &rows[0].1;
    assert!(
        v.contains("1 month") && v.contains("2 days") && v.contains("03:04:05"),
        "rebuilt interval was {v}"
    );
}

#[test]
fn interval_null_sets_all_three_columns_null() {
    let rel = one_col_rel(oids::INTERVAL, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Null], &meta()).unwrap();
    let bytes = write_parquet_bytes(&b.into_record_batch().unwrap()).unwrap();
    let rows = read_parquet_rows(
        &bytes,
        "SELECT 'x', (c_months IS NULL AND c_days IS NULL AND c_micros IS NULL)::VARCHAR FROM {p}",
    );
    assert_eq!(
        rows[0].1, "true",
        "a NULL interval nulls all three siblings"
    );
}

#[test]
fn timetz_offset_sign_roundtrips() {
    // Sign convention pinned here: '+05:30' (east of UTC) stores offset_seconds = +19800.
    let bytes = write_parquet_bytes(&one_col_batch(oids::TIMETZ, -1, "12:34:56+05:30")).unwrap();

    let types = read_parquet_rows(
        &bytes,
        "SELECT typeof(c_micros), typeof(c_offset_seconds) FROM {p}",
    );
    assert_eq!(types[0].0, "BIGINT");
    assert_eq!(types[0].1, "INTEGER");

    let vals = read_parquet_rows(
        &bytes,
        "SELECT CAST(c_micros AS VARCHAR), CAST(c_offset_seconds AS VARCHAR) FROM {p}",
    );
    assert_eq!(
        vals[0].0,
        ((12i64 * 3600 + 34 * 60 + 56) * 1_000_000).to_string()
    );
    assert_eq!(vals[0].1, "19800");

    // A west-of-UTC offset stores the negative — the sign is not dropped the way DMS does.
    let west = write_parquet_bytes(&one_col_batch(oids::TIMETZ, -1, "12:34:56-08")).unwrap();
    let w = read_parquet_rows(
        &west,
        "SELECT 'x', CAST(c_offset_seconds AS VARCHAR) FROM {p}",
    );
    assert_eq!(w[0].1, "-28800");
}

// ---- Tier-2 range / multirange ------------------------------------------------------------------
// range → 5 flat columns (c_lower/c_upper/c_lower_inc/c_upper_inc/c_empty); multirange → LIST<STRUCT>.
// NULL, `empty`, and `unbounded` must read back as three distinct states.

/// Build a one-column batch from a single `TupleValue` (for the whole-column NULL cases).
fn one_col_value_batch(oid: u32, typmod: i32, value: TupleValue) -> arrow::array::RecordBatch {
    let mut b = BatchBuilder::new(&one_col_rel(oid, typmod)).unwrap();
    b.append_row(&[value], &meta()).unwrap();
    b.into_record_batch().unwrap()
}

#[test]
fn range_int4_bounds_inclusivity_and_types() {
    let bytes = write_parquet_bytes(&one_col_batch(oids::INT4RANGE, -1, "[1,10)")).unwrap();
    // Element columns are INTEGER; the flags are BOOLEAN.
    let types = read_parquet_rows(&bytes, "SELECT typeof(c_lower), typeof(c_empty) FROM {p}");
    assert_eq!(types[0], ("INTEGER".to_string(), "BOOLEAN".to_string()));
    // Canonical half-open `[1,10)`: bounds 1/10, lower inclusive, upper exclusive, not empty.
    let vals = read_parquet_rows(
        &bytes,
        "SELECT CAST(c_lower AS VARCHAR), CAST(c_upper AS VARCHAR) FROM {p}",
    );
    assert_eq!(vals[0], ("1".to_string(), "10".to_string()));
    let flags = read_parquet_rows(
        &bytes,
        "SELECT (c_lower_inc AND NOT c_upper_inc AND NOT c_empty)::VARCHAR, 'x' FROM {p}",
    );
    assert_eq!(flags[0].0, "true");
}

#[test]
fn range_empty_sets_empty_true_and_bounds_null() {
    let bytes = write_parquet_bytes(&one_col_batch(oids::INT4RANGE, -1, "empty")).unwrap();
    let rows = read_parquet_rows(
        &bytes,
        "SELECT c_empty::VARCHAR, (c_lower IS NULL AND c_upper IS NULL)::VARCHAR FROM {p}",
    );
    assert_eq!(rows[0], ("true".to_string(), "true".to_string()));
}

#[test]
fn range_unbounded_lower_is_null_with_empty_false() {
    // Distinct from `empty`: the lower bound is NULL but the range is present (`_empty=false`).
    let bytes = write_parquet_bytes(&one_col_batch(oids::INT4RANGE, -1, "(,10)")).unwrap();
    let rows = read_parquet_rows(
        &bytes,
        "SELECT (c_lower IS NULL)::VARCHAR, c_empty::VARCHAR FROM {p}",
    );
    assert_eq!(rows[0], ("true".to_string(), "false".to_string()));
}

#[test]
fn range_whole_null_nulls_all_five_columns() {
    let bytes =
        write_parquet_bytes(&one_col_value_batch(oids::INT4RANGE, -1, TupleValue::Null)).unwrap();
    let rows = read_parquet_rows(
        &bytes,
        "SELECT (c_lower IS NULL AND c_upper IS NULL AND c_lower_inc IS NULL \
         AND c_upper_inc IS NULL AND c_empty IS NULL)::VARCHAR, 'x' FROM {p}",
    );
    assert_eq!(
        rows[0].0, "true",
        "whole-column NULL nulls all five siblings"
    );
}

#[test]
fn multirange_reads_back_as_list_of_structs() {
    let bytes =
        write_parquet_bytes(&one_col_batch(oids::INT4MULTIRANGE, -1, "{[1,4),[7,9)}")).unwrap();
    // Two members, in order.
    let count = read_parquet_rows(&bytes, "SELECT len(c)::VARCHAR, 'x' FROM {p}");
    assert_eq!(count[0].0, "2");
    let members = read_parquet_rows(
        &bytes,
        "SELECT CAST(m.lower AS VARCHAR), CAST(m.upper AS VARCHAR) FROM (SELECT unnest(c) AS m FROM {p})",
    );
    assert_eq!(members[0], ("1".to_string(), "4".to_string()));
    assert_eq!(members[1], ("7".to_string(), "9".to_string()));
}

#[test]
fn multirange_empty_list_is_distinct_from_null() {
    let empty = write_parquet_bytes(&one_col_batch(oids::INT4MULTIRANGE, -1, "{}")).unwrap();
    let e = read_parquet_rows(
        &empty,
        "SELECT (c IS NULL)::VARCHAR, len(c)::VARCHAR FROM {p}",
    );
    assert_eq!(
        e[0],
        ("false".to_string(), "0".to_string()),
        "empty list ≠ NULL"
    );

    let null = write_parquet_bytes(&one_col_value_batch(
        oids::INT4MULTIRANGE,
        -1,
        TupleValue::Null,
    ))
    .unwrap();
    let n = read_parquet_rows(&null, "SELECT (c IS NULL)::VARCHAR, 'x' FROM {p}");
    assert_eq!(n[0].0, "true", "NULL column = NULL list");
}

// ---- Tier-2 geometric ---------------------------------------------------------------------------
// Every native geometric type → a nested STRUCT / LIST<STRUCT> of doubles. We read the nested fields
// back through DuckDB (`c.x`, `c.p1.x`, `c.points`, `unnest(c)`) and compare numerically (avoiding
// DOUBLE text formatting). `path.is_closed` is proven to distinguish an open from a closed path.

#[test]
fn geometric_point_reads_back_as_struct() {
    let bytes = write_parquet_bytes(&one_col_batch(oids::POINT, -1, "(1,2)")).unwrap();
    let rows = read_parquet_rows(
        &bytes,
        "SELECT (c.x = 1.0)::VARCHAR, (c.y = 2.0)::VARCHAR FROM {p}",
    );
    assert_eq!(rows[0], ("true".to_string(), "true".to_string()));
}

#[test]
fn geometric_box_and_lseg_nest_two_points() {
    let bx = write_parquet_bytes(&one_col_batch(oids::BOX, -1, "(2,3),(0,1)")).unwrap();
    let b = read_parquet_rows(
        &bx,
        "SELECT (c.p1.x = 2.0)::VARCHAR, (c.p2.y = 1.0)::VARCHAR FROM {p}",
    );
    assert_eq!(b[0], ("true".to_string(), "true".to_string()));
    let ls = write_parquet_bytes(&one_col_batch(oids::LSEG, -1, "[(0,0),(1,1)]")).unwrap();
    let l = read_parquet_rows(
        &ls,
        "SELECT (c.p1.x = 0.0)::VARCHAR, (c.p2.x = 1.0)::VARCHAR FROM {p}",
    );
    assert_eq!(l[0], ("true".to_string(), "true".to_string()));
}

#[test]
fn geometric_circle_carries_radius_and_line_three_coeffs() {
    let ci = write_parquet_bytes(&one_col_batch(oids::CIRCLE, -1, "<(1,2),3>")).unwrap();
    let c = read_parquet_rows(
        &ci,
        "SELECT (c.x = 1.0 AND c.y = 2.0)::VARCHAR, (c.r = 3.0)::VARCHAR FROM {p}",
    );
    assert_eq!(c[0], ("true".to_string(), "true".to_string()));
    let li = write_parquet_bytes(&one_col_batch(oids::LINE, -1, "{1,2,3}")).unwrap();
    let l = read_parquet_rows(
        &li,
        "SELECT (c.a = 1.0)::VARCHAR, (c.c = 3.0)::VARCHAR FROM {p}",
    );
    assert_eq!(l[0], ("true".to_string(), "true".to_string()));
}

#[test]
fn geometric_polygon_reads_back_as_list_of_points() {
    let bytes =
        write_parquet_bytes(&one_col_batch(oids::POLYGON, -1, "((0,0),(1,0),(1,1))")).unwrap();
    let count = read_parquet_rows(&bytes, "SELECT (len(c) = 3)::VARCHAR, 'x' FROM {p}");
    assert_eq!(count[0].0, "true");
    // Third vertex is (1,1).
    let verts = read_parquet_rows(
        &bytes,
        "SELECT (u.x = 1.0 AND u.y = 1.0)::VARCHAR, 'x' FROM (SELECT unnest(c) AS u FROM {p})",
    );
    assert_eq!(verts.len(), 3);
    assert_eq!(verts[2].0, "true");
}

#[test]
fn geometric_path_open_vs_closed_reads_back_is_closed() {
    // Same two points; the ONLY read-back difference is is_closed.
    let open = write_parquet_bytes(&one_col_batch(oids::PATH, -1, "[(0,0),(1,1)]")).unwrap();
    let o = read_parquet_rows(
        &open,
        "SELECT c.is_closed::VARCHAR, (len(c.points) = 2)::VARCHAR FROM {p}",
    );
    assert_eq!(o[0], ("false".to_string(), "true".to_string()));

    let closed = write_parquet_bytes(&one_col_batch(oids::PATH, -1, "((0,0),(1,1))")).unwrap();
    let c = read_parquet_rows(
        &closed,
        "SELECT c.is_closed::VARCHAR, (len(c.points) = 2)::VARCHAR FROM {p}",
    );
    assert_eq!(c[0], ("true".to_string(), "true".to_string()));
}

// ---- Tier-3 canonical-text carriers -------------------------------------------------------------
// No lossless structural target → one VARCHAR column carrying the canonical text verbatim.

#[test]
fn unconstrained_numeric_is_varchar_verbatim() {
    let (t, v) = typeof_and_value(oids::NUMERIC, -1, "3.14159");
    assert_eq!(t, "VARCHAR");
    assert_eq!(v, "3.14159");
}

#[test]
fn numeric_over_38_digits_survives_exactly_as_varchar() {
    // 42 significant digits — well past DuckDB's DECIMAL precision-38 ceiling. As VARCHAR it is exact;
    // a Decimal128/Decimal256 path would hit DuckDB's Parquet reader downcasting p>38 to DOUBLE and
    // lose digits. Carried as unconstrained numeric (typmod -1).
    let big = "1234567890123456789012345678901234567890.12";
    let (t, v) = typeof_and_value(oids::NUMERIC, -1, big);
    assert_eq!(t, "VARCHAR");
    assert_eq!(v, big, "the exact 40+ digit string must survive");
}

#[test]
fn system_types_carry_canonical_varchar() {
    for (oid, text) in [
        (oids::BIT, "101"),
        (oids::VARBIT, "1011"),
        (oids::INET, "192.168.0.1/24"),
        (oids::CIDR, "10.0.0.0/8"),
        (oids::MACADDR, "08:00:2b:01:02:03"),
        (oids::MACADDR8, "08:00:2b:01:02:03:04:05"),
        (oids::TSVECTOR, "'cat':1 'sat':2"),
        (oids::TSQUERY, "'cat' & 'sat'"),
        (oids::PG_LSN, "16/B374D848"),
        (oids::XID, "1234"),
        (oids::XID8, "1234567890"),
        (oids::XML, "<a>1</a>"),
    ] {
        let (t, v) = typeof_and_value(oid, -1, text);
        assert_eq!(t, "VARCHAR", "oid {oid} should read back as VARCHAR");
        assert_eq!(v, text, "oid {oid} value must be verbatim");
    }
}

// ---- uuid (native) + enum -----------------------------------------------------------------------

#[test]
fn uuid_reads_back_as_native_uuid() {
    // The pinned-arrow-rs canary: the `arrow.uuid` extension → Parquet UUID → DuckDB UUID (not BLOB).
    // If a bump ever drops the annotation this typeof flips to BLOB and fails here.
    let u = "550e8400-e29b-41d4-a716-446655440000";
    let (t, v) = typeof_and_value(oids::UUID, -1, u);
    assert_eq!(
        t, "UUID",
        "arrow.uuid extension must yield a native DuckDB UUID"
    );
    assert_eq!(v, u);
}

#[test]
fn uuid_varchar_fallback_casts_to_uuid() {
    // The escape hatch: carry canonical text as VARCHAR; CAST(x AS UUID) on load recovers the value.
    let u = "550e8400-e29b-41d4-a716-446655440000";
    let field = pg_to_arrow::uuid_enum::uuid_as_varchar("c");
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![field]));
    let batch = arrow::array::RecordBatch::try_new(
        schema,
        vec![std::sync::Arc::new(arrow::array::StringArray::from(vec![
            u,
        ]))],
    )
    .unwrap();
    let bytes = write_parquet_bytes(&batch).unwrap();
    let rows = read_parquet_rows(
        &bytes,
        "SELECT typeof(CAST(c AS UUID)), CAST(CAST(c AS UUID) AS VARCHAR) FROM {p}",
    );
    assert_eq!(rows[0].0, "UUID");
    assert_eq!(rows[0].1, u);
}

#[test]
fn enum_reads_back_as_varchar_verbatim() {
    // Enum OIDs are dynamic (≥16384); the conservative rule maps non-builtin OIDs to enum→VARCHAR.
    let (t, v) = typeof_and_value(16400, -1, "shipped");
    assert_eq!(t, "VARCHAR");
    assert_eq!(v, "shipped");
}
