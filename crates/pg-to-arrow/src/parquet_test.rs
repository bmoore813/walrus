use super::*;
use crate::batch::BatchBuilder;
use crate::oids;
use bytes::Bytes;
use common::{
    ExtractorMeta, Kind, Op, PgColumn, PgRelation, ReplicaIdentity, TupleValue, UtcTimestamp,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn orders() -> PgRelation {
    let col = |name: &str, oid, typmod| PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    };
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", oids::INT4, -1),
            col("amount", oids::NUMERIC, 655366),
            col("created_at", oids::TIMESTAMPTZ, -1),
            col("note", oids::TEXT, -1),
        ],
    }
}

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
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "walrus-extractor-0".to_string(),
        extractor_processed_at: "2026-07-04T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
    }
}

#[test]
fn round_trips_a_batch_through_parquet() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    for v in [
        ["1", "10.00", "2024-01-02 03:04:05+00", "a"],
        ["2", "-3.50", "2024-06-30 23:59:59.999999+00", "b"],
    ] {
        let vals: Vec<TupleValue> = v.iter().map(|s| TupleValue::Text(s.to_string())).collect();
        b.append_row(&vals, &meta()).unwrap();
    }
    let batch = b.into_record_batch().unwrap();

    let bytes = write_parquet_bytes(&batch).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let read: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert_eq!(read.len(), 1);
    // arrow-rs restores the exact schema (incl. Decimal(10,2) and the "UTC" tz) and values.
    assert_eq!(read[0], batch);
}

#[test]
fn row_group_statistics_have_a_fixed_byte_bound() {
    assert_eq!(
        default_writer_properties().statistics_truncate_length(),
        Some(STATISTICS_TRUNCATE_LENGTH),
        "multi-row-group reload footers must not retain whole oversized TEXT/BYTEA values"
    );
}
