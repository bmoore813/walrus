#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Arrow → Parquet → S3 PUT against compose MinIO (`#[ignore]`). A flush lands an object at the
//! epoch-namespaced key; it reads back with MICROS temporals + values intact. (DuckDB's *type
//! inference* over this exact Parquet is proven by pg-to-arrow's conformance suite; here we verify
//! the S3 write path + the native logical types via arrow-rs's own reader.)
//!
//!   cargo test -p extractor --test parquet_put -- --ignored

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use common::{
    ExtractorMeta, Kind, Op, PgColumn, PgRelation, ReplicaIdentity, TupleValue, UtcTimestamp,
};
use extractor::batch::SealedBatch;
use extractor::staging::{FileKind, ParquetStager};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pg_to_arrow::{BatchBuilder, EXTRACTOR_META_COLUMN, oids};
use std::sync::Arc;

fn minio_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .expect("build MinIO store"),
    )
}

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, typmod: i32| PgColumn {
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
            col("amount", oids::NUMERIC, 655366), // numeric(10,2)
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
        commit_ts: "2026-07-07T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        xid: 1,
        epoch: common::EpochNo(7),
        batch_id: "public.orders-0/10".to_string(),
        schema_version: common::SchemaVersionNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "walrus-extractor-0".to_string(),
        extractor_processed_at: UtcTimestamp::now(),
    }
}

fn sealed(lsn_end: &str) -> SealedBatch {
    let mut bb = BatchBuilder::new(&orders()).unwrap();
    bb.append_row(
        &[
            TupleValue::Text("1".into()),
            TupleValue::Text("19.99".into()),
            TupleValue::Text("2024-01-02 03:04:05+00".into()),
            TupleValue::Text("hi".into()),
        ],
        &meta(),
    )
    .unwrap();
    SealedBatch {
        record_batch: bb.into_record_batch().unwrap(),
        schema: "public".to_string(),
        table: "orders".to_string(),
        schema_version: common::SchemaVersionNo(1),
        lsn_start: "0/100".parse().unwrap(),
        lsn_end: lsn_end.parse().unwrap(),
        row_count: 1,
    }
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO)"]
async fn flush_writes_object_at_expected_key() {
    let store = minio_store();
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(7));
    let written = stager.put(sealed("0/2A0")).await.unwrap();

    let lsn_end: common::Lsn = "0/2A0".parse().unwrap();
    assert!(written.key.as_ref().starts_with("7/public/orders/"));
    assert!(written.key.as_ref().contains(&lsn_end.to_string()));
    assert!(written.key.as_ref().ends_with(".parquet"));
    assert_eq!(written.kind, FileKind::Stream);
    assert_eq!(written.s3_uri, format!("s3://walrus/{}", written.key));

    // The object is durably present (put returned only after close()).
    let head = store.head(&written.key).await.unwrap();
    assert!(head.size > 0, "object has bytes");

    let _ = store.delete(&written.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO)"]
async fn object_reads_back_with_micros_temporals_and_values() {
    let store = minio_store();
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(7));
    let written = stager.put(sealed("0/300")).await.unwrap();

    let bytes = store
        .get(&written.key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );

    let schema = batches[0].schema();
    // §2.1: created_at survives as a MICROS timestamp carrying UTC (what DuckDB reads as tz-aware).
    let created = schema.field_with_name("created_at").unwrap();
    assert_eq!(
        created.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "temporal is MICROS + UTC"
    );

    // The meta column carries a UTC-`Z` extractor_processed_at.
    let meta_col = batches[0]
        .column_by_name(EXTRACTOR_META_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let json = meta_col.value(0);
    assert!(json.contains("\"extractor_processed_at\""));
    assert!(
        json.contains('Z'),
        "extractor_processed_at is a UTC Z timestamp: {json}"
    );

    let _ = store.delete(&written.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (grouped; pure)"]
async fn key_is_epoch_namespaced_and_lsn_sortable() {
    let stager = ParquetStager::new(minio_store(), "walrus", common::EpochNo(9));
    let a = stager.object_key("public", "orders", "0/100".parse().unwrap(), "u1");
    let b = stager.object_key("public", "orders", "1/0".parse().unwrap(), "u2");
    assert!(a.as_ref().starts_with("9/public/orders/"));
    assert!(a.as_ref() < b.as_ref(), "keys sort by commit LSN");
}
