#![allow(
    dead_code,
    clippy::unwrap_used,
    reason = "each integration-test crate uses a different subset of shared fixture helpers"
)]

use common::EpochNo;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

/// Build the complete protocol-v2 provenance document carried by one transformer fixture row.
///
/// Accepting the compact wire operation and LSN strings keeps Parquet fixture call sites easy to
/// audit. Invalid values are fixture bugs and intentionally panic here rather than producing an
/// object that the production validator will (correctly) quarantine.
#[allow(
    clippy::too_many_arguments,
    clippy::panic,
    reason = "test fixture keeps every wire-contract field explicit and rejects invalid test data"
)]
pub fn extractor_meta(
    epoch: EpochNo,
    batch_id: &str,
    schema_version: common::SchemaVersionNo,
    source_schema: &str,
    source_table: &str,
    kind: common::Kind,
    op: &str,
    commit_lsn: &str,
    lsn: &str,
) -> common::ExtractorMeta {
    let op = match op {
        "i" => common::Op::Insert,
        "u" => common::Op::Update,
        "d" => common::Op::Delete,
        "t" => common::Op::Truncate,
        other => panic!("invalid fixture op {other:?}"),
    };
    common::ExtractorMeta {
        op,
        lsn: lsn.parse().unwrap(),
        commit_lsn: commit_lsn.parse().unwrap(),
        commit_ts: "2026-07-15T12:00:00Z".parse().unwrap(),
        xid: 42,
        epoch,
        batch_id: batch_id.to_string(),
        schema_version,
        source_schema: source_schema.to_string(),
        source_table: source_table.to_string(),
        kind,
        unchanged_toast: Box::default(),
        extractor_instance: "extractor-fixture".to_string(),
        extractor_processed_at: "2026-07-15T12:00:01Z".parse().unwrap(),
    }
}

/// Remove every retry/publication/ownership row for a transformer integration-test epoch.
///
/// The order follows the protocol-v2 foreign keys. Keeping this in one transaction makes a test
/// rerun start either entirely clean or not clean at all; every database error is surfaced.
pub async fn cleanup_epoch(pool: &sqlx::PgPool, epoch: EpochNo) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_delete_protocol', '2', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance', '2-delete', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.replication_state_maintenance', '1-delete', true)")
        .execute(&mut *tx)
        .await
        .unwrap();

    for table in [
        "file_manifest",
        "stream_manifest_group",
        "stream_txn_publication",
        "manifest_publication_fence",
        "table_integrity_recovery",
        "transformer_checkpoint",
        "table_reload",
        "table_ownership",
        "replication_state",
    ] {
        let statement = format!("DELETE FROM public.walrus_{table} WHERE epoch = $1");
        sqlx::query(&statement)
            .bind(epoch.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}

pub fn store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

pub async fn fingerprint(uri: &str) -> (i64, Vec<u8>) {
    let remainder = uri.strip_prefix("s3://").expect("fixture URI is s3://");
    let (bucket, key) = remainder
        .split_once('/')
        .expect("fixture URI has object key");
    assert_eq!(bucket, "walrus");
    let bytes = store()
        .get(&object_store::path::Path::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    (
        i64::try_from(bytes.len()).unwrap(),
        Sha256::digest(&bytes).to_vec(),
    )
}

pub fn parquet_row_count(uri: &str) -> i64 {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "INSTALL httpfs; LOAD httpfs;
         SET s3_region='us-east-1';
         SET s3_endpoint='localhost:9000';
         SET s3_url_style='path';
         SET s3_use_ssl=false;
         SET s3_access_key_id='minioadmin';
         SET s3_secret_access_key='minioadmin';",
    )
    .unwrap();
    conn.query_row("SELECT count(*) FROM read_parquet(?)", [uri], |row| {
        row.get(0)
    })
    .unwrap()
}

pub async fn acquire_table(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
) -> (String, i64) {
    let owner = format!("transformer-test-{}-{schema}-{table}", epoch.0);
    let lease = control::acquire_lease(pool, epoch, schema, table, &owner, 3_600)
        .await
        .unwrap()
        .expect("fixture owns its table lease");
    (owner, lease.fencing_token)
}
