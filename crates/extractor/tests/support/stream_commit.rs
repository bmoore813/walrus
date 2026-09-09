//! Shared protocol-v2 publication adapter for compose integration tests.
//!
//! A `StreamCommit` is one atomic control-plane receipt. In particular, speculative `spill`
//! objects must never be published through the ordinary one-file path: the transformer may only see
//! them as children of their complete transaction group.

use common::{EpochNo, Lsn, UtcTimestamp};
use extractor::staging::WrittenObject;

pub async fn publish(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    top_xid: u32,
    commit_lsn: Lsn,
    commit_ts: UtcTimestamp,
    objects: &[WrittenObject],
) -> Result<control::PublishStreamOutcome, control::ControlError> {
    let files = objects
        .iter()
        .map(|object| control::NewManifestFile {
            epoch,
            source_schema: object.source_schema.clone(),
            source_table: object.source_table.clone(),
            s3_uri: object.s3_uri.clone(),
            kind: object.kind,
            row_count: i64::try_from(object.row_count).unwrap_or(i64::MAX),
            object_size: i64::try_from(object.object_size).unwrap_or(i64::MAX),
            sha256: object.sha256.to_vec(),
            lsn_start: object.lsn_start,
            lsn_end: object.lsn_end,
            schema_version: object.schema_version,
            reload_id: None,
        })
        .collect();
    control::publish_stream_commit(
        pool,
        &control::NewStreamCommitPublication {
            epoch,
            top_xid,
            commit_lsn,
            commit_ts,
            ddl_rows: Vec::new(),
            registry_rows: Vec::new(),
            files,
        },
    )
    .await
}
