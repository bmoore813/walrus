use super::*;
use crate::staging::FileKind;
use object_store::path::Path;

fn written_stream_object() -> WrittenObject {
    WrittenObject {
        s3_uri: "s3://walrus/7/public/orders/000000000000A100-uuid.parquet".to_string(),
        key: Path::from("7/public/orders/000000000000A100-uuid.parquet"),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        lsn_start: "0/100".parse().unwrap(),
        lsn_end: "0/A100".parse().unwrap(),
        row_count: 42,
        object_size: 128,
        sha256: [7; 32],
        schema_version: common::SchemaVersionNo(3),
        kind: FileKind::Stream,
    }
}

#[test]
fn written_stream_object_maps_to_a_complete_ready_row() {
    // Arrange
    let object = written_stream_object();

    // Act
    let actual = to_ready_row(common::EpochNo(9), &object, None);

    // Assert
    // Compared as a whole record, not field by field: every column the transformer's queue reads is
    // pinned, including the `s3_uri` and `lsn_start` a per-field assertion list is free to forget.
    assert_eq!(
        actual,
        control::NewManifestFile {
            epoch: common::EpochNo(9),
            source_schema: "public".to_string(),
            source_table: "orders".to_string(),
            s3_uri: "s3://walrus/7/public/orders/000000000000A100-uuid.parquet".to_string(),
            kind: FileKind::Stream,
            row_count: 42,
            object_size: 128,
            sha256: vec![7; 32],
            lsn_start: "0/100".parse().unwrap(),
            lsn_end: "0/A100".parse().unwrap(),
            schema_version: common::SchemaVersionNo(3),
            // Stream objects never carry a reload_id — only the chunk exporter sets one.
            reload_id: None,
        }
    );
}

#[test]
fn reload_ready_row_preserves_the_supplied_reload_id() {
    // Arrange
    let object = written_stream_object();
    let reload_id = common::ReloadId(17);

    // Act
    let ready = to_ready_row(common::EpochNo(9), &object, Some(reload_id));

    // Assert
    assert_eq!(ready.reload_id, Some(reload_id));
}
