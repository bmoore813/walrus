use super::*;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::time::Duration;

fn stager() -> ParquetStager {
    ParquetStager::new(
        Arc::new(object_store::memory::InMemory::new()),
        "walrus",
        common::EpochNo(5),
    )
}

#[test]
fn the_bucket_name_is_taken_as_str_or_string() {
    // `new` converts once internally, so a `&str` literal and an owned `String` are interchangeable
    // at the call site and land as the same stored bucket.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let borrowed = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let owned = ParquetStager::new(store, String::from("walrus"), common::EpochNo(5));

    assert_eq!(borrowed.bucket, "walrus");
    assert_eq!(owned.bucket, borrowed.bucket);
}

#[test]
fn object_key_is_epoch_namespaced_and_lsn_sortable() {
    let s = stager();
    let lsn: Lsn = "0/1A2B3C".parse().unwrap();
    let key = s.object_key("public", "orders", lsn, "abcd");
    // <epoch>/<schema>/<table>/<lsn_end 16-hex>-<uuid>.parquet
    assert_eq!(key.as_ref(), format!("5/public/orders/{lsn}-abcd.parquet"));
    assert_eq!(lsn.to_string().len(), 16, "lsn is zero-padded 16-hex");

    // Zero-padded 16-hex means byte-lexical order matches commit-LSN order.
    let lo = s.object_key("public", "orders", "0/100".parse().unwrap(), "u");
    let hi = s.object_key("public", "orders", "1/0".parse().unwrap(), "u");
    assert!(lo.as_ref() < hi.as_ref(), "keys sort by commit LSN");
}

#[test]
fn a_parquet_failure_converts_into_the_encode_class() {
    // The `?` on every writer call routes through this From impl, so the writer path must stay
    // Encode (→ ExitCode::Internal) and must not lose the engine's own message.
    let engine = parquet::errors::ParquetError::General("footer write failed".into());
    let rendered = engine.to_string();

    let error = StagingError::from(engine);

    assert!(matches!(&error, StagingError::Encode(_)));
    assert_eq!(error.to_string(), format!("parquet encode: {rendered}"));
    // …and must not lose the engine error *itself*: it stays in the chain, downcastable, so a
    // reporter can tell a footer write apart from an out-of-spec schema.
    let cause = std::error::Error::source(&error).expect("encode keeps the engine error");
    assert!(cause.is::<parquet::errors::ParquetError>());
    assert_eq!(cause.to_string(), rendered);
}

/// The store is a **trait** dependency (`Arc<dyn ObjectStore>`), so the failure branch is reachable
/// by injecting a *different real implementation of that trait* instead of standing up a broken S3:
/// `LocalFileSystem` answers `NotFound` for a key nothing ever wrote. The `InMemory` fixture above
/// cannot play this part — its `delete` is remove-or-ignore and always returns `Ok` — which is what
/// the trait seam buys that a concrete client would not.
///
/// Nothing is created, so nothing is cleaned up: the epoch-namespaced key carries a fresh uuid and
/// the store only ever tries (and fails) to remove it.
///
/// What this pins is the classification, not the transport. `ParquetStager::delete` is the best-effort
/// cleanup for an aborted streamed txn's speculative files, and "best-effort" must mean
/// *reported*, never *swallowed*: a store refusal stays `StagingError::Store` →
/// `common::Error::ObjectStore` — the transient class — and is never folded into the terminal
/// `Encode` side asserted above.
#[tokio::test]
async fn a_store_failure_propagates_as_the_transient_store_class() {
    let store = LocalFileSystem::new_with_prefix(std::env::temp_dir())
        .expect("the system temp dir is an existing directory");
    let stager = ParquetStager::new(Arc::new(store), "walrus", common::EpochNo(5));
    let file_id = uuid::Uuid::new_v4().to_string();
    let absent = stager.object_key("public", "orders", "0/2A0".parse().unwrap(), &file_id);

    let error = stager.delete(&absent).await.unwrap_err();

    assert!(matches!(&error, StagingError::Store(_)));
    let classified = common::Error::from(error);
    assert!(matches!(classified, common::Error::ObjectStore(_)));
}

fn reload_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn reload_batch(schema: SchemaRef, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
}

#[tokio::test]
async fn reload_stream_writes_incremental_row_groups_then_returns_a_durable_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let schema = reload_schema();
    let fence: Lsn = "0/2A0".parse().unwrap();
    let mut writer = stager
        .begin_reload_stream(
            Arc::clone(&schema),
            "public",
            "orders",
            fence,
            SchemaVersionNo(7),
        )
        .unwrap();
    let key = writer.key().clone();

    writer
        .write_batch(&reload_batch(Arc::clone(&schema), vec![1, 2]))
        .await
        .unwrap();
    assert_eq!(writer.row_count(), 2);
    assert_eq!(writer.writer.flushed_row_groups().len(), 1);
    assert!(
        store.head(&key).await.is_err(),
        "a partial multipart object must not be visible"
    );

    writer
        .write_batch(&reload_batch(schema, vec![3]))
        .await
        .unwrap();
    assert_eq!(writer.writer.flushed_row_groups().len(), 2);
    let written = writer.finish().await.unwrap();

    assert_eq!(written.key, key);
    assert_eq!(written.s3_uri, format!("s3://walrus/{key}"));
    assert_eq!(written.source_schema, "public");
    assert_eq!(written.source_table, "orders");
    assert_eq!(written.lsn_start, fence);
    assert_eq!(written.lsn_end, fence);
    assert_eq!(written.row_count, 3);
    assert_eq!(written.schema_version, SchemaVersionNo(7));
    assert_eq!(written.kind, FileKind::Reload);

    let bytes = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(written.object_size, u64::try_from(bytes.len()).unwrap());
    let digest = Sha256::digest(&bytes);
    assert_eq!(written.sha256.as_slice(), &digest[..]);
    let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let values: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap())
        })
        .collect();
    assert_eq!(values, [1, 2, 3]);
}

#[tokio::test]
async fn reload_stream_bounds_large_string_statistics_in_every_incremental_row_group() {
    const ROW_GROUPS: usize = 6;
    const LARGE_VALUE_BYTES: usize = 256 * 1024;
    const STATISTICS_BYTES_PER_BOUND: usize = 64;

    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let mut writer = stager
        .begin_reload_stream(
            Arc::clone(&schema),
            "public",
            "large_text",
            "0/2A1".parse().unwrap(),
            SchemaVersionNo(7),
        )
        .unwrap();

    for row_group in 0..ROW_GROUPS {
        let value = format!("{row_group:04}-{}", "x".repeat(LARGE_VALUE_BYTES));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .unwrap();
        writer.write_batch(&batch).await.unwrap();
    }
    assert_eq!(writer.writer.flushed_row_groups().len(), ROW_GROUPS);

    let written = writer.finish().await.unwrap();
    let bytes = store
        .get(&written.key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    assert_eq!(reader.metadata().num_row_groups(), ROW_GROUPS);

    let mut retained_statistics_bytes = 0;
    for (row_group, metadata) in reader.metadata().row_groups().iter().enumerate() {
        let statistics = metadata
            .column(0)
            .statistics()
            .unwrap_or_else(|| panic!("row group {row_group} must have string statistics"));
        let min = statistics
            .min_bytes_opt()
            .unwrap_or_else(|| panic!("row group {row_group} must have a minimum"));
        let max = statistics
            .max_bytes_opt()
            .unwrap_or_else(|| panic!("row group {row_group} must have a maximum"));

        assert!(
            min.len() <= STATISTICS_BYTES_PER_BOUND,
            "row group {row_group} retained a {}-byte minimum",
            min.len()
        );
        assert!(
            max.len() <= STATISTICS_BYTES_PER_BOUND,
            "row group {row_group} retained a {}-byte maximum",
            max.len()
        );
        retained_statistics_bytes += min.len() + max.len();
    }
    assert!(
        retained_statistics_bytes <= ROW_GROUPS * 2 * STATISTICS_BYTES_PER_BOUND,
        "footer statistics must be bounded per row group instead of retaining each {LARGE_VALUE_BYTES}-byte value"
    );
}

#[tokio::test]
async fn reload_stream_rejects_empty_finish_without_publishing_an_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let writer = stager
        .begin_reload_stream(
            reload_schema(),
            "public",
            "empty",
            "0/10".parse().unwrap(),
            SchemaVersionNo(1),
        )
        .unwrap();
    let key = writer.key().clone();

    let error = writer.finish().await.unwrap_err();

    assert!(matches!(error, StagingError::EmptyStream));
    assert!(store.head(&key).await.is_err());
}

#[tokio::test]
async fn reload_stream_abort_leaves_no_visible_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let schema = reload_schema();
    let mut writer = stager
        .begin_reload_stream(
            Arc::clone(&schema),
            "public",
            "cancelled",
            "0/20".parse().unwrap(),
            SchemaVersionNo(1),
        )
        .unwrap();
    let key = writer.key().clone();
    writer
        .write_batch(&reload_batch(schema, vec![1]))
        .await
        .unwrap();

    writer.abort().await.unwrap();

    assert!(store.head(&key).await.is_err());
}

#[tokio::test]
async fn reload_stream_schema_mismatch_aborts_and_closes_the_writer() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let stager = ParquetStager::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let mut writer = stager
        .begin_reload_stream(
            reload_schema(),
            "public",
            "mismatch",
            "0/30".parse().unwrap(),
            SchemaVersionNo(1),
        )
        .unwrap();
    let key = writer.key().clone();
    let wrong = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int32,
        false,
    )]));

    let error = writer
        .write_batch(&reload_batch(Arc::clone(&wrong), vec![1]))
        .await
        .unwrap_err();
    assert!(matches!(error, StagingError::SchemaMismatch));
    let error = writer
        .write_batch(&reload_batch(wrong, vec![2]))
        .await
        .unwrap_err();
    assert!(matches!(error, StagingError::ClosedStream));
    assert!(store.head(&key).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn reload_object_write_waits_for_the_remote_part_before_accepting_more() {
    let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        object_store::memory::InMemory::new(),
        ThrottleConfig {
            wait_put_per_call: Duration::from_secs(10),
            ..ThrottleConfig::default()
        },
    ));
    let key = Path::from("reload-backpressure.parquet");
    let buf_writer =
        BufWriter::with_capacity(Arc::clone(&store), key.clone(), RELOAD_MULTIPART_PART_BYTES)
            .with_max_concurrency(1);
    let upload = AbortableObjectWriter::new(buf_writer);
    let mut byte_sink = upload.clone();

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        byte_sink.write(Bytes::from(vec![0; RELOAD_MULTIPART_PART_BYTES])),
    )
    .await;

    assert!(
        result.is_err(),
        "write must remain pending while its only multipart part is remote"
    );
    upload.abort().await.unwrap();
    assert!(store.head(&key).await.is_err());
}
