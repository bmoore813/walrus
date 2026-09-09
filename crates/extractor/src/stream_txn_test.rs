use super::*;
use crate::batch::SystemClock;
use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use common::{ExtractorMeta, PgColumn, PgRelation, ReplicaIdentity};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pg_to_arrow::oids;
use std::num::NonZeroU64;
use std::time::Duration;

fn nz(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn cache() -> RelationCache {
    let rel = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: oids::INT4,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "note".into(),
                type_oid: oids::TEXT,
                type_modifier: -1,
                is_key: false,
            },
        ],
    };
    let mut c = RelationCache::default();
    c.upsert_from_relation(rel, common::SchemaVersionNo(1))
        .unwrap();
    c
}

fn insert_id(id: i32, sub_xid: u32) -> Message {
    Message::Insert {
        xid: Some(sub_xid),
        relation_oid: 42,
        new: vec![
            TupleValue::Text(id.to_string()),
            TupleValue::Text("n".into()),
        ],
    }
}

fn insert_id_v2(id: i32, sub_xid: u32) -> Message {
    Message::Insert {
        xid: Some(sub_xid),
        relation_oid: 42,
        new: vec![
            TupleValue::Text(id.to_string()),
            TupleValue::Text("n".into()),
            TupleValue::Text("extra".into()),
        ],
    }
}

fn move_id(from: i32, to: i32, sub_xid: u32) -> Message {
    Message::Update {
        xid: Some(sub_xid),
        relation_oid: 42,
        old_kind: Some(crate::pgoutput::OldTupleKind::Key),
        old: Some(vec![TupleValue::Text(from.to_string()), TupleValue::Null]),
        new: vec![
            TupleValue::Text(to.to_string()),
            TupleValue::Text("moved".into()),
        ],
    }
}

fn toast_update(sub_xid: u32) -> Message {
    Message::Update {
        xid: Some(sub_xid),
        relation_oid: 42,
        old_kind: Some(crate::pgoutput::OldTupleKind::Key),
        old: Some(vec![TupleValue::Text("1".into()), TupleValue::Null]),
        new: vec![TupleValue::UnchangedToast, TupleValue::UnchangedToast],
    }
}

fn add_v2(cache: &mut RelationCache) {
    let mut relation = cache
        .get(42, common::SchemaVersionNo(1))
        .unwrap()
        .relation
        .clone();
    relation.columns.push(PgColumn {
        name: "extra".into(),
        type_oid: oids::TEXT,
        type_modifier: -1,
        is_key: false,
    });
    cache
        .upsert_from_relation(relation, common::SchemaVersionNo(2))
        .unwrap();
}

fn add_v3(cache: &mut RelationCache) {
    let mut relation = cache
        .get(42, common::SchemaVersionNo(2))
        .unwrap()
        .relation
        .clone();
    relation.columns.push(PgColumn {
        name: "future".into(),
        type_oid: oids::TEXT,
        type_modifier: -1,
        is_key: false,
    });
    cache
        .upsert_from_relation(relation, common::SchemaVersionNo(3))
        .unwrap();
}

fn demux(ceiling: u64) -> StreamDemux {
    StreamDemux::new(
        BatchTriggers {
            max_rows: nz(100_000),
            max_bytes: NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        common::EpochNo(1),
        "test",
        nz(ceiling),
    )
}

fn mem_stager() -> ParquetStager {
    mem_stager_with_store().1
}

fn mem_stager_with_store() -> (Arc<object_store::memory::InMemory>, ParquetStager) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let store_for_stager = Arc::clone(&store);
    let stager = ParquetStager::new(store_for_stager, "walrus", common::EpochNo(1));
    (store, stager)
}

async fn read_written_batch(
    store: &object_store::memory::InMemory,
    written: &WrittenObject,
) -> RecordBatch {
    let bytes = store
        .get(&written.key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}

fn first_meta(batch: &RecordBatch) -> ExtractorMeta {
    let meta = batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    serde_json::from_str(meta.value(0)).unwrap()
}

#[tokio::test]
async fn spill_resolves_the_owning_txn_without_scanning_buffered_changes() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(105_000);

    for (top, rows) in [(100_u32, 500_u32), (200, 550), (300, 500)] {
        d.on_stream_start(top, true, Lsn::new(u64::from(top)))
            .unwrap();
        for row in 0..rows {
            let values = vec![
                TupleValue::Text((top * 10_000 + row).to_string()),
                TupleValue::Text("n".into()),
            ];
            d.claim_stream((TableId(42), top), top, estimate_change_bytes(&values));
            d.open.get_mut(&top).unwrap().push_change(StreamedChange {
                sub_xid: top,
                oid: TableId(42),
                op: Op::Insert,
                values: values.into_boxed_slice(),
                lsn: Lsn::new(u64::from(row)),
                schema_version: common::SchemaVersionNo(1),
            });
        }
        d.on_stream_stop().unwrap();
    }

    assert_eq!(d.owner_len(), 3);
    d.spill_if_over_ceiling(&cache, &stager).await.unwrap();

    assert_eq!(d.spill_count(), 1);
    assert_eq!(d.owner_len(), 2);
    assert_eq!(d.survivor_count(200), 0);
    for top in [100_u32, 300] {
        assert_eq!(d.survivor_count(top), 500);
        let lsns: Vec<Lsn> = d.open[&top]
            .changes
            .iter()
            .map(|change| change.lsn)
            .collect();
        assert_eq!(lsns, (0_u64..500).map(Lsn::new).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn owner_index_is_emptied_by_stream_commit() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    for (id, sub_xid, lsn) in [(1, 100, Lsn::new(1)), (2, 101, Lsn::new(2))] {
        d.on_change(&cache, &insert_id(id, sub_xid), &stager, lsn)
            .await
            .unwrap();
    }
    assert_eq!(d.owner_len(), 2);
    d.on_stream_stop().unwrap();

    d.on_stream_commit(100, Lsn::new(900), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();

    assert_eq!(d.owner_len(), 0);
}

#[tokio::test]
async fn owner_index_is_emptied_by_a_whole_txn_abort() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    for (id, sub_xid, lsn) in [(1, 100, Lsn::new(1)), (2, 101, Lsn::new(2))] {
        d.on_change(&cache, &insert_id(id, sub_xid), &stager, lsn)
            .await
            .unwrap();
    }
    assert_eq!(d.owner_len(), 2);
    d.on_stream_stop().unwrap();

    d.on_stream_abort(100, 100, &stager).await.unwrap();

    assert_eq!(d.owner_len(), 0);
}

#[tokio::test]
async fn a_subtxn_abort_releases_its_index_and_buffer_immediately() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    for (id, sub_xid, lsn) in [(1, 101, Lsn::new(1)), (2, 102, Lsn::new(2))] {
        d.on_change(&cache, &insert_id(id, sub_xid), &stager, lsn)
            .await
            .unwrap();
    }
    let owner_len = d.owner_len();
    let buffered_len = d.open[&100].changes.len();
    d.on_stream_stop().unwrap();

    d.on_stream_abort(100, 101, &stager).await.unwrap();

    assert_eq!(d.owner_len(), owner_len - 1);
    assert_eq!(d.open[&100].changes.len(), buffered_len - 1);
    assert_eq!(d.survivor_count(100), 1);
    let files = d
        .on_stream_commit(100, Lsn::new(900), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(files.iter().map(|file| file.row_count).sum::<u64>(), 1);
    assert_eq!(d.owner_len(), 0);
}

#[tokio::test]
async fn demux_routes_interleaved_xids_to_their_buffers() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX); // no spill
    d.on_stream_start(100, true, "0/100".parse().unwrap())
        .unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    d.on_change(
        &cache,
        &insert_id(1, 100),
        &stager,
        "0/101".parse().unwrap(),
    )
    .await
    .unwrap();
    d.on_stream_stop().unwrap();
    d.on_stream_start(200, true, "0/200".parse().unwrap())
        .unwrap();
    d.bind_relation(200, 200, 42, common::SchemaVersionNo(1));
    d.on_change(
        &cache,
        &insert_id(2, 200),
        &stager,
        "0/201".parse().unwrap(),
    )
    .await
    .unwrap();
    d.on_change(
        &cache,
        &insert_id(3, 200),
        &stager,
        "0/202".parse().unwrap(),
    )
    .await
    .unwrap();
    d.on_stream_stop().unwrap();
    d.on_stream_start(100, false, "0/300".parse().unwrap())
        .unwrap();
    d.on_change(
        &cache,
        &insert_id(4, 100),
        &stager,
        "0/301".parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(d.survivor_count(100), 2);
    assert_eq!(d.survivor_count(200), 2);
}

#[tokio::test]
async fn streamed_key_change_buffers_old_delete_and_new_update() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));

    d.on_change(&cache, &move_id(1, 2, 101), &stager, Lsn::new(110))
        .await
        .unwrap();

    let changes = &d.open[&100].changes;
    assert_eq!(changes.len(), 2);
    assert_eq!((changes[0].op, changes[1].op), (Op::Delete, Op::Update));
    assert_eq!(
        (&changes[0].values[0], &changes[1].values[0]),
        (&TupleValue::Text("1".into()), &TupleValue::Text("2".into()))
    );
}

#[tokio::test]
async fn streamed_materialize_normalizes_key_toast_and_records_non_key_toast() {
    let (cache, (store, stager)) = (cache(), mem_stager_with_store());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    d.on_change(&cache, &toast_update(101), &stager, Lsn::new(110))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();

    let files = d
        .on_stream_commit(100, Lsn::new(120), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    let batch = read_written_batch(store.as_ref(), &files[0]).await;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1, "key sentinel was replaced from old image");
    assert_eq!(
        first_meta(&batch).unchanged_toast.as_ref(),
        &["note".to_string()]
    );
}

#[tokio::test]
async fn speculative_spill_records_unchanged_toast_metadata() {
    let (cache, (store, stager)) = (cache(), mem_stager_with_store());
    let mut d = demux(1);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    d.on_change(&cache, &toast_update(101), &stager, Lsn::new(110))
        .await
        .unwrap();
    assert_eq!(d.spill_count(), 1, "the one-row ceiling forces a spill");
    d.on_stream_stop().unwrap();

    let files = d
        .on_stream_commit(100, Lsn::new(120), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].kind, FileKind::Spill);
    let batch = read_written_batch(store.as_ref(), &files[0]).await;
    assert_eq!(
        first_meta(&batch).unchanged_toast.as_ref(),
        &["note".to_string()]
    );
}

#[tokio::test]
async fn streamed_truncate_buffers_a_data_free_boundary() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    let truncate = Message::Truncate {
        xid: Some(101),
        cascade: false,
        restart_identity: false,
        relations: vec![42],
    };

    assert!(is_streamed_change(&truncate));
    d.on_change(&cache, &truncate, &stager, Lsn::new(110))
        .await
        .unwrap();

    let changes = &d.open[&100].changes;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].op, Op::Truncate);
    assert_eq!(
        changes[0].values.as_ref(),
        &[TupleValue::Null, TupleValue::Null]
    );
}

#[test]
fn open_floor_is_oldest_open_txn_begin_lsn() {
    let mut d = demux(u64::MAX);
    assert_eq!(d.open_floor(), None);
    d.on_stream_start(100, true, "0/500".parse().unwrap())
        .unwrap();
    d.on_stream_stop().unwrap();
    d.on_stream_start(200, true, "0/900".parse().unwrap())
        .unwrap();
    assert_eq!(d.open_floor(), Some("0/500".parse().unwrap()));
    assert_eq!(d.open_stats().count, 2);
    assert_eq!(d.open_stats().oldest_floor, Some("0/500".parse().unwrap()));
}

#[test]
fn open_stats_exposes_count_floor_and_oldest_age() {
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.on_stream_stop().unwrap();
    d.on_stream_start(200, true, Lsn::new(200)).unwrap();
    d.on_stream_stop().unwrap();
    let now = d.clock.now();
    d.open.get_mut(&100).unwrap().opened_at = now - Duration::from_secs(12);
    d.open.get_mut(&200).unwrap().opened_at = now - Duration::from_secs(3);

    let stats = d.open_stats();
    assert_eq!(stats.count, 2);
    assert_eq!(stats.oldest_floor, Some(Lsn::new(100)));
    assert!(stats.oldest_age.unwrap() >= Duration::from_secs(12));
}

#[test]
fn stream_start_enforces_exact_first_segment_semantics_without_mutating_on_error() {
    let mut d = demux(u64::MAX);

    assert_eq!(
        d.on_stream_start(100, false, Lsn::new(10)),
        Err(StreamProtocolError::UnknownContinuation { top_xid: 100 })
    );
    assert_eq!(d.current_top(), None);
    assert_eq!(d.open_floor(), None);

    d.on_stream_start(100, true, Lsn::new(10)).unwrap();
    assert_eq!(
        d.on_stream_start(200, true, Lsn::new(20)),
        Err(StreamProtocolError::SegmentAlreadyActive {
            active_top: 100,
            incoming_top: 200,
        })
    );
    assert_eq!(d.current_top(), Some(100));
    assert_eq!(d.open_floor(), Some(Lsn::new(10)));

    d.on_stream_stop().unwrap();
    assert_eq!(
        d.on_stream_start(100, true, Lsn::new(30)),
        Err(StreamProtocolError::DuplicateFirstSegment { top_xid: 100 })
    );
    assert_eq!(d.current_top(), None);
    assert_eq!(d.open_floor(), Some(Lsn::new(10)));

    d.on_stream_start(100, false, Lsn::new(40)).unwrap();
    d.on_stream_stop().unwrap();
    assert_eq!(
        d.on_stream_stop(),
        Err(StreamProtocolError::StopWithoutStart)
    );
}

#[tokio::test]
async fn stream_outcomes_require_a_stopped_known_top_xid() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);

    let unknown_commit = d
        .on_stream_commit(100, Lsn::new(90), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap_err();
    assert_eq!(
        unknown_commit.downcast_ref::<StreamProtocolError>(),
        Some(&StreamProtocolError::UnknownCommit { top_xid: 100 })
    );
    assert_eq!(
        d.on_stream_abort(100, 100, &stager).await,
        Err(StreamProtocolError::UnknownAbort {
            top_xid: 100,
            sub_xid: 100,
        })
    );

    d.on_stream_start(100, true, Lsn::new(10)).unwrap();
    assert_eq!(
        d.validate_stream_commit(100),
        Err(StreamProtocolError::OutcomeDuringSegment {
            outcome: "StreamCommit",
            outcome_top: 100,
            active_top: 100,
        })
    );
    assert_eq!(
        d.on_stream_abort(100, 100, &stager).await,
        Err(StreamProtocolError::OutcomeDuringSegment {
            outcome: "StreamAbort",
            outcome_top: 100,
            active_top: 100,
        })
    );
    assert_eq!(d.current_top(), Some(100));
    assert_eq!(d.open_floor(), Some(Lsn::new(10)));

    d.on_stream_stop().unwrap();
    let files = d
        .on_stream_commit(100, Lsn::new(100), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert!(files.is_empty());
    assert_eq!(d.open_floor(), None);
}

#[tokio::test]
async fn streamed_change_without_relation_binding_never_uses_a_future_hydrated_version() {
    let (mut cache, stager) = (cache(), mem_stager());
    add_v2(&mut cache);
    add_v3(&mut cache);
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();

    let error = d
        .on_change(&cache, &insert_id_v2(1, 100), &stager, Lsn::new(110))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("streamed change arrived before its Relation binding: oid=42 top_xid=100"),
        "missing protocol state must fail rather than bind replayed v2 data to hydrated v3: {error:#}"
    );
    assert_eq!(d.survivor_count(100), 0);
}

#[tokio::test]
async fn stream_commit_materialises_survivors_stamped_with_commit_lsn() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, "0/100".parse().unwrap())
        .unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    d.on_change(
        &cache,
        &insert_id(1, 100),
        &stager,
        "0/101".parse().unwrap(),
    )
    .await
    .unwrap();
    d.on_change(
        &cache,
        &insert_id(2, 100),
        &stager,
        "0/102".parse().unwrap(),
    )
    .await
    .unwrap();
    d.on_stream_stop().unwrap();
    let commit: Lsn = "0/900".parse().unwrap();
    let files = d
        .on_stream_commit(100, commit, UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(files.iter().map(|f| f.row_count).sum::<u64>(), 2);
    assert!(files.iter().all(|f| f.lsn_end == commit));
    assert_eq!(d.open_floor(), None);
}

#[tokio::test]
async fn txn_open_before_f_commits_as_one_post_h_manifest_group() {
    let (cache, stager) = (cache(), mem_stager());
    let begin = Lsn::new(50);
    let f = Lsn::new(100);
    let h = Lsn::new(200);
    let commit = Lsn::new(300);
    let mut d = demux(1);

    d.on_stream_start(857, true, begin).unwrap();
    d.bind_relation(857, 857, 42, common::SchemaVersionNo(1));
    for id in 1_i32..=8 {
        d.on_change(
            &cache,
            &insert_id(id, 857),
            &stager,
            Lsn::new(59 + u64::try_from(id).unwrap()),
        )
        .await
        .unwrap();
    }
    d.on_stream_stop().unwrap();
    assert!(
        d.open_floor().unwrap() < f,
        "the adversarial transaction genuinely predates the export's F"
    );
    assert_eq!(d.open_stats().count, 1);

    // F and H may both pass on the source while protocol-v2 retains this uncommitted transaction.
    // Its rows become visible only at StreamCommit, stamped as one commit group strictly after H.
    assert!(h > f);
    let files = d
        .on_stream_commit(857, commit, UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert!(files.len() > 1, "the pre-F transaction genuinely spilled");
    assert_eq!(files.iter().map(|file| file.row_count).sum::<u64>(), 8);
    assert!(files.iter().all(|file| file.kind == FileKind::Spill));
    assert!(files.iter().all(|file| file.lsn_end == commit));
    assert!(
        files.iter().all(|file| file.lsn_end > h),
        "the pre-F transaction cannot be folded into or retired by the [F,H] publication"
    );
    assert_eq!(d.open_stats().count, 0);
}

#[tokio::test]
async fn txn_open_during_export_commits_as_one_overlay_group_before_h() {
    let (cache, stager) = (cache(), mem_stager());
    let f = Lsn::new(100);
    let begin = Lsn::new(120);
    let commit = Lsn::new(180);
    let h = Lsn::new(200);
    let mut d = demux(1);

    d.on_stream_start(858, true, begin).unwrap();
    d.bind_relation(858, 858, 42, common::SchemaVersionNo(1));
    for id in 1_i32..=8 {
        let change_lsn = Lsn::new(120 + u64::try_from(id).unwrap());
        d.on_change(&cache, &insert_id(id, 858), &stager, change_lsn)
            .await
            .unwrap();
    }
    d.on_stream_stop().unwrap();
    assert!(begin > f && commit <= h);

    let files = d
        .on_stream_commit(858, commit, UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert!(
        files.len() > 1,
        "the tiny segment budget exercises proto-v2 spill grouping"
    );
    assert_eq!(files.iter().map(|file| file.row_count).sum::<u64>(), 8);
    assert!(files.iter().all(|file| file.lsn_end == commit));
    assert!(
        files
            .iter()
            .all(|file| file.lsn_end > f && file.lsn_end <= h),
        "the whole committed transaction belongs to the reload's (F,H] overlay"
    );
}

#[tokio::test]
async fn stream_commit_separates_rows_bound_to_pre_and_post_ddl_versions() {
    let (mut cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    let top = 100;
    d.on_stream_start(top, true, Lsn::new(100)).unwrap();
    d.bind_relation(top, top, 42, common::SchemaVersionNo(1));
    d.on_change(&cache, &insert_id(1, top), &stager, Lsn::new(101))
        .await
        .unwrap();

    add_v2(&mut cache);
    d.bind_relation(top, top, 42, common::SchemaVersionNo(2));
    d.on_change(&cache, &insert_id_v2(2, top), &stager, Lsn::new(102))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();

    let commit_lsn = Lsn::new(900);
    let files = d
        .on_stream_commit(top, commit_lsn, UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();

    assert_eq!(
        files
            .iter()
            .map(|file| (file.schema_version, file.row_count, file.lsn_end))
            .collect::<Vec<_>>(),
        vec![
            (common::SchemaVersionNo(1), 1, commit_lsn),
            (common::SchemaVersionNo(2), 1, commit_lsn),
        ]
    );
}

#[tokio::test]
async fn lost_ack_replay_routes_streamed_ddl_dml_at_v2_even_after_v3_was_hydrated() {
    let (mut cache, stager) = (cache(), mem_stager());
    add_v2(&mut cache);
    let relation_v2 = cache
        .get(42, common::SchemaVersionNo(2))
        .unwrap()
        .relation
        .clone();
    add_v3(&mut cache); // Later transaction B was durable before the crash.

    let epoch = common::EpochNo(1);
    let commit_lsn = Lsn::new(900);
    let ddl_event = crate::ddl::DdlEvent {
        source_audit_id: 55,
        capture_lsn: Lsn::new(850),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_rel_oid: Some(42),
        c_replica_identity: Some(ReplicaIdentity::Default),
        c_columns: Some(serde_json::json!([
            {"name":"id", "type_oid":23, "type_modifier":-1, "is_key":true},
            {"name":"note", "type_oid":25, "type_modifier":-1, "is_key":false},
            {"name":"extra", "type_oid":25, "type_modifier":-1, "is_key":false}
        ])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE public.orders ADD COLUMN extra text".into()),
        c_table_comment: None,
    };
    let durable_v2 = control::DdlRow {
        id: common::DdlId(1),
        epoch,
        source_audit_id: ddl_event.source_audit_id,
        source_schema: ddl_event.source_schema.clone(),
        source_table: ddl_event.source_table.clone(),
        c_lsn: commit_lsn,
        c_event: ddl_event.c_event.clone(),
        c_tag: ddl_event.c_tag.clone(),
        schema_version: common::SchemaVersionNo(2),
        c_rel_oid: ddl_event.c_rel_oid,
        c_columns: ddl_event.c_columns.clone(),
        c_dropped: ddl_event.c_dropped.clone(),
        c_ddl_text: ddl_event.c_ddl_text.clone(),
        c_table_comment: ddl_event.c_table_comment.clone(),
    };
    let durable_v3 = control::DdlRow {
        id: common::DdlId(2),
        source_audit_id: 56,
        c_lsn: Lsn::new(1_000),
        schema_version: common::SchemaVersionNo(3),
        ..durable_v2.clone()
    };
    let mut ddl = crate::ddl::DdlConsumer::new(epoch);
    ddl.hydrate_versions(&cache);
    ddl.hydrate_history(vec![durable_v2.clone(), durable_v3]);

    // Crash replay restarts before A. Its processed audit id must still become scoped pending
    // state, overriding hydrated committed max v3 for A's Relation and DML.
    let (top_xid, sub_xid) = (857, 858);
    let scope = crate::ddl::TransactionScope::Streamed { top_xid, sub_xid };
    let observation = ddl.observe(scope, ddl_event, Some(&relation_v2));
    assert!(observation.replay);
    let version = ddl
        .relation_version_for(scope, &relation_v2, Lsn::new(875), &cache)
        .unwrap();
    assert_eq!(version, common::SchemaVersionNo(2));
    let cached_v2 = cache.get(42, version).unwrap();
    ddl.stage_registry(
        scope,
        control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            schema_version: version,
            descriptors: cached_v2.descriptors.clone(),
            columns: serde_json::to_value(&cached_v2.relation).unwrap(),
        },
    );

    let mut d = demux(u64::MAX);
    d.on_stream_start(top_xid, true, Lsn::new(800)).unwrap();
    d.bind_relation(top_xid, sub_xid, 42, version);
    d.on_change(&cache, &insert_id_v2(1, sub_xid), &stager, Lsn::new(875))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();

    let prepared = ddl.prepare_stream_commit(top_xid, commit_lsn).unwrap();
    assert_eq!(prepared.ddl_rows().len(), 1);
    assert_eq!(prepared.ddl_rows()[0].source_audit_id, 55);
    assert_eq!(
        prepared.ddl_rows()[0].schema_version,
        common::SchemaVersionNo(2)
    );
    assert_eq!(prepared.registry_rows().len(), 1);
    assert_eq!(
        prepared.registry_rows()[0].schema_version,
        common::SchemaVersionNo(2)
    );
    let files = d
        .on_stream_commit(top_xid, commit_lsn, UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].schema_version, common::SchemaVersionNo(2));

    // AlreadyPublished finalization is idempotent and must not lower B's durable v3 state.
    ddl.finalize_stream_commit(prepared);
    assert_eq!(
        ddl.committed_version_of("public", "orders"),
        common::SchemaVersionNo(3)
    );
}

#[tokio::test]
async fn subabort_restores_parent_relation_binding_for_a_later_segment_without_relation() {
    let (mut cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    let (top, savepoint) = (100, 101);

    d.on_stream_start(top, true, Lsn::new(100)).unwrap();
    d.bind_relation(top, top, 42, common::SchemaVersionNo(1));
    d.on_change(&cache, &insert_id(1, top), &stager, Lsn::new(101))
        .await
        .unwrap();

    // The savepoint changes the relation and writes at v2, then rolls back. Its binding must be
    // removed while the parent's earlier v1 binding survives.
    add_v2(&mut cache);
    d.bind_relation(top, savepoint, 42, common::SchemaVersionNo(2));
    d.on_change(&cache, &insert_id_v2(2, savepoint), &stager, Lsn::new(102))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();
    d.on_stream_abort(top, savepoint, &stager).await.unwrap();
    assert_eq!(
        d.bindings
            .get(&(top, TableId(42)))
            .and_then(|history| history.last()),
        Some(&RelationBinding {
            sub_xid: top,
            version: common::SchemaVersionNo(1),
        })
    );
    // This is the cache cleanup returned by DdlConsumer::on_stream_abort.
    cache.remove_version("public", "orders", common::SchemaVersionNo(2));

    // A valid continuation need not repeat Relation. The surviving parent binding must therefore
    // decode this row at v1 rather than depending on a shared-cache latest fallback.
    d.on_stream_start(top, false, Lsn::new(200)).unwrap();
    d.on_change(&cache, &insert_id(3, top), &stager, Lsn::new(201))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();
    let files = d
        .on_stream_commit(top, Lsn::new(900), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].schema_version, common::SchemaVersionNo(1));
    assert_eq!(files[0].row_count, 2);
}

#[tokio::test]
async fn speculative_spill_partitions_one_subtransaction_by_schema_version() {
    let (mut cache, stager) = (cache(), mem_stager());
    // The v1 row stays below this ceiling; adding the v2 row crosses it, so one spill candidate
    // contains both tuple widths and must be partitioned before Arrow conversion.
    let mut d = demux(150);
    let top = 100;
    d.on_stream_start(top, true, Lsn::new(100)).unwrap();
    d.bind_relation(top, top, 42, common::SchemaVersionNo(1));
    d.on_change(&cache, &insert_id(1, top), &stager, Lsn::new(101))
        .await
        .unwrap();

    add_v2(&mut cache);
    d.bind_relation(top, top, 42, common::SchemaVersionNo(2));
    d.on_change(&cache, &insert_id_v2(2, top), &stager, Lsn::new(102))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();

    let files = d
        .on_stream_commit(top, Lsn::new(900), UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(files.iter().map(|file| file.row_count).sum::<u64>(), 2);
    assert_eq!(
        files
            .iter()
            .map(|file| file.schema_version)
            .collect::<Vec<_>>(),
        vec![common::SchemaVersionNo(1), common::SchemaVersionNo(2)]
    );
    assert!(files.iter().all(|file| file.kind == FileKind::Spill));
}

#[tokio::test]
async fn stream_commit_fails_instead_of_dropping_a_row_if_its_version_was_evicted() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100)).unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    d.on_change(&cache, &insert_id(1, 100), &stager, Lsn::new(101))
        .await
        .unwrap();
    d.on_stream_stop().unwrap();

    let empty_cache = RelationCache::default();
    let error = d
        .on_stream_commit(
            100,
            Lsn::new(900),
            UtcTimestamp::now(),
            &empty_cache,
            &stager,
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stream commit relation version is not cached: oid=42 version=1")
    );
}

#[tokio::test]
async fn commit_materialises_exactly_what_survivors_reports() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    let top_xid = 857;
    d.on_stream_start(top_xid, true, "0/100".parse().unwrap())
        .unwrap();
    d.bind_relation(top_xid, top_xid, 42, common::SchemaVersionNo(1));
    for (id, sub_xid, lsn) in [
        (1, top_xid, "0/101"),
        (2, 858, "0/102"),
        (3, top_xid, "0/103"),
    ] {
        d.on_change(
            &cache,
            &insert_id(id, sub_xid),
            &stager,
            lsn.parse().unwrap(),
        )
        .await
        .unwrap();
    }
    d.on_stream_stop().unwrap();
    d.on_stream_abort(top_xid, 858, &stager).await.unwrap();
    let expected = d.survivor_count(top_xid);

    let files = d
        .on_stream_commit(
            top_xid,
            "0/900".parse().unwrap(),
            UtcTimestamp::now(),
            &cache,
            &stager,
        )
        .await
        .unwrap();

    assert_eq!(
        files.iter().map(|f| f.row_count).sum::<u64>(),
        u64::try_from(expected).unwrap()
    );
}

#[test]
fn survivors_borrows_only_the_aborted_set() {
    assert!(include_str!("stream_txn.rs").contains("let aborted = &self.aborted;"));

    let mut txn = StreamedTxn::new("0/100".parse().unwrap(), std::time::Instant::now());
    txn.push_change(StreamedChange {
        sub_xid: 857,
        oid: TableId(42),
        op: Op::Insert,
        values: Box::default(),
        lsn: "0/101".parse().unwrap(),
        schema_version: common::SchemaVersionNo(1),
    });
    let survivors = txn.iter_survivors();
    let begin_lsn = txn.begin_lsn;
    let staged_len = txn.staged.len();

    assert_eq!(begin_lsn, "0/100".parse().unwrap());
    assert_eq!(staged_len, 0);
    assert_eq!(survivors.count(), 1);
}

#[tokio::test]
async fn whole_txn_stream_abort_drops_the_buffer() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, "0/100".parse().unwrap())
        .unwrap();
    d.bind_relation(100, 100, 42, common::SchemaVersionNo(1));
    d.on_change(
        &cache,
        &insert_id(1, 100),
        &stager,
        "0/101".parse().unwrap(),
    )
    .await
    .unwrap();
    d.on_stream_stop().unwrap();
    d.on_stream_abort(100, 100, &stager).await.unwrap(); // sub == top
    assert_eq!(d.open_floor(), None);
}

/// proto §9b: 3000 kept-A + rolled-back savepoint + 3000 kept-B → exactly 6000 survivors.
#[tokio::test]
async fn subtxn_abort_excludes_only_the_aborted_subxid() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(u64::MAX); // no spill: pure in-memory exclusion
    let begin: Lsn = "0/1000".parse().unwrap();
    d.on_stream_start(857, true, begin).unwrap();
    d.bind_relation(857, 857, 42, common::SchemaVersionNo(1));
    for i in 0..3000 {
        d.on_change(&cache, &insert_id(10_000 + i, 857), &stager, begin)
            .await
            .unwrap();
    }
    for i in 0..2762 {
        d.on_change(&cache, &insert_id(20_000 + i, 858), &stager, begin)
            .await
            .unwrap();
    }
    d.on_stream_stop().unwrap();
    d.on_stream_abort(857, 858, &stager).await.unwrap(); // sub != top
    d.on_stream_start(857, false, begin).unwrap();
    for i in 0..3000 {
        d.on_change(&cache, &insert_id(30_000 + i, 859), &stager, begin)
            .await
            .unwrap();
    }
    assert_eq!(d.survivor_count(857), 6000);
    d.on_stream_stop().unwrap();
    let files = d
        .on_stream_commit(
            857,
            "0/9000".parse().unwrap(),
            UtcTimestamp::now(),
            &cache,
            &stager,
        )
        .await
        .unwrap();
    assert_eq!(
        files.iter().map(|f| f.row_count).sum::<u64>(),
        6000,
        "exactly 6000 — never the rolled-back rows"
    );
}

/// A LOW ceiling forces speculative spills; the aborted sub-xid's spilled file is still excluded.
#[tokio::test]
async fn low_ceiling_spills_yet_still_excludes_the_aborted_subxid() {
    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(500); // tiny ceiling → spill early and often
    let begin: Lsn = "0/1000".parse().unwrap();
    d.on_stream_start(857, true, begin).unwrap();
    d.bind_relation(857, 857, 42, common::SchemaVersionNo(1));
    for i in 0..200 {
        d.on_change(&cache, &insert_id(10_000 + i, 857), &stager, begin)
            .await
            .unwrap(); // kept
    }
    for i in 0..200 {
        d.on_change(&cache, &insert_id(20_000 + i, 858), &stager, begin)
            .await
            .unwrap(); // rolled back
    }
    assert!(
        d.spill_count() > 0,
        "the low ceiling forced speculative spills"
    );
    d.on_stream_stop().unwrap();
    d.on_stream_abort(857, 858, &stager).await.unwrap();
    d.on_stream_start(857, false, begin).unwrap();
    for i in 0..200 {
        d.on_change(&cache, &insert_id(30_000 + i, 859), &stager, begin)
            .await
            .unwrap(); // kept
    }
    d.on_stream_stop().unwrap();
    let commit: Lsn = "0/9000".parse().unwrap();
    let files = d
        .on_stream_commit(857, commit, UtcTimestamp::now(), &cache, &stager)
        .await
        .unwrap();
    assert_eq!(
        files.iter().map(|f| f.row_count).sum::<u64>(),
        400,
        "even with spilling, the aborted sub-xid (200 rows) is excluded → 400 survivors"
    );
    // Promoted spills are tagged `Spill` (their per-row commit_lsn is a placeholder, so the
    // transformer must stamp `lsn_end`), and EVERY returned file — spill or survivor — carries the real
    // commit LSN as `lsn_end`.
    assert!(
        files.iter().any(|f| f.kind == FileKind::Spill),
        "at least one promoted spill is tagged FileKind::Spill"
    );
    assert!(
        files.iter().all(|f| f.lsn_end == commit),
        "every file carries the real commit LSN in lsn_end"
    );
}

#[tokio::test]
async fn spill_preserves_commit_order_of_the_surviving_rows() {
    // The spill drain still moves rows instead of cloning them, and now leaves the
    // survivors in the buffer's own allocation rather than rebuilding one per shed.
    let source = include_str!("stream_txn.rs");
    assert!(source.contains(".extract_if(.., |c| c.oid == oid"));
    assert!(!source.contains("std::mem::take(&mut self.changes)"));

    let (cache, stager) = (cache(), mem_stager());
    let mut d = demux(250);
    let top = 857;
    d.on_stream_start(top, true, "0/100".parse().unwrap())
        .unwrap();

    for (id, sub_xid, lsn) in [
        (1, 858, "0/101"),
        (2, 857, "0/102"),
        (3, 859, "0/103"),
        (4, 857, "0/104"),
        (5, 857, "0/105"),
    ] {
        let values = vec![
            TupleValue::Text(id.to_string()),
            TupleValue::Text("n".into()),
        ];
        d.claim_stream((TableId(42), sub_xid), top, estimate_change_bytes(&values));
        d.open.get_mut(&top).unwrap().push_change(StreamedChange {
            sub_xid,
            oid: TableId(42),
            op: Op::Insert,
            values: values.into_boxed_slice(),
            lsn: lsn.parse().unwrap(),
            schema_version: common::SchemaVersionNo(1),
        });
    }

    d.spill_if_over_ceiling(&cache, &stager).await.unwrap();

    let surviving_lsns: Vec<Lsn> = d.open[&top].changes.iter().map(|c| c.lsn).collect();
    assert_eq!(
        surviving_lsns,
        vec!["0/101".parse().unwrap(), "0/103".parse().unwrap()],
        "draining the spill candidate must preserve survivor commit order"
    );
}

/// The shed loop drains one `(table, sub-xid)` stream out of a transaction that keeps buffering, so
/// the survivors must hold both their order and the allocation the next `push_change` refills.
#[test]
fn take_stream_drains_in_place_and_keeps_both_relative_orders() {
    let lsn = |raw: &str| raw.parse::<Lsn>().unwrap();
    let mut txn = StreamedTxn::new(lsn("0/100"), std::time::Instant::now());
    txn.changes.reserve(64);
    for (sub_xid, at) in [
        (857, "0/101"),
        (858, "0/102"),
        (857, "0/103"),
        (859, "0/104"),
        (857, "0/105"),
    ] {
        txn.push_change(StreamedChange {
            sub_xid,
            oid: TableId(42),
            op: Op::Insert,
            values: Box::default(),
            lsn: lsn(at),
            schema_version: common::SchemaVersionNo(1),
        });
    }
    let capacity = txn.changes.capacity();

    let rows = txn.take_stream(TableId(42), 857);

    let drained: Vec<Lsn> = rows.iter().map(|c| c.lsn).collect();
    let survivors: Vec<Lsn> = txn.changes.iter().map(|c| c.lsn).collect();
    assert_eq!(drained, [lsn("0/101"), lsn("0/103"), lsn("0/105")]);
    assert_eq!(survivors, [lsn("0/102"), lsn("0/104")]);
    assert_eq!(
        txn.changes.capacity(),
        capacity,
        "the survivors keep the allocation the next push_change refills"
    );
    assert!(!txn.keys.contains(&(TableId(42), 857)));
}

// Regression note: the existing HashSet/BTreeSet membership indexes in transformer/ddl.rs,
// extractor/preflight.rs, and extractor/reload_export.rs stay sets. XID_PREFIXED stays a 7-byte slice
// scan, and reload_signal/heartbeat/ddl column lookups stay Vec::position because they need indices.
