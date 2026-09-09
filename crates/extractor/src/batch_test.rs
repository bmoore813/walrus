use super::*;
use crate::relcache::RelationCache;
use arrow::array::StringArray;
use common::{Kind, Op, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo, UtcTimestamp};
use parking_lot::Mutex;
use pg_to_arrow::oids;

/// A hand-advanced clock for the `max_fill` test.
///
/// [`Clock`] is `Send + Sync` and every owner holds its clock behind an `Arc`, so the mutable
/// offset needs a *lock* — a `Cell`/`RefCell` would make `FakeClock` `!Sync` and stop satisfying
/// the bound. `parking_lot::Mutex` is the workspace's non-poisoning lock, so `lock()` hands back
/// the guard directly instead of a `Result` that invites `unwrap()`.
#[derive(Debug)]
struct FakeClock {
    base: Instant,
    offset: Mutex<Duration>,
}
impl FakeClock {
    fn new() -> Arc<Self> {
        Arc::new(FakeClock {
            base: Instant::now(),
            offset: Mutex::new(Duration::ZERO),
        })
    }
    fn advance(&self, d: Duration) {
        *self.offset.lock() += d;
    }
}
impl super::private::Sealed for FakeClock {}
impl Clock for FakeClock {
    fn now(&self) -> Instant {
        self.base + *self.offset.lock()
    }
}

/// A `C: Clock` bound must accept every shape a walrus clock is actually held in: owned, shared
/// (`Arc`, including the `Arc<FakeClock>` the test fake hands back) and borrowed.
#[test]
fn clock_bound_accepts_owned_shared_and_borrowed_clocks() {
    fn tick<C: Clock>(c: C) -> Instant {
        c.now()
    }

    let baseline = Instant::now();
    let owned = SystemClock;
    let shared = Arc::new(SystemClock);
    let fake = FakeClock::new();

    let a = tick(SystemClock);
    let b = tick(shared);
    let c = tick(fake);
    let d = tick::<&SystemClock>(&owned);

    assert!([a, b, c, d].into_iter().all(|instant| instant >= baseline));
}

/// A harness that wants to run the same assertion over several clocks needs one collection of
/// them — the legitimate `dyn` case from the decision table.
///
/// NOTE: `Box::new(FakeClock::new())` is a `Box<Arc<FakeClock>>`; it coerces to `Box<dyn Clock>`
/// only because of the `impl<T: Clock + ?Sized> Clock for Arc<T>`. If this line ever stops
/// compiling, that impl is what went missing.
#[test]
fn clock_is_dyn_compatible_and_usable_in_a_heterogeneous_collection() {
    let baseline = Instant::now();
    let clocks: Vec<Box<dyn Clock>> = vec![Box::new(SystemClock), Box::new(FakeClock::new())];

    for clock in &clocks {
        assert!(clock.now() >= baseline);
    }
}

/// The gated method is reachable from a concrete receiver (`Self: Sized`) — and deliberately NOT
/// through `dyn Clock`, which is what keeps the trait dyn-compatible.
#[test]
fn gated_deadline_is_reachable_from_a_concrete_clock() {
    let clock = FakeClock::new();
    let after = Duration::from_millis(100);
    let deadline = clock
        .deadline(after)
        .expect("100ms deadline is representable");

    assert_eq!(deadline, clock.now().checked_add(after).unwrap());
    clock.advance(after + Duration::from_millis(1));
    assert!(clock.now() > deadline);
    // `clocks[0].deadline(..)` would NOT compile — the method is excluded from the vtable.
}

fn cached() -> Arc<CachedRelation> {
    let rel = PgRelation {
        oid: 42,
        schema: "public".to_string(),
        name: "widgets".to_string(),
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
    RelationCache::default()
        .upsert_from_relation(rel, SchemaVersionNo(1))
        .unwrap()
}

fn meta(lsn: &str) -> ExtractorMeta {
    ExtractorMeta {
        op: Op::Insert,
        lsn: lsn.parse().unwrap(),
        commit_lsn: Lsn::ZERO, // patched at on_commit
        commit_ts: "2026-07-07T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        xid: 7,
        epoch: common::EpochNo(1),
        batch_id: "b1".into(),
        schema_version: SchemaVersionNo(1),
        source_schema: "public".into(),
        source_table: "widgets".into(),
        kind: Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "walrus-extractor-0".into(),
        extractor_processed_at: "2026-07-07T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
    }
}

fn row(id: &str) -> Vec<TupleValue> {
    vec![TupleValue::Text(id.into()), TupleValue::Text("hi".into())]
}

fn triggers(max_rows: u64, max_bytes: u64, max_fill: Duration) -> BatchTriggers {
    BatchTriggers {
        max_rows: NonZeroU64::new(max_rows).unwrap(),
        max_bytes: NonZeroU64::new(max_bytes).unwrap(),
        max_fill,
    }
}

#[test]
fn flushes_on_row_count_at_commit_boundary() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(2, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    b.push(meta("0/20"), &row("2"));
    assert!(!b.should_flush(), "open txn is never flush-eligible");
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    assert!(b.should_flush(), "2 committed rows hit max_rows=2");
    let sealed = b.seal().unwrap();
    assert_eq!(sealed.row_count, 2);
    assert!(!b.should_flush(), "reset after seal");
}

#[test]
fn delete_insert_delete_stays_in_one_batch_until_commit() {
    let mut b = TableBatcher::new(
        cached(),
        // Every individual row exceeds the threshold. The open transaction must still remain
        // indivisible until its commit boundary.
        triggers(1, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();

    for (op, lsn, id) in [
        (Op::Delete, "0/10", "7"),
        (Op::Insert, "0/20", "7"),
        (Op::Delete, "0/30", "7"),
    ] {
        let mut change = meta(lsn);
        change.op = op;
        b.push(change, &row(id));
        assert!(
            !b.should_flush(),
            "a size trigger cannot split an open transaction"
        );
    }
    assert!(matches!(b.seal(), Err(BatchError::OpenTransaction)));

    let commit_lsn: Lsn = "0/40".parse().unwrap();
    b.on_commit(commit_lsn, UtcTimestamp::now()).unwrap();
    assert!(b.should_flush(), "the threshold fires at commit");

    let sealed = b.seal().unwrap();
    assert_eq!(sealed.row_count, 3);
    assert_eq!(sealed.record_batch.num_rows(), 3);
    assert_eq!(sealed.lsn_start, commit_lsn);
    assert_eq!(sealed.lsn_end, commit_lsn);

    let meta_column = sealed
        .record_batch
        .column(sealed.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let changes = meta_column
        .iter()
        .flatten()
        .map(|encoded| serde_json::from_str::<ExtractorMeta>(encoded).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        changes.iter().map(|change| change.op).collect::<Vec<_>>(),
        [Op::Delete, Op::Insert, Op::Delete]
    );
    assert_eq!(
        changes.iter().map(|change| change.lsn).collect::<Vec<_>>(),
        [
            "0/10".parse().unwrap(),
            "0/20".parse().unwrap(),
            "0/30".parse().unwrap(),
        ]
    );
    assert!(
        changes
            .iter()
            .all(|change| change.commit_lsn == commit_lsn && change.xid == 7)
    );
    let batch_id = &changes[0].batch_id;
    assert!(!batch_id.is_empty());
    assert!(
        changes.iter().all(|change| change.batch_id == *batch_id),
        "all changes from the transaction retain one batch identity"
    );
}

#[test]
fn already_committed_rows_bypass_the_open_transaction_buffer() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(2, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    let commit_lsn: Lsn = "0/30".parse().unwrap();
    let mut first = meta("0/10");
    first.commit_lsn = commit_lsn;
    let mut second = meta("0/20");
    second.commit_lsn = commit_lsn;

    b.push_committed(first, &row("1")).unwrap();
    b.push_committed(second, &row("2")).unwrap();

    assert!(!b.has_open_txn(), "reload rows never enter the pending Vec");
    assert!(b.pending.is_empty());
    assert_eq!(b.committed_rows(), 2);
    assert!(b.should_flush());
    let sealed = b.seal().unwrap();
    assert_eq!(sealed.row_count, 2);
    assert_eq!(sealed.lsn_start, commit_lsn);
    assert_eq!(sealed.lsn_end, commit_lsn);

    let meta_column = sealed
        .record_batch
        .column(sealed.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for encoded in meta_column.iter().flatten() {
        let meta: ExtractorMeta = serde_json::from_str(encoded).unwrap();
        assert_eq!(meta.commit_lsn, commit_lsn);
        assert_eq!(meta.batch_id, "public.widgets-0000000000000010");
    }
}

#[test]
fn committed_rows_keep_byte_identical_batch_id_with_clone_from() {
    assert!(include_str!("batch.rs").contains("meta.batch_id.clone_from(batch_id);"));

    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    let expected = b.batch_id.clone().expect("push assigns the batch id");

    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    let sealed = b.seal().unwrap();
    let meta_column = sealed
        .record_batch
        .column(sealed.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let committed: ExtractorMeta = serde_json::from_str(meta_column.value(0)).unwrap();

    assert_eq!(committed.batch_id.as_bytes(), expected.as_bytes());
}

#[test]
fn push_records_unchanged_toast_column_names_in_row_metadata() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(
        meta("0/10"),
        &[TupleValue::Text("1".into()), TupleValue::UnchangedToast],
    );
    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    let sealed = b.seal().unwrap();
    let meta_column = sealed
        .record_batch
        .column(sealed.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let committed: ExtractorMeta = serde_json::from_str(meta_column.value(0)).unwrap();

    assert_eq!(committed.unchanged_toast.as_ref(), &["note".to_string()]);
}

#[test]
fn optional_batch_state_is_none_before_push_and_after_seal() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();

    assert_eq!(b.batch_id, None, "no id before the first row");
    assert!(
        matches!(b.committed, CommittedRows::None),
        "no committed state before commit"
    );

    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    b.seal().unwrap();

    assert_eq!(b.batch_id, None, "seal resets the assigned id");
    assert!(
        matches!(b.committed, CommittedRows::None),
        "seal resets the committed state"
    );
}

/// The rows and their commit-LSN bounds are ONE value, so the count can never disagree with the
/// bounds `seal` reads: the first promoted row fixes the lower bound and the `max_fill` start, and
/// each later commit only extends the upper bound.
#[test]
fn committed_rows_carry_their_lsn_bounds() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();

    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    b.push(meta("0/30"), &row("2"));
    b.push(meta("0/40"), &row("3"));
    b.on_commit("0/50".parse().unwrap(), UtcTimestamp::now())
        .unwrap();

    let CommittedRows::Rows {
        count,
        first_commit_lsn,
        last_commit_lsn,
        ..
    } = b.committed
    else {
        panic!("three rows committed across two transactions");
    };
    assert_eq!(count.get(), 3);
    assert_eq!(first_commit_lsn, "0/20".parse().unwrap());
    assert_eq!(last_commit_lsn, "0/50".parse().unwrap());
    assert_eq!(
        b.undurable_floor(),
        Some(first_commit_lsn),
        "the durability floor IS the batch's first commit LSN"
    );

    let sealed = b.seal().unwrap();
    assert_eq!(sealed.row_count, 3);
    assert_eq!(sealed.lsn_start, first_commit_lsn);
    assert_eq!(sealed.lsn_end, last_commit_lsn);
    assert_eq!(b.undurable_floor(), None, "seal clears the floor");
}

#[test]
fn sealing_without_an_assigned_batch_id_is_an_error() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();

    b.batch_id = None;
    assert!(matches!(b.seal(), Err(BatchError::Unassigned)));
}

#[test]
fn on_commit_reuses_the_pending_buffer_allocation() {
    let clock = FakeClock::new();
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        clock,
    )
    .unwrap();
    for i in 0..64 {
        b.push(meta("0/1"), &row(&i.to_string()));
    }
    let capacity = b.pending.capacity();
    assert!(
        capacity >= 64,
        "pre-condition: the open transaction buffered 64 rows"
    );

    b.on_commit("0/2".parse().unwrap(), UtcTimestamp::now())
        .unwrap();

    assert!(b.pending.is_empty(), "commit promotes every pending row");
    assert!(
        b.pending.capacity() >= capacity,
        "on_commit must reuse the pending allocation, got capacity {}",
        b.pending.capacity()
    );
}

#[test]
fn flushes_on_byte_size_at_commit_boundary() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, 50, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    // One row (~96 overhead + a few value bytes) exceeds the tiny 50-byte ceiling.
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    assert!(b.should_flush(), "committed bytes exceed max_bytes=50");
}

#[test]
fn flushes_on_max_fill_via_fake_clock() {
    let clock = FakeClock::new();
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_millis(100)),
        Arc::<FakeClock>::clone(&clock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    assert!(!b.should_flush(), "no wall time has elapsed yet");
    clock.advance(Duration::from_millis(150));
    assert!(b.should_flush(), "max_fill tripped via the fake clock");
}

/// The erased counterpart of the test above: a `dyn Clock` must not only be *callable* (the
/// `Box<dyn Clock>` collection), it must still satisfy the `C: Clock` bound every owner is written
/// against — otherwise dyn compatibility buys nothing here. That is what the `?Sized` in
/// `impl<T: Clock + ?Sized> Clock for Arc<T>` covers; without it `Arc<dyn Clock>` no longer
/// satisfies `C: Clock` and the `TableBatcher` below stops compiling.
#[test]
fn an_erased_clock_still_satisfies_the_generic_bound() {
    let fake = FakeClock::new();
    let erased: Arc<dyn Clock> = Arc::<FakeClock>::clone(&fake);
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_millis(100)),
        erased,
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    assert!(!b.should_flush(), "no wall time has elapsed yet");
    fake.advance(Duration::from_millis(150));
    assert!(
        b.should_flush(),
        "max_fill tripped through `Arc<dyn Clock>`"
    );
}

#[test]
fn never_seals_with_an_open_transaction() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(1, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1")); // open txn, no commit
    assert!(matches!(b.seal(), Err(BatchError::OpenTransaction)));
}

#[test]
fn drain_seals_committed_rows_and_drops_the_open_txn() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)), // never auto-flushes
        Arc::new(SystemClock),
    )
    .unwrap();
    // A committed txn (flush-eligible, but under all thresholds) plus an OPEN speculative txn.
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    b.push(meta("0/30"), &row("2")); // open, uncommitted
    assert!(b.has_open_txn());
    let sealed = b
        .drain_for_shutdown()
        .unwrap()
        .expect("committed rows seal on drain");
    assert_eq!(sealed.row_count, 1, "only the committed row is sealed");
    assert_eq!(sealed.lsn_end, "0/20".parse().unwrap());
    assert!(
        !b.has_open_txn(),
        "the open speculative buffer was dropped, not forced out"
    );
    assert_eq!(b.committed_rows(), 0, "batch reset after drain");
}

#[test]
fn drain_with_nothing_committed_is_a_noop() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1")); // open only, never committed
    assert!(
        b.drain_for_shutdown().unwrap().is_none(),
        "no committed rows → nothing to seal"
    );
    assert!(!b.has_open_txn(), "the open buffer is still dropped");
}

#[test]
fn stream_commit_boundary_seals_committed_rows_and_preserves_open_txn() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    b.push(meta("0/30"), &row("2"));

    let sealed = b
        .seal_committed_boundary()
        .unwrap()
        .expect("committed prefix is sealed");
    assert_eq!(sealed.row_count, 1);
    assert_eq!(sealed.lsn_end, "0/20".parse().unwrap());
    assert!(b.has_open_txn(), "ordinary speculative row must survive");

    b.on_commit("0/40".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    let survivor = b
        .force_seal_committed()
        .unwrap()
        .expect("preserved row later commits");
    assert_eq!(survivor.row_count, 1);
    assert_eq!(survivor.lsn_end, "0/40".parse().unwrap());
}

#[test]
fn force_seal_committed_cuts_below_threshold_without_dropping_rows() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));
    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    assert!(
        !b.should_flush(),
        "ordinary triggers deliberately stay idle"
    );

    let sealed = b
        .force_seal_committed()
        .unwrap()
        .expect("a durability fence force-seals committed rows");
    assert_eq!(sealed.row_count, 1);
    assert_eq!(sealed.lsn_start, "0/20".parse().unwrap());
    assert_eq!(sealed.lsn_end, "0/20".parse().unwrap());
    assert_eq!(b.committed_rows(), 0);
    assert!(b.force_seal_committed().unwrap().is_none());
}

#[test]
fn force_seal_rejects_and_preserves_an_open_transaction() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(meta("0/10"), &row("1"));

    assert!(matches!(
        b.force_seal_committed(),
        Err(BatchError::OpenTransaction)
    ));
    assert!(b.has_open_txn(), "the fence never drops speculative rows");

    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    assert_eq!(
        b.force_seal_committed()
            .unwrap()
            .expect("the same row remains available after commit")
            .row_count,
        1
    );
}

#[test]
fn lsn_end_equals_last_commit_lsn_not_max_row_lsn() {
    let mut b = TableBatcher::new(
        cached(),
        triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
        Arc::new(SystemClock),
    )
    .unwrap();
    // Row LSNs are HIGHER than the commit LSN — lsn_end must still be the commit LSN.
    b.push(meta("0/500"), &row("1"));
    b.push(meta("0/600"), &row("2"));
    b.on_commit("0/100".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    let sealed = b.seal().unwrap();
    assert_eq!(
        sealed.lsn_end,
        "0/100".parse().unwrap(),
        "lsn_end is the commit LSN"
    );
    assert_eq!(sealed.lsn_start, "0/100".parse().unwrap());
}
