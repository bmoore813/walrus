use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};
use std::path::Path;

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

fn open_fresh(dir: &Path) -> TableDb {
    let db = TableDb::open(dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db
}

#[test]
fn rebuild_wipes_a_stale_generation_and_is_idempotent() {
    // The guard removes the directory on drop — including on an assertion panic below.
    let dir = tempfile::tempdir().unwrap();
    let db = open_fresh(dir.path());

    // A file built for epoch 1 with a row in raw + mirror.
    db.set_built_epoch(common::EpochNo(1)).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO orders VALUES (1, 'x', '0', '0'); \
                 INSERT INTO orders_raw (id, status, walrus_extractor_meta, _walrus_op, \
                     _walrus_commit_lsn, _walrus_lsn, _walrus_extractor_processed_at) \
                 VALUES (1, 'x', '{}', 'Insert', '0', '0', 't');",
        )
        .unwrap();

    // Control epoch bumped to 2 → the file is stale → rebuild wipes it (raw + mirror gone).
    assert!(rebuild_for_new_epoch(&db, "orders", common::EpochNo(2)).unwrap());
    // Recreate empty (as bootstrap does) and confirm the stale rows are gone.
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.set_built_epoch(common::EpochNo(2)).unwrap();
    let mirror: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    let raw: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
        .unwrap();
    assert_eq!((mirror, raw), (0, 0), "the retired generation was wiped");

    // Idempotent: already at epoch 2 → no rebuild.
    assert!(!rebuild_for_new_epoch(&db, "orders", common::EpochNo(2)).unwrap());
}

#[test]
fn fresh_file_is_not_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_fresh(dir.path());
    // Never stamped (built_epoch = None) → a fresh bootstrap, never a rebuild.
    assert!(!rebuild_for_new_epoch(&db, "orders", common::EpochNo(1)).unwrap());
    assert!(!rebuild_for_new_epoch(&db, "orders", common::EpochNo(5)).unwrap());
}

#[test]
fn advance_only_publishes_a_forward_move() {
    let (tx, rx) = tokio::sync::watch::channel(common::EpochNo(3));

    assert!(!advance(&tx, Some(common::EpochNo(3))));
    assert_eq!(*rx.borrow(), common::EpochNo(3));

    assert!(advance(&tx, Some(common::EpochNo(5))));
    assert_eq!(*rx.borrow(), common::EpochNo(5));

    assert!(!advance(&tx, None));
    assert_eq!(*rx.borrow(), common::EpochNo(5));
}

#[test]
fn a_pinned_receiver_still_borrows_after_its_sender_is_gone() {
    let rx = fixed_epoch_watch(common::EpochNo(11));

    assert_eq!(*rx.borrow(), common::EpochNo(11));
}

#[test]
fn same_epoch_total_restart_retires_a_running_transformer() {
    let epoch = common::EpochNo(11);
    assert!(generation_is_retired(
        epoch,
        control::ReplicationStatus::TotalRestart,
        epoch
    ));
    assert!(!generation_is_retired(
        epoch,
        control::ReplicationStatus::Streaming,
        epoch
    ));
    assert!(!generation_is_retired(
        common::EpochNo(10),
        control::ReplicationStatus::TotalRestart,
        epoch
    ));
}
