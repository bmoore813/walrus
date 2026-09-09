#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Full-rebuild / compaction (transformer §5.7, §9.4). Three hermetic tests prove the rebuild matches the
//! incremental mirror, preserves a pruned value via the mirror baseline, and drops `op='d'` winners; the
//! `#[ignore]` test proves the `CREATE OR REPLACE` rebuild reclaims space a `DELETE` would not.
//!
//!   cargo test -p transformer --test compaction              # hermetic
//!   cargo test -p transformer --test compaction -- --ignored # + reclamation

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use tokio_util::sync::CancellationToken;
use transformer::compaction::{full_rebuild, prune_raw, retention_floor};
use transformer::duck::TableDb;
use transformer::transform::{TransformSql, apply_transform};

fn orders_rel() -> PgRelation {
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

fn lsn(n: u64) -> String {
    format!("{n:016X}")
}

fn mem(rel: &PgRelation) -> TableDb {
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables(rel, common::SchemaVersionNo(1)).unwrap();
    db
}

fn seed_raw(conn: &duckdb::Connection, id: i64, status: &str, op: char, clsn: u64, l: u64) {
    conn.execute(
        "INSERT INTO orders_raw (id, status, walrus_extractor_meta, _walrus_op, \
         _walrus_commit_lsn, _walrus_lsn, _walrus_extractor_processed_at) \
         VALUES (?, ?, '{}', ?, ?, ?, 'x')",
        duckdb::params![id, status, op.to_string(), lsn(clsn), lsn(l)],
    )
    .unwrap();
}

fn dump(conn: &duckdb::Connection) -> Vec<(i64, String)> {
    let mut stmt = conn
        .prepare("SELECT id, status FROM orders ORDER BY id")
        .unwrap();
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    rows.map(Result::unwrap).collect()
}

fn status_of(conn: &duckdb::Connection, id: i64) -> Option<String> {
    conn.query_row("SELECT status FROM orders WHERE id = ?", [id], |r| r.get(0))
        .ok()
}

fn raw_count(conn: &duckdb::Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
        .unwrap()
}

fn seed_history(conn: &duckdb::Connection) {
    // key 1: i→u (ends 'b'); key 2: i→d (deleted); key 3: lone i ('c').
    seed_raw(conn, 1, "a", 'i', 100, 1);
    seed_raw(conn, 1, "b", 'u', 101, 2);
    seed_raw(conn, 2, "x", 'i', 100, 3);
    seed_raw(conn, 2, "x", 'd', 102, 4);
    seed_raw(conn, 3, "c", 'i', 100, 5);
}

// ---- The full-rebuild produces a mirror IDENTICAL to the incremental transform over the same history.
#[test]
fn full_rebuild_matches_incremental_mirror() {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();

    let inc = mem(&rel);
    seed_history(inc.conn());
    apply_transform(inc.conn(), &t, Lsn::ZERO).unwrap();

    let reb = mem(&rel);
    seed_history(reb.conn());
    full_rebuild(&reb, &t, &CancellationToken::new()).unwrap();

    assert_eq!(
        dump(inc.conn()),
        dump(reb.conn()),
        "rebuild == incremental (same winners, op='d' dropped)"
    );
    assert_eq!(
        dump(reb.conn()),
        vec![(1, "b".to_string()), (3, "c".to_string())],
        "key 1 ends at its update, key 2 (i→d) is absent, key 3 kept"
    );
}

// ---- A raw value pruned below the floor survives via the current-mirror LSN-floor baseline.
#[test]
fn pruned_value_survives_via_mirror_baseline() {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let db = mem(&rel);

    // Apply a change incrementally so the mirror holds it (with its `_applied_*` tuple).
    seed_raw(db.conn(), 1, "kept", 'i', 100, 1);
    apply_transform(db.conn(), &t, Lsn::ZERO).unwrap();
    assert_eq!(status_of(db.conn(), 1).as_deref(), Some("kept"));

    // Prune ALL of raw (floor above the row) — its only raw evidence is gone.
    let pruned = prune_raw(db.conn(), &t, "0/C8".parse().unwrap()).unwrap();
    assert_eq!(pruned, 1);
    assert_eq!(raw_count(db.conn()), 0, "raw evidence pruned");

    // The rebuild unions the current mirror as a baseline → the value is not lost.
    full_rebuild(&db, &t, &CancellationToken::new()).unwrap();
    assert_eq!(
        status_of(db.conn(), 1).as_deref(),
        Some("kept"),
        "the pruned value survives via the mirror baseline"
    );
}

// ---- op='d' winners are dropped (not resurrected) by the rebuild.
#[test]
fn deleted_keys_stay_absent_after_rebuild() {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let db = mem(&rel);
    seed_raw(db.conn(), 1, "a", 'i', 100, 1);
    seed_raw(db.conn(), 1, "a", 'd', 100, 2);
    full_rebuild(&db, &t, &CancellationToken::new()).unwrap();
    assert_eq!(
        dump(db.conn()).len(),
        0,
        "the delete winner is dropped by the rebuild's WHERE op<>'d'"
    );
}

// ---- A drain observed before the heavy rewrite starts is an intentional no-op.
#[test]
fn pre_cancelled_full_rebuild_does_not_start() {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let db = mem(&rel);
    seed_raw(db.conn(), 1, "not-applied", 'i', 100, 1);
    let cancel = CancellationToken::new();
    cancel.cancel();

    full_rebuild(&db, &t, &cancel).unwrap();

    assert_eq!(
        dump(db.conn()),
        Vec::<(i64, String)>::new(),
        "a pre-cancelled rebuild returns Ok without changing the mirror"
    );
}

// ---- The retention floor is always behind transformed_lsn.
#[test]
fn retention_floor_is_behind_transformed_lsn() {
    let transformed: Lsn = "0/1000".parse().unwrap();
    let floor = retention_floor(transformed, 0x400);
    assert_eq!(floor, "0/C00".parse().unwrap());
    // A lag larger than the LSN saturates at 0, never underflows.
    assert_eq!(retention_floor("0/100".parse().unwrap(), 0xFFFF), Lsn::ZERO);
}

// ---- Reclamation: the CREATE OR REPLACE rebuild frees blocks a DELETE would only tombstone. ----

/// A scratch directory for one test's `.duckdb` file. The returned guard deletes it on drop — even
/// when an assertion panics, which a trailing `remove_dir_all` would skip.
fn tmpdir(name: &str) -> tempfile::TempDir {
    let prefix = format!("walrus-transformer-compact-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

/// DuckDB block accounting — `used_blocks` reflects real storage (the OS file may not truncate, but the
/// rebuild frees blocks the tombstoning `DELETE` does not).
fn used_blocks(conn: &duckdb::Connection) -> i64 {
    conn.execute_batch("CHECKPOINT;").unwrap();
    conn.query_row("SELECT used_blocks FROM pragma_database_size()", [], |r| {
        r.get(0)
    })
    .unwrap()
}

#[test]
#[ignore = "requires a real .duckdb file for block accounting"]
fn rebuild_reclaims_space_and_prune_keeps_mirror_correct() {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let dir = tmpdir("reclaim");
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&rel, common::SchemaVersionNo(1)).unwrap();

    // Seed one key incrementally, then BLOAT the mirror with UPDATE churn (each tombstones the prior row
    // version in DuckDB's MVCC). A wide value makes the bloat span many blocks. The value is held constant
    // so it also matches the raw row at the same tuple — the rebuild result is unambiguous.
    let wide = "z".repeat(2000);
    seed_raw(db.conn(), 1, &wide, 'i', 100, 1);
    apply_transform(db.conn(), &t, Lsn::ZERO).unwrap();
    for _ in 0..4000 {
        db.conn()
            .execute("UPDATE orders SET status = ? WHERE id = 1", [&wide])
            .unwrap();
    }
    let bloated = used_blocks(db.conn());

    // A DELETE + CHECKPOINT only tombstones — it does not free the bloat.
    db.conn()
        .execute_batch("DELETE FROM orders WHERE id = 999; CHECKPOINT;")
        .unwrap();
    assert!(
        used_blocks(db.conn()) >= bloated - 1,
        "DELETE/CHECKPOINT does not reclaim the mirror's UPDATE churn"
    );

    // The CREATE OR REPLACE rebuild rewrites the table to one clean row → blocks freed.
    full_rebuild(&db, &t, &CancellationToken::new()).unwrap();
    let rebuilt = used_blocks(db.conn());
    assert!(
        rebuilt < bloated,
        "the rebuild reclaims space: {rebuilt} < {bloated} used blocks"
    );
    assert_eq!(
        status_of(db.conn(), 1).as_deref(),
        Some(wide.as_str()),
        "the rebuild preserves the current value"
    );

    // Prune the raw evidence, rebuild again — the mirror stays correct via the baseline.
    let floor = retention_floor("0/C8".parse().unwrap(), 0);
    prune_raw(db.conn(), &t, floor).unwrap();
    assert_eq!(raw_count(db.conn()), 0, "raw pruned below the floor");
    full_rebuild(&db, &t, &CancellationToken::new()).unwrap();
    assert_eq!(
        status_of(db.conn(), 1).as_deref(),
        Some(wide.as_str()),
        "prune keeps the mirror correct — the baseline preserved the value"
    );
}
