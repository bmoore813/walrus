#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "bench (harness=false, not test-cfg)"
)]
//! Criterion micro-benches for the transformer's raw→mirror transform.
//!
//! Runs the **production** SQL (`transformer::transform::apply_transform` over `TransformSql`) against an
//! in-memory DuckDB seeded exactly like the transform integration tests — same harness, clock
//! on. Three views: transform scaling over an N×K grid, the unchanged-TOAST back-scan cost isolated
//! as a delta, and mirror-size sensitivity (the MERGE join + PK-index side). No production code is
//! touched; these benches establish the baselines for future measured changes.
//!
//! Seeding uses one `INSERT … SELECT range(N)` per iteration (individual inserts would dwarf the
//! measured transform at 1M rows). `SET threads = 4` is pinned so numbers don't drift with core count.
//!
//! Run: `cargo bench -p transformer --bench transform` (or `just bench`). The 1M grid takes minutes.

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_main};
use duckdb::Connection;
use std::hint::black_box;
use transformer::duck::TableDb;
use transformer::transform::{TransformSql, apply_transform};

fn col(name: &str, oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    }
}

/// `orders(id int4 PK, status text)` — the scaling + mirror-size shape.
fn orders_rel() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

/// `orders(id int4 PK, t1/t2/t3 text)` — the TOAST back-scan shape (3 resolvable columns).
fn toast_rel() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", 23, true),
            col("t1", 25, false),
            col("t2", 25, false),
            col("t3", 25, false),
        ],
    }
}

fn three_key_rel() -> PgRelation {
    PgRelation {
        oid: 43,
        schema: "public".into(),
        name: "line_items".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("tenant_id", 23, true),
            col("order_id", 23, true),
            col("line_no", 23, true),
            col("description", 25, false),
        ],
    }
}

/// A representative wide schema for isolating SQL-render cost without DuckDB execution.
fn wide_rel() -> PgRelation {
    let mut columns = Vec::with_capacity(30);
    columns.push(col("id", 23, true));
    for index in 1..30 {
        columns.push(col(&format!("text_{index}"), 25, false));
    }
    PgRelation {
        oid: 44,
        schema: "public".into(),
        name: "wide_orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

/// A fresh in-memory DB with the mirror (+ hidden `_applied_*` guard cols) and `<table>_raw`, matching
/// the production test schema. `threads` is pinned for reproducibility.
fn db(mirror_cols: &str, raw_cols: &str) -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(&format!(
        "SET threads = 4;
         CREATE TABLE orders ({mirror_cols},
             _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
             _applied_lsn VARCHAR DEFAULT '0000000000000000');
         CREATE TABLE orders_raw ({raw_cols}, walrus_extractor_meta VARCHAR,
             _walrus_op VARCHAR, _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);"
    ))
    .unwrap();
    c
}

fn orders_db() -> Connection {
    db(
        "id INTEGER PRIMARY KEY, status VARCHAR",
        "id INTEGER, status VARCHAR",
    )
}

fn toast_db() -> Connection {
    db(
        "id INTEGER PRIMARY KEY, t1 VARCHAR, t2 VARCHAR, t3 VARCHAR",
        "id INTEGER, t1 VARCHAR, t2 VARCHAR, t3 VARCHAR",
    )
}

/// 16-hex LSN SQL expression from a 0-based row index (offset by 1 so it exceeds the mirror's
/// `'000…0'` default and the Step-3 guard fires).
fn lsn_hex(idx: &str) -> String {
    format!("upper(lpad(to_hex({idx} + 1), 16, '0'))")
}

/// Seed `n` raw events across `n/k` PKs, `k` events/PK, mixed i/d (last-by-`(commit_lsn,lsn)` wins).
fn seed_scaling(c: &Connection, n: usize, k: usize) {
    let npk = (n / k).max(1);
    let lsn = lsn_hex("i");
    c.execute_batch(&format!(
        "INSERT INTO orders_raw
         SELECT (i % {npk}) AS id, 'v' || i AS status, '{{}}',
                CASE WHEN i % 7 = 0 THEN 'd' ELSE 'i' END,
                {lsn}, {lsn}
         FROM range({n}) t(i);"
    ))
    .unwrap();
}

/// Pre-seed the mirror with `m` rows (ids `0..m`, low guard tuple) so the tail MERGE hits UPDATE.
fn seed_mirror(c: &Connection, m: usize) {
    c.execute_batch(&format!(
        "INSERT INTO orders
         SELECT i AS id, 'm' || i, '0000000000000000', '0000000000000000'
         FROM range({m}) t(i);"
    ))
    .unwrap();
}

/// Build the production raw schema and seed a fixed new tail behind `history` older rows. Unlike the
/// original scaling grid, this exposes whether retained history makes an identical transform slower.
fn seed_production_history(history: usize, tail: usize) -> (TableDb, Lsn) {
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables(&orders_rel(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn().execute_batch("SET threads = 4;").unwrap();
    let total = history + tail;
    let lsn = lsn_hex("i");
    db.conn()
        .execute_batch(&format!(
            "INSERT INTO orders_raw
             SELECT i::INTEGER AS id, 'v' || i AS status, '{{}}', 'i', {lsn}, {lsn},
                    '2026-07-04T12:00:00.123Z'
             FROM range({total}) t(i);"
        ))
        .unwrap();
    (db, Lsn::new(u64::try_from(history).unwrap()))
}

/// Seed `n` raw rows over `n/2` PKs (K=2: a setter `'i'` then a winner `'u'`). `pct` % of winners
/// carry the unchanged-TOAST sentinel on t1/t2/t3 (its meta lists them) — the *only* thing that
/// varies between the two back-scan benches, so the delta is pure back-scan.
fn seed_toast(c: &Connection, n: usize, pct: u32) {
    let lsn = lsn_hex("i");
    let sentinel = r#"{"unchanged_toast":["t1","t2","t3"]}"#;
    c.execute_batch(&format!(
        "INSERT INTO orders_raw
         SELECT (i // 2) AS id, 'val1_' || (i // 2), 'val2_' || (i // 2), 'val3_' || (i // 2),
                CASE WHEN (i % 2 = 1) AND ((i // 2) % 100 < {pct}) THEN '{sentinel}' ELSE '{{}}' END,
                CASE WHEN i % 2 = 0 THEN 'i' ELSE 'u' END,
                {lsn}, {lsn}
         FROM range({n}) t(i);"
    ))
    .unwrap();
}

fn bench_transform_scaling(c: &mut Criterion) {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let mut g = c.benchmark_group("transformer/transform");
    g.sample_size(10); // 1M-row iterations are seconds, not micros
    for n in [10_000usize, 100_000, 1_000_000] {
        for k in [1usize, 10] {
            g.throughput(Throughput::Elements(n as u64));
            g.bench_with_input(
                BenchmarkId::new(format!("k{k}"), n),
                &(n, k),
                |b, &(n, k)| {
                    b.iter_batched(
                        || {
                            let db = orders_db();
                            seed_scaling(&db, n, k);
                            db
                        },
                        |db| apply_transform(&db, &t, Lsn::ZERO).unwrap(),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    g.finish();
}

fn bench_toast_backscan(c: &mut Criterion) {
    let rel = toast_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let n = 100_000usize;
    let mut g = c.benchmark_group("transformer/toast_backscan");
    g.sample_size(10);
    for (label, pct) in [("no_toast", 0u32), ("toast_30pct", 30)] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(label), &pct, |b, &pct| {
            b.iter_batched(
                || {
                    let db = toast_db();
                    seed_toast(&db, n, pct);
                    db
                },
                |db| apply_transform(&db, &t, Lsn::ZERO).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }
    g.finish();
}

fn bench_mirror_size(c: &mut Criterion) {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel).unwrap();
    let tail = 100_000usize;
    let mut g = c.benchmark_group("transformer/mirror_size");
    g.sample_size(10);
    for (label, mirror) in [("empty_mirror", 0usize), ("mirror_1m", 1_000_000)] {
        g.throughput(Throughput::Elements(tail as u64));
        g.bench_with_input(BenchmarkId::from_parameter(label), &mirror, |b, &mirror| {
            b.iter_batched(
                || {
                    let db = orders_db();
                    if mirror > 0 {
                        seed_mirror(&db, mirror);
                    }
                    seed_scaling(&db, tail, 1);
                    db
                },
                |db| apply_transform(&db, &t, Lsn::ZERO).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }
    g.finish();
}

fn bench_raw_history(c: &mut Criterion) {
    let rel = orders_rel();
    let transform = TransformSql::from_relation(&rel).unwrap();
    let tail = 5_000usize;
    let mut g = c.benchmark_group("transformer/raw_history");
    g.sample_size(10);
    g.throughput(Throughput::Elements(tail as u64));
    for history in [0usize, 100_000, 1_000_000] {
        g.bench_with_input(
            BenchmarkId::from_parameter(history),
            &history,
            |b, &history| {
                b.iter_batched(
                    || seed_production_history(history, tail),
                    |(db, after)| apply_transform(db.conn(), &transform, after).unwrap(),
                    BatchSize::PerIteration,
                );
            },
        );
    }
    g.finish();
}

fn bench_keycols(c: &mut Criterion) {
    let one_key = orders_rel();
    let three_keys = three_key_rel();
    let mut g = c.benchmark_group("transformer/keycols");
    for (label, relation) in [("one_key", one_key), ("three_keys", three_keys)] {
        g.bench_with_input(BenchmarkId::from_parameter(label), &relation, |b, rel| {
            // Input guarded too — the only non-FFI bench here, so it *can* be const-folded.
            b.iter(|| black_box(black_box(rel).to_key_columns()));
        });
    }
    g.finish();
}

fn bench_render(c: &mut Criterion) {
    let cases = [
        (
            "orders_2_cols",
            TransformSql::from_relation(&orders_rel()).unwrap(),
        ),
        (
            "wide_30_cols",
            TransformSql::from_relation(&wide_rel()).unwrap(),
        ),
    ];
    let mut g = c.benchmark_group("transformer/render");
    for (label, transform) in &cases {
        g.bench_function(*label, |b| {
            b.iter(|| black_box(transform.render(black_box(Lsn::ZERO), black_box(None))));
        });
    }
    g.finish();
}

fn benches() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_transform_scaling(&mut criterion);
    bench_toast_backscan(&mut criterion);
    bench_mirror_size(&mut criterion);
    bench_raw_history(&mut criterion);
    bench_keycols(&mut criterion);
    bench_render(&mut criterion);
}
criterion_main!(benches);
