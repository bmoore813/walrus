#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "bench (harness=false, not test-cfg)"
)]
//! Criterion micro-benches for the transformer's Phase-A append (`TableDb::append_parquet`).
//!
//! Generates a local Parquet fixture with the extractor's own Arrow→Parquet writer, then benches
//! `append_parquet` from a `file` path (no MinIO/httpfs — this isolates DuckDB ingest + the
//! file-ledger transaction cost, not S3 latency). A second bench times the per-file `DESCRIBE`
//! introspection alone, so its overhead is a separate line item. No production code is touched.
//!
//! Run: `cargo bench -p transformer --bench append` (or `just bench`).

use common::{
    EpochNo, ExtractorMeta, Kind, Lsn, Op, PgColumn, PgRelation, ReplicaIdentity, TupleValue,
    UtcTimestamp,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_main};
use pg_to_arrow::{BatchBuilder, write_parquet_bytes};
use std::hint::black_box;
use std::io::Write;
use transformer::duck::TableDb;

const ROWS: usize = 50_000;

fn col(name: &str, oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    }
}

/// `id int4 PK, a int4, b text` — a small row.
fn narrow_rel() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", 23, true),
            col("a", 23, false),
            col("b", 25, false),
        ],
    }
}

/// `id int4 PK + 29 text cols` — a wide row.
fn wide_rel() -> PgRelation {
    let mut columns = vec![col("id", 23, true)];
    columns.extend((0..29).map(|i| col(&format!("c{i}"), 25, false)));
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

/// One row's cell values for `rel` at index `i` (id distinct so the composite raw PK never collides).
fn row_values(rel: &PgRelation, i: usize) -> Vec<TupleValue> {
    rel.columns
        .iter()
        .map(|c| {
            let v = match c.type_oid {
                23 if c.is_key => i.to_string(),
                23 => (i64::try_from(i).unwrap() * 2).to_string(),
                _ => format!("{}_{i}", c.name),
            };
            TupleValue::Text(v)
        })
        .collect()
}

/// A distinct-per-row `ExtractorMeta` (varying `lsn`) so `append_parquet` extracts a unique `_walrus_lsn`.
fn meta(i: usize) -> ExtractorMeta {
    ExtractorMeta {
        op: Op::Insert,
        lsn: Lsn::new(u64::try_from(i).unwrap() + 1),
        commit_lsn: Lsn::new(u64::try_from(i).unwrap() + 1),
        commit_ts: "2026-07-04T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        xid: 1,
        epoch: EpochNo(7),
        batch_id: "3f2a0000-0000-0000-0000-000000000001".to_string(),
        schema_version: common::SchemaVersionNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "walrus-extractor-0".to_string(),
        extractor_processed_at: "2026-07-04T12:00:00.123Z".parse::<UtcTimestamp>().unwrap(),
    }
}

/// Build a `ROWS`-row Parquet fixture for `rel` with the extractor's writer, into a temp file.
fn gen_parquet(rel: &PgRelation) -> tempfile::NamedTempFile {
    let mut bb = BatchBuilder::new(rel).unwrap();
    for i in 0..ROWS {
        bb.append_row(&row_values(rel, i), &meta(i)).unwrap();
    }
    let bytes = write_parquet_bytes(&bb.into_record_batch().unwrap()).unwrap();
    let mut f = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    f.write_all(&bytes).unwrap();
    f.flush().unwrap();
    f
}

fn bench_append(c: &mut Criterion) {
    let mut g = c.benchmark_group("transformer/append_parquet");
    for (name, rel) in [("narrow", narrow_rel()), ("wide", wide_rel())] {
        let file = gen_parquet(&rel);
        let uri = file.path().to_string_lossy().into_owned();
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(name), &rel, |b, rel| {
            b.iter_batched(
                || {
                    let db = TableDb::open(":memory:").unwrap();
                    db.ensure_tables(rel, common::SchemaVersionNo(1)).unwrap();
                    db
                },
                |db| {
                    black_box(
                        db.append_parquet(
                            "orders",
                            common::ManifestId(1),
                            &uri,
                            common::SchemaVersionNo(1),
                            None,
                        )
                        .unwrap(),
                    )
                },
                BatchSize::PerIteration,
            );
        });
    }
    g.finish();
}

/// The per-file `DESCRIBE SELECT * FROM read_parquet(...)` introspection alone — the same SQL
/// `append_parquet` runs internally to map columns by name.
fn bench_describe(c: &mut Criterion) {
    let mut g = c.benchmark_group("transformer/parquet_describe");
    for (name, rel) in [("narrow", narrow_rel()), ("wide", wide_rel())] {
        let file = gen_parquet(&rel);
        let uri = file.path().to_string_lossy().into_owned();
        let sql = format!("DESCRIBE SELECT * FROM read_parquet('{uri}')");
        g.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, sql| {
            b.iter_batched(
                || TableDb::open(":memory:").unwrap(),
                |db| {
                    let mut stmt = db.conn().prepare(sql).unwrap();
                    let cols: Vec<String> = stmt
                        .query_map([], |r| r.get::<_, String>(0))
                        .unwrap()
                        .map(Result::unwrap)
                        .collect();
                    black_box(cols);
                },
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

fn benches() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_append(&mut criterion);
    bench_describe(&mut criterion);
}
criterion_main!(benches);
