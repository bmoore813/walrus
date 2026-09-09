#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "bench (harness=false, not test-cfg)"
)]
//! Criterion micro-benches for the Arrow batch-building hot path.
//!
//! Measures `BatchBuilder::append_row` across Tier-1 shapes (narrow/wide/text-heavy) and a Tier-2
//! fan-out shape (interval + range + timetz), plus a whole-batch `into_record_batch()`. It also
//! isolates the per-row `serde_json::to_string(meta)` cost that the design flags as a suspect: two
//! identical micro-benches append the meta column, one serialising the `ExtractorMeta` per row and one
//! appending a pre-serialised constant, so the JSON cost reads as a direct subtraction. No
//! production code is touched, so this is a stable baseline for future optimizations.
//!
//! Run: `cargo bench -p pg-to-arrow --bench batch` (or `just bench`).

use arrow::array::StringBuilder;
use common::{
    ExtractorMeta, Kind, Op, PgColumn, PgRelation, ReplicaIdentity, TupleValue, UtcTimestamp,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_main};
use pg_to_arrow::{BatchBuilder, oids};
use std::hint::black_box;

/// Rows appended per measured iteration (throughput reads as rows/s).
const ROWS: usize = 1_000;

fn meta() -> ExtractorMeta {
    ExtractorMeta {
        op: Op::Insert,
        lsn: "0/10".parse().unwrap(),
        commit_lsn: "0/20".parse().unwrap(),
        commit_ts: "2026-07-04T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        xid: 1,
        epoch: common::EpochNo(7),
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

#[derive(Clone, Copy)]
enum Shape {
    NarrowInt4,
    Wide30,
    TextHeavy,
    Tier2Fanout,
}

impl Shape {
    const fn name(self) -> &'static str {
        match self {
            Shape::NarrowInt4 => "narrow_int4",
            Shape::Wide30 => "wide30",
            Shape::TextHeavy => "text_heavy",
            Shape::Tier2Fanout => "tier2_fanout",
        }
    }

    /// `(column_name, type_oid, cell_value)` — the value must be valid text for its type, since
    /// `append_row` *parses* it into the typed Arrow builder (unlike the decoder, which stores raw).
    fn cells(self) -> Vec<(String, u32, TupleValue)> {
        let t = |s: &str| TupleValue::Text(s.to_string());
        match self {
            Shape::NarrowInt4 => vec![
                ("a".into(), oids::INT4, t("1")),
                ("b".into(), oids::INT4, t("22")),
                ("c".into(), oids::INT4, t("333")),
                ("d".into(), oids::INT4, t("4444")),
            ],
            Shape::Wide30 => (0..30)
                .map(|i| match i % 3 {
                    0 => (format!("c{i}"), oids::INT4, t("42")),
                    1 => (
                        format!("c{i}"),
                        oids::TEXT,
                        t(&format!("cell_value_{i:03}")),
                    ),
                    _ => (format!("c{i}"), oids::FLOAT8, t("3.14159")),
                })
                .collect(),
            Shape::TextHeavy => (0..10)
                .map(|i| {
                    (
                        format!("t{i}"),
                        oids::TEXT,
                        t(&format!("{i:02}").repeat(100)),
                    )
                })
                .collect(),
            Shape::Tier2Fanout => vec![
                ("dur".into(), oids::INTERVAL, t("1 mon 2 days 03:04:05")),
                ("span".into(), oids::INT4RANGE, t("[1,10)")),
                ("tz".into(), oids::TIMETZ, t("12:34:56+05:30")),
            ],
        }
    }

    fn relation(self) -> PgRelation {
        let columns = self
            .cells()
            .into_iter()
            .map(|(name, type_oid, _)| PgColumn {
                name,
                type_oid,
                type_modifier: -1,
                is_key: false,
            })
            .collect();
        PgRelation {
            oid: 16_397,
            schema: "public".to_string(),
            name: "bench_tbl".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns,
        }
    }

    fn row(self) -> Vec<TupleValue> {
        self.cells().into_iter().map(|(_, _, v)| v).collect()
    }
}

const SHAPES: [Shape; 4] = [
    Shape::NarrowInt4,
    Shape::Wide30,
    Shape::TextHeavy,
    Shape::Tier2Fanout,
];

/// Per-row `append_row` cost. Both operands are `black_box`ed *inside* the loop: they are captured
/// loop-invariants (criterion only guards `setup()` output and the closure's return), and under the
/// bench profile's thin LTO the optimiser could otherwise hoist the per-row text parse and meta
/// serialisation out and measure `ROWS` copies of a single result.
fn bench_append_row(c: &mut Criterion) {
    let m = meta();
    let mut g = c.benchmark_group("arrow/append_row");
    for shape in SHAPES {
        let rel = shape.relation();
        let row = shape.row();
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(shape.name()), &shape, |b, _| {
            b.iter_batched(
                || BatchBuilder::new(&rel).unwrap(),
                |mut bb| {
                    for _ in 0..ROWS {
                        bb.append_row(black_box(&row), black_box(&m)).unwrap();
                    }
                    black_box(bb.len());
                },
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

/// The whole-batch cost: append `ROWS` rows (setup, untimed) then `into_record_batch()` →
/// RecordBatch (timed).
fn bench_finish(c: &mut Criterion) {
    let m = meta();
    let mut g = c.benchmark_group("arrow/finish");
    for shape in SHAPES {
        let rel = shape.relation();
        let row = shape.row();
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(shape.name()), &shape, |b, _| {
            b.iter_batched(
                || {
                    let mut bb = BatchBuilder::new(&rel).unwrap();
                    for _ in 0..ROWS {
                        bb.append_row(&row, &m).unwrap();
                    }
                    bb
                },
                |bb| black_box(bb.into_record_batch().unwrap()),
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

/// Isolate the per-row meta serialisation. Both benches build an identical `StringBuilder` of `ROWS`
/// entries; only `serialize` pays `serde_json::to_string(meta)` per row, `const` appends a pre-canned
/// string. `serialize − const` is the meta-JSON cost that the production implementation amortizes.
fn bench_meta_json(c: &mut Criterion) {
    let m = meta();
    let precanned = serde_json::to_string(&m).unwrap();
    let mut g = c.benchmark_group("arrow/meta_json");
    g.throughput(Throughput::Elements(ROWS as u64));

    g.bench_function("serialize", |b| {
        b.iter_batched(
            StringBuilder::new,
            |mut sb| {
                for _ in 0..ROWS {
                    let j = serde_json::to_string(black_box(&m)).unwrap();
                    sb.append_value(&j);
                }
                black_box(sb.finish());
            },
            BatchSize::SmallInput,
        );
    });

    g.bench_function("const", |b| {
        b.iter_batched(
            StringBuilder::new,
            |mut sb| {
                for _ in 0..ROWS {
                    sb.append_value(black_box(&precanned));
                }
                black_box(sb.finish());
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

fn benches() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_append_row(&mut criterion);
    bench_finish(&mut criterion);
    bench_meta_json(&mut criterion);
}
criterion_main!(benches);
