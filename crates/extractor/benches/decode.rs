#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "bench (harness=false, not test-cfg)"
)]
//! Criterion micro-benches for the pgoutput decoder hot path.
//!
//! We synthesize *valid* pgoutput byte streams programmatically (the same message layouts the golden
//! vectors in `tests/pgoutput_vectors.rs` prove — `docs/proto-version.md` §4–§8) and measure
//! `parse_stream` end-to-end and `parse_tuple` alone across three table shapes plus a streamed
//! variant. No production code is touched, so this remains a stable optimization baseline. The first
//! suspect the design flags is the per-cell `String` allocation in the `'t'` branch — the text-heavy
//! shape is built to expose it.
//!
//! Run: `cargo bench -p extractor --bench decode` (or `just bench`).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_main};
use extractor::pgoutput::{Reader, StreamCtx, parse_stream, parse_tuple};
use std::hint::black_box;

const ROWS: usize = 10_000;

// --- big-endian byte writers (values are immaterial to decode cost; layout is what matters) -------

fn be16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn be32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn be64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn cstr(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(s.as_bytes());
    b.push(0);
}

// --- table shapes ---------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum TableShape {
    /// 4 narrow int4 columns — the small-row baseline.
    NarrowInt4,
    /// 30 mixed columns — a wide row (more per-cell dispatch).
    Wide30,
    /// 10 × ~200-byte text columns — allocation-bound (the `'t'`-branch `String` per cell).
    TextHeavy,
}

impl TableShape {
    const fn name(self) -> &'static str {
        match self {
            TableShape::NarrowInt4 => "narrow_int4",
            TableShape::Wide30 => "wide30",
            TableShape::TextHeavy => "text_heavy",
        }
    }

    /// `(column_name, type_oid)` list. OIDs are cosmetic to the decoder (it ships raw text/bytes),
    /// but a realistic shape keeps the bench honest for readers.
    fn columns(self) -> Vec<(String, u32)> {
        match self {
            TableShape::NarrowInt4 => (0..4).map(|i| (format!("c{i}"), 23)).collect(),
            // int4 / text / float8 round-robin.
            TableShape::Wide30 => (0..30)
                .map(|i| (format!("c{i}"), [23u32, 25, 701][i % 3]))
                .collect(),
            TableShape::TextHeavy => (0..10).map(|i| (format!("t{i}"), 25)).collect(),
        }
    }

    /// One row's worth of textual cell values (pgoutput ships `'t'` text for every type).
    fn row_values(self) -> Vec<String> {
        match self {
            TableShape::NarrowInt4 => vec!["1".into(), "22".into(), "333".into(), "4444".into()],
            TableShape::Wide30 => (0..30).map(|i| format!("cell_value_{i:03}")).collect(),
            TableShape::TextHeavy => (0..10).map(|i| format!("{i:02}").repeat(100)).collect(), // ~200 bytes
        }
    }
}

// --- frame builders (layouts per proto-version.md §4–§8) ------------------------------------------

fn begin(out: &mut Vec<u8>) {
    out.push(b'B');
    be64(out, 0x0199_BAC8); // final_lsn
    be64(out, 12_345_678); // commit_ts
    be32(out, 501); // xid
}

fn commit(out: &mut Vec<u8>) {
    out.push(b'C');
    out.push(0); // flags
    be64(out, 0x0199_BAC8); // commit_lsn
    be64(out, 0x0199_BAD0); // end_lsn
    be64(out, 12_345_678); // commit_ts
}

fn relation(out: &mut Vec<u8>, oid: u32, cols: &[(String, u32)], streaming: bool, xid: u32) {
    out.push(b'R');
    if streaming {
        be32(out, xid);
    }
    be32(out, oid);
    cstr(out, "public");
    cstr(out, "bench_tbl");
    out.push(b'd'); // replica identity DEFAULT
    be16(out, u16::try_from(cols.len()).unwrap());
    for (name, coid) in cols {
        out.push(0); // flags (not a key column)
        cstr(out, name);
        be32(out, *coid);
        be32(out, 0xFFFF_FFFF); // atttypmod -1
    }
}

fn insert(out: &mut Vec<u8>, oid: u32, values: &[String], streaming: bool, xid: u32) {
    out.push(b'I');
    if streaming {
        be32(out, xid);
    }
    be32(out, oid);
    out.push(b'N'); // new-tuple marker
    be16(out, u16::try_from(values.len()).unwrap());
    for v in values {
        out.push(b't'); // text value
        be32(out, u32::try_from(v.len()).unwrap());
        out.extend_from_slice(v.as_bytes());
    }
}

/// `Begin, Relation, ROWS×Insert, Commit` — a non-streamed transaction.
fn synth_stream(shape: TableShape, rows: usize) -> Vec<u8> {
    let oid = 16_397;
    let cols = shape.columns();
    let values = shape.row_values();
    let mut out = Vec::new();
    begin(&mut out);
    relation(&mut out, oid, &cols, false, 0);
    for _ in 0..rows {
        insert(&mut out, oid, &values, false, 0);
    }
    commit(&mut out);
    out
}

/// `StreamStart, Relation, ROWS×Insert, StreamStop` — every change carries the sub-xid prefix.
fn synth_streamed(shape: TableShape, rows: usize) -> Vec<u8> {
    let oid = 16_397;
    let xid = 777;
    let cols = shape.columns();
    let values = shape.row_values();
    let mut out = Vec::new();
    out.push(b'S'); // StreamStart
    be32(&mut out, xid);
    out.push(1); // first_segment
    relation(&mut out, oid, &cols, true, xid);
    for _ in 0..rows {
        insert(&mut out, oid, &values, true, xid);
    }
    out.push(b'E'); // StreamStop
    out
}

/// Just the `TupleData` bytes for one row, for `parse_tuple` in isolation.
fn synth_tuple(shape: TableShape) -> Vec<u8> {
    let values = shape.row_values();
    let mut out = Vec::new();
    be16(&mut out, u16::try_from(values.len()).unwrap());
    for v in &values {
        out.push(b't');
        be32(&mut out, u32::try_from(v.len()).unwrap());
        out.extend_from_slice(v.as_bytes());
    }
    out
}

// --- benches --------------------------------------------------------------------------------------

fn bench_parse_stream(c: &mut Criterion) {
    let mut g = c.benchmark_group("pgoutput/parse_stream");
    for shape in [
        TableShape::NarrowInt4,
        TableShape::Wide30,
        TableShape::TextHeavy,
    ] {
        let data = synth_stream(shape, ROWS);
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(shape.name()), &data, |b, d| {
            b.iter(|| {
                let mut ctx = StreamCtx { in_stream: false };
                black_box(parse_stream(black_box(d.as_slice()), &mut ctx).unwrap());
            });
        });
    }
    g.finish();
}

fn bench_parse_stream_streamed(c: &mut Criterion) {
    let mut g = c.benchmark_group("pgoutput/parse_stream_streamed");
    for shape in [
        TableShape::NarrowInt4,
        TableShape::Wide30,
        TableShape::TextHeavy,
    ] {
        let data = synth_streamed(shape, ROWS);
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(shape.name()), &data, |b, d| {
            b.iter(|| {
                let mut ctx = StreamCtx { in_stream: false };
                black_box(parse_stream(black_box(d.as_slice()), &mut ctx).unwrap());
            });
        });
    }
    g.finish();
}

fn bench_parse_tuple(c: &mut Criterion) {
    let mut g = c.benchmark_group("pgoutput/parse_tuple");
    for shape in [
        TableShape::NarrowInt4,
        TableShape::Wide30,
        TableShape::TextHeavy,
    ] {
        let data = synth_tuple(shape);
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::from_parameter(shape.name()), &data, |b, d| {
            b.iter(|| {
                let mut r = Reader::new(black_box(d.as_slice()));
                black_box(parse_tuple(&mut r).unwrap());
            });
        });
    }
    g.finish();
}

fn benches() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_parse_stream(&mut criterion);
    bench_parse_stream_streamed(&mut criterion);
    bench_parse_tuple(&mut criterion);
}
criterion_main!(benches);
