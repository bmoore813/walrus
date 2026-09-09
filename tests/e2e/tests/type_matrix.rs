#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end type-fidelity matrix (`architecture.md` "Types"): one wide `types_matrix` row driven
//! through the LIVE pipeline (Postgres → Arrow → Parquet → DuckDB), asserting each mapped type's DuckDB
//! `typeof` AND value in the mirror. Reuses the `pg-to-arrow` conformance expectations.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

fn tyof(h: &Harness, col: &str) -> String {
    h.duckdb_rows(
        "types_matrix",
        &format!("SELECT typeof({col}) FROM types_matrix"),
    )
    .unwrap()
    .remove(0)
}

fn val(h: &Harness, col: &str) -> String {
    h.duckdb_rows(
        "types_matrix",
        &format!("SELECT CAST({col} AS VARCHAR) FROM types_matrix"),
    )
    .unwrap()
    .remove(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn every_mapped_type_round_trips_with_correct_typeof() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec(
        "INSERT INTO public.types_matrix (id, n, j, u, ts, b, iv, rng, big, s) VALUES ( \
             1, 12345.6789, '{\"a\":1}', '00000000-0000-0000-0000-000000000001', \
             '2026-07-08 12:00:00+00', '\\xdeadbeef', '1 mon 2 days 03:04:05', '[1,10)', 'g', 's')",
    )
    .await
    .unwrap();
    h.await_transformed_past("types_matrix", before, Duration::from_secs(90))
        .await
        .expect("pipeline converges");
    h.stop_transformer().await.unwrap();

    // (1) DuckDB `typeof` — the load-bearing assertion (a value can look right with the wrong physical
    // type). Tier-1 scalars, the recombined INTERVAL, and the range's flat sibling columns.
    for (col, want) in [
        ("n", "DECIMAL(10,4)"),
        ("j", "VARCHAR"), // jsonb is a Tier-3 canonical-text carrier → VARCHAR
        ("u", "UUID"),
        ("ts", "TIMESTAMP WITH TIME ZONE"),
        ("b", "BLOB"),
        ("iv", "INTERVAL"),
        ("rng_lower", "INTEGER"),
        ("rng_upper", "INTEGER"),
        ("rng_lower_inc", "BOOLEAN"),
        ("rng_empty", "BOOLEAN"),
    ] {
        assert_eq!(tyof(&h, col), want, "typeof({col})");
    }

    // (2) Values survive the full transport.
    assert_eq!(val(&h, "n"), "12345.6789", "numeric");
    assert!(val(&h, "j").contains('1'), "jsonb: {}", val(&h, "j"));
    assert_eq!(val(&h, "u"), "00000000-0000-0000-0000-000000000001", "uuid");
    assert!(
        val(&h, "ts").contains("2026-07-08"),
        "timestamptz: {}",
        val(&h, "ts")
    );
    let iv = val(&h, "iv");
    assert!(
        iv.contains("1 month") && iv.contains("2 days") && iv.contains("03:04:05"),
        "interval recombined: {iv}"
    );
    assert_eq!(val(&h, "rng_lower"), "1", "range lower");
    assert_eq!(val(&h, "rng_upper"), "10", "range upper");
    assert_eq!(val(&h, "rng_lower_inc"), "true", "range lower inclusive");
    assert_eq!(val(&h, "rng_upper_inc"), "false", "range upper exclusive");
    assert_eq!(val(&h, "rng_empty"), "false", "range not empty");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn all_nulls_round_trip_as_null() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let before = h.source_wal_lsn().await.unwrap();
    // Every nullable column NULL (only the PK is set).
    h.source_exec("INSERT INTO public.types_matrix (id) VALUES (7)")
        .await
        .unwrap();
    h.await_transformed_past("types_matrix", before, Duration::from_secs(90))
        .await
        .expect("pipeline converges");
    h.stop_transformer().await.unwrap();

    let nulls = h
        .duckdb_scalar(
            "types_matrix",
            "SELECT (n IS NULL AND j IS NULL AND u IS NULL AND ts IS NULL AND b IS NULL \
                 AND iv IS NULL AND rng_lower IS NULL AND rng_upper IS NULL AND big IS NULL \
                 AND s IS NULL)::INTEGER FROM types_matrix WHERE id = 7",
        )
        .unwrap();
    assert_eq!(
        nulls, 1,
        "every nullable column round-trips as NULL (validity, not empty/0)"
    );
}
