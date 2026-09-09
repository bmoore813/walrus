//! `extractor` — the Walrus Postgres extractor library.
//!
//! The hand-rolled pgoutput decoder lives in [`pgoutput`] (driven by golden byte vectors from
//! `extractor/tests/`). The crate is also a **runnable service**: [`config`] loads and
//! validates settings, [`bootstrap`] runs the ordered fail-fast preflight, [`health`] serves the K8s
//! probes, and [`shutdown`] fans one `CancellationToken` out of SIGTERM/SIGINT. [`app`] wires them
//! together — [`app::run`] is the whole service lifecycle, so the `walrus-extractor` binary
//! (`src/main.rs`) is only config, tracing, a runtime and an exit code.

#![cfg_attr(
    test,
    allow(
        clippy::let_underscore_must_use,
        reason = "unit tests intentionally discard cleanup results"
    )
)]

pub mod app;
pub mod batch;
pub mod bootstrap;
pub mod checkpoint;
pub mod config;
pub mod consume;
pub mod ddl;
pub mod epoch;
pub mod exit;
pub mod guard_monitor;
pub mod health;
pub mod heartbeat;
pub mod manifest;
pub mod memory;
pub(crate) mod orphan;
pub mod pgoutput;
pub mod preflight;
pub mod relcache;
pub mod reload;
pub mod reload_event;
pub mod reload_export;
pub mod reload_signal;
pub mod replication;
pub mod shutdown;
pub mod slot;
pub mod source_catalog;
pub mod staging;
pub mod stream_txn;
