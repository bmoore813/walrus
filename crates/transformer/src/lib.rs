//! `walrus-transformer` — reads the extractor's staged Parquet from S3 and materialises it into a shared
//! PostgreSQL-catalogued DuckLake (`<table>` mirror + `<table>_raw` CDC log per source table). The
//! ordered fail-fast [`bootstrap`] proves exclusive ownership (control-plane [`lease`] + a table-keyed
//! catalog advisory lock) before manifest claiming and apply work begin, while [`health`] exposes the
//! service state.
//!
//! [`app`] is the entry point: [`app::run`] is the whole service lifecycle, so the `walrus-transformer`
//! binary (`src/main.rs`) is only config, tracing, a runtime and an exit code.
//!
//! # Concurrency
//!
//! One apply worker and transient DuckDB connection per table, all on a single `LocalSet`. [`duck`]'s
//! [`TableDb`](duck::TableDb) is `Send + !Sync`, so a future holding a `&` borrow of
//! [`TableCtx`](phase_a::TableCtx) or that [`TableDb`](duck::TableDb) across an await is
//! intentionally `!Send` and cannot be handed to `tokio::spawn` — which is what the crate-level
//! `clippy::future_not_send` allow above records for Clippy.

#![cfg_attr(
    test,
    allow(
        clippy::let_underscore_must_use,
        reason = "unit tests intentionally discard cleanup results"
    )
)]
#![allow(
    clippy::future_not_send,
    reason = "futures borrowing Send + !Sync TableDb values are intentionally driven on a LocalSet"
)]

pub mod app;
pub mod apply_loop;
pub mod bootstrap;
pub mod compaction;
pub mod config;
pub mod ddl;
pub mod duck;
pub mod duck_ext;
pub mod epoch;
pub mod error;
pub mod health;
pub mod lease;
pub mod ownership;
pub mod phase_a;
pub mod phase_b;
// The registry → DuckDB schema plan is an implementation detail of `duck`, `transform` and the three
// drivers (`bootstrap`, `phase_a`, `phase_b`): no binary, integration test or bench names it. Crate
// visibility keeps `TablePlan`'s shape free to move with the type system it bridges, and is why the
// four entry points that build or consume one are `pub(crate)` too.
pub(crate) mod plan;
pub mod shutdown;
pub mod supervisor;
pub mod table_name;
pub mod transform;
