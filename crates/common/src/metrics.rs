//! Prometheus metrics for both binaries.
//!
//! The design's Observability section enumerates a fixed set of series; this module owns their **stable
//! names** (a rename breaks the committed dashboard + alerts), installs the process-wide Prometheus
//! recorder, and renders the `/metrics` text exposition the health server serves.
//!
//! Two properties keep this cheap and safe:
//! - The `metrics` façade macros are **no-ops until a recorder is installed**, so every instrumentation
//!   call sprinkled through the pipeline is inert in unit/integration tests that never call [`init`].
//! - [`init`] both *describes* and *zero-initialises* every global series, so a fresh `/metrics` lists
//!   the whole catalogue (the scrape tests assert this) before any real traffic moves a needle.
//!
//! Scope note: series computable at an
//! existing call site are populated there via the helpers below; the few that would need a **new**
//! query — files-ready / ddl-pending backlog counts, dead-letter failed-file counts, and the
//! not-yet-wired pause-poll counter — are registered (so the dashboard/alerts have a target) but
//! left at zero here.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

/// Stable metric-name constants. Call sites and the scrape tests import these so a rename is a single
/// edit that the tests catch. Transformer series are per-table, labelled by [`names::TABLE_LABEL`].
pub mod names {
    // --- extractor (global) ---
    /// WAL not yet confirmed: `pg_current_wal_lsn − confirmed_flush_lsn`.
    pub const EXTRACTOR_REPLICATION_LAG_BYTES: &str = "walrus_extractor_replication_lag_bytes";
    /// WAL bytes the replication slot pins on the source's disk.
    pub const EXTRACTOR_SLOT_RETAINED_WAL_BYTES: &str = "walrus_extractor_slot_retained_wal_bytes";
    /// Bytes PostgreSQL reports can still be written before `max_slot_wal_keep_size` is exhausted.
    pub const EXTRACTOR_SLOT_SAFE_WAL_BYTES: &str = "walrus_extractor_slot_safe_wal_bytes";
    /// Whether the configured replication slot was present at the last successful catalog poll.
    pub const EXTRACTOR_SLOT_PRESENT: &str = "walrus_extractor_slot_present";
    /// Whether this PostgreSQL exposes `pg_replication_slots.wal_status`.
    pub const EXTRACTOR_SLOT_WAL_STATUS_SUPPORTED: &str =
        "walrus_extractor_slot_wal_status_supported";
    /// Whether this PostgreSQL exposes `pg_replication_slots.safe_wal_size`.
    pub const EXTRACTOR_SLOT_SAFE_WAL_SIZE_SUPPORTED: &str =
        "walrus_extractor_slot_safe_wal_size_supported";
    /// Whether the last replication-slot catalog poll completed successfully.
    pub const EXTRACTOR_SLOT_GUARD_POLL_OK: &str = "walrus_extractor_slot_guard_poll_ok";
    /// Categorical gauge: 0 reserved · 1 unreserved · 2 lost (alert on ≥ 1).
    pub const EXTRACTOR_WAL_STATUS: &str = "walrus_extractor_wal_status";
    /// Protocol-v2 transactions currently open in the in-process streamed-transaction demux.
    pub const EXTRACTOR_OPEN_STREAM_TXNS: &str = "walrus_extractor_open_stream_txns";
    /// Age of the oldest open protocol-v2 transaction observed by the demux.
    pub const EXTRACTOR_OLDEST_OPEN_STREAM_TXN_AGE_SECONDS: &str =
        "walrus_extractor_oldest_open_stream_txn_age_seconds";
    /// Seconds since the last heartbeat round-trip confirmed.
    pub const EXTRACTOR_HEARTBEAT_CONFIRMED_AGE_SECONDS: &str =
        "walrus_extractor_heartbeat_confirmed_age_seconds";
    /// Age of the last heartbeat write→observe-return — the slot-liveness signal.
    pub const EXTRACTOR_HEARTBEAT_ROUNDTRIP_AGE_SECONDS: &str =
        "walrus_extractor_heartbeat_roundtrip_age_seconds";
    /// Gap between the latest sent and the last observed heartbeat `beat_seq`.
    pub const EXTRACTOR_BEAT_SEQ_GAP: &str = "walrus_extractor_beat_seq_gap";
    /// Seconds since the last standby-status feedback — keep well under `wal_sender_timeout`.
    pub const EXTRACTOR_FEEDBACK_AGE_SECONDS: &str = "walrus_extractor_feedback_age_seconds";
    /// Batch flush latency: encode → Parquet → S3 PUT → manifest commit. Global histogram.
    pub const EXTRACTOR_BATCH_FLUSH_LATENCY_SECONDS: &str =
        "walrus_extractor_batch_flush_latency_seconds";
    /// Rows PUT to object storage as Parquet — the extractor's throughput counter.
    pub const EXTRACTOR_PARQUET_ROWS_WRITTEN: &str = "walrus_extractor_parquet_rows_written_total";
    /// Aggregate in-memory buffered bytes across all Arrow builders.
    pub const EXTRACTOR_INFLIGHT_BYTES: &str = "walrus_extractor_inflight_bytes";
    /// Memory-ceiling flush / speculative spill events.
    pub const EXTRACTOR_SPILL_COUNT: &str = "walrus_extractor_spill_total";
    /// Bytes staged speculatively for open streamed transactions.
    pub const EXTRACTOR_SPECULATIVE_OPEN_TXN_BYTES: &str =
        "walrus_extractor_speculative_open_txn_bytes";
    /// Back-pressure pause-poll activations — the last-resort shed step.
    pub const EXTRACTOR_PAUSE_POLL_COUNT: &str = "walrus_extractor_pause_poll_total";
    /// Streamed transactions (or subtransactions) that aborted.
    pub const EXTRACTOR_ABORTED_TXN_COUNT: &str = "walrus_extractor_aborted_txn_total";
    /// Files that failed to write / PUT.
    pub const EXTRACTOR_FAILED_FILE_COUNT: &str = "walrus_extractor_failed_file_total";
    // --- reload (single-table-reload subsystem) ---
    /// Non-terminal reloads in flight, labelled by [`FLAVOR_LABEL`] — a gauge the controller inc/decs
    /// as exporters start and end; returns to 0 when the queue drains.
    pub const RELOAD_ACTIVE: &str = "walrus_reload_active";
    /// Chunk files exported, per table.
    pub const RELOAD_CHUNKS_TOTAL: &str = "walrus_reload_chunks_total";
    /// Rows exported across all chunks, per table.
    pub const RELOAD_ROWS_EXPORTED_TOTAL: &str = "walrus_reload_rows_exported_total";
    /// Echo round-trip latency (H1): signal INSERT → decoded-commit echo. Its p99 bounds reload
    /// throughput and tracks end-to-end decode latency. Global histogram.
    pub const RELOAD_ECHO_WAIT_SECONDS: &str = "walrus_reload_echo_wait_seconds";
    /// DDL-restarts of a reload attempt, per table (reload H9): a schema change past the
    /// reload's first watermark invalidates the attempt and re-exports at the new schema.
    pub const RELOAD_RESTARTS_TOTAL: &str = "walrus_reload_restarts_total";
    /// Reloads that reached a terminal `failed`, per table (preflight rejection, echo timeout, or
    /// restart-cap exhaustion).
    pub const RELOAD_FAILED_TOTAL: &str = "walrus_reload_failed_total";
    /// Reload echo cross-check failures (`embedded wal_insert_lsn >= commit LSN`) — any tick means
    /// the watermark model is wrong (page severity). Global.
    pub const RELOAD_CROSSCHECK_VIOLATIONS: &str = "walrus_reload_crosscheck_violations_total";
    /// Reloads abandoned because they hit `reload_max_restarts`: the export could not win
    /// the race against DDL within the cap and is now `failed`. Global page-worthy signal.
    pub const RELOAD_RESTART_CAP_EXHAUSTED_TOTAL: &str =
        "walrus_reload_restart_cap_exhausted_total";
    /// Count of `exporting` reloads whose lease has expired with nobody renewing — the
    /// controller sets this each tick from `stuck_exporting`. The stuck-lease alert reads it (a
    /// gauge, not a control-pg query, since the stack has no SQL-exporter). Global.
    pub const RELOAD_LEASE_STALE: &str = "walrus_reload_lease_stale";
    /// The reload-active gauge's one label — the flavor (`reload` | `resync`). Bounded (two values).
    pub const FLAVOR_LABEL: &str = "flavor";

    // --- transformer (per-table; labelled by TABLE_LABEL = "schema.table") ---
    /// Manifest files in state `ready` awaiting apply.
    pub const TRANSFORMER_FILES_READY: &str = "walrus_transformer_files_ready";
    /// Phase-A backlog: the extractor's `lsn_end` − `raw_appended_lsn`.
    pub const TRANSFORMER_RAW_APPEND_LAG_BYTES: &str = "walrus_transformer_raw_append_lag_bytes";
    /// Phase-B backlog: `raw_appended_lsn` − `transformed_lsn`.
    pub const TRANSFORMER_TRANSFORM_LAG_BYTES: &str = "walrus_transformer_transform_lag_bytes";
    /// Row count of the `<table>_raw` landing table.
    pub const TRANSFORMER_RAW_ROW_COUNT: &str = "walrus_transformer_raw_row_count";
    /// Size of the table's `.duckdb` file on disk.
    pub const TRANSFORMER_RAW_FILE_BYTES: &str = "walrus_transformer_raw_file_bytes";
    /// DDL events recorded for the table but not yet applied.
    pub const TRANSFORMER_DDL_PENDING: &str = "walrus_transformer_ddl_pending";
    /// Files the transformer failed to apply.
    pub const TRANSFORMER_FAILED_FILE_COUNT: &str = "walrus_transformer_failed_file_total";

    /// The one label on every transformer series — a fully-qualified `schema.table`. Bounded cardinality:
    /// per-table, **never** per-row/xid/batch (those high-cardinality ids live in `tracing` fields).
    pub const TABLE_LABEL: &str = "table";

    /// Every global (unlabelled) series, for zero-init + the extractor scrape test.
    pub const EXTRACTOR_ALL: &[&str] = &[
        EXTRACTOR_REPLICATION_LAG_BYTES,
        EXTRACTOR_SLOT_RETAINED_WAL_BYTES,
        EXTRACTOR_SLOT_SAFE_WAL_BYTES,
        EXTRACTOR_SLOT_PRESENT,
        EXTRACTOR_SLOT_WAL_STATUS_SUPPORTED,
        EXTRACTOR_SLOT_SAFE_WAL_SIZE_SUPPORTED,
        EXTRACTOR_SLOT_GUARD_POLL_OK,
        EXTRACTOR_WAL_STATUS,
        EXTRACTOR_OPEN_STREAM_TXNS,
        EXTRACTOR_OLDEST_OPEN_STREAM_TXN_AGE_SECONDS,
        EXTRACTOR_HEARTBEAT_CONFIRMED_AGE_SECONDS,
        EXTRACTOR_HEARTBEAT_ROUNDTRIP_AGE_SECONDS,
        EXTRACTOR_BEAT_SEQ_GAP,
        EXTRACTOR_FEEDBACK_AGE_SECONDS,
        EXTRACTOR_BATCH_FLUSH_LATENCY_SECONDS,
        EXTRACTOR_PARQUET_ROWS_WRITTEN,
        EXTRACTOR_INFLIGHT_BYTES,
        EXTRACTOR_SPILL_COUNT,
        EXTRACTOR_SPECULATIVE_OPEN_TXN_BYTES,
        EXTRACTOR_PAUSE_POLL_COUNT,
        EXTRACTOR_ABORTED_TXN_COUNT,
        EXTRACTOR_FAILED_FILE_COUNT,
        RELOAD_ECHO_WAIT_SECONDS,
        RELOAD_CROSSCHECK_VIOLATIONS,
        RELOAD_RESTART_CAP_EXHAUSTED_TOTAL,
        RELOAD_LEASE_STALE,
    ];

    /// Every per-table reload series — labelled by [`TABLE_LABEL`], like the transformer set.
    /// Not zero-inited globally (reloads are rare operator events); each appears on its first
    /// emission.
    pub const RELOAD_PER_TABLE: &[&str] = &[
        RELOAD_CHUNKS_TOTAL,
        RELOAD_ROWS_EXPORTED_TOTAL,
        RELOAD_RESTARTS_TOTAL,
        RELOAD_FAILED_TOTAL,
    ];

    /// Every per-table transformer series, for per-table zero-init + the transformer scrape test.
    pub const TRANSFORMER_ALL: &[&str] = &[
        TRANSFORMER_FILES_READY,
        TRANSFORMER_RAW_APPEND_LAG_BYTES,
        TRANSFORMER_TRANSFORM_LAG_BYTES,
        TRANSFORMER_RAW_ROW_COUNT,
        TRANSFORMER_RAW_FILE_BYTES,
        TRANSFORMER_DDL_PENDING,
        TRANSFORMER_FAILED_FILE_COUNT,
    ];
}

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the process-wide Prometheus recorder and register every global series. Idempotent: safe to
/// call from `main` and from each scrape test; later calls are no-ops. Until this runs, the call-site
/// helpers below do nothing.
///
/// # Panics
///
/// Panics if some *other* global `metrics` recorder was already installed. That is a programming
/// error rather than a runtime condition, so it aborts loudly instead of returning a `Result` no
/// caller could act on — contrast [`crate::telemetry::init_tracing`], whose already-installed case
/// is a legitimate outcome and is therefore swallowed. The `OnceLock` below runs the install at most
/// once, so repeated `init` calls never reach it; only a foreign recorder can.
pub fn init() {
    HANDLE.get_or_init(|| {
        // Install-once at process init: `install_recorder` only errors if a *different* global
        // recorder is already set (a programming error, not a runtime condition), and `get_or_init`
        // guarantees this closure runs at most once — so the panic is unreachable in practice. `init`
        // is infallible by signature and called from `main`/scrape-test setup; threading a `Result`
        // out (stable `OnceLock` has no `get_or_try_init`) would ripple with no recoverable path.
        #[expect(
            clippy::expect_used,
            reason = "install-once invariant: OnceLock::get_or_init runs this closure at most once, \
                      so install_recorder can only fail if another global recorder exists — a bug"
        )]
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("BUG: a second global Prometheus recorder was installed");
        describe_all();
        zero_init_global();
        // The reload-active gauge is flavor-labelled: seed both flavors at 0 so the panel shows a
        // flat line (not a gap) before the first reload of that flavor.
        for flavor in ["reload", "resync"] {
            metrics::gauge!(names::RELOAD_ACTIVE, names::FLAVOR_LABEL => flavor).set(0.0);
        }
        handle
    });
}

/// Render the current Prometheus text exposition. Empty until [`init`] installs the recorder.
///
/// Rendering allocates a fresh exposition string and changes nothing, so a discarded call is pure
/// waste. `clippy::must_use_candidate` cannot say so — reading the `HANDLE` static counts as
/// touching a mutable static, which makes the lint treat this function as side-effecting — so the
/// attribute is spelled out.
#[must_use]
pub fn render() -> String {
    HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

/// Zero-init every per-table transformer series for one `schema.table`, so it renders before the first real
/// update. Called once per owned table at transformer bootstrap, and by the transformer scrape test for a demo
/// table. No-op until [`init`] runs.
pub fn init_table_series(table: &str) {
    for name in names::TRANSFORMER_ALL {
        if name.ends_with("_total") {
            metrics::counter!(*name, names::TABLE_LABEL => table.to_string()).increment(0);
        } else {
            metrics::gauge!(*name, names::TABLE_LABEL => table.to_string()).set(0.0);
        }
    }
}

fn describe_all() {
    use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
    describe_gauge!(
        names::EXTRACTOR_REPLICATION_LAG_BYTES,
        Unit::Bytes,
        "WAL not yet confirmed: pg_current_wal_lsn − confirmed_flush_lsn"
    );
    describe_gauge!(
        names::EXTRACTOR_SLOT_RETAINED_WAL_BYTES,
        Unit::Bytes,
        "WAL bytes the slot pins on disk"
    );
    describe_gauge!(
        names::EXTRACTOR_SLOT_SAFE_WAL_BYTES,
        Unit::Bytes,
        "bytes PostgreSQL reports can still be written before max_slot_wal_keep_size is exhausted"
    );
    describe_gauge!(
        names::EXTRACTOR_SLOT_PRESENT,
        "configured replication slot present at the last successful catalog poll (1 present, 0 absent)"
    );
    describe_gauge!(
        names::EXTRACTOR_SLOT_WAL_STATUS_SUPPORTED,
        "source catalog exposes pg_replication_slots.wal_status (1 supported, 0 unsupported)"
    );
    describe_gauge!(
        names::EXTRACTOR_SLOT_SAFE_WAL_SIZE_SUPPORTED,
        "source catalog exposes pg_replication_slots.safe_wal_size (1 supported, 0 unsupported)"
    );
    describe_gauge!(
        names::EXTRACTOR_SLOT_GUARD_POLL_OK,
        "last replication-slot catalog poll completed successfully (1 success, 0 unavailable)"
    );
    describe_gauge!(
        names::EXTRACTOR_WAL_STATUS,
        "slot wal_status: 0 reserved, 1 unreserved, 2 lost"
    );
    describe_gauge!(
        names::EXTRACTOR_OPEN_STREAM_TXNS,
        "protocol-v2 transactions currently open in the streamed-transaction demux"
    );
    describe_gauge!(
        names::EXTRACTOR_OLDEST_OPEN_STREAM_TXN_AGE_SECONDS,
        Unit::Seconds,
        "age of the oldest open protocol-v2 transaction observed by the demux"
    );
    describe_gauge!(
        names::EXTRACTOR_HEARTBEAT_CONFIRMED_AGE_SECONDS,
        Unit::Seconds,
        "seconds since the last heartbeat round-trip confirmed"
    );
    describe_gauge!(
        names::EXTRACTOR_HEARTBEAT_ROUNDTRIP_AGE_SECONDS,
        Unit::Seconds,
        "age of the last heartbeat write→observe-return (the slot-liveness signal)"
    );
    describe_gauge!(
        names::EXTRACTOR_BEAT_SEQ_GAP,
        "gap between the latest sent and last observed heartbeat beat_seq"
    );
    describe_gauge!(
        names::EXTRACTOR_FEEDBACK_AGE_SECONDS,
        Unit::Seconds,
        "seconds since the last standby-status feedback (keep well under wal_sender_timeout)"
    );
    describe_histogram!(
        names::EXTRACTOR_BATCH_FLUSH_LATENCY_SECONDS,
        Unit::Seconds,
        "batch flush latency: encode → Parquet → S3 PUT → manifest commit"
    );
    describe_counter!(
        names::EXTRACTOR_PARQUET_ROWS_WRITTEN,
        "total rows PUT to object storage as Parquet (throughput)"
    );
    describe_gauge!(
        names::EXTRACTOR_INFLIGHT_BYTES,
        Unit::Bytes,
        "aggregate in-memory buffered bytes across all builders"
    );
    describe_counter!(
        names::EXTRACTOR_SPILL_COUNT,
        "memory-ceiling flush / speculative spill events"
    );
    describe_gauge!(
        names::EXTRACTOR_SPECULATIVE_OPEN_TXN_BYTES,
        Unit::Bytes,
        "bytes staged speculatively for open streamed txns"
    );
    describe_counter!(
        names::EXTRACTOR_PAUSE_POLL_COUNT,
        "back-pressure pause-poll activations"
    );
    describe_counter!(
        names::EXTRACTOR_ABORTED_TXN_COUNT,
        "streamed transactions (or subtransactions) that aborted"
    );
    describe_counter!(
        names::EXTRACTOR_FAILED_FILE_COUNT,
        "files that failed to write / PUT"
    );
    describe_gauge!(
        names::RELOAD_ACTIVE,
        "non-terminal reloads in flight, by flavor"
    );
    describe_counter!(
        names::RELOAD_CHUNKS_TOTAL,
        "reload chunk files exported, per table"
    );
    describe_counter!(
        names::RELOAD_ROWS_EXPORTED_TOTAL,
        "rows exported across reload chunks, per table"
    );
    describe_histogram!(
        names::RELOAD_ECHO_WAIT_SECONDS,
        Unit::Seconds,
        "reload echo round-trip: signal INSERT → decoded-commit echo (≈ end-to-end decode latency)"
    );
    describe_counter!(
        names::RELOAD_RESTARTS_TOTAL,
        "reload attempts restarted because DDL bumped schema_version mid-export, per table"
    );
    describe_counter!(
        names::RELOAD_FAILED_TOTAL,
        "reloads that reached terminal 'failed' (preflight, echo timeout, or cap), per table"
    );
    describe_counter!(
        names::RELOAD_CROSSCHECK_VIOLATIONS,
        "reload echo cross-check failures (embedded wal_insert_lsn >= commit LSN) — any tick means \
         the watermark model is wrong"
    );
    describe_counter!(
        names::RELOAD_RESTART_CAP_EXHAUSTED_TOTAL,
        "reloads failed after exhausting reload_max_restarts against mid-export DDL"
    );
    describe_gauge!(
        names::RELOAD_LEASE_STALE,
        "exporting reloads with an expired, unadopted lease (nobody renewing) — the stuck signal"
    );

    describe_gauge!(
        names::TRANSFORMER_FILES_READY,
        "manifest files in state 'ready' awaiting apply, per table"
    );
    describe_gauge!(
        names::TRANSFORMER_RAW_APPEND_LAG_BYTES,
        Unit::Bytes,
        "extractor lsn_end − raw_appended_lsn, per table (Phase-A backlog)"
    );
    describe_gauge!(
        names::TRANSFORMER_TRANSFORM_LAG_BYTES,
        Unit::Bytes,
        "raw_appended_lsn − transformed_lsn, per table (Phase-B backlog)"
    );
    describe_gauge!(
        names::TRANSFORMER_RAW_ROW_COUNT,
        "<table>_raw row count, per table"
    );
    describe_gauge!(
        names::TRANSFORMER_RAW_FILE_BYTES,
        Unit::Bytes,
        ".duckdb file size, per table"
    );
    describe_gauge!(
        names::TRANSFORMER_DDL_PENDING,
        "DDL events not yet applied, per table"
    );
    describe_counter!(
        names::TRANSFORMER_FAILED_FILE_COUNT,
        "files the transformer failed to apply, per table"
    );
}

fn zero_init_global() {
    for name in names::EXTRACTOR_ALL {
        if *name == names::EXTRACTOR_BATCH_FLUSH_LATENCY_SECONDS
            || *name == names::RELOAD_ECHO_WAIT_SECONDS
        {
            // A histogram only appears in the exposition once it has a sample; seed one 0s observation so
            // the series (and the dashboard panel) exists from startup. Negligible against real traffic.
            metrics::histogram!(*name).record(0.0);
        } else if name.ends_with("_total") {
            metrics::counter!(*name).increment(0);
        } else {
            metrics::gauge!(*name).set(0.0);
        }
    }
}

// ---------------------------------------------------------------------------------------------------
// Call-site helpers. Each is a no-op until `init` installs the recorder, so the pipeline can call them
// unconditionally. Only the signals computable at an existing site are wired (see the module note).
// ---------------------------------------------------------------------------------------------------

/// Slot `wal_status` as the categorical gauge (0 reserved / 1 unreserved / 2 lost).
pub fn set_wal_status(code: u8) {
    metrics::gauge!(names::EXTRACTOR_WAL_STATUS).set(f64::from(code));
}

/// Refresh the replication-slot guard gauges from one read-only source-catalog sample.
pub fn set_slot_guard(
    replication_lag_bytes: u64,
    retained_wal_bytes: u64,
    safe_wal_bytes: Option<u64>,
    present: bool,
    wal_status_supported: bool,
    safe_wal_size_supported: bool,
    wal_status_code: Option<u8>,
) {
    metrics::gauge!(names::EXTRACTOR_REPLICATION_LAG_BYTES).set(replication_lag_bytes as f64);
    metrics::gauge!(names::EXTRACTOR_SLOT_RETAINED_WAL_BYTES).set(retained_wal_bytes as f64);
    metrics::gauge!(names::EXTRACTOR_SLOT_SAFE_WAL_BYTES)
        .set(safe_wal_bytes.map_or(f64::NAN, |bytes| bytes as f64));
    metrics::gauge!(names::EXTRACTOR_SLOT_PRESENT).set(if present { 1.0 } else { 0.0 });
    metrics::gauge!(names::EXTRACTOR_SLOT_WAL_STATUS_SUPPORTED).set(if wal_status_supported {
        1.0
    } else {
        0.0
    });
    metrics::gauge!(names::EXTRACTOR_SLOT_SAFE_WAL_SIZE_SUPPORTED)
        .set(if safe_wal_size_supported { 1.0 } else { 0.0 });
    metrics::gauge!(names::EXTRACTOR_WAL_STATUS).set(wal_status_code.map_or(f64::NAN, f64::from));
    metrics::gauge!(names::EXTRACTOR_SLOT_GUARD_POLL_OK).set(1.0);
}

/// A failed/unstarted poll must not leave the previous healthy sample looking current.
pub fn set_slot_guard_unknown() {
    for name in [
        names::EXTRACTOR_REPLICATION_LAG_BYTES,
        names::EXTRACTOR_SLOT_RETAINED_WAL_BYTES,
        names::EXTRACTOR_SLOT_SAFE_WAL_BYTES,
        names::EXTRACTOR_SLOT_PRESENT,
        names::EXTRACTOR_SLOT_WAL_STATUS_SUPPORTED,
        names::EXTRACTOR_SLOT_SAFE_WAL_SIZE_SUPPORTED,
        names::EXTRACTOR_WAL_STATUS,
    ] {
        metrics::gauge!(name).set(f64::NAN);
    }
    metrics::gauge!(names::EXTRACTOR_SLOT_GUARD_POLL_OK).set(0.0);
}

/// Refresh the in-process protocol-v2 open-transaction guard gauges.
pub fn set_open_stream_txns(count: usize, oldest_age_seconds: f64) {
    metrics::gauge!(names::EXTRACTOR_OPEN_STREAM_TXNS).set(count as f64);
    metrics::gauge!(names::EXTRACTOR_OLDEST_OPEN_STREAM_TXN_AGE_SECONDS).set(oldest_age_seconds);
}

/// One reload echo cross-check violation (`embedded >= commit`) — the watermark model is
/// wrong; the alert on this counter is page severity.
pub fn record_reload_crosscheck_violation() {
    metrics::counter!(names::RELOAD_CROSSCHECK_VIOLATIONS).increment(1);
}

/// One reload attempt re-issued because DDL bumped `table`'s `schema_version` mid-export.
pub fn record_reload_restart(table: &str) {
    metrics::counter!(names::RELOAD_RESTARTS_TOTAL, names::TABLE_LABEL => table.to_string())
        .increment(1);
}

/// One reload abandoned at the restart cap: visible waste, not silent corruption.
pub fn record_reload_restart_cap_exhausted() {
    metrics::counter!(names::RELOAD_RESTART_CAP_EXHAUSTED_TOTAL).increment(1);
}

/// A reload exporter started / ended: inc/dec the in-flight gauge for its flavor. The
/// pair balances per exporter task, so the gauge returns to 0 when the queue drains.
pub fn inc_reload_active(flavor: &str) {
    metrics::gauge!(names::RELOAD_ACTIVE, names::FLAVOR_LABEL => flavor.to_string()).increment(1.0);
}
/// The decrement half of that pair — see [`inc_reload_active`] for the balance it maintains.
pub fn dec_reload_active(flavor: &str) {
    metrics::gauge!(names::RELOAD_ACTIVE, names::FLAVOR_LABEL => flavor.to_string()).decrement(1.0);
}

/// One reload chunk file exported: bump the per-table chunk and row counters.
pub fn record_reload_chunk(table: &str, rows: u64) {
    metrics::counter!(names::RELOAD_CHUNKS_TOTAL, names::TABLE_LABEL => table.to_string())
        .increment(1);
    metrics::counter!(names::RELOAD_ROWS_EXPORTED_TOTAL, names::TABLE_LABEL => table.to_string())
        .increment(rows);
}

/// One reload echo round-trip observed (H1): signal INSERT → decoded-commit echo.
pub fn record_reload_echo_wait(secs: f64) {
    metrics::histogram!(names::RELOAD_ECHO_WAIT_SECONDS).record(secs);
}

/// One reload reached terminal `failed`, per table — preflight, echo timeout, or cap.
pub fn record_reload_failed(table: &str) {
    metrics::counter!(names::RELOAD_FAILED_TOTAL, names::TABLE_LABEL => table.to_string())
        .increment(1);
}

/// How many `exporting` reloads have a stale, unadopted lease right now — set each
/// controller tick, so the stuck-lease alert reads a gauge instead of querying control-pg.
pub fn set_reload_lease_stale(count: u64) {
    metrics::gauge!(names::RELOAD_LEASE_STALE).set(count as f64);
}

/// One batch flush: its wall-clock latency and the row count written (Parquet throughput).
pub fn record_batch_flush(latency_secs: f64, rows: u64) {
    metrics::histogram!(names::EXTRACTOR_BATCH_FLUSH_LATENCY_SECONDS).record(latency_secs);
    metrics::counter!(names::EXTRACTOR_PARQUET_ROWS_WRITTEN).increment(rows);
}

/// A memory-ceiling flush / speculative spill happened.
pub fn inc_spill() {
    metrics::counter!(names::EXTRACTOR_SPILL_COUNT).increment(1);
}

/// A streamed transaction (or subtransaction) aborted.
pub fn inc_aborted_txn() {
    metrics::counter!(names::EXTRACTOR_ABORTED_TXN_COUNT).increment(1);
}

/// Current aggregate in-memory buffered bytes.
pub fn set_inflight_bytes(bytes: u64) {
    metrics::gauge!(names::EXTRACTOR_INFLIGHT_BYTES).set(bytes as f64);
}

/// Phase-B transform lag for one table: `raw_appended_lsn − transformed_lsn` in bytes.
pub fn set_transform_lag(table: &str, bytes: u64) {
    metrics::gauge!(names::TRANSFORMER_TRANSFORM_LAG_BYTES, names::TABLE_LABEL => table.to_string())
        .set(bytes as f64);
}

/// Phase-A raw-append lag for one table: `extractor lsn_end − raw_appended_lsn` in bytes.
pub fn set_raw_append_lag(table: &str, bytes: u64) {
    metrics::gauge!(names::TRANSFORMER_RAW_APPEND_LAG_BYTES, names::TABLE_LABEL => table.to_string())
        .set(bytes as f64);
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
