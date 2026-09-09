//! Tracing setup and the structured-field convention.
//!
//! walrus never `println!`s — `clippy::print_stdout`/`print_stderr` are denied workspace-wide, and
//! the only production carve-out is each binary's `main`, for the window before a subscriber
//! exists. Every log line is a [`tracing`] event with **structured fields** so a Grafana/Loki query
//! can follow one transaction through both services. [`init_tracing`] installs the
//! process-wide subscriber once at the top of `main`; the [`fields`] module fixes the canonical
//! field-key spellings (`xid`, `commit_lsn`, `lsn`, `batch_uuid`, …) that every call site must use —
//! e.g. `info!({XID} = xid, {COMMIT_LSN} = %commit_lsn, "flushed batch")`.

use serde::Deserialize;
use tracing_subscriber::EnvFilter;
#[cfg(feature = "tokio-console")]
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Canonical structured-field keys. Use these constants at every `tracing` call site so dashboards
/// and log queries key on **one** spelling across both services — never a free-form format string.
pub mod fields {
    /// Postgres transaction id of the change being processed.
    pub const XID: &str = "xid";
    /// Commit LSN — *the* order/watermark key (see [`crate::Lsn`]).
    pub const COMMIT_LSN: &str = "commit_lsn";
    /// Per-row WAL LSN — the intra-transaction tiebreaker.
    pub const LSN: &str = "lsn";
    /// UUID of the Parquet batch a row belongs to.
    pub const BATCH_UUID: &str = "batch_uuid";
    /// Generation counter that namespaces all control-plane state.
    pub const EPOCH: &str = "epoch";
    /// Structural schema version of the affected relation.
    pub const SCHEMA_VERSION: &str = "schema_version";
    /// Stable identity of the extractor pod that produced a batch.
    pub const EXTRACTOR_INSTANCE: &str = "extractor_instance";
}

/// Fallback filter directive when neither `cfg.filter` nor `RUST_LOG` supplies one — so a missing
/// env var never means "silent".
const DEFAULT_FILTER: &str = "info";

/// How to render logs. `json` on in the cluster, off (pretty) for local dev.
#[derive(Debug, Clone, Deserialize)]
// Defaults permit omitted keys; strict field checking makes present-but-misspelled keys fatal.
#[serde(deny_unknown_fields, default)]
pub struct TelemetryConfig {
    /// Emit newline-delimited JSON (one object per event) instead of the pretty formatter.
    pub json: bool,
    /// [`EnvFilter`] directive, e.g. `"info,extractor=debug,transformer=debug"` — targets are matched by
    /// prefix against the **lib** crate names, so `walrus=…` would reach only the two `main.rs`
    /// roots. Empty → fall back to `RUST_LOG`, then `DEFAULT_FILTER`.
    pub filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        TelemetryConfig {
            json: false,
            filter: DEFAULT_FILTER.to_string(),
        }
    }
}

/// Build the [`EnvFilter`]: an explicit `cfg.filter` wins; an empty one falls back to `RUST_LOG`,
/// then to [`DEFAULT_FILTER`]. A malformed directive degrades to the default rather than silently
/// disabling logging.
fn build_env_filter(cfg: &TelemetryConfig) -> EnvFilter {
    if cfg.filter.trim().is_empty() {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER))
    } else {
        EnvFilter::try_new(&cfg.filter).unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER))
    }
}

/// Build the [`EnvFilter`] + fmt layer (pretty or JSON per `cfg.json`) and install it as the global
/// default subscriber.
///
/// **Only a binary's `main` may call this.** A subscriber is process-wide and installs once, so
/// `common` is the one crate here that depends on `tracing-subscriber` and holds the one install;
/// every other crate — `control`, `pg-to-arrow`, and the extractor and transformer *libraries* beneath the
/// two `main`s — emits through the `tracing` facade alone, leaving format, level filter and
/// destination to whoever owns `main`. `crates/common/tests/subscriber_install_policy.rs` is what
/// goes red when a library takes that choice back.
///
/// Installing through [`SubscriberInitExt::try_init`] — rather than `set_global_default` — is what
/// also registers `tracing_log::LogTracer`, so the dependencies that still emit through the `log`
/// facade (`tokio-postgres`, `sqlx` and `object_store`'s HTTP client among them) arrive here as
/// events instead of being dropped. That bridge rides `tracing-subscriber`'s `tracing-log`
/// feature, which `crates/common/Cargo.toml` names explicitly for exactly this reason.
///
/// Idempotent: a global subscriber can only be installed once per process, so a second call is a
/// **handled outcome** — it logs at `debug` and returns `Ok(())` rather than panicking, keeping
/// tests and re-entrant bootstraps safe.
///
/// # Errors
///
/// This function currently produces no [`crate::Error`] variant: malformed filters fall back to
/// `DEFAULT_FILTER`, and an already-installed global subscriber is treated as an idempotent
/// success. The `Result` return preserves the bootstrap API contract for future fallible layers.
pub fn init_tracing(cfg: &TelemetryConfig) -> crate::Result<()> {
    let filter = build_env_filter(cfg);
    let registry = tracing_subscriber::registry().with(filter);

    let installed = if cfg.json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()
    } else {
        registry.with(tracing_subscriber::fmt::layer()).try_init()
    };

    if let Err(e) = installed {
        // A global subscriber is already installed (expected under test / a re-entrant bootstrap).
        // Keep the existing one; this is not a failure.
        tracing::debug!(error = ?e, "tracing subscriber already initialised; keeping the existing one");
    }
    Ok(())
}

/// Install the normal Walrus logging layers plus a local [`console_subscriber`] server.
///
/// This is available only to binaries built with their `tokio-console` diagnostic feature. The
/// console layer carries its own Tokio/runtime filter; the regular [`EnvFilter`] is attached only
/// to the formatting layer so an ordinary `info` log filter cannot discard the trace-level task
/// events the console needs. The caller must also compile Tokio with `--cfg tokio_unstable`.
///
/// As with [`init_tracing`], a pre-existing subscriber is an idempotent success.
///
/// # Errors
///
/// Reserved for future fallible layers; current setup handles an existing subscriber internally.
#[cfg(feature = "tokio-console")]
pub fn init_tracing_with_console(cfg: &TelemetryConfig) -> crate::Result<()> {
    let filter = build_env_filter(cfg);
    let console = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .spawn();
    let registry = tracing_subscriber::registry().with(console);

    let installed = if cfg.json {
        registry
            .with(tracing_subscriber::fmt::layer().json().with_filter(filter))
            .try_init()
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().with_filter(filter))
            .try_init()
    };

    if let Err(e) = installed {
        tracing::debug!(error = ?e, "tracing subscriber already initialised; keeping the existing one");
    }
    Ok(())
}

#[cfg(test)]
#[path = "telemetry_test.rs"]
mod tests;
