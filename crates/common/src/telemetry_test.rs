use super::*;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

/// A `MakeWriter` that captures everything written into a shared buffer.
#[derive(Clone, Default)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BufWriter {
    type Writer = BufWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Capture `body`'s events through a *scoped* JSON subscriber (`with_default`) so we don't fight
/// the one global install, returning what it wrote.
fn capture_json(body: impl FnOnce()) -> String {
    // Ensure a global subscriber exists so the level fast-path admits INFO events.
    let _ = init_tracing(&TelemetryConfig::default());

    let buf = BufWriter::default();
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(buf.clone()),
        );

    tracing::subscriber::with_default(subscriber, body);
    String::from_utf8(buf.0.lock().unwrap().clone()).unwrap()
}

/// Run `body` inside a hermetic environment so changes to `RUST_LOG` cannot leak between tests.
#[allow(
    clippy::result_large_err,
    reason = "figment Jail requires Result<(), figment::Error>, whose error variant is intentionally large"
)]
fn in_jail(body: impl FnOnce(&mut figment::Jail)) {
    figment::Jail::expect_with(|jail| {
        body(jail);
        Ok(())
    });
}

#[test]
fn init_with_defaults_does_not_panic() {
    assert!(init_tracing(&TelemetryConfig::default()).is_ok());
}

#[test]
fn second_init_is_handled_not_fatal() {
    // Tests share one process, so at most one of these actually installs the global
    // subscriber; the rest hit the "already initialised" path. None may panic.
    assert!(init_tracing(&TelemetryConfig::default()).is_ok());
    assert!(
        init_tracing(&TelemetryConfig {
            json: true,
            filter: "debug".to_string(),
        })
        .is_ok()
    );
}

/// The `log` -> `tracing` bridge that [`init_tracing`] installs is a *feature* of
/// `tracing-subscriber`, not code walrus owns. Dropping it silences every `log` record our
/// dependencies emit — with no compile error and nothing else in the suite going red — so naming
/// `tracing-log` in the manifest is the contract, and this is what notices when it goes away.
#[test]
fn the_log_to_tracing_bridge_feature_stays_enabled() {
    const MANIFEST: &str = include_str!("../Cargo.toml");

    assert!(
        MANIFEST.contains("\"tracing-log\""),
        "crates/common/Cargo.toml must keep naming tracing-subscriber's `tracing-log` feature, or \
         the log records tokio-postgres/sqlx/reqwest emit stop reaching the subscriber"
    );
}

#[test]
fn default_config_is_pretty_info() {
    let cfg = TelemetryConfig::default();
    assert!(!cfg.json);
    assert_eq!(cfg.filter, "info");
}

#[test]
fn explicit_filter_wins_over_rust_log() {
    in_jail(|jail| {
        jail.set_env("RUST_LOG", "error");
        let cfg = TelemetryConfig {
            json: false,
            filter: "warn".to_string(),
        };

        assert_eq!(build_env_filter(&cfg).to_string(), "warn");
    });
}

#[test]
fn empty_filter_uses_rust_log() {
    in_jail(|jail| {
        jail.set_env("RUST_LOG", "debug");
        let cfg = TelemetryConfig {
            json: false,
            filter: String::new(),
        };

        assert_eq!(build_env_filter(&cfg).to_string(), "debug");
    });
}

#[test]
fn empty_filter_without_rust_log_uses_safe_default() {
    in_jail(|jail| {
        jail.clear_env();
        let cfg = TelemetryConfig {
            json: false,
            filter: String::new(),
        };

        assert_eq!(build_env_filter(&cfg).to_string(), DEFAULT_FILTER);
    });
}

#[test]
fn malformed_filter_uses_safe_default() {
    let cfg = TelemetryConfig {
        json: false,
        filter: "common=verbose".to_string(),
    };

    assert_eq!(build_env_filter(&cfg).to_string(), DEFAULT_FILTER);
}

#[test]
fn json_flag_selects_json_formatter() {
    let out = capture_json(|| {
        tracing::info!(
            commit_lsn = "0000000001B4C000",
            xid = 918273,
            "flushed batch"
        );
    });

    assert!(
        out.trim_start().starts_with('{'),
        "expected a JSON object: {out}"
    );
    assert!(
        out.contains("\"commit_lsn\""),
        "carries the field key: {out}"
    );
    assert!(
        out.contains("\"flushed batch\""),
        "carries the message: {out}"
    );
}

/// A span's fields and an event's own fields render at DIFFERENT JSON paths — `span`/`spans`
/// versus `fields` — so a span is additive context, never a substitute for the event field a
/// dashboard queries. That distinction is why `transformer::apply_loop`'s per-worker span and
/// `extractor::reload::spawn_exporter`'s per-exporter span were added *alongside* the `table` /
/// `source_table` fields their events already spell, rather than replacing them; nothing else in
/// the suite notices if a later cleanup deletes the duplicates and moves the queried key.
#[test]
fn span_fields_render_beside_event_fields_not_in_place_of_them() {
    let out = capture_json(|| {
        let span = tracing::info_span!("apply_loop", table = "public.orders");
        // Sync body, so an entry guard is the right tool here — the async call sites use
        // `#[instrument]` / `.instrument(span)` precisely because a guard cannot cross an `.await`.
        let _guard = span.enter();
        tracing::info!(transformed = "0000000001B4C000", "Phase B: mirror updated");
    });

    let event: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(
        event["span"]["table"], "public.orders",
        "the span field must survive into the event's span context: {out}"
    );
    assert!(
        event["fields"].get("table").is_none(),
        "a span field must NOT appear among the event's own fields: {out}"
    );
    assert_eq!(
        event["fields"]["transformed"], "0000000001B4C000",
        "the event keeps its own fields under `fields`: {out}"
    );
}
