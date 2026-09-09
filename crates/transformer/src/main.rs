//! The `walrus-transformer` binary — the pod lifecycle shell, and nothing else. Everything a test could
//! reach lives in the `transformer` library; this file keeps only the four steps that cannot. `main`
//! loads+validates config, inits tracing (the one install a process owns), builds the runtime, and
//! does the **only** error → `ExitCode` mapping (context in the loop, exit code at `main`). The
//! lifecycle itself is [`transformer::app::run`], whose `TransformerError` carries the distinct exit code
//! `main` surfaces so a broken deploy is greppable in `kubectl logs`.

use common::FailureClass;
use std::process::ExitCode;
use transformer::config::TransformerConfig;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// The pre-subscriber window, and the only stderr in this binary: config validation and
// `init_tracing` both run before any `tracing` event has a subscriber to reach, so their failures
// would be silent as events. Everything from the runtime build down is a `tracing` event.
#[allow(
    clippy::print_stderr,
    reason = "config and tracing-init failures precede the subscriber they would otherwise log to"
)]
fn main() -> ExitCode {
    #[cfg(feature = "dhat-heap")]
    let _dhat_profiler = dhat::Profiler::builder()
        .file_name(std::env::var_os("DHAT_OUTPUT").unwrap_or_else(|| "dhat-heap.json".into()))
        .build();

    let mut args = std::env::args_os();
    let _program = args.next();
    let command = args.next();
    if matches!(command.as_deref(), Some(flag) if flag == "--install-duckdb-extensions") {
        let Some(directory) = args.next() else {
            eprintln!(
                "walrus-transformer: --install-duckdb-extensions requires a destination directory"
            );
            return common::ExitCode::Config.into();
        };
        if args.next().is_some() {
            eprintln!("walrus-transformer: unexpected extra installer argument");
            return common::ExitCode::Config.into();
        }
        return match transformer::duck::install_extensions(std::path::Path::new(&directory)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("walrus-transformer: extension installation failed: {error:?}");
                common::ExitCode::Internal.into()
            }
        };
    }
    let migrate_catalog =
        matches!(command.as_deref(), Some(flag) if flag == "--migrate-ducklake-catalog");
    if command.is_some() && !migrate_catalog {
        eprintln!("walrus-transformer: unknown argument");
        return common::ExitCode::Config.into();
    }
    if args.next().is_some() {
        eprintln!("walrus-transformer: unexpected extra argument");
        return common::ExitCode::Config.into();
    }

    let cfg = match TransformerConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("walrus-transformer: invalid transformer configuration: {e}");
            return common::ExitCode::Config.into();
        }
    };
    #[cfg(feature = "tokio-console")]
    let tracing_result = common::init_tracing_with_console(&cfg.common.telemetry);
    #[cfg(not(feature = "tokio-console"))]
    let tracing_result = common::init_tracing(&cfg.common.telemetry);
    if let Err(e) = tracing_result {
        eprintln!("walrus-transformer: tracing init failed: {e}");
        return common::ExitCode::Internal.into();
    }
    if migrate_catalog {
        let s3 = transformer::app::duck_s3_access(&cfg);
        return match transformer::duck::migrate_catalog(&cfg.transformer.ducklake, &s3) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(error = ?error, "DuckLake catalog migration failed");
                error.exit_code().into()
            }
        };
    }
    // The multi-thread FLAVOR is load-bearing; the worker count is not. `block_on` drives the
    // pipeline — and with it the `LocalSet`'s apply loops — on THIS thread, which a full rebuild's
    // `CREATE OR REPLACE` blocks synchronously for seconds. Everything `tokio::spawn`ed (health
    // server, lease renewer, epoch watch, and `compaction::full_rebuild_abortable`'s interrupt
    // watcher) lives on the worker pool instead, so it keeps running across that block: the lease
    // stays renewed and SIGTERM still aborts the rewrite. `new_current_thread` would put all of
    // it on the blocked thread and lose both. One worker suffices — it is a thread of its own.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("walrus-transformer")
        .worker_threads(common::runtime::resolve_worker_threads(
            cfg.transformer.worker_threads,
        ))
        .max_blocking_threads(common::runtime::MAX_BLOCKING_THREADS)
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(error = ?e, "failed to build tokio runtime");
            return common::ExitCode::Internal.into();
        }
    };
    match runtime.block_on(transformer::app::run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // `?e`, not `%e`: this is the one place a failure is HANDLED, and `TransformerError`'s
            // `Duck`/`ControlTxn`/`RegistryDecode`/`LsnParse`/`Health` variants deliberately name only
            // the operation in `Display`, keeping the engine/driver failure in `#[source]` (see
            // `error_test.rs`). `Debug` is what walks that chain into the log — `%e` would exit on
            // "DuckDB: append …" with the reason nowhere on disk.
            tracing::error!(error = ?e, "walrus-transformer exiting");
            e.exit_code().into()
        }
    }
}
