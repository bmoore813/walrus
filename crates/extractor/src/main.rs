//! The `walrus-extractor` binary — the pod lifecycle shell, and nothing else.
//!
//! Everything a test could reach lives in the `extractor` library; this file keeps only the four steps
//! that cannot. `main` loads+validates config, inits tracing (the one install a process owns),
//! builds the runtime, and does the **only** `anyhow::Error → ExitCode` mapping in the whole binary
//! (the "context in the loop, exit code at `main`" idiom — a broken deploy is greppable in
//! `kubectl logs`). The lifecycle itself is [`extractor::app::run`], which returns
//! `anyhow::Result<()>`; the application boundary recovers each typed failure's distinct exit code.

use extractor::config::ExtractorConfig;
use std::process::ExitCode;

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

    // Step 1: config. Terminal on failure — before tracing exists, so report on stderr.
    let Ok(cfg) = ExtractorConfig::load()
        .inspect_err(|e| eprintln!("walrus-extractor: invalid configuration: {e}"))
    else {
        return common::ExitCode::Config.into();
    };
    #[cfg(feature = "tokio-console")]
    let tracing_result = common::init_tracing_with_console(&cfg.common.telemetry);
    #[cfg(not(feature = "tokio-console"))]
    let tracing_result = common::init_tracing(&cfg.common.telemetry);
    if let Err(e) = tracing_result {
        eprintln!("walrus-extractor: tracing init failed: {e}");
        return common::ExitCode::Internal.into();
    }

    let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("walrus-extractor")
        .worker_threads(common::runtime::resolve_worker_threads(
            cfg.extractor.worker_threads,
        ))
        .max_blocking_threads(common::runtime::MAX_BLOCKING_THREADS)
        .build()
        .inspect_err(|e| tracing::error!(error = ?e, "failed to build tokio runtime"))
    else {
        return common::ExitCode::Internal.into();
    };

    match runtime.block_on(extractor::app::run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = ?e, "walrus-extractor exiting");
            extractor::exit::code_for(&e).into()
        }
    }
}
