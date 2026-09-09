//! The fail-fast bootstrap scaffold — shared steps 2–4 (§4.2, architecture "Shared
//! bootstrap"). Step 1 (config load/validate) is [`ExtractorConfig::load`]; step 4 (bind health) is in
//! `main::run`. This module does the two dependency checks between them:
//!
//! 2. **control Postgres reachable + migrations current** — `connect` failures are *transient*
//!    (Postgres may still be coming up during a rollout), retried with backoff to the startup
//!    deadline; ensuring migrations is idempotent.
//! 3. **object-store canary** — `put` + `get` + `delete` of a tiny key (not just `head`: some
//!    S3-compatibles answer `head` on a nonexistent bucket differently). Transient, same retry.
//!
//! **Transient vs terminal is modelled as data** ([`common::Error::is_terminal`]): a terminal error
//! (bad config) returns immediately; a transient one (S3 5xx, PG "still coming up") is retried until
//! the deadline, after which the last error is returned and `main` maps its distinct exit code.

use crate::config::{ExtractorConfig, ObjectStoreConfig};
use crate::preflight::{self, SourcePreflight};
use common::{Error, FailureClass};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use sqlx::PgPool;
use std::ops::AsyncFnMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// What the shared bootstrap hands the replication loop: the control-plane pool, a live
/// canary-verified object store, and the preflighted source connection. The application opens the
/// actual streaming replication connection after classifying the slot.
#[derive(Debug)]
pub struct BootstrapCtx {
    /// Control-Postgres pool, already migrated to the current schema.
    pub control_pool: PgPool,
    /// The staging object store, proven writable by the bootstrap canary rather than merely
    /// configured — so a bad bucket fails at startup instead of at the first flush.
    pub object_store: Arc<dyn ObjectStore>,
    /// The preflighted **SQL** connection to the source. Not the replication connection: that is a
    /// separate one, opened later.
    pub source_client: tokio_postgres::Client,
}

const CANARY_PAYLOAD: &[u8] = b"walrus bootstrap canary";

/// First retry pause, and the floor [`next_backoff`] clamps up to.
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Ceiling for the exponential retry pause. `INITIAL_BACKOFF <= MAX_BACKOFF` is the `clamp`
/// precondition; keep the bounds adjacent so an edit cannot silently invert them.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Double the retry pause without overflow, then bound it between the fixed retry limits.
fn next_backoff(current: Duration) -> Duration {
    current
        .saturating_mul(2)
        .clamp(INITIAL_BACKOFF, MAX_BACKOFF)
}

/// Time budget for a single dependency attempt: at most 5s, and never more than the deadline leaves
/// (with a small floor so a nearly-elapsed deadline still gets one real attempt).
fn attempt_budget(deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(Instant::now())
        .clamp(Duration::from_millis(500), Duration::from_secs(5))
}

/// Run shared steps 2–4's preconditions, retrying transient deps until `deadline`.
///
/// # Errors
///
/// Returns [`Error::ControlDb`], [`Error::ObjectStore`], or [`Error::SourceDb`] after a transient
/// dependency remains unavailable through the deadline. Terminal source checks return
/// [`Error::Preflight`] or [`Error::KeylessTable`], and invalid object-store construction returns
/// [`Error::ObjectStore`] immediately.
pub async fn run_shared(cfg: &ExtractorConfig, deadline: Instant) -> Result<BootstrapCtx, Error> {
    // Steps 2 and 3 are independent. Running them together keeps `/startup` at 503 for the maximum
    // of their durations instead of their sum, and drops the other branch when either fails. Their
    // success logs may appear in either order; if both fail, the first error determines the exit
    // class (`ControlDb` or `ObjectStore`).
    let (control_pool, object_store) = tokio::try_join!(
        bootstrap_control(cfg, deadline),
        bootstrap_object_store(cfg, deadline),
    )?;

    // Step 6: source-side preflight. The connect is transient (server may be coming up); every
    // assertion is terminal — a wrong wal_level / missing publication / keyless table can't self-heal.
    let source_client = retry_transient(deadline, "source database", async || {
        preflight::connect_source(cfg.extractor.source_db_url.expose()).await
    })
    .await?;
    let pf = SourcePreflight::new(&source_client, cfg);
    let server = pf.assert_server_prereqs().await?;
    // Signal-table existence BEFORE publication coverage: a missing table must yield the
    // migration-naming error, not a failed ALTER PUBLICATION under manage_publication.
    pf.assert_reload_signal().await?;
    pf.assert_reload_event().await?;
    pf.assert_publication_covers().await?;
    pf.assert_table_lock_privileges().await?;
    pf.assert_ddl_capture().await?;
    let pk_mode = if cfg.extractor.strict_keys {
        crate::preflight::PkMode::Strict
    } else {
        crate::preflight::PkMode::Lenient
    };
    let pk = pf.assert_tables_have_pk(pk_mode).await?;
    tracing::info!(
        version_num = server.version_num,
        ok_tables = pk.ok.len(),
        quarantined = pk.quarantined.len(),
        "source preflight passed"
    );

    Ok(BootstrapCtx {
        control_pool,
        object_store,
        source_client,
    })
}

/// Step 2: connect to control Postgres and ensure its migrations are current.
async fn bootstrap_control(cfg: &ExtractorConfig, deadline: Instant) -> Result<PgPool, Error> {
    // Each connect attempt is bounded so sqlx's own pool-acquire timeout cannot exceed the shared
    // startup deadline.
    let control_pool = retry_transient(deadline, "control database", async || {
        let budget = attempt_budget(deadline);
        match tokio::time::timeout(budget, control::connect(cfg.common.control_db_url.expose()))
            .await
        {
            Ok(Ok(pool)) => Ok(pool),
            Ok(Err(e)) => Err(Error::ControlDb(e.to_string())),
            Err(_) => Err(Error::ControlDb(format!(
                "connect attempt did not complete within {budget:?}"
            ))),
        }
    })
    .await?;
    control::run_migrations(&control_pool)
        .await
        .map_err(|e| Error::ControlDb(format!("ensure control migrations current: {e}")))?;
    tracing::info!("control database reachable and migrations current");
    Ok(control_pool)
}

/// Step 3: build the object store and verify it with a put/get/delete canary.
async fn bootstrap_object_store(
    cfg: &ExtractorConfig,
    deadline: Instant,
) -> Result<Arc<dyn ObjectStore>, Error> {
    // Build once (config-derived, not retried), then run the canary until it succeeds or the shared
    // deadline passes.
    let object_store = build_object_store(&cfg.common.object_store)
        .map_err(|e| Error::ObjectStore(format!("build object store: {e}")))?;
    retry_transient(deadline, "object store", async || {
        object_store_canary(object_store.as_ref(), &cfg.extractor.instance)
            .await
            .map_err(|e| Error::ObjectStore(e.to_string()))
    })
    .await?;
    tracing::info!("object-store canary (put/get/delete) passed");
    Ok(object_store)
}

/// Build the S3/MinIO client from config. Credentials come from the environment
/// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`); a `Some(endpoint)` selects MinIO/localstack and
/// allows plain HTTP.
///
/// The `Arc<dyn ObjectStore>` is not a flexibility hedge — only `AmazonS3` is ever built. It is the
/// shape [`object_store::buffered::BufWriter::new`] takes **by value**, so
/// [`ParquetStager`](crate::staging::ParquetStager) has to hold one to clone per file; erasing here rather
/// than at each writer is what keeps that a single `Arc` for the process. (Contrast the transformer,
/// which needs no writer and so keeps its client concrete.)
fn build_object_store(cfg: &ObjectStoreConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region);
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }
    Ok(Arc::new(builder.build()?))
}

/// Prove write→read→delete round-trips against the bucket. A `head` alone is not enough — some
/// S3-compatibles answer `head` on a missing bucket ambiguously.
async fn object_store_canary(
    store: &dyn ObjectStore,
    instance: &str,
) -> Result<(), object_store::Error> {
    let key = Path::from(format!("_walrus/canary/{instance}"));
    store
        .put(&key, PutPayload::from_static(CANARY_PAYLOAD))
        .await?;
    let got = store.get(&key).await?.bytes().await?;
    if got.as_ref() != CANARY_PAYLOAD {
        return Err(object_store::Error::Generic {
            store: "canary",
            source: "read-back bytes did not match what was written".into(),
        });
    }
    store.delete(&key).await?;
    Ok(())
}

/// Retry `op` while it returns a *transient* error, backing off up to `deadline`. Returns
/// immediately on success or a *terminal* error; after the deadline, returns the last transient
/// error (whose distinct exit code `main` surfaces).
async fn retry_transient<T, F>(deadline: Instant, what: &str, mut op: F) -> Result<T, Error>
where
    F: AsyncFnMut() -> Result<T, Error>,
{
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if e.is_terminal() => return Err(e),
            Err(e) => {
                let now = Instant::now();
                if now >= deadline {
                    // Names the dependency `main` cannot see; the error itself is propagated and
                    // logged there once, when it becomes the process exit code. The per-attempt
                    // `warn!` below is the absorbed-retry record, so nothing is lost by omitting it.
                    tracing::error!(
                        dependency = what,
                        "dependency still unavailable at the startup deadline"
                    );
                    return Err(e);
                }
                let wait = backoff.min(deadline.saturating_duration_since(now));
                tracing::warn!(
                    dependency = what,
                    retry_in = ?wait,
                    error = ?e,
                    "dependency unavailable (transient); retrying"
                );
                tokio::time::sleep(wait).await;
                backoff = next_backoff(backoff);
            }
        }
    }
}

#[cfg(test)]
#[path = "bootstrap_test.rs"]
mod tests;
