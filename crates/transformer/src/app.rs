//! The transformer's service orchestration — everything the pod does between a validated config and an
//! exit code.
//!
//! [`run`] owns the whole lifecycle: signal handlers, the health server bound before the slow
//! bootstrap, the apply-worker fleet on its `LocalSet`, and the ordered wind-down. It hands failures
//! back as a [`TransformerError`] value, leaving `src/main.rs` with only what cannot live in a library —
//! the config load, the one `init_tracing` install, the runtime build, and the single
//! error → `ExitCode` mapping. That split is what puts this module behind the `transformer` *library*, so
//! `crates/transformer/tests/` — which links the library and never sees the binary — can reach it.

use crate::bootstrap;
use crate::config::{LeaseTtl, TransformerConfig};
use crate::error::TransformerError;
use crate::health::{self, TransformerState};
use crate::lease;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// The transformer lifecycle, from the signal handlers to the last released lease.
///
/// Binds the health endpoints *before* the bootstrap so `/startup` answers 503 while the lease and
/// the DuckDB opens proceed, then drains that server however the pipeline ended.
///
/// # Errors
///
/// Returns [`TransformerError::Health`] when the endpoints cannot be bound, or when the server task
/// cannot be joined or fails while serving. Every other variant reaches here from the pipeline
/// below; the caller logs the chain with `?e` and surfaces its exit code.
pub async fn run(cfg: TransformerConfig) -> Result<(), TransformerError> {
    let token = crate::shutdown::install_signal_handlers();
    let state = TransformerState::new();
    // Install the Prometheus recorder before anything can serve /metrics or emit a series.
    common::metrics::init();

    // Bind health *before* bootstrap so `/startup` answers 503 while leases, catalog fences, and
    // DuckLake attachments are established.
    let listener = tokio::net::TcpListener::bind(cfg.transformer.health_addr)
        .await
        .map_err(|source| TransformerError::Health {
            op: "bind",
            source: Box::new(source),
        })?;
    tracing::info!(addr = %cfg.transformer.health_addr, "health endpoints listening; bootstrapping");
    let server = tokio::spawn(health::serve_on(
        listener,
        Arc::clone(&state),
        token.clone(),
    ));

    let result = crate::shutdown::cancel_on_exit(&token, pipeline(&cfg, &token, &state)).await;
    server
        .await
        .map_err(|source| TransformerError::Health {
            op: "join",
            source: Box::new(source),
        })?
        .map_err(|source| TransformerError::Health {
            op: "serve",
            source: source.into(),
        })?;
    result
}

/// The fallible middle of the transformer lifecycle. The caller holds a cancellation drop guard for
/// this future's entire lifetime, so every early return winds down token-driven side tasks.
async fn pipeline(
    cfg: &TransformerConfig,
    token: &CancellationToken,
    state: &Arc<TransformerState>,
) -> Result<(), TransformerError> {
    let pool = control::connect(cfg.common.control_db_url.expose()).await?;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(build_store(cfg)?);
    let s3 = duck_s3_access(cfg);

    // `&store` unsize-coerces to the `&dyn ObjectStore` bootstrap takes, so the concrete client is
    // erased at that one boundary and nowhere else.
    let bootstrapped = bootstrap::bootstrap(cfg, &pool, store.as_ref(), &s3, state).await?;
    let bootstrap::BootstrapResult {
        epoch,
        tables: owned,
        catalog_fence,
    } = bootstrapped;
    let current = control::read_current_epoch(&pool).await?.ok_or_else(|| {
        TransformerError::Internal("control epoch disappeared after transformer bootstrap".into())
    })?;
    if current.epoch != epoch {
        return Err(TransformerError::EpochBumped {
            from: epoch,
            to: current.epoch,
        });
    }
    if current.status == control::ReplicationStatus::TotalRestart {
        state.mark_generation_retired();
        return Err(TransformerError::Internal(format!(
            "control generation {epoch} is retired by a pending total restart; refusing to serve it"
        )));
    }
    let generation_ready_watch = (current.status == control::ReplicationStatus::Bootstrapping)
        .then(|| {
            state.mark_reconciling();
            crate::epoch::spawn_generation_ready_watch(
                pool.clone(),
                epoch,
                cfg.transformer.poll_interval,
                Arc::clone(state),
                token.clone(),
            )
        });
    if generation_ready_watch.is_none() {
        state.mark_ready();
    }
    // Local bootstrap itself is progress. This keeps a legitimate zero-table shard live while the
    // global all-table request is still reconciling.
    state.stamp_poll();
    let held_leases: Vec<lease::HeldLease> = owned
        .iter()
        .map(bootstrap::OwnedTable::to_held_lease)
        .collect();
    let keys: Vec<(String, String)> = held_leases
        .iter()
        .map(|lease| (lease.schema.clone(), lease.table.clone()))
        .collect();
    // Zero-init every per-table transformer series so /metrics lists the owned tables from the first scrape,
    // before any apply cycle has moved a needle.
    for (schema, table) in &keys {
        common::metrics::init_table_series(&format!("{schema}.{table}"));
    }
    tracing::info!(
        tables = keys.len(),
        generation_ready = generation_ready_watch.is_none(),
        "bootstrap complete; starting apply loops"
    );

    // Keep the lease alive off the apply thread until SIGTERM. `load()` already admitted this TTL;
    // re-parsing it here is what hands the renewer the proof instead of a bare duration.
    let renewer = lease::spawn_renewer(
        pool.clone(),
        epoch,
        held_leases.clone(),
        cfg.transformer.instance.clone(),
        LeaseTtl::new(cfg.transformer.lease_ttl)?,
        token.clone(),
    );
    let (epoch_rx, epoch_watch) = crate::epoch::spawn_epoch_watch(
        pool.clone(),
        epoch,
        cfg.transformer.poll_interval,
        Arc::clone(state),
        token.clone(),
    );
    let maintenance = (cfg.transformer.effective_shard_index()? == 0).then(|| {
        spawn_catalog_maintenance(cfg.transformer.ducklake.clone(), s3.clone(), token.clone())
    });

    // One apply loop per owned table. DuckDB's `Connection` is `Send + !Sync` because it holds
    // an interior `RefCell`, so a future holding `&TableCtx` is not `Send` and cannot go to
    // `tokio::spawn`. The loops run on a `LocalSet` (this thread), the whole parallelism model being
    // one worker task per attached source table. Those tasks all share this one driver thread, so a long
    // compaction stalls its siblings; isolate owned connections only if profiling justifies it.
    let local = tokio::task::LocalSet::new();
    let (failures_tx, failures_rx) = crate::supervisor::failure_channel(keys.len());
    let fence_stop = CancellationToken::new();
    let fence_failures = failures_tx.clone();
    let fence_watch = tokio::spawn({
        let fence_stop = fence_stop.clone();
        let period = cfg.transformer.lease_ttl / 3;
        async move {
            if let Err(error) = catalog_fence.watch(period, fence_stop).await {
                crate::supervisor::report(
                    &fence_failures,
                    crate::supervisor::WorkerFailure {
                        schema: "_walrus".to_string(),
                        table: "catalog_fence".to_string(),
                        error,
                    },
                );
            }
        }
    });
    // A `JoinSet` rather than a `Vec<JoinHandle>`: the worker count is whatever bootstrap owns, so
    // this is a dynamic collection, and the set owns the whole fleet as one value. The drain below
    // then reaps each worker in COMPLETION order instead of index order (a panic is logged when it
    // happens, not after every earlier worker has stopped), and an early exit out of this function
    // aborts the survivors instead of detaching them.
    let mut workers = tokio::task::JoinSet::new();
    for o in owned {
        // `apply_loop` consumes the ctx, so the failure report needs names of its own: these two
        // locals are the `async move` block's captures. Only ONE side clones — `series` is built
        // while both names are still borrowable, then the ctx takes `o`'s originals, because this
        // `OwnedTable` dies with the iteration and has nothing left to keep them for.
        let schema = o.schema.clone();
        let table = o.table.clone();
        let series = format!("{}.{}", o.schema, o.table);
        let ctx = crate::phase_a::TableCtx {
            pool: pool.clone(),
            epoch,
            epoch_rx: epoch_rx.clone(),
            owner_pod: cfg.transformer.instance.clone(),
            fencing_token: o.fencing_token,
            store: Arc::clone(&store),
            staging_bucket: cfg.common.object_store.bucket.clone(),
            schema: o.schema,
            table: o.table,
            series,
            rel: o.relation,
            db: o.db,
            state: Arc::clone(state),
            max_files: cfg.transformer.max_files_per_cycle,
            max_integrity_resnapshots: cfg.transformer.max_integrity_resnapshots,
            poll_interval: cfg.transformer.poll_interval,
            compaction_interval: cfg.transformer.compaction_interval,
            retention_lsn_lag: cfg.transformer.retention_lsn_lag,
            pause_logged: Default::default(),
        };
        let worker_token = token.clone();
        let failures_tx = failures_tx.clone();
        // `spawn_local_on`, not `spawn_local`: this `LocalSet` is not running yet — these tasks
        // start when `run_until` below first drives it, exactly as they did before.
        workers.spawn_local_on(
            async move {
                if let Err(error) = crate::apply_loop::apply_loop(ctx, worker_token).await {
                    crate::supervisor::report(
                        &failures_tx,
                        crate::supervisor::WorkerFailure {
                            schema,
                            table,
                            error,
                        },
                    );
                }
            },
            &local,
        );
    }
    let no_workers = workers.is_empty();
    let idle_token = token.clone();
    // Workers consume cloned epoch receivers. A zero-table shard has no clone, so retain the
    // original and turn a forward move into the same supervised failure a table worker would
    // report. Dropping it here would close the epoch poller and leave an empty shard serving a
    // retired generation forever.
    let idle_epoch_guard = if no_workers {
        Some((epoch_rx, failures_tx.clone()))
    } else {
        drop(epoch_rx);
        None
    };
    // The receiver closes when the final worker exits; nothing above may keep an extra sender alive.
    drop(failures_tx);
    let first_failure = local
        .run_until(crate::supervisor::supervise(
            failures_rx,
            token,
            async move {
                // A legitimate shard can be empty (especially while scaling out). Keep that pod
                // alive and its catalog-fence watcher fenced instead of returning success into a
                // StatefulSet restart loop. It still observes epoch changes: otherwise a shard
                // with no table worker would remain ready on a retired generation indefinitely.
                if let Some((epoch_rx, failures_tx)) = idle_epoch_guard {
                    guard_idle_epoch(epoch_rx, epoch, idle_token, failures_tx).await;
                }
                // Once the supervisor sees a failure it cancels `token`, so healthy workers leave
                // their loops and this drain always makes progress. It must run to completion:
                // `supervise` only returns when this future does, and the lease release below depends
                // on every worker having been joined (see the drop-order note).
                while let Some(joined) = workers.join_next().await {
                    if let Err(error) = joined {
                        tracing::error!(error = ?error, "apply worker panicked");
                    }
                }
            },
        ))
        .await;

    // DROP ORDER, load-bearing (see `transformer::shutdown` steps 4-5): each `join_next` above joined a
    // worker task, which dropped its `TableCtx` and transient DuckDB connection. Then stop the
    // dedicated catalog session, releasing all table advisory locks, and only then release the
    // control leases. The two fences come off in reverse bootstrap order; keep `release_all` after
    // both joins, never beside them.
    tracing::info!("workers drained (DuckLake connections closed); releasing catalog fence");
    fence_stop.cancel();
    if let Err(error) = fence_watch.await {
        tracing::error!(error = ?error, "DuckLake catalog-fence task panicked");
    }
    tracing::info!("catalog advisory locks released; releasing ownership leases");
    if let Err(error) = epoch_watch.await {
        tracing::error!(error = ?error, "epoch watch task panicked");
    }
    if let Some(generation_ready_watch) = generation_ready_watch
        && let Err(error) = generation_ready_watch.await
    {
        tracing::error!(error = ?error, "generation readiness task panicked");
    }
    if let Some(maintenance) = maintenance
        && let Err(error) = maintenance.await
    {
        tracing::error!(error = ?error, "DuckLake maintenance task panicked");
    }
    // Cancel-then-join, never `abort()`: the renewer already observes this token, and `abort()` only
    // *requests* a stop — it returns while an in-flight renewal may still be on the wire. Exact
    // owner+token guards now make a renewal from an older acquisition harmless after a successor
    // acquires, but ordering the final renewal before release still makes this process's graceful
    // handoff immediate and deterministic. The join is bounded by the same control-DB latency
    // `release_all` is about to pay (the cancellation arm is `biased`, so at most the current round).
    token.cancel();
    if let Err(error) = renewer.await {
        tracing::error!(error = ?error, "lease renewer task panicked");
    }
    lease::release_all(&pool, epoch, &held_leases, &cfg.transformer.instance).await;
    if let Some(failure) = first_failure {
        return Err(failure.error);
    }
    Ok(())
}

/// Give a zero-table shard the same total-restart guard normally supplied by an apply worker.
///
/// The caller runs this inside [`crate::supervisor::supervise`]'s drain future. Reporting through
/// that channel (rather than merely returning) preserves the typed terminal error and lets the
/// supervisor cancel every token-driven side task before the ordered joins begin.
async fn guard_idle_epoch(
    mut epoch_rx: tokio::sync::watch::Receiver<common::EpochNo>,
    baseline: common::EpochNo,
    token: CancellationToken,
    failures: tokio::sync::mpsc::Sender<crate::supervisor::WorkerFailure>,
) {
    loop {
        let changed = tokio::select! {
            biased;
            () = token.cancelled() => return,
            changed = epoch_rx.changed() => changed,
        };
        if changed.is_err() {
            crate::supervisor::report(
                &failures,
                crate::supervisor::WorkerFailure {
                    schema: "_walrus".to_string(),
                    table: "epoch_guard".to_string(),
                    error: TransformerError::Internal(
                        "epoch watch stopped while an empty shard was still running".into(),
                    ),
                },
            );
            // Keep the supervisor's drain future pending until it consumes the report and cancels
            // the shared token. Returning in the same poll that enqueues the failure could let the
            // concurrently-polled drain branch win before `rx.recv()` is polled again.
            token.cancelled().await;
            return;
        }
        let observed = *epoch_rx.borrow_and_update();
        if observed > baseline {
            crate::supervisor::report(
                &failures,
                crate::supervisor::WorkerFailure {
                    schema: "_walrus".to_string(),
                    table: "epoch_guard".to_string(),
                    error: TransformerError::EpochBumped {
                        from: baseline,
                        to: observed,
                    },
                },
            );
            token.cancelled().await;
            return;
        }
    }
}

fn spawn_catalog_maintenance(
    ducklake: crate::config::DuckLakeConfig,
    s3: crate::duck::S3Access,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(ducklake.maintenance_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tokio::select! {
                biased;
                () = token.cancelled() => return,
                _ = tick.tick() => {}
            }
            match crate::duck::maintain_catalog(&ducklake, &s3) {
                Ok(()) => tracing::info!(
                    retention_seconds = ducklake.snapshot_retention.as_secs(),
                    cleanup_grace_seconds = ducklake.cleanup_grace.as_secs(),
                    "DuckLake catalog maintenance complete"
                ),
                Err(error) => tracing::error!(
                    error = ?error,
                    "DuckLake catalog maintenance failed; retrying next interval"
                ),
            }
        }
    })
}

/// The transformer's one object-store client.
///
/// Returns the concrete `AmazonS3`, not an `Arc<dyn ObjectStore>`: the transformer builds exactly one
/// store, spends it on a single `head` probe in [`bootstrap::bootstrap`], and never clones, shares,
/// or stores it — so the `Arc` allocation and the vtable would both buy nothing here. `extractor`
/// keeps its `Arc<dyn ObjectStore>` for the opposite reason: `object_store::buffered::BufWriter::new`
/// takes one by value, so the erasure there is the upstream API's, not a choice.
fn build_store(cfg: &TransformerConfig) -> Result<AmazonS3, TransformerError> {
    // `from_env` so the AWS credential env (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) is honoured —
    // `new()` alone falls back to the EC2 IMDS credential chain, which hangs/fails off-EC2 (e.g. MinIO).
    let mut b = AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.common.object_store.bucket)
        .with_region(&cfg.common.object_store.region);
    if let Some(endpoint) = &cfg.common.object_store.endpoint {
        b = b.with_endpoint(endpoint).with_allow_http(true);
    }
    b.build().map_err(|source| TransformerError::ObjectStore {
        op: "build S3 client",
        source: Box::new(source),
    })
}

/// DuckDB httpfs credentials for `read_parquet('s3://…')`, from the object-store config + the AWS env
/// (the same `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` the `object_store` client reads). DuckDB wants a
/// scheme-less `host:port` endpoint; the scheme selects TLS.
#[must_use]
pub fn duck_s3_access(cfg: &TransformerConfig) -> crate::duck::S3Access {
    let raw = cfg
        .common
        .object_store
        .endpoint
        .as_deref()
        .unwrap_or_default();
    let (use_ssl, endpoint) = match raw.strip_prefix("https://") {
        Some(host) => (true, host),
        None => (false, raw.strip_prefix("http://").unwrap_or(raw)),
    };
    crate::duck::S3Access {
        endpoint: endpoint.to_string(),
        region: cfg.common.object_store.region.clone(),
        access_key_id: aws_credential("AWS_ACCESS_KEY_ID"),
        secret_access_key: aws_credential("AWS_SECRET_ACCESS_KEY").into(),
        use_ssl,
    }
}

/// One AWS credential from the environment, empty when it is not readable.
///
/// The empty fallback stays: DuckDB httpfs with no key reaches an anonymous bucket, and failing
/// startup here would reject that deployment. What does not stay is the silence. Every
/// configuration walrus ships sets both variables (`.env.example`, `scripts/extractor-smoke.sh`,
/// `scripts/bench-e2e.sh`, the e2e harness), so an unset one is almost always the cause of the
/// opaque 403 a worker meets much later, on its first `read_parquet('s3://…')` — with nothing on
/// disk connecting the two. `unwrap_or_default` dropped exactly the `VarError` that names it.
///
/// What reaches the log is the *variant*, not the error: `VarError::NotUnicode`'s `Display` renders
/// the offending `OsString`, and for `AWS_SECRET_ACCESS_KEY` that string is the credential itself
/// (`obs-no-sensitive-data`). Which of the two happened is the whole diagnostic anyway — it tells an
/// operator whether to set the variable or to fix its encoding.
fn aws_credential(var: &'static str) -> String {
    match std::env::var(var) {
        Ok(value) => value,
        Err(error) => {
            let reason = match error {
                std::env::VarError::NotPresent => "not set",
                std::env::VarError::NotUnicode(_) => "not valid unicode",
            };
            tracing::warn!(
                var,
                reason,
                "AWS credential unreadable from the environment; DuckDB will read the staging \
                 bucket without one — expect an S3 authentication failure unless it is public"
            );
            String::new()
        }
    }
}

#[cfg(test)]
#[path = "app_test.rs"]
mod tests;
