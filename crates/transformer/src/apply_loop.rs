//! The per-table apply loop (transformer §8.4) — **one worker and transient DuckDB connection per source
//! table**. Each poll interval: Phase A (append) then Phase B (transform), then stamp
//! `last_poll_completed_at` — **every** cycle, even a no-op poll, so an idle-but-healthy transformer stays
//! `/healthz` green. On a **slower, distinct** cadence, and on THIS same worker thread (serialized right
//! after an apply cycle — no quiescing dance), it runs the full-rebuild + retention prune.
//! Exits cleanly on the shutdown token.

use crate::error::TransformerError;
use crate::phase_a::{TableCtx, run_phase_a};
use crate::phase_b::run_phase_b;
use common::{EpochNo, Lsn};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Return the terminal total-restart error when a running worker observes a newer generation than
/// its loop-start baseline.
pub(crate) fn epoch_guard(
    observed: EpochNo,
    baseline: EpochNo,
    started_at: EpochNo,
) -> Result<(), TransformerError> {
    if observed > baseline {
        Err(TransformerError::EpochBumped {
            from: started_at,
            to: observed,
        })
    } else {
        Ok(())
    }
}

/// Drive one owned table until `shutdown`. Phase A + Phase B share one poll cadence in v1 (two txns);
/// compaction runs on its own slower cadence on this thread.
///
/// # Errors
///
/// Returns [`TransformerError::EpochBumped`] when the control generation advances while this worker is
/// running. Control-plane, DuckDB, registry, quarantine, and watermark failures from the two phases
/// or compaction propagate through their corresponding [`TransformerError`] variants and are terminal.
///
/// # Panics
///
/// Panics if `ctx.poll_interval` is zero — [`tokio::time::interval`] rejects a zero period. The
/// transformer's config admits no zero cadence, so only a hand-built [`TableCtx`] can reach this.
// One span per worker task: every owned table's loop runs concurrently on the SAME `LocalSet`
// driver thread, so their lines interleave. What the span adds is the context an event cannot
// spell for itself — `duck::in_txn`'s rollback warning, and every `log`-bridged sqlx/DuckDB record
// emitted while this worker is polled, now name the table they belong to. The events below keep
// their own `table` field regardless: a span ADDS context rather than relocating it, since the
// JSON formatter renders event fields under `fields` and span fields under `span`/`spans` — two
// query paths, pinned by `crates/common/src/telemetry_test.rs`.
// `#[instrument]` and not `span.enter()`, because this loop awaits and a guard held across
// `.await` follows the executor onto whichever task it resumed next. `ctx` is skipped (it owns the
// pool and the DuckDB connection); `series` is its precomputed `"<schema>.<table>"`.
#[tracing::instrument(skip_all, fields(table = %ctx.series))]
pub async fn apply_loop(
    ctx: TableCtx,
    shutdown: CancellationToken,
) -> Result<(), TransformerError> {
    let mut tick = tokio::time::interval(ctx.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Track the last compaction so the (slower) rebuild cadence is independent of the poll interval.
    let mut last_compaction = Instant::now();
    // The generation baseline for the total-restart guard: the highest epoch present as of loop start.
    // In production this equals `ctx.epoch` (bootstrap read the MAX), so the guard fires on ANY later
    // bump; using the observed MAX (not `ctx.epoch`) means a shared test control-plane carrying unrelated
    // higher epochs doesn't trip it — only a genuine bump *while this transformer runs* does.
    let baseline_epoch = (*ctx.epoch_rx.borrow()).max(ctx.epoch);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return drain(&ctx),
            _ = tick.tick() => {}
        }
        // Total-restart guard (§1.8): if the control plane opened a newer generation than the one we
        // started on, this transformer is now running a RETIRED epoch. Exit loudly (→ `app::pipeline`
        // cancels the token and the process restarts) so bootstrap wipes + rebuilds every table namespace
        // under the new epoch — never rebuild in place mid-run.
        let observed = *ctx.epoch_rx.borrow();
        epoch_guard(observed, baseline_epoch, ctx.epoch)?;
        // Drain step 2+3: `run_phase_a`/`run_phase_b` are never interrupted mid-flight, so the in-flight
        // cycle FINISHES atomically (append + the control-DB `raw_appended_lsn`+DELETE txn, then the
        // transform + `transformed_lsn`) even if SIGTERM arrives now — no new crash window. A crash
        // BETWEEN the two phases is absorbed by the next cycle's plain re-run (both idempotent).
        run_phase_a(&ctx).await?;
        crate::ddl::reconcile_comments(&ctx.db, &ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
            .await?;
        run_phase_b(&ctx).await?;
        // A reload cutover can replace the mirror during Phase B and clears the local comment
        // watermark atomically with that swap. Reconcile once more so the externally visible
        // transformed checkpoint never remains converged with metadata missing from the new table.
        if ctx.db.applied_comment_ddl_id()?.is_none() {
            crate::ddl::reconcile_comments(&ctx.db, &ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
                .await?;
        }
        ctx.state.stamp_poll();

        // Compaction on its own cadence, serialized AFTER the apply cycle on this same worker thread — it
        // needs the exclusive writer and ~2× transient space, so it can never contend with the writer. Do
        // NOT start a new rebuild once draining (an already-running one is aborted inside `compact`).
        if !shutdown.is_cancelled() && last_compaction.elapsed() >= ctx.compaction_interval {
            compact(&ctx, &shutdown).await?;
            last_compaction = Instant::now();
        }
    }
}

/// Drain one worker on SIGTERM: the in-flight cycle already finished (both watermarks committed).
/// Native test databases checkpoint their local WAL; a DuckLake `CHECKPOINT` means catalog-wide file
/// maintenance and must not run independently in every worker, so production simply closes the
/// already-committed connection. The catalog advisory-lock session and control leases are released by
/// `app::pipeline` only after all workers have drained.
fn drain(ctx: &TableCtx) -> Result<(), TransformerError> {
    if !ctx.db.is_ducklake()
        && let Err(e) = ctx.db.conn().execute_batch("CHECKPOINT;")
    {
        tracing::warn!(table = %format_args!("{}.{}", ctx.schema, ctx.table), error = ?e, "drain CHECKPOINT failed");
    }
    tracing::info!(table = %format_args!("{}.{}", ctx.schema, ctx.table), "apply loop drained (watermarks committed)");
    Ok(())
}

/// One compaction pass: full-rebuild (self-heal + reclaim) then prune raw below the retention floor.
async fn compact(ctx: &TableCtx, shutdown: &CancellationToken) -> Result<(), TransformerError> {
    // The global checkpoint intentionally follows the hidden generation while it catches up to H.
    // Rebuilding/pruning the still-published old generation against that newer frontier would be
    // meaningless work and would complicate recovery evidence, so defer maintenance until cutover.
    if let Some(build) = ctx.db.reload_build()? {
        tracing::debug!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            reload_id = %build.reload_id,
            "compaction deferred while a reload shadow is being reconciled"
        );
        return Ok(());
    }
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?;
    let transformed = cp.map(|c| c.transformed_lsn).unwrap_or(Lsn::ZERO);
    let t = crate::phase_b::current_transform(ctx).await?;

    // The rebuild is abortable: a SIGTERM mid-rewrite interrupts it, rolls back, and returns Ok.
    crate::compaction::full_rebuild_abortable(&ctx.db, &t, shutdown).await?;
    if shutdown.is_cancelled() {
        return Ok(()); // draining — skip the prune, the rebuild was aborted; both re-run next start
    }
    // CREATE OR REPLACE TABLE rebuilds the mirror's physical object and therefore drops its
    // comments. Invalidate and immediately restore the authoritative snapshot before maintenance.
    ctx.db.clear_comment_ddl_id()?;
    crate::ddl::reconcile_comments(&ctx.db, &ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?;
    // Prune only below `transformed_lsn - retention_lsn_lag` (always behind transformed_lsn) — the rebuild
    // just captured every current value into the mirror baseline, so pruned raw can lose nothing.
    let floor = crate::compaction::retention_floor(transformed, ctx.retention_lsn_lag);
    let pruned = if ctx.db.is_ducklake() {
        crate::compaction::prune_raw_ducklake(ctx.db.conn(), &t, floor)?
    } else {
        crate::compaction::prune_raw(ctx.db.conn(), &t, floor)?
    };
    ctx.db.maintain_files(&ctx.table)?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        floor = %floor,
        pruned,
        "compaction: mirror rebuilt (reclaimed), raw pruned below the retention floor"
    );
    Ok(())
}

#[cfg(test)]
#[path = "apply_loop_test.rs"]
mod tests;
