//! The self-heal-and-reclaim job (transformer §5.7, §9.4) — the **only** thing that actually reclaims disk.
//!
//! **DuckDB storage truth:** a `DELETE` merely tombstones rows (the file does not shrink), and
//! `VACUUM FULL` is unimplemented. Space is reclaimed only by rewriting the table — here, an atomic
//! `CREATE OR REPLACE TABLE <table> AS SELECT …` over **retained raw ∪ the current mirror injected as an
//! LSN-floor baseline**, dropping `op='d'` winners. It reuses the incremental transform's dedup/collapse
//! (via [`TransformSql::render_rebuild`]) so the two paths can't drift, and the mirror baseline
//! guarantees a value whose last real write was already pruned is **never lost**.
//!
//! It runs inside this table's worker **task** on the shared `LocalSet` driver thread, serialized after
//! an apply cycle (no separate connection, no quiescing dance), holds the exclusive writer, and needs
//! ~2× transient space for the rewrite. Because every table task shares the LocalSet driver,
//! a long rebuild can delay sibling tables; isolate owned connections only if that stall becomes
//! measurable.

use crate::duck::TableDb;
use crate::duck_ext::{DuckResultExt, duck_err};
use crate::error::TransformerError;
use crate::transform::TransformSql;
use common::Lsn;
use tokio_util::sync::CancellationToken;

/// Atomic full-rebuild: `CREATE OR REPLACE TABLE <table> AS SELECT <collapse over retained raw ∪
/// mirror-baseline, drop op='d'>`. Wrapped in one DuckDB transaction so a crash mid-rewrite rolls back
/// to the intact old mirror (readers on another connection see the old table until COMMIT). Reuses the
/// transform's dedup/collapse (TRUNCATE tuple boundary, TOAST resolution, `(commit_lsn, lsn)` ranking).
///
/// The `cancel` token is the abort hook: checked before the rewrite starts, and — via
/// [`full_rebuild_abortable`], which interrupts the running DuckDB query — an in-flight rewrite that is
/// interrupted rolls back and returns `Ok(())` (an intentional drain abort; the idempotent rebuild
/// re-runs next cycle). Only a genuine (non-cancel) failure is an error.
///
/// # Errors
///
/// Returns [`TransformerError::LsnParse`] if a retained truncate boundary is corrupt, or
/// [`TransformerError::Duck`] if beginning, executing, or committing the atomic rebuild fails for a reason
/// other than cancellation.
pub fn full_rebuild(
    db: &TableDb,
    t: &TransformSql,
    cancel: &CancellationToken,
) -> Result<(), TransformerError> {
    if cancel.is_cancelled() {
        return Ok(()); // shutting down — don't even start the heavy rewrite
    }
    // Rebuild over ALL retained raw (from LSN 0) plus the mirror baseline; the truncate boundary comes
    // from the retained tail exactly as the incremental path resolves it.
    let boundary = t.latest_truncate(db.conn(), Lsn::ZERO)?;
    let rebuild_op = format!("full rebuild {}", t.table());
    let rewrite = db.in_txn("rebuild", |conn| {
        conn.execute_batch(&t.render_rebuild(boundary))
            .map_err(|source| duck_err(rebuild_op.clone(), source))
    });
    match rewrite {
        Ok(()) => Ok(()),
        Err(TransformerError::Duck { op, .. }) if cancel.is_cancelled() && op == rebuild_op => {
            // Interrupted by the drain (the watcher called `interrupt()`): an intentional abort, the old
            // mirror is intact because `in_txn` rolled back, and the rebuild re-runs next cycle. NOT an
            // error. Begin/commit errors have different operation labels and remain errors even if the
            // token becomes cancelled at the same time.
            tracing::info!(
                table = t.table(),
                "full-rebuild aborted by drain (rolled back)"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// [`full_rebuild`] wrapped so an in-flight rewrite is **aborted** the instant `cancel` fires.
/// The blocking `CREATE OR REPLACE` runs inside this worker task on the shared `LocalSet` driver
/// thread; a watcher task on the runtime pool holds the connection's [`InterruptHandle`](duckdb)
/// (`Send + Sync`, asserted in [`crate::duck`]) and calls `interrupt()` on cancellation, which makes
/// the running query error →
/// `full_rebuild` rolls back and returns `Ok`. The watcher is aborted once the rewrite returns
/// (whether it completed or was interrupted).
///
/// # Errors
///
/// Returns the [`TransformerError::LsnParse`] or [`TransformerError::Duck`] produced by [`full_rebuild`]; a
/// cancellation-triggered DuckDB interrupt is deliberately converted to `Ok(())`.
pub async fn full_rebuild_abortable(
    db: &TableDb,
    t: &TransformSql,
    cancel: &CancellationToken,
) -> Result<(), TransformerError> {
    let handle = db.conn().interrupt_handle();
    let watch = cancel.clone();
    let watcher = tokio::spawn(async move {
        watch.cancelled().await;
        handle.interrupt(); // cancel the running rewrite from another thread
    });
    let result = full_rebuild(db, t, cancel);
    watcher.abort();
    result
}

/// Reclaim: `DELETE FROM <table>_raw WHERE commit_lsn < floor` then `CHECKPOINT`. The `floor` must be
/// **behind `transformed_lsn`** (the caller guarantees it) so the incremental transform never loses a row
/// it still needs — and the rebuild's mirror baseline preserves any current value whose raw was pruned.
/// Returns the rows deleted. (The `DELETE` only tombstones; the space itself is reclaimed by the rebuild
/// above — assert reclamation before/after `full_rebuild`, not after `prune_raw`.)
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if deleting the retained tail or checkpointing the database fails.
pub fn prune_raw(
    conn: &duckdb::Connection,
    t: &TransformSql,
    floor: Lsn,
) -> Result<u64, TransformerError> {
    // The delete target comes back from `TransformSql::to_raw` tagged `DuckTable<Raw>` rather than
    // being re-suffixed here: this `DELETE` pointed at the mirror would erase every current row, so
    // the one place that derives `<table>_raw` is the typed-name layer.
    let raw = t.to_raw();
    let n = conn
        .execute(
            &format!(
                "DELETE FROM \"{}\" WHERE _walrus_commit_lsn < ?",
                raw.as_str()
            ),
            duckdb::params![floor.to_string()],
        )
        .duck_with(|| format!("prune {}", raw.as_str()))?;
    conn.execute_batch("CHECKPOINT;")
        .duck_with(|| format!("checkpoint after prune {}", t.table()))?;
    Ok(u64::try_from(n).unwrap_or(u64::MAX))
}

/// DuckLake raw pruning. Unlike [`prune_raw`], this does not issue the global `CHECKPOINT`
/// statement: per-table merge/rewrite maintenance follows through [`TableDb::maintain_files`], and
/// catalog-wide snapshot expiration is serialized by shard zero.
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if DuckLake cannot delete rows below `floor`.
pub fn prune_raw_ducklake(
    conn: &duckdb::Connection,
    t: &TransformSql,
    floor: Lsn,
) -> Result<u64, TransformerError> {
    let raw = t.to_raw();
    let n = conn
        .execute(
            &format!(
                "DELETE FROM \"{}\" WHERE _walrus_commit_lsn < ?",
                raw.as_str()
            ),
            duckdb::params![floor.to_string()],
        )
        .duck_with(|| format!("prune DuckLake {}", raw.as_str()))?;
    Ok(u64::try_from(n).unwrap_or(u64::MAX))
}

/// The retention floor for a table: `transformed_lsn - retention_lsn_lag`, saturating at 0. Always `<=
/// transformed_lsn`, so pruning below it can never drop a row the incremental transform still reads.
#[must_use = "returns the computed floor; it does not prune anything by itself"]
pub const fn retention_floor(transformed_lsn: Lsn, retention_lsn_lag: u64) -> Lsn {
    transformed_lsn.saturating_sub_bytes(retention_lsn_lag)
}
