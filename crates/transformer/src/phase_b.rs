//! Phase B — transform the un-transformed tail of `<table>_raw` into the mirror `<table>` (transformer §4).
//! Read only `commit_lsn > transformed_lsn`, run the dedup + `MERGE` **inside one DuckDB
//! transaction**, commit, then advance `transformed_lsn = max(commit_lsn)` applied in a **separate**
//! control-DB transaction (the two databases can't share one).
//!
//! **Naturally idempotent:** the watermark bounds what is read and the LWW dedup picks the same winners,
//! so re-running over the same tail produces a byte-identical mirror. A crash between the DuckDB commit
//! and the control advance just re-runs Phase B — no bespoke recovery.

use crate::duck_ext::DuckResultExt;
use crate::error::TransformerError;
use crate::phase_a::{TableCtx, publication_for_build, validate_build_publication};
use crate::plan::TablePlan;
use crate::table_name::{DuckTable, Mirror};
use crate::transform::{TransformSql, apply_transform, apply_transform_ducklake};
use common::{Lsn, PgRelation};

/// Build the transform for a table at its CURRENT reconciled `schema_version`: read the registry
/// (columns + type descriptors) into a [`TablePlan`] (Tier-2 emit/recombine); fall back to the
/// bootstrap relation's scalar shape when there is no registry row (single-version / hermetic setups).
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if the local schema watermark cannot be read,
/// [`TransformerError::Control`] if the registry lookup fails, or [`TransformerError::RegistryDecode`] if the
/// stored relation shape is invalid.
pub(crate) async fn current_transform(ctx: &TableCtx) -> Result<TransformSql, TransformerError> {
    let ver = ctx.db.schema_version()?;
    transform_at_version(ctx, ver, &ctx.table).await
}

async fn transform_at_version(
    ctx: &TableCtx,
    ver: common::SchemaVersionNo,
    physical_table: &str,
) -> Result<TransformSql, TransformerError> {
    match control::read_registry(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, ver).await? {
        Some(r) => {
            // The `schema.table` label is built INSIDE the closure: `map_err` only runs it on a
            // decode failure, so the every-cycle success path allocates nothing.
            let rel: PgRelation = serde_json::from_value(r.columns).map_err(|source| {
                TransformerError::RegistryDecode {
                    table: format!("{}.{}", ctx.schema, ctx.table),
                    version: ver.0,
                    source,
                }
            })?;
            let plan = TablePlan::from_registry(&rel, &r.descriptors)?.for_table(physical_table);
            Ok(TransformSql::from_plan(&plan))
        }
        None => Ok(TransformSql::from_plan(
            &TablePlan::tier1(&ctx.rel)?.for_table(physical_table),
        )),
    }
}

/// Persist the control-plane no-more-manifests-through-H barrier in its own transaction. That
/// transaction must commit before Duck swaps generations; otherwise a source publisher could land
/// older work in the gap between an unlocked "drained" read and the swap.
async fn seal_manifest_prefix(
    ctx: &TableCtx,
    publication: &control::ReloadPublication,
) -> Result<bool, TransformerError> {
    let mut tx = ctx
        .pool
        .begin()
        .await
        .map_err(|source| TransformerError::ControlTxn {
            op: "begin manifest publication seal",
            source,
        })?;
    let sealed = control::reload::seal_publication_if_drained(
        &mut tx,
        publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|source| TransformerError::ControlTxn {
            op: "commit manifest publication seal",
            source,
        })?;
    Ok(sealed)
}

/// One Phase-B pass. Returns the max `commit_lsn` applied, or `None` if the tail was empty.
///
/// # Errors
///
/// Returns [`TransformerError::Control`] for checkpoint/registry/watermark operations,
/// [`TransformerError::Duck`] for local scan or transaction failures, [`TransformerError::LsnParse`] for an
/// invalid stored watermark, or [`TransformerError::RegistryDecode`] for an invalid relation shape.
pub async fn run_phase_b(ctx: &TableCtx) -> Result<Option<Lsn>, TransformerError> {
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
        .await?
        .ok_or_else(|| {
            TransformerError::Internal(format!("no checkpoint for {}.{}", ctx.schema, ctx.table))
        })?;
    let build = ctx.db.reload_build()?;
    if let Some(build) = &build
        && build.phase == crate::duck::ReloadPhase::Published
    {
        let receipt = control::reload::read_publication(&ctx.pool, build.reload_id)
            .await?
            .ok_or_else(|| {
                TransformerError::Internal(format!(
                    "Duck has published reload {} but control has no publication receipt",
                    build.reload_id
                ))
            })?;
        validate_build_publication(ctx, build, &receipt)?;
        if receipt.status == control::ReloadStatus::Complete {
            ctx.db
                .clear_reload_publication(build.reload_id, build.publication_nonce)?;
            return Ok(Some(build.final_lsn));
        }
        let publication = publication_for_build(ctx, build).await?;
        finish_published_reload(ctx, build, &publication).await?;
        return Ok(Some(build.final_lsn));
    }
    let after = build
        .as_ref()
        .map_or(cp.transformed_lsn, |build| build.transformed_lsn);
    // Phase-B transform lag = raw_appended_lsn − transformed_lsn; pure math from the checkpoint
    // just read, no extra query. Labelled per table (bounded cardinality).
    common::metrics::set_transform_lag(&ctx.series, cp.raw_appended_lsn - cp.transformed_lsn);

    // The max commit LSN in the tail we (re)transform, bounded `>= transformed_lsn` (16-hex text
    // sorts as the LSN, so `max` = latest). The `>=` is load-bearing for the snapshot/stream
    // boundary: equal-`lsn_end` snapshot files carry `commit_lsn =
    // consistent_point`, and if a later transformer batch appends one *after* `transformed_lsn` already reached
    // that point, a strict `>` scan would skip it forever. Re-including the boundary re-applies rows the
    // mirror already has — the per-PK `_applied_*` guard makes those a no-op, so the mirror stays exact
    // (`max()` is NULL only when `<table>_raw` is empty). A source that sits idle at the boundary re-scans
    // that one commit's rows each poll; normal streaming advances `transformed_lsn` past it immediately.
    let physical_table = build
        .as_ref()
        .map_or(ctx.table.as_str(), |build| build.shadow_table.as_str());
    let conn = ctx.db.conn();
    let raw = DuckTable::<Mirror>::new(physical_table).to_raw();
    let max_hex: Option<String> = conn
        .query_row(
            &format!(
                "SELECT max(_walrus_commit_lsn) FROM \"{}\" WHERE _walrus_commit_lsn >= ?",
                raw.as_str()
            ),
            [after.to_string()],
            |r| r.get(0),
        )
        .duck("scan un-transformed tail")?;
    let mut applied = None;
    if let Some(max_hex) = max_hex {
        let max_lsn: Lsn = max_hex
            .parse()
            .map_err(|source| TransformerError::LsnParse {
                field: "max commit_lsn",
                source,
            })?;

        // A live transform uses the canonical schema watermark; a reload transform uses the frozen
        // version persisted with its shadow. In both cases the physical name is explicit.
        let version = build
            .as_ref()
            .map_or(ctx.db.schema_version()?, |build| build.schema_version);
        let t = transform_at_version(ctx, version, physical_table).await?;
        let ducklake = ctx.db.is_ducklake();
        ctx.db.in_txn("transform", |conn| {
            if ducklake {
                apply_transform_ducklake(conn, &t, after)
            } else {
                apply_transform(conn, &t, after)
            }
        })?;

        // Advance the watermark AFTER the DuckDB commit. The CHECK
        // (transformed_lsn <= raw_appended_lsn) holds because Phase A ran first this cycle.
        if let Some(build) = &build {
            ctx.db
                .advance_reload_transformed(build.reload_id, build.publication_nonce, max_lsn)?;
        } else {
            control::advance_transformed(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, max_lsn)
                .await?;
        }
        if max_lsn > after {
            tracing::info!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                transformed = %max_lsn,
                generation = physical_table,
                "Phase B: mirror updated, transformed_lsn advanced"
            );
        } else {
            tracing::debug!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                transformed = %max_lsn,
                generation = physical_table,
                "Phase B: boundary re-transform, transformed_lsn unchanged"
            );
        }
        applied = Some(max_lsn);
    }

    let Some(build) = build else {
        return Ok(applied);
    };

    let publication = publication_for_build(ctx, &build).await?;
    if !seal_manifest_prefix(ctx, &publication).await? {
        return Ok(applied);
    }
    let current = ctx.db.reload_build()?.ok_or_else(|| {
        TransformerError::Internal(format!(
            "reload {} lost its local building receipt before publication",
            build.reload_id
        ))
    })?;
    if current.raw_appended_lsn != current.transformed_lsn {
        return Err(TransformerError::Internal(format!(
            "reload {} queue drained at H but local transformed frontier {} trails raw {}",
            current.reload_id, current.transformed_lsn, current.raw_appended_lsn
        )));
    }
    ctx.db
        .seal_reload_at_h(build.reload_id, build.publication_nonce, build.final_lsn)?;
    if ctx.db.publish_reload_shadow(&ctx.table, build.reload_id)? {
        // Only publication replaces the quarantined live generation. Beginning an export must not
        // make readiness healthy while users still see the old incompatible table.
        ctx.state.clear_table_quarantine(&ctx.schema, &ctx.table);
        tracing::info!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            reload_id = %build.reload_id,
            final_lsn = %build.final_lsn,
            "reload shadow atomically published at its explicit end marker"
        );
    }
    let published = ctx.db.reload_build()?.ok_or_else(|| {
        TransformerError::Internal(format!(
            "reload {} published without leaving a durable Duck receipt",
            build.reload_id
        ))
    })?;
    finish_published_reload(ctx, &published, &publication).await?;
    Ok(Some(
        applied.map_or(build.final_lsn, |lsn| lsn.max(build.final_lsn)),
    ))
}

async fn finish_published_reload(
    ctx: &TableCtx,
    build: &crate::duck::ReloadBuild,
    publication: &control::ReloadPublication,
) -> Result<(), TransformerError> {
    validate_build_publication(ctx, build, publication)?;
    if build.phase != crate::duck::ReloadPhase::Published {
        return Err(TransformerError::Internal(format!(
            "reload {} cannot finish control publication before Duck is published",
            build.reload_id
        )));
    }
    if !seal_manifest_prefix(ctx, publication).await? {
        return Err(TransformerError::Internal(format!(
            "reload {} was published before its manifest prefix through H was sealed",
            build.reload_id
        )));
    }
    let transitioned = control::reload::finish_publication(
        &ctx.pool,
        publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?;
    ctx.db
        .clear_reload_publication(build.reload_id, build.publication_nonce)?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %build.reload_id,
        final_lsn = %build.final_lsn,
        transitioned,
        "reload publication complete; canonical checkpoints advanced to H"
    );
    Ok(())
}
