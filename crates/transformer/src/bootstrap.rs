//! The transformer's ordered, fail-fast bootstrap (transformer §8.2). For each owned table: **(1)** acquire the
//! ownership lease (the first fence) → **(2)** take a table-keyed advisory lock on a dedicated
//! DuckLake-catalog PostgreSQL session (the second fence), attach a transient DuckDB connection, and
//! ensure both `<table>` and `<table>_raw` → **(3)** load both
//! checkpoints and assert `transformed_lsn <= raw_appended_lsn`. Then verify the S3 read path once. The
//! **lease precedes the lock precedes any watermark read** — the fence is fully in place before the
//! read-then-write apply cycle starts.

use crate::config::TransformerConfig;
use crate::duck::{S3Access, TableDb};
use crate::error::TransformerError;
use crate::health::TransformerState;
use crate::lease;
use common::{EpochNo, Lsn, PgRelation};
use object_store::ObjectStore;
use sqlx::Connection as _;
use std::collections::HashSet;

/// The two commit-LSN watermarks for one table.
#[derive(Debug, Clone, Copy)]
pub struct Checkpoints {
    /// Phase A frontier — the `<table>_raw` CDC log is durable up to this commit LSN.
    pub raw_appended_lsn: Lsn,
    /// Phase B frontier — the mirror is derived up to this commit LSN. Never ahead of the above.
    pub transformed_lsn: Lsn,
}

/// One table this transformer instance owns after bootstrap.
#[derive(Debug)]
pub struct OwnedTable {
    /// Source schema of the owned table.
    pub schema: String,
    /// Source table name.
    pub table: String,
    /// The table's shape at the version bootstrap resolved, used to render the transform SQL.
    pub relation: PgRelation,
    /// The lease token held for this table — the first of the two fences.
    pub fencing_token: i64,
    /// The open transient DuckDB handle attached to this table's DuckLake namespace. The catalog
    /// session owned by [`BootstrapResult`] holds the second fence.
    pub db: TableDb,
    /// Where the two phases had reached when bootstrap read them.
    pub checkpoints: Checkpoints,
}

/// Tables plus the session that holds their PostgreSQL advisory locks.
#[derive(Debug)]
pub struct BootstrapResult {
    /// Exact generation whose registry, leases, checkpoints, and files were opened.
    pub epoch: EpochNo,
    /// Tables assigned to this shard.
    pub tables: Vec<OwnedTable>,
    /// Catalog-session second fence, held until every table worker has drained.
    pub catalog_fence: CatalogFence,
}

/// One PostgreSQL session holding all table-keyed advisory locks assigned to this transformer shard.
pub struct CatalogFence {
    connection: sqlx::PgConnection,
    held: usize,
}

impl std::fmt::Debug for CatalogFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogFence")
            .field("held", &self.held)
            .finish_non_exhaustive()
    }
}

impl CatalogFence {
    async fn connect(cfg: &TransformerConfig) -> Result<Self, TransformerError> {
        let connection = sqlx::PgConnection::connect(cfg.transformer.ducklake.catalog_url.expose())
            .await
            .map_err(|source| TransformerError::Catalog {
                op: "connect advisory-lock session",
                source,
            })?;
        Ok(Self {
            connection,
            held: 0,
        })
    }

    async fn acquire(
        &mut self,
        _epoch: common::EpochNo,
        schema: &str,
        table: &str,
        instance: &str,
    ) -> Result<(), TransformerError> {
        // Deliberately epoch-independent: all generations reuse one physical DuckLake namespace,
        // so an old worker and its total-restart successor must contend on the same hard fence.
        let key = format!("walrus:{schema}:{table}");
        let acquired: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
                .bind(key)
                .fetch_one(&mut self.connection)
                .await
                .map_err(|source| TransformerError::Catalog {
                    op: "acquire table advisory lock",
                    source,
                })?;
        if !acquired {
            return Err(TransformerError::LeaseContended {
                table: format!("{schema}.{table}"),
                owner: format!("another DuckLake writer (requester {instance})"),
            });
        }
        self.held += 1;
        Ok(())
    }

    /// Keep the lock session observable. A broken session drops every server-side lock and returns
    /// an error so the app supervisor cancels all writers before a successor is admitted.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Catalog`] when the dedicated PostgreSQL session cannot be pinged.
    pub async fn watch(
        mut self,
        period: std::time::Duration,
        stop: tokio_util::sync::CancellationToken,
    ) -> Result<(), TransformerError> {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(()),
                _ = tick.tick() => {}
            }
            sqlx::query("SELECT 1")
                .execute(&mut self.connection)
                .await
                .map_err(|source| TransformerError::Catalog {
                    op: "advisory-lock session health check",
                    source,
                })?;
        }
    }
}

impl OwnedTable {
    /// Clone the exact control-plane ownership capability retained for renewal and release.
    #[must_use]
    pub fn to_held_lease(&self) -> lease::HeldLease {
        lease::HeldLease {
            schema: self.schema.clone(),
            table: self.table.clone(),
            fencing_token: self.fencing_token,
        }
    }
}

/// Run the ordered bootstrap for the current epoch's registered tables. Returns the owned tables (with
/// their open DuckDB connections and lease tokens). Any terminal step returns a classified
/// [`TransformerError`] that `main` maps to an exit code.
///
/// # Errors
///
/// Returns [`TransformerError::Internal`] when no epoch exists, [`TransformerError::RegistryDecode`] for an
/// invalid stored relation, [`TransformerError::LeaseContended`] for a live competing owner, or the
/// classified [`TransformerError::Control`], [`TransformerError::Duck`], [`TransformerError::ObjectStore`], and
/// [`TransformerError::CorruptCheckpoint`] variants from the ordered dependency and invariant checks.
///
/// `store` is `&dyn`, not `&impl ObjectStore`, deliberately: this is a once-per-process async fn
/// whose whole use of the store is one `head` (step 5), so the vtable costs a single indirect call
/// on a cold path — while a generic parameter would monomorphize this entire state machine per
/// store type for nothing. Callers pass their concrete client (`app::build_store`'s `AmazonS3`) and
/// the coercion happens here, at the boundary.
pub async fn bootstrap(
    cfg: &TransformerConfig,
    pool: &sqlx::PgPool,
    store: &dyn ObjectStore,
    s3: &S3Access,
    state: &TransformerState,
) -> Result<BootstrapResult, TransformerError> {
    // The generation to load. The extractor establishes it; until it does, there is nothing to own.
    let epoch = control::read_current_epoch(pool)
        .await?
        .ok_or_else(|| {
            TransformerError::Internal(
                "no epoch established yet (extractor has not bootstrapped)".into(),
            )
        })?
        .epoch;

    let registry = control::read_all_latest_registry(pool, epoch).await?;
    // Unlike `reload_supersede_floor`, this includes requested/exporting attempts before F exists.
    // That distinction matters after an integrity failure: bootstrap must not cast the old live
    // generation forward while the fresh replacement snapshot is still waiting to start.
    let active_rebuilds = control::reload::active_rebuilds(pool, epoch)
        .await?
        .into_iter()
        .map(|reload| (reload.source_schema, reload.source_table))
        .collect::<HashSet<_>>();
    let mut owned = Vec::with_capacity(registry.len());
    let mut catalog_fence = CatalogFence::connect(cfg).await?;
    for row in registry {
        let shard = crate::duck::table_shard(
            epoch,
            &row.source_schema,
            &row.source_table,
            cfg.transformer.shard_count,
        );
        if shard != cfg.transformer.effective_shard_index()? {
            continue;
        }
        let version = row.schema_version;
        let table = format!("{}.{}", row.source_schema, row.source_table);
        let rel: PgRelation = serde_json::from_value(row.columns).map_err(|source| {
            TransformerError::RegistryDecode {
                table,
                version: version.0,
                source,
            }
        })?;

        // (1) FIRST FENCE: the ownership lease — before we ever touch the file.
        let lease = acquire_for_shard(cfg, pool, epoch, &rel.schema, &rel.name).await?;
        if let Err(error) =
            acquire_catalog_for_shard(cfg, &mut catalog_fence, epoch, &rel.schema, &rel.name).await
        {
            // Do not leave a full-TTL availability delay when the hard catalog fence says another
            // writer is still live. The compare-by-owner release cannot disturb that writer's lease.
            lease::release_all(
                pool,
                epoch,
                &[lease::HeldLease {
                    schema: rel.schema.clone(),
                    table: rel.name.clone(),
                    fencing_token: lease.fencing_token,
                }],
                &cfg.transformer.instance,
            )
            .await;
            return Err(error);
        }

        // (2) Attach a transient DuckDB connection to this table's isolated schema in the shared
        // DuckLake + ensure both tables, then (4) reconcile a resumed table whose persisted
        // `_walrus_meta` version is below the registered latest
        // UP TO that version — applying any additive DDL it missed before it processes more data.
        // For a FRESH file this is a no-op (created at the latest shape, watermark already there); the
        // steady-state per-file forward reconcile lives in Phase A.
        let db =
            TableDb::open_ducklake(&cfg.transformer.ducklake, epoch, &rel.schema, &rel.name, s3)?;
        // Total-restart rebuild (§1.8): if this `.duckdb` was built for a retired generation (its
        // `_walrus_meta['epoch']` < the control epoch), wipe its mirror + raw so the new generation's
        // full-table reconciliation rebuilds it. A no-op for a fresh file or a same-epoch resume. Both watermarks reset for
        // free — the new epoch's `transformer_checkpoint` (loaded below) is a fresh `0/0`.
        crate::epoch::rebuild_for_new_epoch(&db, &rel.name, epoch)?;
        // Quarantine-recovery: a pending reload (either persisted flavor) will replace
        // this table's mirror at the reload's version in Phase A. Reconciling the resumed
        // `.duckdb` to the registry's LATEST version here would re-run the very lossy cast that
        // quarantined it (the mirror still holds the un-castable data) and RE-QUARANTINE during
        // bootstrap — before Phase A can ever reach the reload chunk that recovers it. So when a
        // rebuild is pending, ensure the tables at the `.duckdb`'s CURRENT version and SKIP the
        // forward reconcile; the rebuild resets both shape and data. Steady-state has no pending
        // rebuild, so this is the normal ensure-at-latest + reconcile.
        let integrity =
            control::read_integrity_recovery(pool, epoch, &rel.schema, &rel.name).await?;
        let integrity_active = integrity.as_ref().is_some_and(|recovery| {
            matches!(
                recovery.status,
                control::IntegrityRecoveryStatus::Retrying
                    | control::IntegrityRecoveryStatus::Quarantined
            )
        });
        if integrity_active {
            // Seed the process-wide readiness latch before `app::pipeline` calls `mark_ready`.
            // `TransformerState::mark_ready` preserves this per-table set, closing the restart window in
            // which a poisoned table could otherwise briefly serve readiness 200.
            state.quarantine_table(&rel.schema, &rel.name);
        }
        let pending_rebuild =
            integrity_active || active_rebuilds.contains(&(rel.schema.clone(), rel.name.clone()));
        if pending_rebuild {
            // A brand-new `.duckdb` has no persisted version to hold at, so it falls back to the
            // registry's. A *failed* read must not answer with that same fallback: it would send
            // this branch to the exact reconcile target the branch exists to avoid, so the DuckDB
            // error propagates and fails the bootstrap instead.
            let cur = db.stored_schema_version()?.unwrap_or(version);
            let cur_plan =
                match control::read_registry(pool, epoch, &rel.schema, &rel.name, cur).await? {
                    Some(r) => {
                        let decode_table = format!("{}.{}", r.source_schema, r.source_table);
                        let decode_version = r.schema_version;
                        let rel_cur: PgRelation =
                            serde_json::from_value(r.columns).map_err(|source| {
                                TransformerError::RegistryDecode {
                                    table: decode_table,
                                    version: decode_version.0,
                                    source,
                                }
                            })?;
                        crate::plan::TablePlan::from_registry(&rel_cur, &r.descriptors)
                    }
                    None => crate::plan::TablePlan::from_registry(&rel, &row.descriptors),
                }?;
            db.ensure_tables_planned(&cur_plan, cur)?;
            db.set_built_epoch(epoch)?;
            tracing::warn!(
                table = %format_args!("{}.{}", rel.schema, rel.name),
                current_version = %cur,
                registry_version = %version,
                "bootstrap: a rebuild reload is pending — skipping the forward reconcile (Phase A rebuilds this table)"
            );
        } else {
            // Build the DuckDB shape from the registry descriptors (Tier-2 emit/recombine);
            // with no descriptors this is the plain scalar shape. Stamp the built generation, then
            // reconcile a RESUMED file forward to the registered latest version.
            db.ensure_tables_planned(
                &crate::plan::TablePlan::from_registry(&rel, &row.descriptors)?,
                version,
            )?;
            db.set_built_epoch(epoch)?;
            crate::ddl::reconcile_to_version(&db, pool, epoch, &rel.schema, &rel.name, version)
                .await?;
        }

        // Seed comments captured at the source catalog fence (or resume the newest durable
        // metadata revision) before this table is announced as owned and ready.
        crate::ddl::reconcile_comments(&db, pool, epoch, &rel.schema, &rel.name).await?;

        // (3) Load both watermarks (the fence is already held) and assert the DB-enforced invariant.
        control::ensure_checkpoint(pool, epoch, &rel.schema, &rel.name).await?;
        let cp = control::read_checkpoint(pool, epoch, &rel.schema, &rel.name)
            .await?
            .ok_or_else(|| {
                TransformerError::Internal(format!(
                    "checkpoint missing after ensure for {}.{}",
                    rel.schema, rel.name
                ))
            })?;
        if cp.transformed_lsn > cp.raw_appended_lsn {
            return Err(TransformerError::CorruptCheckpoint {
                table: format!("{}.{}", rel.schema, rel.name),
            });
        }

        tracing::info!(
            table = %format_args!("{}.{}", rel.schema, rel.name),
            fencing_token = lease.fencing_token,
            raw_appended = %cp.raw_appended_lsn,
            transformed = %cp.transformed_lsn,
            shard,
            "owned: lease held, DuckLake attached, watermarks loaded"
        );
        owned.push(OwnedTable {
            schema: rel.schema.clone(),
            table: rel.name.clone(),
            relation: rel,
            fencing_token: lease.fencing_token,
            db,
            checkpoints: Checkpoints {
                raw_appended_lsn: cp.raw_appended_lsn,
                transformed_lsn: cp.transformed_lsn,
            },
        });
    }

    // (5) Verify the S3 read path once (a `head` on a probe key: NotFound proves reachability).
    verify_s3_read(store).await?;

    // Liveness: stamp one poll so an idle-but-healthy transformer is `/healthz` green (no poll loop yet).
    state.stamp_poll();
    Ok(BootstrapResult {
        epoch,
        tables: owned,
        catalog_fence,
    })
}

async fn acquire_for_shard(
    cfg: &TransformerConfig,
    pool: &sqlx::PgPool,
    epoch: common::EpochNo,
    schema: &str,
    table: &str,
) -> Result<control::Lease, TransformerError> {
    let deadline = tokio::time::Instant::now() + cfg.transformer.startup_deadline;
    loop {
        match lease::acquire(
            pool,
            epoch,
            schema,
            table,
            &cfg.transformer.instance,
            cfg.transformer.lease_ttl,
        )
        .await
        {
            Ok(lease) => return Ok(lease),
            Err(TransformerError::LeaseContended { .. })
                if cfg.transformer.shard_count.get() > 1
                    && tokio::time::Instant::now() < deadline =>
            {
                tracing::info!(schema, table, "waiting for shard ownership handoff");
                tokio::time::sleep(
                    (cfg.transformer.lease_ttl / 3).min(std::time::Duration::from_secs(5)),
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn acquire_catalog_for_shard(
    cfg: &TransformerConfig,
    fence: &mut CatalogFence,
    epoch: common::EpochNo,
    schema: &str,
    table: &str,
) -> Result<(), TransformerError> {
    let deadline = tokio::time::Instant::now() + cfg.transformer.startup_deadline;
    loop {
        match fence
            .acquire(epoch, schema, table, &cfg.transformer.instance)
            .await
        {
            Ok(()) => return Ok(()),
            Err(TransformerError::LeaseContended { .. })
                if cfg.transformer.shard_count.get() > 1
                    && tokio::time::Instant::now() < deadline =>
            {
                tracing::info!(schema, table, "waiting for DuckLake advisory-lock handoff");
                tokio::time::sleep(
                    (cfg.transformer.lease_ttl / 3).min(std::time::Duration::from_secs(5)),
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Prove the staging bucket is readable. A `NotFound` on a probe key means "reachable, key absent" —
/// exactly what we want; any other error is a real object-store failure.
async fn verify_s3_read(store: &dyn ObjectStore) -> Result<(), TransformerError> {
    let probe = object_store::path::Path::from("__walrus_transformer_probe__");
    classify_s3_read(store.head(&probe).await)
}

fn classify_s3_read(
    result: object_store::Result<object_store::ObjectMeta>,
) -> Result<(), TransformerError> {
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(source) => Err(TransformerError::ObjectStore {
            op: "staging bucket not readable",
            source: Box::new(source),
        }),
    }
}

#[cfg(test)]
#[path = "bootstrap_test.rs"]
mod tests;
