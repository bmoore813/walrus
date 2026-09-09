//! `table_reload` models: the single-table-reload state machine (reload H4/H5/H10).
//!
//! Control-pg owns the reload's brain; every transition below is a **guarded UPDATE** —
//! `UPDATE … WHERE status = <expected>` — so a lost race or an illegal jump changes zero rows and
//! surfaces as the typed [`ControlError::ReloadTransition`], never a silent double-claim. The
//! status walk is `requested → exporting → export_complete → complete`, with `failed` terminal
//! from the two middle states. There is deliberately **no** `superseded` status: a DDL restart
//! is [`fail()`](fail) with an explanatory reason plus a fresh successor row.
//!
//! `reload_id` remains the monotonic attempt identity used by the transformer. Source-WAL requests also
//! carry a UUID idempotency key: one event may fan out to several tables, while replay of the same
//! `(epoch, request UUID, table)` returns the original attempt. Direct control-plane requests keep
//! the historical one-live-request behavior but also persist a private UUID namespace for their
//! source-side fence events.

use crate::{ControlError, parse::ParseEnumError};
use common::string_enum;
use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use sqlx::{Connection, PgConnection, PgExecutor, Postgres, Row, postgres::PgRow};
use uuid::Uuid;

string_enum! {
    /// Persisted spelling of a full-table reconciliation request. `resync` is retained for database
    /// and API compatibility, but is a behavioral alias of `reload`: both pause claims and publish
    /// a hidden-generation full rebuild through the same state machine.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "lowercase")]
    pub enum ReloadFlavor {
        error = ParseEnumError;
        column = "table_reload.flavor";
        Reload => "reload",
        Resync => "resync",
    }
}

string_enum! {
    /// Whether the source event names one table or is one child of an all-published-tables fanout.
    /// Every persisted `table_reload` row still represents exactly one concrete target table.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "snake_case")]
    pub enum ReloadScope {
        error = ParseEnumError;
        column = "table_reload.request_scope";
        Table => "table",
        AllPublished => "all_published",
    }
}

string_enum! {
    /// Durable, data-free boundaries for a reload. `Baseline` is the safe lower fence `F`; `End`
    /// is the capture-durability barrier `H`. Their rows exist even when an export has zero data
    /// files, so lifecycle progress never depends on a Parquet row.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "lowercase")]
    pub enum ReloadMarkerKind {
        error = ParseEnumError;
        column = "table_reload_marker.marker_kind";
        Baseline => "baseline",
        End => "end",
    }
}

/// A source-WAL reload request after it has been expanded to one concrete table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceReloadRequest<'a> {
    /// Slot generation in which the request event was decoded.
    pub epoch: EpochNo,
    /// Stable UUID carried by the source event. All-table children may share this UUID.
    pub source_request_id: Uuid,
    /// Optional group identity used to correlate a larger orchestration request.
    pub parent_request_id: Option<Uuid>,
    /// Single-table request or child of an all-published fanout.
    pub scope: ReloadScope,
    /// Concrete child table schema.
    pub source_schema: &'a str,
    /// Concrete child table name.
    pub source_table: &'a str,
    /// Persisted request spelling; `resync` remains accepted as a rebuild alias.
    pub flavor: ReloadFlavor,
}

/// One durable reload boundary record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReloadMarkerRow {
    /// Attempt the marker belongs to.
    pub reload_id: ReloadId,
    /// Lower baseline fence or upper durability barrier.
    pub kind: ReloadMarkerKind,
    /// Commit LSN of the boundary.
    pub lsn: Lsn,
    /// Frozen schema used by the attempt at this boundary.
    pub schema_version: SchemaVersionNo,
}

/// Immutable source identity carried by a start/end fence. Source-backed attempts require the
/// request UUID to match `source_request_id` (or a successor's/direct request's
/// `parent_request_id`). `None` remains representable only for reading pre-migration rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReloadFenceIdentity<'a> {
    /// Stable source-event namespace for this fence.
    pub request_id: Option<Uuid>,
    /// Exact target schema.
    pub source_schema: &'a str,
    /// Exact target table.
    pub source_table: &'a str,
    /// Frozen structural version carried by the event.
    pub schema_version: SchemaVersionNo,
}

string_enum! {
    /// `requested → exporting → export_complete → publishing → complete`; `failed` is terminal
    /// from the middle. The SQL CHECK carries the same six values — belt and braces, like
    /// `transformer_checkpoint`'s CHECK.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "snake_case")]
    pub enum ReloadStatus {
        error = ParseEnumError;
        column = "table_reload.status";
        Requested => "requested",
        Exporting => "exporting",
        ExportComplete => "export_complete",
        Publishing => "publishing",
        Complete => "complete",
        Failed => "failed",
    }
}

/// One reload attempt. `lease_expiry`/timestamps stay out of the model — every time comparison
/// happens against SQL's per-statement clock, like `table_ownership`, so the Rust side never holds
/// a clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadRow {
    /// This attempt's identity — the `bigserial` "latest wins" key described in the module docs.
    pub reload_id: ReloadId,
    /// Generation the attempt belongs to; part of the one-live-reload uniqueness key.
    pub epoch: EpochNo,
    /// Schema of the table being reloaded.
    pub source_schema: String,
    /// Table being reloaded.
    pub source_table: String,
    /// Persisted request spelling; both values use the same rebuild behavior. See [`ReloadFlavor`].
    pub flavor: ReloadFlavor,
    /// Stable source-WAL request identity; `None` for legacy direct control-plane requests.
    pub source_request_id: Option<Uuid>,
    /// Fanout/ancestor UUID, or a direct request's private durable fence namespace.
    pub parent_request_id: Option<Uuid>,
    /// Whether this row is a direct table request or an all-published child.
    pub scope: ReloadScope,
    /// Where the attempt sits in the state walk; see [`ReloadStatus`].
    pub status: ReloadStatus,
    /// Last COMPLETED chunk; 0 = none exported yet.
    pub chunk_no: i64,
    /// Last exported PK bound (a JSON array, so composite PKs need no special casing); `None` = start.
    pub cursor_pk: Option<serde_json::Value>,
    /// Authoritative safe lower fence `F`, recorded before the first export query and therefore
    /// present for empty tables too. Legacy attempts may leave it `None`.
    pub start_lsn: Option<Lsn>,
    /// Legacy L₁ — the first data chunk's echo watermark. It remains for rolling compatibility,
    /// but new reconciliation correctness uses `start_lsn` instead.
    pub first_lsn: Option<Lsn>,
    /// H — set at `export_complete`; the transformer flips `complete` once `transformed_lsn >= H`.
    pub final_lsn: Option<Lsn>,
    /// The single schema version this attempt exports at; frozen with the explicit start fence for
    /// unified attempts or the first chunk for legacy attempts.
    pub schema_version: Option<SchemaVersionNo>,
    /// DDL restarts consumed so far; `reload_max_restarts` caps it.
    pub restart_count: i32,
    /// The instance currently holding the exporter lease; `None` when unclaimed. Compared only in
    /// SQL against `statement_timestamp()`, so this side never decides whether the lease is live.
    pub lease_holder: Option<String>,
    /// Monotonic fencing token minted on every claim/adoption. Export mutations must carry this
    /// exact value; a holder name alone cannot distinguish a delayed process from its replacement.
    pub exporter_generation: i64,
    /// Whether this attempt already opened and durably recorded a source snapshot plan. The
    /// snapshot itself is intentionally not exposed as resumable state.
    pub has_export_plan: bool,
    /// Why the attempt reached `failed` — an operator-facing reason, set only on that transition.
    pub error: Option<String>,
}

/// Immutable ownership proof carried by one exporter and all of its parallel COPY workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExporterLease {
    /// Attempt this token owns.
    pub reload_id: ReloadId,
    /// Pod/process identity recorded on the row.
    pub holder: String,
    /// Monotonic generation minted atomically by claim/adoption.
    pub generation: i64,
}

impl ReloadRow {
    /// Resolve the fencing token returned by the claim/adoption that produced this row.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::ReloadTransition`] for an unowned row, a holder mismatch, or an
    /// invalid generation.
    pub fn exporter_lease(&self, expected_holder: &str) -> Result<ExporterLease, ControlError> {
        if self.status != ReloadStatus::Exporting
            || self.lease_holder.as_deref() != Some(expected_holder)
            || self.exporter_generation <= 0
        {
            return Err(ControlError::ReloadTransition {
                reload_id: self.reload_id,
                expected: "exporting with this holder's positive exporter generation",
            });
        }
        Ok(ExporterLease {
            reload_id: self.reload_id,
            holder: expected_holder.to_string(),
            generation: self.exporter_generation,
        })
    }
}

/// One immutable physical range in a durable parallel snapshot plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportRangePlan {
    /// Stable zero-based ordinal assigned before any worker starts COPY.
    pub range_no: i64,
    /// A non-heap relation is exported as one full scan.
    pub full_scan: bool,
    /// Inclusive heap block lower bound; absent for a full scan.
    pub start_block: Option<i64>,
    /// Exclusive heap block upper bound; absent for the open-ended final range/full scan.
    pub end_block: Option<i64>,
}

/// Immutable identity of the PostgreSQL snapshot shared by all COPY workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportSnapshot<'a> {
    /// Exact `pg_current_snapshot()::text` value seen by coordinator and importers.
    pub identity: &'a str,
    /// Snapshot xmin (`pg_snapshot_xmin`).
    pub xmin: i64,
    /// Snapshot xmax (`pg_snapshot_xmax`).
    pub xmax: i64,
}

/// Durable receipt proving that every planned range and every reload manifest is present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportSeal {
    /// Number of ready reload objects included in the seal.
    pub file_count: i64,
    /// Total rows across the completed range plan/manifests.
    pub row_count: i64,
}

/// A transformer-owned publication attempt with immutable `[F,H]` boundaries and a stable crash-recovery
/// nonce. The publisher identity is copied from `table_ownership` at every claim/adoption; every
/// mutating publication API rechecks both that identity and the still-live ownership row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadPublication {
    /// Reload attempt being published.
    pub reload_id: ReloadId,
    /// Slot generation that owns the attempt.
    pub epoch: EpochNo,
    /// Source schema of the rebuilt table.
    pub source_schema: String,
    /// Source table being rebuilt.
    pub source_table: String,
    /// Current guarded state of the publication attempt.
    pub status: ReloadStatus,
    /// Immutable lower source WAL fence `F`.
    pub start_lsn: Lsn,
    /// Immutable upper source WAL fence `H`.
    pub final_lsn: Lsn,
    /// Frozen structural version exported by the attempt.
    pub schema_version: SchemaVersionNo,
    /// Stable identity used to adopt publication after a crash.
    pub publication_nonce: Uuid,
    /// Current table-ownership holder allowed to publish.
    pub publisher_owner_pod: String,
    /// Monotonic table-ownership token required for every mutation.
    pub publisher_fencing_token: i64,
}

/// Decode the common dynamic projection shared by every reload-row reader. These queries are
/// intentionally runtime-checked because protocol migrations add fencing fields that must be
/// deployed together rather than silently decoded through stale SQLx offline metadata.
fn reload_from_row(row: &PgRow) -> Result<ReloadRow, ControlError> {
    Ok(ReloadRow {
        reload_id: ReloadId(row.try_get("reload_id")?),
        epoch: EpochNo(row.try_get("epoch")?),
        source_schema: row.try_get("source_schema")?,
        source_table: row.try_get("source_table")?,
        flavor: row.try_get::<String, _>("flavor")?.parse()?,
        source_request_id: row.try_get("source_request_id")?,
        parent_request_id: row.try_get("parent_request_id")?,
        scope: row.try_get::<String, _>("request_scope")?.parse()?,
        status: row.try_get::<String, _>("status")?.parse()?,
        chunk_no: row.try_get("chunk_no")?,
        cursor_pk: row.try_get("cursor_pk")?,
        start_lsn: row.try_get("start_lsn")?,
        first_lsn: row.try_get("first_lsn")?,
        final_lsn: row.try_get("final_lsn")?,
        schema_version: row
            .try_get::<Option<i64>, _>("schema_version")?
            .map(SchemaVersionNo),
        restart_count: row.try_get("restart_count")?,
        lease_holder: row.try_get("lease_holder")?,
        exporter_generation: row.try_get("exporter_generation")?,
        has_export_plan: row.try_get("has_export_plan")?,
        error: row.try_get("error")?,
    })
}

/// INSERT a reload request (`status='requested'`); returns the new `reload_id`.
///
/// A second request while the table has a live reload violates the `walrus_table_reload_one_live`
/// partial unique index and maps to the typed [`ControlError::ReloadInProgress`] — matched by
/// SQLSTATE + constraint *name*, never by message text. After `complete`/`failed` the row leaves
/// the index and a new request succeeds.
///
/// Retired pre-v2 keyset-cursor compatibility entry point. Protocol-v2 claims always mint a
/// positive exporter generation, and the guarded SQL rejects them so an accidental call cannot
/// bypass the snapshot-plan/file-count protocol.
///
/// # Errors
///
/// Returns [`ControlError::ReloadInProgress`] when the table already has a live attempt,
/// [`ControlError::CheckViolation`] for another rejected invariant, or
/// [`ControlError::Connect`] when the request cannot reach control Postgres.
pub async fn request(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    flavor: ReloadFlavor,
) -> Result<ReloadId, ControlError> {
    let fence_request_id = Uuid::new_v4();
    let rec = sqlx::query_file!(
        "sql/postgres/queries/request_reload.sql",
        epoch.0,
        source_schema,
        source_table,
        flavor.as_str(),
        fence_request_id,
    )
    .fetch_one(ex)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(db) = &e
            && db.code().as_deref() == Some("23505")
            && db.constraint() == Some("walrus_table_reload_one_live")
        {
            return ControlError::ReloadInProgress {
                schema: source_schema.to_string(),
                table: source_table.to_string(),
            };
        }
        ControlError::from(e)
    })?;
    Ok(rec.reload_id.into())
}

/// Idempotently persist one concrete table child decoded from a source-WAL request event.
///
/// Replaying the same `(epoch, source_request_id, schema, table)` returns the original
/// [`ReloadId`], including after that attempt is terminal. An all-published request deliberately
/// reuses its source UUID across children; the table coordinates distinguish them. Reusing the key
/// with a different flavor, scope, or parent is rejected instead of silently changing history. A
/// distinct source request for a busy table is durably appended in `requested`; [`claim_requested`]
/// serializes those rows in per-table `reload_id` order once the current attempt is terminal.
///
/// # Errors
///
/// Returns [`ControlError::SourceRequestConflict`] when the UUID's immutable payload changed, or a
/// database connectivity/invariant error.
pub async fn request_from_source(
    ex: impl PgExecutor<'_>,
    request: &SourceReloadRequest<'_>,
) -> Result<ReloadId, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/request_reload_from_source.sql",
        request.epoch.0,
        request.source_schema,
        request.source_table,
        request.flavor.as_str(),
        request.source_request_id,
        request.parent_request_id,
        request.scope.as_str(),
    )
    .fetch_optional(ex)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(db) = &e
            && db.code().as_deref() == Some("23505")
            && db.constraint() == Some("walrus_table_reload_one_live")
        {
            return ControlError::ReloadInProgress {
                schema: request.source_schema.to_string(),
                table: request.source_table.to_string(),
            };
        }
        ControlError::from(e)
    })?;

    rec.map(|row| row.reload_id.into())
        .ok_or_else(|| ControlError::SourceRequestConflict {
            request_id: request.source_request_id,
            schema: request.source_schema.to_string(),
            table: request.source_table.to_string(),
        })
}

/// Claim up to `limit` `requested` rows for this holder: set the lease, flip to `exporting`.
///
/// `FOR UPDATE SKIP LOCKED` under the guarded UPDATE makes concurrent claimers partition the
/// queue instead of double-exporting; a fully-raced claimer just gets an empty `Vec`. Source-WAL
/// rows form a per-table FIFO: only the oldest may claim, and it waits until the current
/// `exporting | export_complete` attempt becomes terminal. Legacy direct requests keep their
/// immediate duplicate rejection and take priority over an unclaimed source queue.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the atomic claim/update query fails or its rows cannot be
/// decoded.
pub async fn claim_requested(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/claim_requested.sql"))
        .bind(epoch.0)
        .bind(holder)
        .bind(lease_ttl_secs as f64)
        .bind(limit)
        .fetch_all(ex)
        .await?;
    rows.iter().map(reload_from_row).collect()
}

/// Return a claimed-but-never-started row to the queue: `exporting → requested`, lease cleared.
///
/// The controller's un-claim for infra failures BETWEEN claim and exporter spawn — a
/// dead preflight connection, a control-pg blip while recording a rejection. An infra error must
/// neither terminally `fail` a valid request nor leave it `exporting` unowned; back in
/// `requested`, the next tick re-claims and retries. Holder-guarded (only the claimant un-claims)
/// and `exporting`-guarded, so it can never clobber a row someone else adopted. The SQL also
/// requires the pristine cursor and no start fence: once F exists, returning to ordinary pickup
/// would lose ownership of the connection-local source snapshot and is therefore forbidden.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded release update cannot execute.
pub async fn release_claim(
    ex: impl PgExecutor<'_>,
    lease: &ExporterLease,
) -> Result<bool, ControlError> {
    let done = sqlx::query(include_str!("../sql/postgres/queries/release_claim.sql"))
        .bind(lease.reload_id.0)
        .bind(&lease.holder)
        .bind(lease.generation)
        .execute(ex)
        .await?;
    Ok(done.rows_affected() > 0)
}

/// Renew this holder's lease on a live export. Affects zero rows — returning `false` — if we no
/// longer hold it or the export left `exporting` (a phantom exporter must not renew).
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded lease update cannot execute.
pub async fn renew_lease(
    ex: impl PgExecutor<'_>,
    lease: &ExporterLease,
    lease_ttl_secs: i64,
) -> Result<bool, ControlError> {
    let done = sqlx::query(include_str!("../sql/postgres/queries/renew_lease.sql"))
        .bind(lease.reload_id.0)
        .bind(&lease.holder)
        .bind(lease_ttl_secs as f64)
        .bind(lease.generation)
        .execute(ex)
        .await?;
    Ok(done.rows_affected() > 0)
}

const fn export_transition(reload_id: ReloadId) -> ControlError {
    ControlError::ReloadTransition {
        reload_id,
        expected: "live exporter generation with one exact, unsealed source snapshot plan",
    }
}

/// Finish a known logical-rejection path before returning its typed error. `Transaction::drop`
/// only queues a rollback; awaiting it here guarantees PostgreSQL has reached `ReadyForQuery`
/// before a pooled connection can be reused by a successor exporter.
async fn rollback_rejection<T>(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    error: ControlError,
) -> Result<T, ControlError> {
    tx.rollback().await?;
    Err(error)
}

fn valid_export_ranges(ranges: &[ExportRangePlan]) -> bool {
    if ranges.len() == 1 && ranges[0].full_scan {
        return ranges[0].range_no == 0
            && ranges[0].start_block.is_none()
            && ranges[0].end_block.is_none();
    }
    if ranges.is_empty() || ranges.iter().any(|range| range.full_scan) {
        return false;
    }
    let mut expected_start = 0;
    for (index, range) in ranges.iter().enumerate() {
        let is_final = index + 1 == ranges.len();
        if range.range_no != i64::try_from(index).unwrap_or(-1)
            || range.start_block != Some(expected_start)
        {
            return false;
        }
        match (is_final, range.end_block) {
            (false, Some(end)) if end > expected_start => expected_start = end,
            (true, None) => {}
            _ => return false,
        }
    }
    true
}

/// Durably freeze the exact source snapshot identity and complete physical range plan before any
/// COPY worker starts reading. An exact retry by the same generation is idempotent. A new
/// generation can never reuse the old plan: exported snapshot IDs cease to be importable when the
/// exporting transaction ends, so adoption must recover a durable H or supersede the attempt.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for a malformed/different plan, a stale generation,
/// an expired lease, or an attempt that already progressed/sealed.
pub async fn begin_export_plan(
    conn: &mut PgConnection,
    lease: &ExporterLease,
    start_lsn: Lsn,
    schema_version: SchemaVersionNo,
    snapshot: ExportSnapshot<'_>,
    ranges: &[ExportRangePlan],
) -> Result<(), ControlError> {
    if snapshot.identity.is_empty()
        || snapshot.xmin < 0
        || snapshot.xmax < snapshot.xmin
        || !valid_export_ranges(ranges)
    {
        return Err(export_transition(lease.reload_id));
    }
    let range_count =
        i64::try_from(ranges.len()).map_err(|_| export_transition(lease.reload_id))?;
    let mut tx = conn.begin().await?;
    sqlx::query("SELECT set_config('walrus.reload_export_plan_protocol', '2', true)")
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query(
        "SELECT export_snapshot, export_snapshot_xmin, export_snapshot_xmax,
                export_range_count,
                status = 'exporting'
                  AND lease_holder = $2
                  AND exporter_generation = $3
                  AND lease_expiry > statement_timestamp()
                  AND start_lsn = $4
                  AND schema_version = $5
                  AND cursor_pk IS NULL
                  AND export_sealed_at IS NULL AS owned
         FROM public.walrus_table_reload
         WHERE reload_id = $1
         FOR UPDATE",
    )
    .bind(lease.reload_id.0)
    .bind(&lease.holder)
    .bind(lease.generation)
    .bind(start_lsn)
    .bind(schema_version.0)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return rollback_rejection(tx, export_transition(lease.reload_id)).await;
    };
    if !row.try_get::<bool, _>("owned")? {
        return rollback_rejection(tx, export_transition(lease.reload_id)).await;
    }

    let existing_snapshot = row.try_get::<Option<String>, _>("export_snapshot")?;
    if let Some(existing_snapshot) = existing_snapshot {
        let header_matches = existing_snapshot == snapshot.identity
            && row.try_get::<Option<i64>, _>("export_snapshot_xmin")? == Some(snapshot.xmin)
            && row.try_get::<Option<i64>, _>("export_snapshot_xmax")? == Some(snapshot.xmax)
            && row.try_get::<Option<i64>, _>("export_range_count")? == Some(range_count);
        let persisted = sqlx::query(
            "SELECT exporter_generation, range_no, full_scan, start_block, end_block
             FROM public.walrus_table_reload_export_range
             WHERE reload_id = $1
             ORDER BY range_no",
        )
        .bind(lease.reload_id.0)
        .fetch_all(&mut *tx)
        .await?;
        let ranges_match = persisted.len() == ranges.len()
            && persisted.iter().zip(ranges).all(|(row, expected)| {
                row.try_get::<i64, _>("exporter_generation").ok() == Some(lease.generation)
                    && row.try_get::<i64, _>("range_no").ok() == Some(expected.range_no)
                    && row.try_get::<bool, _>("full_scan").ok() == Some(expected.full_scan)
                    && row.try_get::<Option<i64>, _>("start_block").ok()
                        == Some(expected.start_block)
                    && row.try_get::<Option<i64>, _>("end_block").ok() == Some(expected.end_block)
            });
        if !header_matches || !ranges_match {
            return rollback_rejection(tx, export_transition(lease.reload_id)).await;
        }
        sqlx::query("SELECT pg_catalog.set_config('walrus.reload_export_plan_protocol', '', true)")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(());
    }

    let updated = sqlx::query(
        "UPDATE public.walrus_table_reload
         SET export_snapshot = $4,
             export_snapshot_xmin = $5,
             export_snapshot_xmax = $6,
             export_range_count = $7,
             updated_at = now()
         WHERE reload_id = $1
           AND status = 'exporting'
           AND lease_holder = $2
           AND exporter_generation = $3
           AND lease_expiry > statement_timestamp()
           AND export_snapshot IS NULL
           AND export_snapshot_xmin IS NULL
           AND export_snapshot_xmax IS NULL
           AND export_range_count IS NULL",
    )
    .bind(lease.reload_id.0)
    .bind(&lease.holder)
    .bind(lease.generation)
    .bind(snapshot.identity)
    .bind(snapshot.xmin)
    .bind(snapshot.xmax)
    .bind(range_count)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return rollback_rejection(tx, export_transition(lease.reload_id)).await;
    }
    for range in ranges {
        sqlx::query(
            "INSERT INTO public.walrus_table_reload_export_range
                (reload_id, exporter_generation, range_no, full_scan, start_block, end_block)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(lease.reload_id.0)
        .bind(lease.generation)
        .bind(range.range_no)
        .bind(range.full_scan)
        .bind(range.start_block)
        .bind(range.end_block)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("SELECT pg_catalog.set_config('walrus.reload_export_plan_protocol', '', true)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Mark one planned range complete only after all of its object manifests are durable.
/// Exact replay is accepted; a changed count or stale generation is rejected.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for negative counts, an unknown/already-different
/// range receipt, or a stale/expired exporter generation. Database failures retain their typed
/// [`ControlError`] mapping.
pub async fn record_export_range(
    ex: impl PgExecutor<'_>,
    lease: &ExporterLease,
    range_no: i64,
    file_count: i64,
    row_count: i64,
) -> Result<(), ControlError> {
    if range_no < 0 || file_count < 0 || row_count < 0 {
        return Err(export_transition(lease.reload_id));
    }
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/record_export_range.sql"
    ))
    .bind(lease.reload_id.0)
    .bind(&lease.holder)
    .bind(lease.generation)
    .bind(range_no)
    .bind(file_count)
    .bind(row_count)
    .fetch_one(ex)
    .await?;
    if row.try_get::<Option<i64>, _>("range_no")?.is_none() {
        return Err(export_transition(lease.reload_id));
    }
    Ok(())
}

/// Seal the snapshot baseline after validating every planned range, manifest count, and row count
/// under the live exporter generation. H must be emitted only after this receipt is durable.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless every range and reload manifest agrees with
/// the live generation's immutable F/schema plan, or the underlying typed database error.
pub async fn seal_export(
    conn: &mut PgConnection,
    lease: &ExporterLease,
    start_lsn: Lsn,
    schema_version: SchemaVersionNo,
) -> Result<ExportSeal, ControlError> {
    let mut tx = conn.begin().await?;
    sqlx::query("SELECT set_config('walrus.reload_export_seal_protocol', '2', true)")
        .execute(&mut *tx)
        .await?;
    let owned = sqlx::query_scalar::<_, bool>(
        "SELECT true FROM public.walrus_table_reload
         WHERE reload_id = $1 AND status = 'exporting'
           AND lease_holder = $2 AND exporter_generation = $3
           AND lease_expiry > statement_timestamp()
           AND start_lsn = $4 AND schema_version = $5
           AND export_snapshot IS NOT NULL
         FOR UPDATE",
    )
    .bind(lease.reload_id.0)
    .bind(&lease.holder)
    .bind(lease.generation)
    .bind(start_lsn)
    .bind(schema_version.0)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(false);
    if !owned {
        return rollback_rejection(tx, export_transition(lease.reload_id)).await;
    }
    let row = sqlx::query(include_str!("../sql/postgres/queries/seal_export.sql"))
        .bind(lease.reload_id.0)
        .bind(&lease.holder)
        .bind(lease.generation)
        .bind(start_lsn)
        .bind(schema_version.0)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(row) = row else {
        return rollback_rejection(tx, export_transition(lease.reload_id)).await;
    };
    let seal = ExportSeal {
        file_count: row.try_get("file_count")?,
        row_count: row.try_get("row_count")?,
    };
    sqlx::query("SELECT pg_catalog.set_config('walrus.reload_export_seal_protocol', '', true)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(seal)
}

/// Freeze the authoritative safe lower fence `F` and schema, and create its durable baseline
/// marker in the same control-Postgres statement.
///
/// Unlike legacy `first_lsn`, this happens before any table query or Parquet file. Consequently an
/// empty export still has a baseline that can trigger reconstruction. An exact retry is accepted
/// while the attempt remains `exporting`; a changed LSN/schema or illegal state changes zero rows.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is `exporting` with compatible
/// frozen values, or a database connectivity/invariant error.
pub async fn record_start_fence(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    start_lsn: Lsn,
    identity: ReloadFenceIdentity<'_>,
) -> Result<(), ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/record_start_fence.sql",
        reload_id.0,
        start_lsn as Lsn,
        identity.schema_version.0,
        identity.request_id,
        identity.source_schema,
        identity.source_table,
    )
    .fetch_one(ex)
    .await?;
    if row.reload_id.is_none() {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (same start_lsn and schema_version)",
        });
    }
    Ok(())
}

/// Persist the data-free upper barrier `H` after target WAL through `H` is durable.
///
/// This does not change status: [`complete_export`] separately verifies the exact marker and then
/// performs `exporting → export_complete`. Splitting the operations makes the durability point
/// explicit and crash-replayable. The marker works for empty tables because it has no file/row
/// dependency.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is `exporting`, has a frozen
/// baseline/schema, and `H >= F`, or when an existing end marker disagrees.
pub async fn record_end_marker(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    final_lsn: Lsn,
    identity: ReloadFenceIdentity<'_>,
) -> Result<(), ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/record_end_marker.sql",
        reload_id.0,
        final_lsn as Lsn,
        identity.schema_version.0,
        identity.request_id,
        identity.source_schema,
        identity.source_table,
    )
    .fetch_one(ex)
    .await?;
    if row.reload_id.is_none() {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (frozen baseline, H >= F, same end marker)",
        });
    }
    Ok(())
}

/// Read the durable boundary records for one attempt in baseline-then-end order.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the marker rows cannot be read or decoded.
pub async fn read_markers(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
) -> Result<Vec<ReloadMarkerRow>, ControlError> {
    let rows = sqlx::query_file!("sql/postgres/queries/read_reload_markers.sql", reload_id.0,)
        .fetch_all(ex)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ReloadMarkerRow {
            reload_id: row.reload_id.into(),
            kind: row.marker_kind,
            lsn: row.lsn,
            schema_version: row.schema_version.into(),
        })
        .collect())
}

/// Record chunk `chunk_no` done: bump the cursor, store the new PK bound.
///
/// On the first chunk this retains `first_lsn` as diagnostic compatibility data (the `COALESCE`
/// prevents later chunks from overwriting it). Every executable attempt has already frozen the
/// authoritative `start_lsn` and `schema_version` through [`record_start_fence`], and SQL requires
/// each chunk watermark to equal that exact F.
/// `schema_version` is always **asserted**: every reload attempt is
/// single-schema *by construction* (H9), so a later chunk arriving with a different version means
/// the export engine missed a DDL restart — the WHERE rejects it and the mismatch is the
/// same loud zero-rows error as any illegal transition, never a silent swallow. The
/// `chunk_no = $new - 1` guard makes the cursor strictly in-order: a duplicate or out-of-order
/// advance changes zero rows and errors. A restart uses a *fresh* row rather than ever
/// mutating the frozen fields.)
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for an out-of-order chunk, stale status, or changed
/// schema version; database failures become [`ControlError::Connect`] or
/// [`ControlError::CheckViolation`].
pub async fn advance_cursor(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    chunk_no: i64,
    cursor_pk: &serde_json::Value,
    chunk_lsn: Lsn,
    schema_version: SchemaVersionNo,
) -> Result<(), ControlError> {
    let done = sqlx::query(include_str!("../sql/postgres/queries/advance_cursor.sql"))
        .bind(reload_id.0)
        .bind(chunk_no)
        .bind(cursor_pk)
        .bind(chunk_lsn)
        .bind(schema_version.0)
        .execute(ex)
        .await?;
    if done.rows_affected() == 0 {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "pre-v2 generation-zero exporting row with in-order keyset progress",
        });
    }
    Ok(())
}

/// Record one completely uploaded reload file without imposing worker completion order.
///
/// Parallel workers can finish remote objects in any order, so the persisted `chunk_no` is a
/// completed-file count rather than a caller-assigned sequence number. The guarded increment is
/// atomic and returns the unique count assigned by Postgres. Callers insert the file manifest and
/// invoke this function in the **same control transaction**, after the remote object is complete;
/// either both rows become visible or neither does.
///
/// `cursor_pk IS NULL` prevents a rolling deployment from mixing this mode with the legacy
/// keyset-cursor exporter. `first_lsn` remains diagnostic compatibility data and is frozen to the
/// attempt's already-durable start fence on the first successful file. The exact start fence and
/// schema guards reject stale workers after a restart or DDL successor.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is still `exporting`, belongs to
/// the supplied start fence and schema, and has never entered legacy cursor mode. Database failures
/// become [`ControlError::Connect`] or [`ControlError::CheckViolation`].
pub async fn record_exported_file(
    ex: impl PgExecutor<'_>,
    lease: &ExporterLease,
    start_lsn: Lsn,
    schema_version: SchemaVersionNo,
) -> Result<i64, ControlError> {
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/record_exported_file.sql"
    ))
    .bind(lease.reload_id.0)
    .bind(start_lsn)
    .bind(schema_version.0)
    .bind(&lease.holder)
    .bind(lease.generation)
    .fetch_optional(ex)
    .await?;
    let Some(row) = row else {
        return Err(ControlError::ReloadTransition {
            reload_id: lease.reload_id,
            expected: "live exporter generation (unsealed, cursor-free, same F/schema)",
        });
    };
    Ok(row.try_get("chunk_no")?)
}

/// `exporting → export_complete`, recording the final watermark `H`. The extractor's last act; from
/// here the TRANSFORMER finishes the walk (`complete` once `transformed_lsn >= H`). Every request source
/// and both persisted flavor spellings must already have matching durable baseline/end markers.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is still `exporting` and its
/// markers match; database failures become [`ControlError::Connect`] or
/// [`ControlError::CheckViolation`].
pub async fn complete_export(
    ex: impl PgExecutor<'_>,
    lease: &ExporterLease,
    final_lsn: Lsn,
) -> Result<(), ControlError> {
    let row = sqlx::query(include_str!("../sql/postgres/queries/complete_export.sql"))
        .bind(lease.reload_id.0)
        .bind(final_lsn)
        .bind(&lease.holder)
        .bind(lease.generation)
        .fetch_one(ex)
        .await?;
    if row.try_get::<Option<i64>, _>("reload_id")?.is_none() {
        return Err(ControlError::ReloadTransition {
            reload_id: lease.reload_id,
            expected: "live exporter generation with matching sealed snapshot and F/H markers",
        });
    }
    Ok(())
}

/// Legacy `export_complete → complete` compatibility for pre-v2 rows only. A protocol-v2 attempt
/// has `exporter_generation > 0`, so this function deliberately rejects it: only
/// [`finish_publication`] may make that attempt terminal after the hidden Duck generation and
/// canonical checkpoints are atomically published.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is a generation-zero
/// `export_complete` legacy row, or [`ControlError::Connect`] if the guarded update fails.
pub async fn complete(ex: impl PgExecutor<'_>, reload_id: ReloadId) -> Result<(), ControlError> {
    let done = sqlx::query(include_str!("../sql/postgres/queries/complete.sql"))
        .bind(reload_id.0)
        .execute(ex)
        .await?;
    if done.rows_affected() == 0 {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "legacy generation-zero export_complete",
        });
    }
    Ok(())
}

/// `exporting → failed`, and — in the SAME transaction — delete this reload's
/// staged manifest rows. A failed reload must leave nothing for the transformer to claim (H9), and
/// coupling the purge to the flip means no crash window can separate them.
///
/// Takes a connection (not an executor) because this is a locked multi-statement transaction;
/// inside an outer transaction it nests as a savepoint, so fail-and-reissue callers can wrap it
/// with the successor INSERT atomically. The purge needs no `kind`
/// filter — only reload files carry a `reload_id` (that is the point of the nullable column).
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is in a failable live status;
/// transaction, update, purge, or commit failures become [`ControlError::Connect`] or
/// [`ControlError::CheckViolation`].
pub async fn fail(
    conn: &mut PgConnection,
    reload_id: ReloadId,
    reason: &str,
) -> Result<(), ControlError> {
    let mut tx = conn.begin().await?;
    // Lock the parent before deleting its children. Reload-manifest insertion takes the same
    // parent lock, so no late worker can commit a child between this purge and the terminal flip.
    let failable = sqlx::query_scalar::<_, bool>(
        "SELECT true
         FROM public.walrus_table_reload
         WHERE reload_id = $1 AND status = 'exporting'
         FOR UPDATE",
    )
    .bind(reload_id.0)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(false);
    if !failable {
        return rollback_rejection(
            tx,
            ControlError::ReloadTransition {
                reload_id,
                expected: "exporting",
            },
        )
        .await;
    }
    sqlx::query(include_str!("../sql/postgres/queries/fail_purge_files.sql"))
        .bind(reload_id.0)
        .execute(&mut *tx)
        .await?;
    let done = sqlx::query(include_str!("../sql/postgres/queries/fail.sql"))
        .bind(reload_id.0)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
    if done.rows_affected() != 1 {
        return rollback_rejection(
            tx,
            ControlError::ReloadTransition {
                reload_id,
                expected: "the row-locked exporting attempt",
            },
        )
        .await;
    }
    crate::integrity::note_recovery_reload_failed(&mut *tx, reload_id, reason).await?;
    tx.commit().await?;
    Ok(())
}

/// Fail an exporter-owned attempt only while its exact fencing generation still owns a live
/// lease. This is the extractor-facing failure path; [`fail`] remains the administrative/state-machine
/// primitive used inside already-locked control transactions.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] when the lease is stale, expired, or no longer owns
/// an exporting attempt; otherwise returns the typed database failure from the atomic fail/purge.
pub async fn fail_owned(
    conn: &mut PgConnection,
    lease: &ExporterLease,
    reason: &str,
) -> Result<(), ControlError> {
    let mut tx = conn.begin().await?;
    let owned = sqlx::query_scalar::<_, bool>(
        "SELECT true
         FROM public.walrus_table_reload
         WHERE reload_id = $1
           AND status = 'exporting'
           AND lease_holder = $2
           AND exporter_generation = $3
           AND lease_expiry > statement_timestamp()
         FOR UPDATE",
    )
    .bind(lease.reload_id.0)
    .bind(&lease.holder)
    .bind(lease.generation)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(false);
    if !owned {
        return rollback_rejection(
            tx,
            ControlError::ReloadTransition {
                reload_id: lease.reload_id,
                expected: "exporting under the caller's live exporter generation",
            },
        )
        .await;
    }
    if let Err(error) = fail(&mut tx, lease.reload_id, reason).await {
        return rollback_rejection(tx, error).await;
    }
    tx.commit().await?;
    Ok(())
}

/// Would restarting an attempt with `restart_count` push it past `max_restarts`? The next
/// attempt would carry `restart_count + 1`, so the cap is exceeded when that exceeds the max — a
/// `max_restarts` of 0 fails the very first mid-export DDL. Pure so it unit-tests without a DB.
///
/// The successor count is `checked_add`ed, not `+`ed: `restart_count` is whatever the `int` column
/// holds, and at [`i32::MAX`] a bare add wraps to [`i32::MIN`] in release — reporting a *spent* cap
/// as unspent, which is the one answer a restart cap must never reach by accident. `None` is also
/// the exact answer, since no `i32` cap can hold `i32::MAX + 1`.
#[must_use]
pub const fn restart_would_exceed_cap(restart_count: i32, max_restarts: i32) -> bool {
    match restart_count.checked_add(1) {
        Some(next) => next > max_restarts,
        None => true,
    }
}

/// Why one exporter attempt must be superseded by a fresh fenced attempt.
#[derive(Debug, Clone, Copy)]
enum AttemptRestartCause {
    Ddl(SchemaVersionNo),
    LostSnapshot,
}

impl AttemptRestartCause {
    fn reason(self, capped: bool, max_restarts: i32) -> String {
        let cap = if capped {
            format!("; restart cap {max_restarts} exhausted")
        } else {
            String::new()
        };
        match self {
            Self::Ddl(new_schema_version) => {
                format!("superseded: ddl bumped schema_version to {new_schema_version}{cap}")
            }
            Self::LostSnapshot => {
                format!("superseded: exporter lost its source snapshot ownership{cap}")
            }
        }
    }
}

/// Restart one exporter attempt: in ONE transaction, fail the old attempt — [`fail`]'s coupling
/// purges its `kind='reload'` manifest rows, so no observer ever sees a terminal attempt with
/// claimable chunk files — and, unless the restart cap is spent, INSERT its successor.
///
/// The successor is born `exporting`, carrying the old row's identity **and its lease** (an
/// `INSERT … SELECT` copies `lease_holder`/`lease_expiry` verbatim, so the running exporter keeps
/// ownership and no pickup round-trip is spent) with a FRESH cursor: `chunk_no` 0, `cursor_pk`
/// NULL, and `schema_version` NULL so the successor resolves the current registry version before
/// opening its new snapshot. `restart_count` is `old + 1`. The `walrus_table_reload_one_live` partial unique index
/// tolerates the successor only because the predecessor turns terminal in the SAME transaction.
///
/// Returns the successor `reload_id`, or `None` when `restart_count + 1 > max_restarts`: then the
/// attempt is failed-only (the cap named in the reason) and no successor is written — visible
/// waste, never silent mis-reconciliation (the design's H9 choice).
///
async fn restart_attempt(
    conn: &mut PgConnection,
    old: &ReloadRow,
    max_restarts: i32,
    cause: AttemptRestartCause,
) -> Result<Option<ReloadId>, ControlError> {
    // Computed before `capped` is known, so it saturates for the same reason the cap check is
    // checked: an `i32::MAX` predecessor must not wrap here either. The capped path discards it, and
    // on the path that keeps it the cap has already proved the successor count is exact.
    let next_restart = old.restart_count.saturating_add(1);
    let capped = restart_would_exceed_cap(old.restart_count, max_restarts);
    let reason = cause.reason(capped, max_restarts);

    let mut tx = conn.begin().await?;
    let owns_attempt = sqlx::query_scalar::<_, bool>(
        "SELECT true
         FROM public.walrus_table_reload
         WHERE reload_id = $1
           AND status = 'exporting'
           AND lease_holder = $2
           AND exporter_generation = $3
           AND lease_expiry > statement_timestamp()
         FOR UPDATE",
    )
    .bind(old.reload_id.0)
    .bind(&old.lease_holder)
    .bind(old.exporter_generation)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(false);
    if !owns_attempt {
        return rollback_rejection(
            tx,
            ControlError::ReloadTransition {
                reload_id: old.reload_id,
                expected: "exporting under the caller's live exporter generation",
            },
        )
        .await;
    }
    // Reuse fail() (a savepoint inside this tx): one place owns "terminal ⇒ no claimable files".
    // The Transaction auto-derefs to the PgConnection fail() wants; its inner begin() nests as a
    // savepoint under this transaction.
    if let Err(error) = fail(&mut tx, old.reload_id, &reason).await {
        return rollback_rejection(tx, error).await;
    }
    if capped {
        // Fail-only: the reload is abandoned, its chunk files already purged by fail().
        tx.commit().await?;
        return Ok(None);
    }
    // The successor: copy identity + lease from the (now failed) predecessor, reset the cursor and
    // schema_version, bump restart_count. Selecting only the carried columns leaves chunk_no/
    // cursor_pk/first_lsn/final_lsn/schema_version/error at their table defaults (fresh start).
    let rec = sqlx::query(include_str!("../sql/postgres/queries/restart_for_ddl.sql"))
        .bind(old.reload_id.0)
        .bind(next_restart)
        .fetch_one(&mut *tx)
        .await?;
    let successor_id = ReloadId(rec.try_get("reload_id")?);
    crate::integrity::relink_recovery_reload(&mut *tx, old.reload_id, successor_id).await?;
    tx.commit().await?;
    Ok(Some(successor_id))
}

/// H9 restart-on-DDL. The successor gets a fresh cursor, schema resolution, and F fence.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor can no longer be failed, or
/// [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction, purge,
/// successor insert, or commit fails.
pub async fn restart_for_ddl(
    conn: &mut PgConnection,
    old: &ReloadRow,
    new_schema_version: SchemaVersionNo,
    max_restarts: i32,
) -> Result<Option<ReloadId>, ControlError> {
    restart_attempt(
        conn,
        old,
        max_restarts,
        AttemptRestartCause::Ddl(new_schema_version),
    )
    .await
}

/// Crash recovery for an adopted exporter whose source snapshot no longer exists. A read-only
/// PostgreSQL snapshot is connection-local and cannot be resumed from a durable chunk cursor, so
/// the predecessor is failed/purged and a lease-carrying successor starts at chunk zero with a
/// fresh F. This uses the same bounded restart budget as DDL supersession.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor can no longer be failed, or
/// [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction, purge,
/// successor insert, or commit fails.
pub async fn restart_for_lost_snapshot(
    conn: &mut PgConnection,
    old: &ReloadRow,
    max_restarts: i32,
) -> Result<Option<ReloadId>, ControlError> {
    restart_attempt(conn, old, max_restarts, AttemptRestartCause::LostSnapshot).await
}

/// Crash recovery for an adopted attempt that has not committed any baseline chunk. The old
/// connection may nevertheless have appended F or H to source WAL just before it disappeared, so
/// reusing its fence identity would let a delayed marker bound a later snapshot. Atomically fail
/// the predecessor and create a fresh lease-carrying successor, but preserve `restart_count`: no
/// durable baseline work was lost, so this recovery does not spend the bounded DDL/snapshot-loss
/// restart budget.
///
/// The successor insert rechecks the pristine cursor under the same transaction. If the abandoned
/// exporter raced adoption and committed a chunk after the caller read `old`, the whole transition
/// rolls back and reports [`ControlError::ReloadTransition`]; a later adoption will then use the
/// ordinary, budgeted lost-snapshot path.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor is no longer live or no longer
/// pristine, or [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction,
/// purge, successor insert, or commit fails.
pub async fn restart_pristine_adoption(
    conn: &mut PgConnection,
    old: &ReloadRow,
) -> Result<ReloadId, ControlError> {
    let mut tx = conn.begin().await?;
    let owns_attempt = sqlx::query_scalar::<_, bool>(
        "SELECT true
         FROM public.walrus_table_reload
         WHERE reload_id = $1
           AND status = 'exporting'
           AND lease_holder = $2
           AND exporter_generation = $3
           AND lease_expiry > statement_timestamp()
         FOR UPDATE",
    )
    .bind(old.reload_id.0)
    .bind(&old.lease_holder)
    .bind(old.exporter_generation)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(false);
    if !owns_attempt {
        return rollback_rejection(
            tx,
            ControlError::ReloadTransition {
                reload_id: old.reload_id,
                expected: "pristine attempt under the caller's live exporter generation",
            },
        )
        .await;
    }
    if let Err(error) = fail(
        &mut tx,
        old.reload_id,
        "superseded: adopted pristine attempt gets a fresh source fence identity",
    )
    .await
    {
        return rollback_rejection(tx, error).await;
    }
    let successor = sqlx::query(
        "INSERT INTO public.walrus_table_reload
             (epoch, source_schema, source_table, flavor, status, restart_count,
              lease_holder, lease_expiry, exporter_generation, parent_request_id, request_scope)
         SELECT epoch, source_schema, source_table, flavor, 'exporting', $2,
                lease_holder, lease_expiry, exporter_generation,
                COALESCE(source_request_id, parent_request_id),
                request_scope
         FROM public.walrus_table_reload
         WHERE reload_id = $1 AND chunk_no = 0 AND cursor_pk IS NULL
         RETURNING reload_id",
    )
    .bind(old.reload_id.0)
    .bind(old.restart_count)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(successor) = successor else {
        return rollback_rejection(
            tx,
            ControlError::ReloadTransition {
                reload_id: old.reload_id,
                expected: "exporting with no durable chunk progress",
            },
        )
        .await;
    };
    let successor_id = ReloadId(successor.try_get("reload_id")?);
    crate::integrity::relink_recovery_reload(&mut *tx, old.reload_id, successor_id).await?;
    tx.commit().await?;
    Ok(successor_id)
}

/// Legacy checkpoint-based completion for generation-zero rows created before protocol v2. Every
/// protocol-v2 attempt has `exporter_generation > 0`, so it is intentionally invisible here even
/// after `transformed_lsn >= H`; its only completion path is [`finish_publication`]. Kept solely for
/// rolling compatibility with durable pre-v2 attempts.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the checkpoint-joined completion update cannot execute.
pub async fn complete_reached(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Vec<ReloadId>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/complete_reached.sql"))
        .bind(epoch.0)
        .bind(source_schema)
        .bind(source_table)
        .fetch_all(ex)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.try_get::<i64, _>("reload_id").map(ReloadId))
        .collect::<Result<Vec<_>, sqlx::Error>>()?)
}

/// The authoritative `start_lsn` (`F`) below which a pending **rebuild** supersedes this table's
/// manifest files. A live `reload`-flavor reload's rebuild trigger will `CREATE OR
/// REPLACE` the mirror at the new schema and `delete_superseded` every non-reload file with
/// `lsn_end <= start_lsn` — so the transformer must NOT reconcile (and possibly quarantine on) such a
/// file: it skips it and lets the rebuild replace the mirror. Only the explicit start fence is
/// authoritative; legacy `first_lsn` is diagnostic and can never authorize reconciliation. There
/// is at most one active (`exporting | export_complete`) attempt per table; later source-backed
/// `requested` rows carry no fence yet and remain queued behind it.
///
/// This is what closes the quarantine-recovery loop: without it, a lossy-`ALTER` stream file
/// (lower `lsn_end` than the reload's `first_lsn`) re-quarantines the transformer on every restart before
/// it can reach the reload chunk file that would clear the quarantine.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the pending-rebuild lookup cannot execute or decode.
pub async fn reload_supersede_floor(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<Lsn>, ControlError> {
    let rec = sqlx::query_scalar::<_, Lsn>(include_str!(
        "../sql/postgres/queries/reload_supersede_floor.sql"
    ))
    .bind(epoch.0)
    .bind(source_schema)
    .bind(source_table)
    .fetch_optional(ex)
    .await?;
    Ok(rec)
}

/// Crash recovery (H7): the `exporting` reloads this extractor may resume — its OWN live lease when
/// `include_own_live_lease` is enabled for startup, or an EXPIRED lease on any later scan. Re-acquires the
/// lease in the SAME guarded `UPDATE … RETURNING` (with `FOR UPDATE SKIP LOCKED`) so two racing
/// pods can never both adopt one row. A live FOREIGN lease (`lease_holder <> me AND lease_expiry >
/// statement_timestamp()`) is deliberately excluded — never stolen. `requested` rows are excluded too: those go
/// through ordinary pickup ([`claim_requested`]), keeping the two paths disjoint on status.
///
/// Recovery reads from control-pg, NOT from WAL redelivery (H7): by restart time the signals' LSNs
/// are behind `confirmed_flush`, acked and gone — the chunk cursor on the returned row is the only
/// thing a resume needs.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded adoption query fails or its rows cannot be
/// decoded.
pub async fn adopt_resumable(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
    include_own_live_lease: bool,
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/adopt_resumable.sql"))
        .bind(epoch.0)
        .bind(holder)
        .bind(lease_ttl_secs as f64)
        .bind(limit)
        .bind(include_own_live_lease)
        .fetch_all(ex)
        .await?;
    rows.iter().map(reload_from_row).collect()
}

/// Genuinely stuck exports: `exporting` rows whose lease has expired and which nobody is
/// renewing — a dead exporter no startup scan adopted. Surfaced as a per-tick warning and alert.
/// `export_complete` rows with an expired lease are NOT stuck — they are waiting on
/// the transformer, by design — so the filter is `exporting` only.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if expired exporters cannot be queried or decoded.
pub async fn stuck_exporting(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<(ReloadId, Option<String>)>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/stuck_exporting.sql"))
        .bind(epoch.0)
        .fetch_all(ex)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                ReloadId(row.try_get("reload_id")?),
                row.try_get("lease_holder")?,
            ))
        })
        .collect()
}

/// Tables mid-rebuild — the transformer-pause predicate's input.
///
/// This includes both persisted flavors because `resync` is a compatibility alias for the same
/// rebuild. The generic live-table claim stays paused throughout
/// `requested | exporting | export_complete | publishing`; once the export is marker-complete,
/// only the fenced publication-specific claim may drain the frozen `[F,H]` set. The row leaves
/// this result only after publication reaches a terminal status.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if active rebuild rows cannot be queried or decoded.
pub async fn active_rebuilds(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/active_rebuilds.sql"))
        .bind(epoch.0)
        .fetch_all(ex)
        .await?;
    rows.iter().map(reload_from_row).collect()
}

/// Return the marker-complete rebuild ready for this table's hidden-generation reconciliation.
///
/// This is deliberately independent of `file_manifest`: an empty table has no reload Parquet file,
/// but its matching baseline/end records are sufficient to initialize and publish an empty shadow.
/// The query verifies that both marker LSNs and schema versions exactly match the attempt before
/// exposing it to the transformer.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the ready rebuild cannot be queried or decoded.
pub async fn ready_rebuild(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<ReloadRow>, ControlError> {
    let row = sqlx::query(include_str!("../sql/postgres/queries/ready_rebuild.sql"))
        .bind(epoch.0)
        .bind(source_schema)
        .bind(source_table)
        .fetch_optional(ex)
        .await?;
    row.as_ref().map(reload_from_row).transpose()
}

/// Read one reload attempt, if it exists.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the reload row cannot be queried or decoded.
pub async fn get(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
) -> Result<Option<ReloadRow>, ControlError> {
    let row = sqlx::query(include_str!("../sql/postgres/queries/get.sql"))
        .bind(reload_id.0)
        .fetch_optional(ex)
        .await?;
    row.as_ref().map(reload_from_row).transpose()
}

fn publication_from_row(
    row: &PgRow,
    reload_id: ReloadId,
) -> Result<ReloadPublication, ControlError> {
    let missing = || ControlError::ReloadTransition {
        reload_id,
        expected: "marker-valid publishing receipt",
    };
    let value_reload_id = row
        .try_get::<Option<i64>, _>("reload_id")?
        .map(ReloadId)
        .ok_or_else(missing)?;
    let status = row
        .try_get::<Option<String>, _>("status")?
        .ok_or_else(missing)?
        .parse()?;
    Ok(ReloadPublication {
        reload_id: value_reload_id,
        epoch: EpochNo(
            row.try_get::<Option<i64>, _>("epoch")?
                .ok_or_else(missing)?,
        ),
        source_schema: row
            .try_get::<Option<String>, _>("source_schema")?
            .ok_or_else(missing)?,
        source_table: row
            .try_get::<Option<String>, _>("source_table")?
            .ok_or_else(missing)?,
        status,
        start_lsn: row
            .try_get::<Option<Lsn>, _>("start_lsn")?
            .ok_or_else(missing)?,
        final_lsn: row
            .try_get::<Option<Lsn>, _>("final_lsn")?
            .ok_or_else(missing)?,
        schema_version: SchemaVersionNo(
            row.try_get::<Option<i64>, _>("schema_version")?
                .ok_or_else(missing)?,
        ),
        publication_nonce: row
            .try_get::<Option<Uuid>, _>("publication_nonce")?
            .ok_or_else(missing)?,
        publisher_owner_pod: row
            .try_get::<Option<String>, _>("publisher_owner_pod")?
            .ok_or_else(missing)?,
        publisher_fencing_token: row
            .try_get::<Option<i64>, _>("publisher_fencing_token")?
            .ok_or_else(missing)?,
    })
}

/// Claim the marker-complete export for hidden-generation publication, or adopt its durable
/// `publishing` state after a crash. The stable nonce is minted exactly once. The transition is
/// rejected unless `owner_pod`/`fencing_token` still identify an unexpired table-ownership lease.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] when ownership or boundary markers are invalid, or
/// the underlying typed database/decode error when the atomic claim cannot be read or written.
pub async fn claim_publication(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<Option<ReloadPublication>, ControlError> {
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/claim_reload_publication.sql"
    ))
    .bind(epoch.0)
    .bind(source_schema)
    .bind(source_table)
    .bind(owner_pod)
    .bind(fencing_token)
    .fetch_one(ex)
    .await?;
    let Some(candidate_id) = row.try_get::<Option<i64>, _>("candidate_reload_id")? else {
        return Ok(None);
    };
    let reload_id = ReloadId(candidate_id);
    if !row.try_get::<bool, _>("ownership_valid")? {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "export_complete/publishing with the current live table-ownership fence",
        });
    }
    if !row.try_get::<bool, _>("boundaries_valid")? {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "export_complete/publishing with exactly matching baseline and end markers",
        });
    }
    let publication = publication_from_row(&row, reload_id)?;
    if publication.status != ReloadStatus::Publishing {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "publishing",
        });
    }
    Ok(Some(publication))
}

/// Read a durable `publishing` or `complete` publication receipt by attempt id. This intentionally
/// does not require current ownership: a successor uses it to reconcile a Duck `published` receipt
/// after the prior owner's crash.
///
/// # Errors
///
/// Returns the underlying typed database or decode error when the receipt cannot be read exactly.
pub async fn read_publication(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
) -> Result<Option<ReloadPublication>, ControlError> {
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/read_reload_publication.sql"
    ))
    .bind(reload_id.0)
    .fetch_optional(ex)
    .await?;
    row.as_ref()
        .map(|row| publication_from_row(row, reload_id))
        .transpose()
}

/// Claim the next complete manifest units through this publication's H without consulting the
/// ordinary reload pause predicate. A protocol-v2 stream group is indivisible: if its first child
/// fits the file budget, every child is returned (so one large head group may exceed `limit`).
/// This compatibility view stops before a zero-child schema barrier; new transformer code must use
/// [`claim_publication_ready_units`] to process those ordered work items.
///
/// # Errors
///
/// Returns a typed database/decode error or [`ControlError::ManifestInvariant`] when a selected
/// stream group is incomplete or inconsistent.
pub async fn claim_publication_ready(
    ex: impl PgExecutor<'_>,
    publication: &ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
    limit: i64,
) -> Result<Vec<crate::manifest::ManifestRow>, ControlError> {
    let units =
        claim_publication_ready_units(ex, publication, owner_pod, fencing_token, limit).await?;
    let mut files = Vec::new();
    for unit in units {
        match unit {
            crate::manifest::ReadyManifestUnit::Files(mut unit_files) => {
                files.append(&mut unit_files);
            }
            crate::manifest::ReadyManifestUnit::SchemaBarrier(_) => break,
        }
    }
    Ok(files)
}

/// Claim the next complete manifest or zero-child schema-barrier units through a fenced reload
/// publication's H. Units are commit ordered and streamed file groups remain indivisible.
///
/// # Errors
///
/// Returns a typed database/decode error, [`ControlError::ReloadTransition`] when the publication
/// fence is no longer live, or [`ControlError::ManifestInvariant`] for inconsistent grouped work.
pub async fn claim_publication_ready_units(
    ex: impl PgExecutor<'_>,
    publication: &ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
    limit: i64,
) -> Result<Vec<crate::manifest::ReadyManifestUnit>, ControlError> {
    let rows = sqlx::query(include_str!(
        "../sql/postgres/queries/claim_publication_ready_units.sql"
    ))
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .bind(owner_pod)
    .bind(fencing_token)
    .bind(limit)
    .fetch_all(ex)
    .await?;
    let authorized = rows
        .first()
        .ok_or(ControlError::ReloadTransition {
            reload_id: publication.reload_id,
            expected: "publication claim returned no authorization result",
        })?
        .try_get::<bool, _>("claim_authorized")?;
    if !authorized {
        return Err(ControlError::ReloadTransition {
            reload_id: publication.reload_id,
            expected: "publishing with the current live table-ownership fence",
        });
    }
    let mut work = Vec::new();
    for row in rows {
        if row.try_get::<bool, _>("claim_has_work")? {
            work.push(row);
        }
    }
    crate::manifest::ready_manifest_units_from_pg(work)
}

/// Observational sub-check used only while [`seal_publication_if_drained`] holds the table's
/// publication fence. It returns true only when no manifest row in any status and no active/corrupt
/// group remains through H. It is deliberately private: an unlocked drained observation is not a
/// cutover proof.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for an invalid publication/ownership fence, or the
/// underlying typed database error when the pending-row count cannot be read.
async fn publication_drained(
    ex: impl PgExecutor<'_>,
    publication: &ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<bool, ControlError> {
    let pending = sqlx::query_as::<_, (i64, bool)>(include_str!(
        "../sql/postgres/queries/publication_pending_through.sql"
    ))
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .bind(owner_pod)
    .bind(fencing_token)
    .fetch_optional(ex)
    .await?;
    let (pending, ungrouped_straddler) = pending.ok_or(ControlError::ReloadTransition {
        reload_id: publication.reload_id,
        expected: "publishing with the current live table-ownership fence",
    })?;
    if ungrouped_straddler {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "reload {} cannot seal while an ungrouped manifest straddles H {}",
                publication.reload_id, publication.final_lsn
            ),
        });
    }
    Ok(pending == 0)
}

/// Atomically seal this table's manifest publication prefix through the reload's H after proving
/// that prefix is drained. The caller must commit `tx` before publishing the Duck shadow. Source
/// publishers serialize on the same table row, so after commit no fresh manifest/group at or below
/// H can appear. Repeating the exact seal is idempotent.
///
/// Lock order is global and deliberate: table ownership, reload receipt, table-key advisory lock,
/// then manifest fence. The pending-work read is a later statement in the same transaction, so if
/// fence acquisition waited for a publisher it receives a fresh READ COMMITTED snapshot containing
/// that publisher's work.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for a stale publication/ownership identity,
/// [`ControlError::ManifestInvariant`] if a newer incompatible seal already exists, or the
/// underlying database error.
pub async fn seal_publication_if_drained(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    publication: &ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<bool, ControlError> {
    lock_publication(&mut **tx, publication, owner_pod, fencing_token).await?;
    crate::manifest::lock_manifest_publication_table(
        tx,
        publication.epoch,
        &publication.source_schema,
        &publication.source_table,
    )
    .await?;

    let existing: (Option<Lsn>, Option<i64>, Option<Uuid>) = sqlx::query_as(
        "INSERT INTO public.walrus_manifest_publication_fence AS fence
           (epoch, source_schema, source_table)
         VALUES ($1,$2,$3)
         ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
           SET updated_at = fence.updated_at
         RETURNING sealed_through_lsn, sealed_reload_id, sealed_publication_nonce",
    )
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .fetch_one(&mut **tx)
    .await?;

    let exact_existing = existing.0 == Some(publication.final_lsn)
        && existing.1 == Some(publication.reload_id.0)
        && existing.2 == Some(publication.publication_nonce);
    if existing
        .0
        .is_some_and(|sealed| sealed > publication.final_lsn)
    {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "reload {} cannot replace newer manifest seal {:?}",
                publication.reload_id, existing.0
            ),
        });
    }
    if !publication_drained(&mut **tx, publication, owner_pod, fencing_token).await? {
        if exact_existing {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "reload {} has pending manifest work at or below its durable seal",
                    publication.reload_id
                ),
            });
        }
        return Ok(false);
    }
    if exact_existing {
        return Ok(true);
    }

    sqlx::query("SELECT set_config('walrus.manifest_seal_protocol', '2', true)")
        .execute(&mut **tx)
        .await?;

    let changed = sqlx::query(
        "UPDATE public.walrus_manifest_publication_fence
         SET sealed_through_lsn=$4, sealed_reload_id=$5,
             sealed_publication_nonce=$6, updated_at=now()
         WHERE epoch=$1 AND source_schema=$2 AND source_table=$3
           AND (sealed_through_lsn IS NULL OR sealed_through_lsn <= $4)",
    )
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .bind(publication.final_lsn)
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .execute(&mut **tx)
    .await?;
    // The caller owns `tx`; keep the trigger capability scoped to the guarded write instead of
    // accidentally authorizing arbitrary later statements in the same transaction.
    sqlx::query("SELECT pg_catalog.set_config('walrus.manifest_seal_protocol', '', true)")
        .execute(&mut **tx)
        .await?;
    if changed.rows_affected() != 1 {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "reload {} failed to persist its manifest publication seal",
                publication.reload_id
            ),
        });
    }
    Ok(true)
}

/// Lock and verify the publication plus ownership rows. Call this inside the same control
/// transaction that retires the appended manifest ids; the row locks prevent either fence from
/// changing between authorization and deletion.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] when the immutable receipt or live ownership fence
/// differs, or the underlying typed database/decode error when the rows cannot be locked/read.
pub async fn lock_publication(
    ex: impl PgExecutor<'_>,
    publication: &ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<(), ControlError> {
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/lock_reload_publication.sql"
    ))
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .bind(owner_pod)
    .bind(fencing_token)
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .fetch_optional(ex)
    .await?;
    let exact = row.is_some_and(|row| {
        row.try_get::<i64, _>("epoch").ok() == Some(publication.epoch.0)
            && row.try_get::<String, _>("source_schema").ok().as_deref()
                == Some(publication.source_schema.as_str())
            && row.try_get::<String, _>("source_table").ok().as_deref()
                == Some(publication.source_table.as_str())
            && row.try_get::<Lsn, _>("start_lsn").ok() == Some(publication.start_lsn)
            && row.try_get::<Lsn, _>("final_lsn").ok() == Some(publication.final_lsn)
            && row.try_get::<i64, _>("schema_version").ok() == Some(publication.schema_version.0)
            && row.try_get::<Uuid, _>("publication_nonce").ok()
                == Some(publication.publication_nonce)
            && row
                .try_get::<String, _>("publisher_owner_pod")
                .ok()
                .as_deref()
                == Some(publication.publisher_owner_pod.as_str())
            && row.try_get::<i64, _>("publisher_fencing_token").ok()
                == Some(publication.publisher_fencing_token)
    });
    if !exact {
        return Err(ControlError::ReloadTransition {
            reload_id: publication.reload_id,
            expected: "the exact immutable publishing receipt with the current live ownership fence",
        });
    }
    Ok(())
}

/// After the Duck shadow is durably `published`, atomically set both canonical checkpoints to H
/// and transition `publishing → complete`. The exact durable manifest seal created by
/// [`seal_publication_if_drained`] is mandatory. A retry after an ambiguous commit is validated by
/// a later READ COMMITTED statement, so a concurrent finisher cannot cause a stale-snapshot false
/// failure. Once complete, the immutable reload receipt is the durable attestation that an exact-H
/// seal/checkpoint proof existed; later checkpoint progress or a newer reload seal remains valid.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] when neither the fenced transition nor its exact
/// committed replay is valid, or the underlying typed database/decode error.
pub async fn finish_publication<'a>(
    ex: impl sqlx::Acquire<'a, Database = Postgres>,
    publication: &ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<bool, ControlError> {
    let mut connection = sqlx::Acquire::acquire(ex).await?;
    let mut tx = Connection::begin(&mut *connection).await?;
    let prepared: bool = sqlx::query_scalar(include_str!(
        "../sql/postgres/queries/finish_reload_publication.sql"
    ))
    .bind(publication.reload_id.0)
    .bind(publication.publication_nonce)
    .bind(owner_pod)
    .bind(fencing_token)
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .bind(publication.start_lsn)
    .bind(publication.final_lsn)
    .bind(publication.schema_version.0)
    .bind(&publication.publisher_owner_pod)
    .bind(publication.publisher_fencing_token)
    .fetch_one(&mut *tx)
    .await?;

    if prepared {
        let transitioned: bool = sqlx::query_scalar(include_str!(
            "../sql/postgres/queries/complete_reload_publication.sql"
        ))
        .bind(publication.reload_id.0)
        .bind(publication.epoch.0)
        .bind(&publication.source_schema)
        .bind(&publication.source_table)
        .bind(publication.publication_nonce)
        .bind(publication.start_lsn)
        .bind(publication.final_lsn)
        .bind(publication.schema_version.0)
        .bind(&publication.publisher_owner_pod)
        .bind(publication.publisher_fencing_token)
        .fetch_one(&mut *tx)
        .await?;
        if !transitioned {
            tx.rollback().await?;
            return Err(ControlError::ReloadTransition {
                reload_id: publication.reload_id,
                expected: "the row-locked publishing receipt to complete after its exact seal/checkpoint proof",
            });
        }
        tx.commit().await?;
        return Ok(true);
    }

    // End the attempted transition before validating a replay. At READ COMMITTED this next
    // statement takes a fresh snapshot after any lock wait in the preparation statement.
    tx.rollback().await?;
    let already_complete: bool = sqlx::query_scalar(include_str!(
        "../sql/postgres/queries/read_completed_reload_publication.sql"
    ))
    .bind(publication.reload_id.0)
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .bind(publication.publication_nonce)
    .bind(publication.start_lsn)
    .bind(publication.final_lsn)
    .bind(publication.schema_version.0)
    .bind(&publication.publisher_owner_pod)
    .bind(publication.publisher_fencing_token)
    .fetch_one(&mut *connection)
    .await?;
    if !already_complete {
        return Err(ControlError::ReloadTransition {
            reload_id: publication.reload_id,
            expected: "exact publishing transition with live ownership/seal, or its immutable completed receipt with checkpoints through H",
        });
    }
    Ok(false)
}

#[cfg(test)]
#[path = "reload_test.rs"]
mod tests;
