//! `ddl_manifest` model: one row per schema-change event, in commit-LSN order.
//!
//! **History, never a queue** — never pruned. Because the source's `ddl_audit` INSERTs ride the
//! same replication slot as DML, each event's `c_lsn` (commit LSN) is directly comparable to
//! `file_manifest.lsn_end` and the checkpoints. The transformer crosses a `schema_version` boundary by
//! applying the pending DDL whose `c_lsn` it is about to pass — there is no separate
//! ordering.

use crate::ControlError;
use common::{DdlId, EpochNo, Lsn, SchemaVersionNo};
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, Row};

/// A decoded schema-change event. `c_columns` and `c_dropped` remain JSON because they preserve the
/// source event payload; transformer-side parsing gives the fields their operational shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlRow {
    /// Assigned by the DB on insert; ignored by [`insert_ddl`].
    pub id: DdlId,
    /// Generation the event was recorded under.
    pub epoch: EpochNo,
    /// Identity primary key of the source `public.walrus_ddl_audit` row. Stable across WAL replay.
    pub source_audit_id: i64,
    /// Schema of the table the DDL changed.
    pub source_schema: String,
    /// Table the DDL changed.
    pub source_table: String,
    /// Commit LSN of the DDL — orders it relative to DML.
    pub c_lsn: Lsn,
    /// `ddl_command_end` | `sql_drop`.
    pub c_event: String,
    /// `CREATE TABLE` | `ALTER TABLE` | `DROP TABLE` | `COMMENT` | …
    pub c_tag: String,
    /// The `schema_version` this DDL produces.
    pub schema_version: SchemaVersionNo,
    /// Affected source relation OID, when the event still has one.
    pub c_rel_oid: Option<u32>,
    /// Structured post-change columns snapshot.
    pub c_columns: Option<serde_json::Value>,
    /// Structured dropped-object payload.
    pub c_dropped: Option<serde_json::Value>,
    /// Best-effort source SQL text, retained for audit/debugging but never replayed for correctness.
    pub c_ddl_text: Option<String>,
    /// Post-change table comment. `None` represents `COMMENT ON TABLE ... IS NULL`.
    pub c_table_comment: Option<String>,
}

/// Record an immutable decoded schema-change event from the extractor. `c_rel_oid` + `c_columns` are the
/// structured schema-diff payload (the source's post-change column snapshot) the transformer applies —
/// schema-DIFF, not a replay of the DDL text. An exact source-audit replay returns the original id;
/// changed content under that identity is rejected. Returns the assigned `id`.
///
/// # Errors
///
/// Returns [`ControlError::ImmutableHistoryConflict`] for changed replay content,
/// [`ControlError::Connect`] if the event cannot be inserted, or [`ControlError::CheckViolation`]
/// if its values violate a control-plane invariant.
pub async fn insert_ddl(ex: impl PgExecutor<'_>, row: &DdlRow) -> Result<DdlId, ControlError> {
    let c_rel_oid = row.c_rel_oid.map(sqlx::postgres::types::Oid);
    let rec = sqlx::query(include_str!("../sql/postgres/queries/insert_ddl.sql"))
        .bind(row.epoch.0)
        .bind(row.source_audit_id)
        .bind(&row.source_schema)
        .bind(&row.source_table)
        .bind(row.c_lsn)
        .bind(&row.c_event)
        .bind(&row.c_tag)
        .bind(row.schema_version.0)
        .bind(c_rel_oid)
        .bind(&row.c_columns)
        .bind(&row.c_dropped)
        .bind(&row.c_ddl_text)
        .bind(&row.c_table_comment)
        .fetch_optional(ex)
        .await?;
    let rec = rec.ok_or_else(|| ControlError::ImmutableHistoryConflict {
        entity: "ddl_manifest",
        key: format!(
            "epoch={} source_audit_id={}",
            row.epoch, row.source_audit_id
        ),
    })?;
    Ok(DdlId(rec.try_get("id")?))
}

/// DDL the transformer must apply before transforming past `after_lsn`, in `c_lsn` order (`id` breaks
/// ties) — the events with `c_lsn > after_lsn`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if control Postgres cannot execute or decode the query.
pub async fn read_pending_ddl(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    after_lsn: Lsn,
) -> Result<Vec<DdlRow>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/read_pending_ddl.sql"))
        .bind(epoch.0)
        .bind(schema)
        .bind(table)
        .bind(after_lsn)
        .fetch_all(ex)
        .await?;
    rows.into_iter().map(decode_ddl_row).collect()
}

/// Read the newest DDL metadata snapshot that is both newer in `(c_lsn, id)` order than the local
/// apply watermark and valid for the mirror's current structural version. `after_id` locates that
/// row's durable LSN, so a future-version row inserted out of order is not skipped. Every captured
/// row contains the complete post-change comment state, so intermediate revisions need not replay.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if control Postgres cannot execute or decode the query.
pub async fn read_latest_ddl_snapshot(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    after_id: DdlId,
    through_version: SchemaVersionNo,
) -> Result<Option<DdlRow>, ControlError> {
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/read_latest_ddl_snapshot.sql"
    ))
    .bind(epoch.0)
    .bind(schema)
    .bind(table)
    .bind(after_id.0)
    .bind(through_version.0)
    .fetch_optional(ex)
    .await?;
    row.map(decode_ddl_row).transpose()
}

/// Highest structural schema version whose source transaction committed no later than
/// `through_lsn`. Reload end-fence validation uses this boundary-aware read so DDL committed after
/// an already-durable H cannot invalidate the completed F..H snapshot.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if control Postgres cannot execute or decode the query.
pub async fn read_latest_ddl_version_through(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    through_lsn: Lsn,
) -> Result<Option<SchemaVersionNo>, ControlError> {
    let version = sqlx::query_scalar::<_, Option<i64>>(include_str!(
        "../sql/postgres/queries/read_latest_ddl_version_through.sql"
    ))
    .bind(epoch.0)
    .bind(schema)
    .bind(table)
    .bind(through_lsn)
    .fetch_one(ex)
    .await?;
    Ok(version.map(SchemaVersionNo))
}

/// Read the epoch's complete DDL history. The extractor hydrates source audit identities and committed
/// schema versions from this on restart so replay is idempotent before the first repeated event arrives.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if control Postgres cannot execute or decode the query.
pub async fn read_all_ddl(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<DdlRow>, ControlError> {
    let rows = sqlx::query(include_str!("../sql/postgres/queries/read_all_ddl.sql"))
        .bind(epoch.0)
        .fetch_all(ex)
        .await?;
    rows.into_iter().map(decode_ddl_row).collect()
}

fn decode_ddl_row(row: PgRow) -> Result<DdlRow, ControlError> {
    Ok(DdlRow {
        id: DdlId(row.try_get("id")?),
        epoch: EpochNo(row.try_get("epoch")?),
        source_audit_id: row.try_get("source_audit_id")?,
        source_schema: row.try_get("source_schema")?,
        source_table: row.try_get("source_table")?,
        c_lsn: row.try_get("c_lsn")?,
        c_event: row.try_get("c_event")?,
        c_tag: row.try_get("c_tag")?,
        schema_version: SchemaVersionNo(row.try_get("schema_version")?),
        c_rel_oid: row
            .try_get::<Option<sqlx::postgres::types::Oid>, _>("c_rel_oid")?
            .map(|oid| oid.0),
        c_columns: row.try_get("c_columns")?,
        c_dropped: row.try_get("c_dropped")?,
        c_ddl_text: row.try_get("c_ddl_text")?,
        c_table_comment: row.try_get("c_table_comment")?,
    })
}
