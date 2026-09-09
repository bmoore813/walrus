//! `schema_registry` model: the versioned per-column type-mapping descriptors.
//!
//! **History, never a queue** — never pruned. The extractor writes one row per structural
//! `schema_version` (a `Vec` of [`TypeDescriptor`]s from `common`, plus a snapshot of the
//! resulting column set); the transformer reads it back to rebuild the exact source types for a given
//! file's `schema_version`. A `DELETE` here would make old-version Parquet files un-reconstructable.

use crate::ControlError;
use common::{EpochNo, SchemaVersionNo, TypeDescriptor};
use sqlx::types::Json;
use sqlx::{PgExecutor, Row};

/// One `schema_version` of a table's type mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRow {
    /// Generation this mapping was registered under.
    pub epoch: EpochNo,
    /// Schema of the table this version describes.
    pub source_schema: String,
    /// Table this version describes.
    pub source_table: String,
    /// The structural version these descriptors describe. Manifest rows name this value, which is
    /// how a Parquet file finds the mapping it was written against.
    pub schema_version: SchemaVersionNo,
    /// The per-column descriptors (stored as `jsonb`).
    pub descriptors: Vec<TypeDescriptor>,
    /// The resulting structural relation snapshot, optionally enriched with the catalog fence's
    /// initial table/column comments for bootstrap metadata reconciliation.
    pub columns: serde_json::Value,
}

/// Write the immutable descriptor set for one `schema_version`. An exact replay reuses the existing
/// key; a replay with different descriptors or relation columns is rejected rather than rewriting
/// history already referenced by manifest files.
///
/// # Errors
///
/// Returns [`ControlError::ImmutableHistoryConflict`] when an existing key has different content,
/// [`ControlError::Connect`] when persistence fails, or [`ControlError::CheckViolation`] if the row
/// violates a database invariant.
pub async fn upsert_registry(
    ex: impl PgExecutor<'_>,
    row: &RegistryRow,
) -> Result<(), ControlError> {
    let persisted = sqlx::query(include_str!("../sql/postgres/queries/upsert_registry.sql"))
        .bind(row.epoch.0)
        .bind(&row.source_schema)
        .bind(&row.source_table)
        .bind(row.schema_version.0)
        .bind(Json(&row.descriptors))
        .bind(&row.columns)
        .fetch_optional(ex)
        .await?;
    if persisted.is_none() {
        return Err(ControlError::ImmutableHistoryConflict {
            entity: "schema_registry",
            key: format!(
                "epoch={} table={}.{} version={}",
                row.epoch, row.source_schema, row.source_table, row.schema_version
            ),
        });
    }
    Ok(())
}

/// Read every historical registry row for an epoch in stable table/version order. The extractor uses
/// this complete history on restart so replayed WAL can bind an old `Relation` message to the exact
/// schema version it originally described rather than falling forward to the latest version.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the history cannot be queried or decoded.
pub async fn read_all_registry(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<RegistryRow>, ControlError> {
    let rows = sqlx::query(
        r#"
        SELECT epoch, source_schema, source_table, schema_version, descriptors, columns
        FROM public.walrus_schema_registry
        WHERE epoch = $1
        ORDER BY source_schema, source_table, schema_version
        "#,
    )
    .bind(epoch)
    .fetch_all(ex)
    .await?;
    rows.into_iter().map(registry_from_row).collect()
}

/// Read the descriptors for an **exact** `schema_version` — the transformer rebuilds types from this.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the registry row cannot be queried or decoded.
pub async fn read_registry(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    version: SchemaVersionNo,
) -> Result<Option<RegistryRow>, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/read_registry.sql",
        epoch.0,
        schema,
        table,
        version.0,
    )
    .fetch_optional(ex)
    .await?;

    Ok(rec.map(|r| RegistryRow {
        epoch: r.epoch.into(),
        source_schema: r.source_schema,
        source_table: r.source_table,
        schema_version: r.schema_version.into(),
        descriptors: r.descriptors.0,
        columns: r.columns,
    }))
}

/// Read one table's immutable registry history across an inclusive version range in version order.
/// The primary-key prefix `(epoch, source_schema, source_table)` keeps this a single bounded index
/// scan; callers can validate contiguity without issuing one query per claimed version number.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the range query fails or a row cannot be decoded.
pub async fn read_registry_range(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    first: SchemaVersionNo,
    last: SchemaVersionNo,
) -> Result<Vec<RegistryRow>, ControlError> {
    let rows = sqlx::query(
        r#"
        SELECT epoch, source_schema, source_table, schema_version, descriptors, columns
        FROM public.walrus_schema_registry
        WHERE epoch = $1
          AND source_schema = $2
          AND source_table = $3
          AND schema_version BETWEEN $4 AND $5
        ORDER BY schema_version
        "#,
    )
    .bind(epoch.0)
    .bind(schema)
    .bind(table)
    .bind(first.0)
    .bind(last.0)
    .fetch_all(ex)
    .await?;
    rows.into_iter().map(registry_from_row).collect()
}

/// The current (max) `schema_version` for a table, or `None` if it has no registry rows yet.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the version query cannot reach or read control Postgres.
pub async fn read_latest_version(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
) -> Result<Option<SchemaVersionNo>, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/read_latest_version.sql",
        epoch.0,
        schema,
        table,
    )
    .fetch_one(ex)
    .await?;
    Ok(rec.max_version.map(Into::into))
}

/// The **latest** registry row for every `(source_schema, source_table)` under `epoch` — what the
/// extractor hydrates its relation cache from at bootstrap (step 7). A runtime query (not `query!`) so it
/// needs no offline cache entry; the `jsonb` columns decode via `Json<_>` / `serde_json::Value`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the latest-row query fails or any typed column cannot be
/// decoded from control Postgres.
pub async fn read_all_latest_registry(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<RegistryRow>, ControlError> {
    let rows = sqlx::query(
        r#"
        SELECT r.epoch, r.source_schema, r.source_table, r.schema_version,
               r.descriptors, r.columns
        FROM public.walrus_schema_registry r
        JOIN (
            SELECT source_schema, source_table, MAX(schema_version) AS max_v
            FROM public.walrus_schema_registry
            WHERE epoch = $1
            GROUP BY source_schema, source_table
        ) latest
          ON r.source_schema = latest.source_schema
         AND r.source_table = latest.source_table
         AND r.schema_version = latest.max_v
        WHERE r.epoch = $1
        "#,
    )
    .bind(epoch)
    .fetch_all(ex)
    .await?;

    rows.into_iter().map(registry_from_row).collect()
}

fn registry_from_row(row: sqlx::postgres::PgRow) -> Result<RegistryRow, ControlError> {
    Ok(RegistryRow {
        epoch: row.try_get::<i64, _>("epoch")?.into(),
        source_schema: row.try_get("source_schema")?,
        source_table: row.try_get("source_table")?,
        schema_version: row.try_get::<i64, _>("schema_version")?.into(),
        descriptors: row
            .try_get::<Json<Vec<TypeDescriptor>>, _>("descriptors")?
            .0,
        columns: row.try_get("columns")?,
    })
}
