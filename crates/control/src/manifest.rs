//! `file_manifest` models: the extractor's insert-ready, the transformer's claim-in-commit-order, and the
//! queue's retire-by-delete.
//!
//! The manifest is a **work queue, not a history**. The single load-bearing line is the claim
//! ordering: `ORDER BY lsn_end, id` — commit LSN, then `id` as the tiebreaker. It is *not*
//! `lsn_end > raw_appended_lsn`: legacy snapshot files can share one `consistent_point` and therefore
//! one `lsn_end`, so a `>` filter would skip them forever. And it keys on the **commit** LSN, not
//! a max-row LSN, or a late-committing large transaction would be silently dropped. Retiring a file
//! is a `DELETE`, not a status flip — the queue's frontier advances by removal.

use crate::{
    ControlError, ddl_manifest::DdlRow, parse::ParseEnumError, schema_registry::RegistryRow,
};
use common::string_enum;
use common::{EpochNo, Lsn, ManifestId, ReloadId, SchemaVersionNo, UtcTimestamp};
use sqlx::{PgExecutor, PgPool, Postgres, Row, Transaction, postgres::PgRow};
use std::collections::{BTreeMap, BTreeSet};

// Each table below is the exact persisted string contract for its `file_manifest` text column.
string_enum! {
    /// The kind of a `file_manifest` row — the canonical enum for the `kind` text column, shared by the
    /// extractor (which writes it; extractor re-exports this as `FileKind`) and the transformer (which routes on it).
    ///
    /// `Spill` is a *single* streamed transaction written before its commit LSN was known; the
    /// transformer treats the file's `lsn_end` — not the per-row placeholder — as the authoritative
    /// `commit_lsn` for its rows. `Reload` chunk files enter the same `(lsn_end, id)` claim
    /// order carrying a `reload_id`; `Snapshot` is retained for legacy manifest compatibility.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ManifestKind {
        error = ParseEnumError;
        column = "file_manifest.kind";
        Snapshot => "snapshot",
        Stream => "stream",
        Spill => "spill",
        Reload => "reload",
    }
}

string_enum! {
    /// The lifecycle state of a `file_manifest` row: `Ready` to claim, or integrity-fenced `Failed`.
    /// A failed row is paired with table-level recovery state and deliberately blocks publication;
    /// it is never a license to skip data. Applied rows are DELETED (see [`delete_claimed`]).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ManifestStatus {
        error = ParseEnumError;
        column = "file_manifest.status";
        Ready => "ready",
        Failed => "failed",
    }
}

/// A `ready` file the transformer can claim. The column set is exactly what the claim query reads.
///
/// `kind` is `Snapshot | Stream | Spill | Reload` — reload chunk files enter this same
/// queue and sort into the same `(lsn_end, id)` order, carrying the `reload_id` the transformer's
/// rebuild trigger routes on. `Snapshot` remains readable for legacy manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRow {
    /// The row's primary key, and the *tiebreaker* half of the `(lsn_end, id)` claim order — so it
    /// is load-bearing for ordering, not just a handle.
    pub id: ManifestId,
    /// Generation this file belongs to; a retired generation's rows are never claimed.
    pub epoch: EpochNo,
    /// Source schema the file's rows came from.
    pub source_schema: String,
    /// Source table the file's rows came from.
    pub source_table: String,
    /// Where the Parquet lives. Durable in object storage *before* this row exists.
    pub s3_uri: String,
    /// Which producer wrote it, which is what the transformer routes on; see [`ManifestKind`].
    pub kind: ManifestKind,
    /// Rows in the file, for backlog accounting. Not a correctness input.
    pub row_count: i64,
    /// Exact stored object size in bytes.
    pub object_size: i64,
    /// SHA-256 of the exact Parquet object bytes. The database constrains this to 32 bytes.
    pub sha256: Vec<u8>,
    /// Lowest commit LSN in the file — diagnostic only; the queue never orders on it.
    pub lsn_start: Lsn,
    /// Highest **commit** LSN in the file: the frontier this file advances, and the primary claim
    /// sort key. Legacy snapshot files can share one `lsn_end`, which is why the claim uses
    /// `>=`-free ordering rather than a `>` filter.
    pub lsn_end: Lsn,
    /// The relation shape these rows were encoded at, so the transformer can reconstruct the types.
    pub schema_version: SchemaVersionNo,
    /// `Ready` to claim, or dead-lettered `Failed`; see [`ManifestStatus`].
    pub status: ManifestStatus,
    /// `Some` only for `kind='reload'` chunk files; the purge/routing key.
    pub reload_id: Option<ReloadId>,
    /// Atomic per-table stream group. `None` only for legacy/ordinary/reload singleton rows.
    pub stream_group_id: Option<ManifestGroupId>,
    /// Stable zero-based position within `stream_group_id`.
    pub stream_group_ordinal: Option<i64>,
    /// Real streamed-transaction commit timestamp, used to correct speculative spill metadata.
    pub stream_commit_ts: Option<String>,
    /// Top-level transaction id from the protocol-v2 `StreamCommit` receipt.
    pub stream_top_xid: Option<i64>,
    /// Number of children that must be appended atomically for this group.
    pub stream_group_expected_files: Option<i64>,
    /// Sum of all child row counts recorded by the atomic publication transaction.
    pub stream_group_row_count: Option<i64>,
    /// Final structural schema reached by this table in the streamed transaction. This may be newer
    /// than every child file when the transaction ends with DDL and emits no post-DDL row.
    pub stream_group_final_schema_version: Option<SchemaVersionNo>,
}

/// One per-table atomic publication group produced by a protocol-v2 streamed transaction.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestGroupId(pub i64);

/// A protocol-v2 source commit that advances one table's structural schema but emits no data file
/// for that table. This covers streamed and ordinary structural DDL-only transactions. It
/// participates in the same commit-LSN ordering as manifest units and is retired only after the
/// transformer has durably reconciled through `final_schema_version`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSchemaBarrier {
    /// Durable stream-group receipt id used to retire exactly this barrier.
    pub id: ManifestGroupId,
    /// Slot generation that owns the source commit.
    pub epoch: EpochNo,
    /// Source schema whose table must reconcile before this work item is retired.
    pub source_schema: String,
    /// Source table whose table must reconcile before this work item is retired.
    pub source_table: String,
    /// Authoritative source commit LSN and work-order key.
    pub commit_lsn: Lsn,
    /// Structural registry version the transformer must reach before retirement.
    pub final_schema_version: SchemaVersionNo,
}

/// One indivisible table-local item returned by the ordered protocol-v2 work claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadyManifestUnit {
    /// One legacy/snapshot singleton or every child of one streamed transaction group.
    Files(Vec<ManifestRow>),
    /// A structural source commit for this table with no data-file child.
    SchemaBarrier(StreamSchemaBarrier),
}

/// What the extractor inserts after its Parquet is durable in S3.
///
/// Comparable like the [`ManifestRow`] it becomes, so a mapper that builds one can be asserted as a
/// whole record instead of field by field (which silently skips whatever the assertion forgot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewManifestFile {
    /// Generation this file belongs to.
    pub epoch: EpochNo,
    /// Source schema the file's rows came from.
    pub source_schema: String,
    /// Source table the file's rows came from.
    pub source_table: String,
    /// Where the Parquet already lives — the insert happens only after the PUT is durable.
    pub s3_uri: String,
    /// Which producer wrote it; see [`ManifestKind`].
    pub kind: ManifestKind,
    /// Rows in the file, for backlog accounting.
    pub row_count: i64,
    /// Exact stored object size in bytes.
    pub object_size: i64,
    /// SHA-256 of the exact Parquet object bytes.
    pub sha256: Vec<u8>,
    /// Lowest commit LSN in the file — diagnostic only.
    pub lsn_start: Lsn,
    /// Highest **commit** LSN in the file — the value the claim order sorts on.
    pub lsn_end: Lsn,
    /// The relation shape these rows were encoded at.
    pub schema_version: SchemaVersionNo,
    /// Set (with `kind=Reload`) only by the chunk export engine; `None` otherwise.
    pub reload_id: Option<ReloadId>,
}

/// One protocol-v2 source commit publication. Large transactions supply their `StreamCommit`;
/// ordinary structural DDL-only transactions use the same atomic receipt with no files. The
/// control layer groups table work and publishes its schema history in one PostgreSQL commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewStreamCommitPublication {
    /// Slot generation that owns the receipt and every child row.
    pub epoch: EpochNo,
    /// Protocol-v2 top-level transaction id (`StreamCommit` xid or ordinary `Begin` final xid).
    pub top_xid: u32,
    /// Authoritative source commit LSN shared by DDL and every file.
    pub commit_lsn: Lsn,
    /// Authoritative source commit timestamp.
    pub commit_ts: UtcTimestamp,
    /// DDL audit rows whose source transaction ends at this same `StreamCommit`.
    pub ddl_rows: Vec<DdlRow>,
    /// Provisional relation shapes made globally visible by this same `StreamCommit`.
    pub registry_rows: Vec<RegistryRow>,
    /// Every object materialised for the transaction, grouped per table during publication. A table
    /// represented only by structural DDL plus its registry row receives a zero-child schema-barrier
    /// group.
    pub files: Vec<NewManifestFile>,
}

/// Whether this exact streamed commit became visible now or had already committed before a crash
/// that prevented the source ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStreamOutcome {
    /// The streamed transaction became durable in this call.
    Published,
    /// An identical durable publication receipt already existed.
    AlreadyPublished,
}

/// Result of atomically publishing one ordinary, ungrouped manifest file against the table's
/// durable reload seal.
///
/// A source replay at or below a committed seal needs no new queue row: the sealed reload shadow
/// already owns that source prefix. The caller may therefore discard its newly written object and
/// count the file's `lsn_end` as durable. Files above the seal are inserted normally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishManifestOutcome {
    /// A fresh queue row was committed.
    Published(ManifestId),
    /// No queue row was inserted because the durable reload seal covers this file's commit LSN.
    CoveredBySeal(Lsn),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamFileReplayShape {
    kind: String,
    row_count: i64,
    lsn_start: Lsn,
    lsn_end: Lsn,
    schema_version: SchemaVersionNo,
}

impl From<&NewManifestFile> for StreamFileReplayShape {
    fn from(file: &NewManifestFile) -> Self {
        Self {
            kind: file.kind.as_str().to_string(),
            row_count: file.row_count,
            lsn_start: file.lsn_start,
            lsn_end: file.lsn_end,
            schema_version: file.schema_version,
        }
    }
}

fn stream_file_shape_json(files: &[StreamFileReplayShape]) -> serde_json::Value {
    serde_json::Value::Array(
        files
            .iter()
            .map(|file| {
                serde_json::json!({
                    "kind": file.kind,
                    "row_count": file.row_count,
                    "lsn_start": file.lsn_start.as_u64(),
                    "lsn_end": file.lsn_end.as_u64(),
                    "schema_version": file.schema_version.0,
                })
            })
            .collect(),
    )
}

/// Final per-table schema barrier for every publication table represented by a file or structural
/// registry row. Starting from the child maximum makes the barrier total for data-only
/// transactions. Only registry versions backed by an exact non-COMMENT DDL key may advance or seed
/// a barrier.
fn stream_group_final_versions(
    publication: &NewStreamCommitPublication,
) -> BTreeMap<(&str, &str), SchemaVersionNo> {
    let mut versions = BTreeMap::new();
    for file in &publication.files {
        versions
            .entry((file.source_schema.as_str(), file.source_table.as_str()))
            .and_modify(|version: &mut SchemaVersionNo| {
                *version = (*version).max(file.schema_version);
            })
            .or_insert(file.schema_version);
    }
    let structural_versions = publication
        .ddl_rows
        .iter()
        .filter(|row| !row.c_tag.eq_ignore_ascii_case("COMMENT"))
        .map(|row| {
            (
                row.source_schema.as_str(),
                row.source_table.as_str(),
                row.schema_version,
            )
        })
        .collect::<BTreeSet<_>>();
    for row in &publication.registry_rows {
        let version_key = (
            row.source_schema.as_str(),
            row.source_table.as_str(),
            row.schema_version,
        );
        if structural_versions.contains(&version_key) {
            let key = (row.source_schema.as_str(), row.source_table.as_str());
            versions
                .entry(key)
                .and_modify(|version| *version = (*version).max(row.schema_version))
                .or_insert(row.schema_version);
        }
    }

    versions
}

/// Acquire the transaction-scoped advisory lock shared by manifest publishers and reload seals
/// for one table. Callers publishing several tables must invoke this in sorted table-key order and
/// acquire every advisory lock before touching any `manifest_publication_fence` row.
///
/// # Errors
///
/// Returns the underlying typed database error if the blocking lock statement fails.
pub async fn lock_manifest_publication_table(
    tx: &mut Transaction<'_, Postgres>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<(), ControlError> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(\
           public.walrus_manifest_publication_lock_key($1,$2,$3))",
    )
    .bind(epoch.0)
    .bind(source_schema)
    .bind(source_table)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Acquire every table-local publication fence for a fresh multi-table commit in deterministic
/// order. A committed reload seal is a hard boundary: accepting an older source commit after that
/// cutover would make the replacement generation incomplete. Existing receipt replays call this
/// only after their exact durable identity has been recognized and therefore bypass the fresh-work
/// rejection safely.
async fn lock_fresh_publication_fences<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    publication: &NewStreamCommitPublication,
    tables: impl Iterator<Item = (&'a str, &'a str)>,
) -> Result<(), ControlError> {
    let tables = tables.collect::<Vec<_>>();
    for &(source_schema, source_table) in &tables {
        lock_manifest_publication_table(tx, publication.epoch, source_schema, source_table).await?;
    }
    for (source_schema, source_table) in tables {
        let sealed_through: Option<Lsn> = sqlx::query_scalar(
            "INSERT INTO public.walrus_manifest_publication_fence AS fence
               (epoch, source_schema, source_table)
             VALUES ($1,$2,$3)
             ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
               SET updated_at = fence.updated_at
             RETURNING sealed_through_lsn",
        )
        .bind(publication.epoch.0)
        .bind(source_schema)
        .bind(source_table)
        .fetch_one(&mut **tx)
        .await?;
        if let Some(sealed) = sealed_through
            && publication.commit_lsn <= sealed
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "fresh source commit {} for {source_schema}.{source_table} is at or below durable reload seal {}",
                    publication.commit_lsn, sealed
                ),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamDdlReplayShape {
    source_audit_id: i64,
    source_schema: String,
    source_table: String,
    c_event: String,
    c_tag: String,
    schema_version: SchemaVersionNo,
    c_rel_oid: Option<u32>,
    c_columns: Option<serde_json::Value>,
    c_dropped: Option<serde_json::Value>,
    c_ddl_text: Option<String>,
    c_table_comment: Option<String>,
}

impl From<&DdlRow> for StreamDdlReplayShape {
    fn from(row: &DdlRow) -> Self {
        Self {
            source_audit_id: row.source_audit_id,
            source_schema: row.source_schema.clone(),
            source_table: row.source_table.clone(),
            c_event: row.c_event.clone(),
            c_tag: row.c_tag.clone(),
            schema_version: row.schema_version,
            c_rel_oid: row.c_rel_oid,
            c_columns: row.c_columns.clone(),
            c_dropped: row.c_dropped.clone(),
            c_ddl_text: row.c_ddl_text.clone(),
            c_table_comment: row.c_table_comment.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamRegistryReplayShape {
    source_schema: String,
    source_table: String,
    schema_version: SchemaVersionNo,
    descriptors: Vec<common::TypeDescriptor>,
    columns: serde_json::Value,
}

impl From<&RegistryRow> for StreamRegistryReplayShape {
    fn from(row: &RegistryRow) -> Self {
        Self {
            source_schema: row.source_schema.clone(),
            source_table: row.source_table.clone(),
            schema_version: row.schema_version,
            descriptors: row.descriptors.clone(),
            columns: row.columns.clone(),
        }
    }
}

const fn stream_publication_conflict(publication: &NewStreamCommitPublication) -> ControlError {
    ControlError::StreamPublicationConflict {
        epoch: publication.epoch,
        top_xid: publication.top_xid,
        commit_lsn: publication.commit_lsn,
    }
}

async fn validate_existing_stream_publication(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    publication_id: i64,
    publication: &NewStreamCommitPublication,
) -> Result<(), ControlError> {
    let expected_commit_ts = publication.commit_ts.to_string();
    let expected_final_versions = stream_group_final_versions(publication);
    let mut expected_groups = BTreeMap::<(String, String), Vec<StreamFileReplayShape>>::new();
    for file in &publication.files {
        expected_groups
            .entry((file.source_schema.clone(), file.source_table.clone()))
            .or_default()
            .push(file.into());
    }
    for schema_table in expected_final_versions.keys() {
        expected_groups
            .entry((schema_table.0.to_string(), schema_table.1.to_string()))
            .or_default();
    }
    for files in expected_groups.values_mut() {
        files.sort();
    }

    // Lock the durable group parents while reading their children. Transformer retirement updates the
    // parent in the same transaction that removes the children, so this sees either a complete
    // ready/failed child set or the durable applied aggregate, never a torn transition.
    let durable_groups = sqlx::query(
        "SELECT id, epoch, top_xid, source_schema, source_table, commit_lsn, commit_ts, \
                expected_files, row_count, final_schema_version, file_shape, status \
         FROM public.walrus_stream_manifest_group WHERE publication_id = $1 \
         ORDER BY source_schema, source_table FOR SHARE",
    )
    .bind(publication_id)
    .fetch_all(&mut **tx)
    .await?;
    if durable_groups.len() != expected_groups.len() {
        return Err(stream_publication_conflict(publication));
    }

    let mut seen_groups = BTreeSet::new();
    for group in durable_groups {
        let group_id: i64 = group.try_get("id")?;
        let source_schema: String = group.try_get("source_schema")?;
        let source_table: String = group.try_get("source_table")?;
        let key = (source_schema, source_table);
        if !seen_groups.insert(key.clone()) {
            return Err(ControlError::ManifestInvariant {
                message: format!("stream publication {publication_id} has a duplicate table group"),
            });
        }
        let Some(expected_files) = expected_groups.get(&key) else {
            return Err(stream_publication_conflict(publication));
        };
        let expected_count =
            i64::try_from(expected_files.len()).map_err(|_| ControlError::ManifestInvariant {
                message: format!("too many replay files in {}.{}", key.0, key.1),
            })?;
        let expected_rows = expected_files.iter().try_fold(0_i64, |sum, file| {
            sum.checked_add(file.row_count)
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("replay row-count overflow in {}.{}", key.0, key.1),
                })
        })?;
        let durable_epoch: i64 = group.try_get("epoch")?;
        let durable_top_xid: i64 = group.try_get("top_xid")?;
        let durable_commit_lsn: Lsn = group.try_get("commit_lsn")?;
        let durable_commit_ts: String = group.try_get("commit_ts")?;
        let durable_count: i64 = group.try_get("expected_files")?;
        let durable_rows: i64 = group.try_get("row_count")?;
        let durable_final_version = SchemaVersionNo(group.try_get("final_schema_version")?);
        let durable_shape: serde_json::Value = group.try_get("file_shape")?;
        let status: String = group.try_get("status")?;
        let expected_final_version = expected_final_versions.get(&(key.0.as_str(), key.1.as_str()));
        if durable_epoch != publication.epoch.0
            || durable_top_xid != i64::from(publication.top_xid)
            || durable_commit_lsn != publication.commit_lsn
            || durable_commit_ts != expected_commit_ts
            || durable_count != expected_count
            || durable_rows != expected_rows
            || Some(&durable_final_version) != expected_final_version
            || durable_shape != stream_file_shape_json(expected_files)
        {
            return Err(stream_publication_conflict(publication));
        }

        let children = sqlx::query(
            "SELECT kind, row_count, lsn_start, lsn_end, schema_version \
             FROM public.walrus_file_manifest WHERE stream_group_id = $1 \
             ORDER BY stream_group_ordinal",
        )
        .bind(group_id)
        .fetch_all(&mut **tx)
        .await?;
        if children.is_empty() && matches!(status.as_str(), "applied" | "superseded") {
            // Applied and snapshot-superseded child manifests are intentionally retired. The
            // parent permanently retains and has just proven their complete semantic shape.
            continue;
        }
        if children.len() != expected_files.len() || !matches!(status.as_str(), "ready" | "failed")
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {group_id} status {status:?} has {} children but expected {durable_count}",
                    children.len()
                ),
            });
        }
        let mut durable_files = children
            .into_iter()
            .map(|child| {
                Ok(StreamFileReplayShape {
                    kind: child.try_get("kind")?,
                    row_count: child.try_get("row_count")?,
                    lsn_start: child.try_get("lsn_start")?,
                    lsn_end: child.try_get("lsn_end")?,
                    schema_version: SchemaVersionNo(child.try_get("schema_version")?),
                })
            })
            .collect::<Result<Vec<_>, ControlError>>()?;
        durable_files.sort();
        if &durable_files != expected_files {
            return Err(stream_publication_conflict(publication));
        }
    }
    if seen_groups.len() != expected_groups.len() {
        return Err(stream_publication_conflict(publication));
    }

    let mut expected_ddl = publication
        .ddl_rows
        .iter()
        .map(StreamDdlReplayShape::from)
        .collect::<Vec<_>>();
    expected_ddl.sort_by_key(|row| row.source_audit_id);
    let mut durable_ddl = sqlx::query(
        "SELECT source_audit_id, source_schema, source_table, c_event, c_tag, schema_version, \
                c_rel_oid, c_columns, c_dropped, c_ddl_text, c_table_comment \
         FROM public.walrus_ddl_manifest WHERE epoch = $1 AND c_lsn = $2 \
         ORDER BY source_audit_id",
    )
    .bind(publication.epoch.0)
    .bind(publication.commit_lsn)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|row| {
        Ok(StreamDdlReplayShape {
            source_audit_id: row.try_get("source_audit_id")?,
            source_schema: row.try_get("source_schema")?,
            source_table: row.try_get("source_table")?,
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
    })
    .collect::<Result<Vec<_>, ControlError>>()?;
    durable_ddl.sort_by_key(|row| row.source_audit_id);
    if durable_ddl != expected_ddl {
        return Err(stream_publication_conflict(publication));
    }

    // Registry rows are immutable history keyed by table/version rather than receipt id. Validate
    // every replayed key and its exact shape; DdlConsumer only prepares registry rows owned by the
    // DDL events compared above.
    for expected in publication
        .registry_rows
        .iter()
        .map(StreamRegistryReplayShape::from)
    {
        let durable = sqlx::query(
            "SELECT descriptors, columns FROM public.walrus_schema_registry \
             WHERE epoch=$1 AND source_schema=$2 AND source_table=$3 AND schema_version=$4",
        )
        .bind(publication.epoch.0)
        .bind(&expected.source_schema)
        .bind(&expected.source_table)
        .bind(expected.schema_version.0)
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| {
            Ok::<_, ControlError>(StreamRegistryReplayShape {
                source_schema: expected.source_schema.clone(),
                source_table: expected.source_table.clone(),
                schema_version: expected.schema_version,
                descriptors: row
                    .try_get::<sqlx::types::Json<Vec<common::TypeDescriptor>>, _>("descriptors")?
                    .0,
                columns: row.try_get("columns")?,
            })
        })
        .transpose()?;
        if durable.as_ref() != Some(&expected) {
            return Err(stream_publication_conflict(publication));
        }
    }
    Ok(())
}

/// Validate that every protocol-v2 group in one claim is complete and internally identical to its
/// durable parent receipt. A malformed group is never handed to DuckLake as a partial transaction.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for incomplete, duplicated, or internally
/// inconsistent group metadata.
pub fn validate_claimed_groups(rows: &[ManifestRow]) -> Result<(), ControlError> {
    let mut groups: BTreeMap<ManifestGroupId, Vec<&ManifestRow>> = BTreeMap::new();
    for row in rows {
        if row.object_size <= 0 || row.sha256.len() != 32 || row.row_count <= 0 {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "manifest {} has an invalid object fingerprint/count",
                    row.id
                ),
            });
        }
        match row.stream_group_id {
            Some(group_id) => groups.entry(group_id).or_default().push(row),
            None => {
                if row.status != ManifestStatus::Ready
                    || row.stream_group_ordinal.is_some()
                    || row.stream_commit_ts.is_some()
                    || row.stream_top_xid.is_some()
                    || row.stream_group_expected_files.is_some()
                    || row.stream_group_row_count.is_some()
                    || row.stream_group_final_schema_version.is_some()
                {
                    return Err(ControlError::ManifestInvariant {
                        message: format!(
                            "ungrouped manifest {} carries stream-group metadata",
                            row.id
                        ),
                    });
                }
            }
        }
    }

    for (group_id, children) in groups {
        let Some(first) = children.first().copied() else {
            return Err(ControlError::ManifestInvariant {
                message: format!("stream group {} has no children", group_id.0),
            });
        };
        if first.status != ManifestStatus::Ready {
            return Err(ControlError::ManifestInvariant {
                message: format!("stream group {} is not ready", group_id.0),
            });
        }
        let expected =
            first
                .stream_group_expected_files
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("stream group {} has no expected file count", group_id.0),
                })?;
        let group_rows =
            first
                .stream_group_row_count
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("stream group {} has no expected row count", group_id.0),
                })?;
        let final_schema_version = first.stream_group_final_schema_version.ok_or_else(|| {
            ControlError::ManifestInvariant {
                message: format!("stream group {} has no final schema version", group_id.0),
            }
        })?;
        let top_xid = first
            .stream_top_xid
            .ok_or_else(|| ControlError::ManifestInvariant {
                message: format!("stream group {} has no top xid", group_id.0),
            })?;
        let commit_ts =
            first
                .stream_commit_ts
                .as_deref()
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("stream group {} has no commit timestamp", group_id.0),
                })?;
        if expected <= 0
            || usize::try_from(expected).ok() != Some(children.len())
            || !(0..=i64::from(u32::MAX)).contains(&top_xid)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {} returned {} children but expected {expected}",
                    group_id.0,
                    children.len()
                ),
            });
        }

        let mut ordinals = BTreeSet::new();
        let mut actual_rows = 0_i64;
        for child in children {
            if !matches!(child.kind, ManifestKind::Stream | ManifestKind::Spill)
                || child.status != ManifestStatus::Ready
                || child.reload_id.is_some()
                || child.stream_group_expected_files != Some(expected)
                || child.stream_group_row_count != Some(group_rows)
                || child.stream_group_final_schema_version != Some(final_schema_version)
                || child.schema_version > final_schema_version
                || child.stream_top_xid != Some(top_xid)
                || child.stream_commit_ts.as_deref() != Some(commit_ts)
                || child.lsn_end != first.lsn_end
                || child.epoch != first.epoch
                || child.source_schema != first.source_schema
                || child.source_table != first.source_table
            {
                return Err(ControlError::ManifestInvariant {
                    message: format!(
                        "stream group {} has inconsistent child {}",
                        group_id.0, child.id
                    ),
                });
            }
            let ordinal =
                child
                    .stream_group_ordinal
                    .ok_or_else(|| ControlError::ManifestInvariant {
                        message: format!(
                            "stream group {} child {} has no ordinal",
                            group_id.0, child.id
                        ),
                    })?;
            if ordinal < 0 || ordinal >= expected || !ordinals.insert(ordinal) {
                return Err(ControlError::ManifestInvariant {
                    message: format!(
                        "stream group {} has invalid/duplicate ordinal {ordinal}",
                        group_id.0
                    ),
                });
            }
            actual_rows = actual_rows.checked_add(child.row_count).ok_or_else(|| {
                ControlError::ManifestInvariant {
                    message: format!("stream group {} row count overflow", group_id.0),
                }
            })?;
        }
        if actual_rows != group_rows {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {} row count {actual_rows} does not match receipt {group_rows}",
                    group_id.0
                ),
            });
        }
    }
    Ok(())
}

fn manifest_row_from_pg(row: &PgRow) -> Result<ManifestRow, ControlError> {
    let kind: String = row.try_get("kind")?;
    let status: String = row.try_get("status")?;
    Ok(ManifestRow {
        id: ManifestId(row.try_get("id")?),
        epoch: EpochNo(row.try_get("epoch")?),
        source_schema: row.try_get("source_schema")?,
        source_table: row.try_get("source_table")?,
        s3_uri: row.try_get("s3_uri")?,
        kind: kind.parse()?,
        row_count: row.try_get("row_count")?,
        object_size: row.try_get("object_size")?,
        sha256: row.try_get("sha256")?,
        lsn_start: row.try_get("lsn_start")?,
        lsn_end: row.try_get("lsn_end")?,
        schema_version: SchemaVersionNo(row.try_get("schema_version")?),
        status: status.parse()?,
        reload_id: row.try_get::<Option<i64>, _>("reload_id")?.map(ReloadId),
        stream_group_id: row
            .try_get::<Option<i64>, _>("stream_group_id")?
            .map(ManifestGroupId),
        stream_group_ordinal: row.try_get("stream_group_ordinal")?,
        stream_commit_ts: row.try_get("stream_commit_ts")?,
        stream_top_xid: row.try_get("stream_top_xid")?,
        stream_group_expected_files: row.try_get("stream_group_expected_files")?,
        stream_group_row_count: row.try_get("stream_group_row_count")?,
        stream_group_final_schema_version: row
            .try_get::<Option<i64>, _>("stream_group_final_schema_version")?
            .map(SchemaVersionNo),
    })
}

pub(crate) fn ready_manifest_units_from_pg(
    rows: Vec<PgRow>,
) -> Result<Vec<ReadyManifestUnit>, ControlError> {
    let mut units = Vec::<ReadyManifestUnit>::new();
    for row in rows {
        if !row.try_get::<bool, _>("unit_valid")? {
            let group_id = row.try_get::<Option<i64>, _>("stream_group_id")?;
            return Err(ControlError::ManifestInvariant {
                message: group_id.map_or_else(
                    || "non-ready ungrouped manifest blocks ordered work".to_string(),
                    |id| format!("stream group {id} is non-ready, incomplete, or inconsistent"),
                ),
            });
        }
        if row.try_get::<bool, _>("is_schema_barrier")? {
            let group_id = row
                .try_get::<Option<i64>, _>("stream_group_id")?
                .map(ManifestGroupId)
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: "schema barrier has no stream group id".to_string(),
                })?;
            let expected_files: Option<i64> = row.try_get("stream_group_expected_files")?;
            let row_count: Option<i64> = row.try_get("stream_group_row_count")?;
            let final_schema_version = row
                .try_get::<Option<i64>, _>("stream_group_final_schema_version")?
                .map(SchemaVersionNo)
                .filter(|version| version.0 > 0)
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("schema barrier {} has no valid final version", group_id.0),
                })?;
            if row.try_get::<Option<i64>, _>("id")?.is_some()
                || expected_files != Some(0)
                || row_count != Some(0)
            {
                return Err(ControlError::ManifestInvariant {
                    message: format!("schema barrier {} has file payload", group_id.0),
                });
            }
            units.push(ReadyManifestUnit::SchemaBarrier(StreamSchemaBarrier {
                id: group_id,
                epoch: EpochNo(row.try_get("epoch")?),
                source_schema: row.try_get("source_schema")?,
                source_table: row.try_get("source_table")?,
                commit_lsn: row.try_get("lsn_end")?,
                final_schema_version,
            }));
            continue;
        }

        let manifest = manifest_row_from_pg(&row)?;
        if let Some(group_id) = manifest.stream_group_id
            && let Some(ReadyManifestUnit::Files(files)) = units.last_mut()
            && files
                .first()
                .is_some_and(|first| first.stream_group_id == Some(group_id))
        {
            files.push(manifest);
        } else {
            units.push(ReadyManifestUnit::Files(vec![manifest]));
        }
    }
    for unit in &units {
        if let ReadyManifestUnit::Files(files) = unit {
            validate_claimed_groups(files)?;
        }
    }
    Ok(units)
}

/// Atomically publish all DDL, registry rows, and per-table file/barrier groups belonging to one
/// protocol-v2 source commit. This covers both `StreamCommit` and ordinary structural DDL-only
/// commits. The unique transaction receipt is retained for epoch-long replay idempotency, and a
/// replay must match its durable semantic shape before it is accepted.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for an invalid payload or corrupt durable group,
/// [`ControlError::StreamPublicationConflict`] when an existing receipt has different semantics,
/// [`ControlError::ImmutableHistoryConflict`] when a new receipt collides with different durable
/// DDL/registry history, or another [`ControlError`] if PostgreSQL cannot validate or commit the
/// publication.
pub async fn publish_stream_commit(
    pool: &PgPool,
    publication: &NewStreamCommitPublication,
) -> Result<PublishStreamOutcome, ControlError> {
    let mut ddl_audit_ids = BTreeSet::new();
    let mut structural_ddl_versions = BTreeSet::new();
    for row in &publication.ddl_rows {
        if row.epoch != publication.epoch
            || row.c_lsn != publication.commit_lsn
            || row.schema_version.0 <= 0
            || !ddl_audit_ids.insert(row.source_audit_id)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream xid {} contains DDL audit {} outside epoch/commit boundary",
                    publication.top_xid, row.source_audit_id
                ),
            });
        }
        if !row.c_tag.eq_ignore_ascii_case("COMMENT") {
            structural_ddl_versions.insert((
                row.source_schema.as_str(),
                row.source_table.as_str(),
                row.schema_version,
            ));
        }
    }
    let mut registry_versions = BTreeSet::new();
    for row in &publication.registry_rows {
        let key = (
            row.source_schema.as_str(),
            row.source_table.as_str(),
            row.schema_version,
        );
        if row.epoch != publication.epoch
            || row.schema_version.0 <= 0
            || !registry_versions.insert(key)
            || !structural_ddl_versions.contains(&key)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream xid {} contains registry row {}.{} v{} outside epoch {}",
                    publication.top_xid,
                    row.source_schema,
                    row.source_table,
                    row.schema_version,
                    publication.epoch
                ),
            });
        }
    }
    if let Some(row) = publication.ddl_rows.iter().find(|row| {
        !row.c_tag.eq_ignore_ascii_case("COMMENT")
            && !registry_versions.contains(&(
                row.source_schema.as_str(),
                row.source_table.as_str(),
                row.schema_version,
            ))
    }) {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "stream xid {} contains structural DDL audit {} without an exact registry row",
                publication.top_xid, row.source_audit_id
            ),
        });
    }
    let mut replay_uris = BTreeSet::new();
    for file in &publication.files {
        if file.epoch != publication.epoch
            || !matches!(file.kind, ManifestKind::Stream | ManifestKind::Spill)
            || file.reload_id.is_some()
            || file.lsn_end != publication.commit_lsn
            || file.schema_version.0 <= 0
            || file.row_count <= 0
            || file.object_size <= 0
            || file.sha256.len() != 32
            || !replay_uris.insert(file.s3_uri.clone())
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream xid {} contains invalid file {} for commit {}",
                    publication.top_xid, file.s3_uri, publication.commit_lsn
                ),
            });
        }
    }
    let replay_uris = replay_uris.into_iter().collect::<Vec<_>>();

    let final_schema_versions = stream_group_final_versions(publication);
    let commit_ts = publication.commit_ts.to_string();
    let mut tx = pool.begin().await?;
    let inserted: Option<i64> = sqlx::query_scalar(
        "INSERT INTO public.walrus_stream_txn_publication \
         (epoch, top_xid, commit_lsn, commit_ts) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (epoch, commit_lsn) DO NOTHING RETURNING id",
    )
    .bind(publication.epoch.0)
    .bind(i64::from(publication.top_xid))
    .bind(publication.commit_lsn)
    .bind(&commit_ts)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(publication_id) = inserted else {
        let existing: Option<(i64, i64, String)> = sqlx::query_as(
            "SELECT id, top_xid, commit_ts FROM public.walrus_stream_txn_publication \
             WHERE epoch=$1 AND commit_lsn=$2 FOR SHARE",
        )
        .bind(publication.epoch.0)
        .bind(publication.commit_lsn)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((publication_id, existing_top_xid, existing_commit_ts)) = existing else {
            return Err(ControlError::StreamPublicationConflict {
                epoch: publication.epoch,
                top_xid: publication.top_xid,
                commit_lsn: publication.commit_lsn,
            });
        };
        if existing_top_xid != i64::from(publication.top_xid) || existing_commit_ts != commit_ts {
            return Err(ControlError::StreamPublicationConflict {
                epoch: publication.epoch,
                top_xid: publication.top_xid,
                commit_lsn: publication.commit_lsn,
            });
        }
        validate_existing_stream_publication(&mut tx, publication_id, publication).await?;
        let referenced_replay_uri: Option<String> = sqlx::query_scalar(
            "SELECT s3_uri FROM public.walrus_file_manifest \
             WHERE s3_uri = ANY($1::text[]) ORDER BY s3_uri LIMIT 1",
        )
        .bind(&replay_uris)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(s3_uri) = referenced_replay_uri {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "already-published stream replay URI {s3_uri} is still referenced by live manifest work"
                ),
            });
        }
        tx.rollback().await?;
        return Ok(PublishStreamOutcome::AlreadyPublished);
    };

    // Only a fresh receipt reaches this point. Serialize its complete table set with reload
    // cutover before making any history or queue child visible. BTreeMap keys provide the global
    // `(schema, table)` lock order for multi-table transactions.
    lock_fresh_publication_fences(&mut tx, publication, final_schema_versions.keys().copied())
        .await?;

    // The receipt, schema history, provisional relation shapes, and every data object become
    // visible in one control transaction. In particular, no transformer can observe a streamed DDL
    // boundary whose sibling files have not committed yet (or vice versa).
    for row in &publication.ddl_rows {
        crate::ddl_manifest::insert_ddl(&mut *tx, row).await?;
    }
    for row in &publication.registry_rows {
        crate::schema_registry::upsert_registry(&mut *tx, row).await?;
    }

    let mut by_table: BTreeMap<(&str, &str), Vec<&NewManifestFile>> = BTreeMap::new();
    for file in &publication.files {
        by_table
            .entry((&file.source_schema, &file.source_table))
            .or_default()
            .push(file);
    }
    for schema_table in final_schema_versions.keys() {
        by_table.entry(*schema_table).or_default();
    }

    for ((schema, table), files) in by_table {
        let final_schema_version = final_schema_versions
            .get(&(schema, table))
            .copied()
            .ok_or_else(|| ControlError::ManifestInvariant {
                message: format!("stream group {schema}.{table} has no final schema version"),
            })?;
        let expected_files =
            i64::try_from(files.len()).map_err(|_| ControlError::ManifestInvariant {
                message: format!("too many files in stream group {schema}.{table}"),
            })?;
        let row_count = files
            .iter()
            .try_fold(0_i64, |sum, file| sum.checked_add(file.row_count))
            .ok_or_else(|| ControlError::ManifestInvariant {
                message: format!("row count overflow in stream group {schema}.{table}"),
            })?;
        let mut replay_shape = files
            .iter()
            .map(|file| StreamFileReplayShape::from(*file))
            .collect::<Vec<_>>();
        replay_shape.sort();
        let replay_shape = stream_file_shape_json(&replay_shape);
        let group_id: i64 = sqlx::query_scalar(
            "INSERT INTO public.walrus_stream_manifest_group \
             (publication_id, epoch, top_xid, source_schema, source_table, commit_lsn, \
              commit_ts, expected_files, row_count, final_schema_version, file_shape, status) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'ready') RETURNING id",
        )
        .bind(publication_id)
        .bind(publication.epoch.0)
        .bind(i64::from(publication.top_xid))
        .bind(schema)
        .bind(table)
        .bind(publication.commit_lsn)
        .bind(&commit_ts)
        .bind(expected_files)
        .bind(row_count)
        .bind(final_schema_version.0)
        .bind(replay_shape)
        .fetch_one(&mut *tx)
        .await?;

        for (ordinal, file) in files.into_iter().enumerate() {
            let ordinal = i64::try_from(ordinal).map_err(|_| ControlError::ManifestInvariant {
                message: format!("file ordinal overflow in stream group {schema}.{table}"),
            })?;
            sqlx::query(
                "INSERT INTO public.walrus_file_manifest \
                 (epoch, source_schema, source_table, s3_uri, kind, row_count, object_size, \
                  sha256, lsn_start, lsn_end, schema_version, status, reload_id, \
                  stream_group_id, stream_group_ordinal) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'ready',NULL,$12,$13)",
            )
            .bind(file.epoch.0)
            .bind(&file.source_schema)
            .bind(&file.source_table)
            .bind(&file.s3_uri)
            .bind(file.kind.as_str())
            .bind(file.row_count)
            .bind(file.object_size)
            .bind(&file.sha256)
            .bind(file.lsn_start)
            .bind(file.lsn_end)
            .bind(file.schema_version.0)
            .bind(group_id)
            .bind(ordinal)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(PublishStreamOutcome::Published)
}

/// Return every durable object URI still referenced by this epoch's manifest, regardless of queue
/// status. Startup orphan collection uses the complete set so a failed or concurrently claimed row
/// is never mistaken for garbage.
///
/// # Errors
///
/// Returns [`ControlError`] if the manifest inventory cannot be read.
pub async fn list_manifest_uris(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<String>, ControlError> {
    Ok(sqlx::query_scalar(
        "SELECT s3_uri FROM public.walrus_file_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch.0)
    .fetch_all(executor)
    .await?)
}

fn validate_ordinary_manifest(f: &NewManifestFile) -> Result<(), ControlError> {
    let kind_is_ordinary = matches!(f.kind, ManifestKind::Snapshot | ManifestKind::Stream);
    if !kind_is_ordinary
        || f.reload_id.is_some()
        || f.row_count <= 0
        || f.object_size <= 0
        || f.sha256.len() != 32
        || f.lsn_start > f.lsn_end
        || f.schema_version.0 <= 0
    {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "ordinary manifest {} for {}.{} has invalid kind/reload identity, size, fingerprint, LSN range, or schema version",
                f.s3_uri, f.source_schema, f.source_table
            ),
        });
    }
    Ok(())
}

/// Atomically publish one ordinary ungrouped file, or recognize that a durable reload seal already
/// covers its source commit. This is the extractor-facing crash-replay path; reload chunks and callers
/// that require a strict INSERT continue to use [`insert_ready`].
///
/// The transaction-scoped table lock and fence row serialize the decision with both reload cutover
/// and protocol-v2 grouped publication. Static manifest semantics are validated before consulting
/// the seal so malformed replay payloads can never become ACK-eligible merely by being old.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for a malformed/non-ordinary file, or the underlying
/// database error when the table lock, fence operation, manifest insert, or commit fails.
pub async fn publish_ready_manifest<'a>(
    acquire: impl sqlx::Acquire<'a, Database = sqlx::Postgres>,
    f: &NewManifestFile,
) -> Result<PublishManifestOutcome, ControlError> {
    validate_ordinary_manifest(f)?;

    let mut tx = acquire.begin().await?;
    lock_manifest_publication_table(&mut tx, f.epoch, &f.source_schema, &f.source_table).await?;
    let sealed_through: Option<Lsn> = sqlx::query_scalar(
        "INSERT INTO public.walrus_manifest_publication_fence AS fence
           (epoch, source_schema, source_table)
         VALUES ($1,$2,$3)
         ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
           SET updated_at = fence.updated_at
         RETURNING sealed_through_lsn",
    )
    .bind(f.epoch.0)
    .bind(&f.source_schema)
    .bind(&f.source_table)
    .fetch_one(&mut *tx)
    .await?;

    let outcome = match sealed_through {
        Some(sealed) if f.lsn_end <= sealed => {
            let uri_referenced: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM public.walrus_file_manifest WHERE s3_uri=$1)",
            )
            .bind(&f.s3_uri)
            .fetch_one(&mut *tx)
            .await?;
            if uri_referenced {
                return Err(ControlError::ManifestInvariant {
                    message: format!(
                        "seal-covered ordinary manifest URI {} is still referenced by live queue work",
                        f.s3_uri
                    ),
                });
            }
            PublishManifestOutcome::CoveredBySeal(sealed)
        }
        Some(sealed) if f.lsn_start <= sealed => {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "ordinary manifest {} range [{}, {}] straddles durable reload seal {}",
                    f.s3_uri, f.lsn_start, f.lsn_end, sealed
                ),
            });
        }
        _ => PublishManifestOutcome::Published(insert_ready(&mut *tx, f).await?),
    };
    tx.commit().await?;
    Ok(outcome)
}

/// Insert a `status='ready'` row with `lsn_end` set to the commit LSN; returns the new `id`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] when the insert fails, or [`ControlError::CheckViolation`] if
/// the manifest values violate a database invariant.
pub async fn insert_ready(
    executor: impl PgExecutor<'_>,
    f: &NewManifestFile,
) -> Result<ManifestId, ControlError> {
    let reload_id = f.reload_id.map(|id| id.0);
    let rec = sqlx::query(include_str!("../sql/postgres/queries/insert_ready.sql"))
        .bind(f.epoch.0)
        .bind(&f.source_schema)
        .bind(&f.source_table)
        .bind(&f.s3_uri)
        .bind(f.kind.as_str())
        .bind(f.row_count)
        .bind(f.object_size)
        .bind(&f.sha256)
        .bind(f.lsn_start)
        .bind(f.lsn_end)
        .bind(f.schema_version.0)
        .bind(reload_id)
        .fetch_one(executor)
        .await?;
    Ok(ManifestId(rec.try_get("id")?))
}

/// Claim the next `ready` files for a table **in commit order**.
///
/// `ORDER BY lsn_end, id` — `id` breaks equal-`lsn_end` ties. There is deliberately **no**
/// `lsn_end > raw_appended_lsn` predicate: that would skip equal-`lsn_end` legacy snapshot files.
/// This file-only compatibility view stops before the first zero-child schema barrier; new transformer
/// code must use [`claim_ready_units`] to reconcile and retire that typed work item.
///
/// **The pause predicate (reload §2/H8):** while either persisted reload flavor is
/// `requested|exporting|export_complete|publishing`, this generic live-table claim remains closed.
/// Rows accumulate `ready` and the ordinary frontier freezes at `W`; only the publication-specific
/// claim path may drain the frozen `[F,H]` set while it owns the publishing lease. Keeping the pause
/// in the query makes the gate one statement with no check-then-claim TOCTOU, and prevents a generic
/// worker from retiring rows that the atomic reload publication still owns. The legacy `resync`
/// spelling has exactly the same pause and cutover behavior.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the atomic claim query fails, or [`ControlError::Decode`] if
/// a stored manifest kind or status is outside its checked enum set.
pub async fn claim_ready(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    limit: i64,
) -> Result<Vec<ManifestRow>, ControlError> {
    let units = claim_ready_units(executor, epoch, source_schema, source_table, limit).await?;
    let mut files = Vec::new();
    for unit in units {
        match unit {
            ReadyManifestUnit::Files(mut unit_files) => files.append(&mut unit_files),
            // This compatibility API cannot acknowledge a typed barrier. Returning only the file
            // prefix prevents an older caller from processing work after an unapplied schema cut.
            ReadyManifestUnit::SchemaBarrier(_) => break,
        }
    }
    Ok(files)
}

/// Claim the next table-local work units in commit order, including streamed structural commits
/// that emitted no data files. File groups remain indivisible and a schema barrier consumes one
/// scheduling slot. The generic reload/integrity pause gates are evaluated in the claim statement.
///
/// # Errors
///
/// Returns a typed database/decode error or [`ControlError::ManifestInvariant`] when a selected
/// file group or zero-child barrier is incomplete or inconsistent.
pub async fn claim_ready_units(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    limit: i64,
) -> Result<Vec<ReadyManifestUnit>, ControlError> {
    let rows = sqlx::query(include_str!(
        "../sql/postgres/queries/claim_ready_units.sql"
    ))
    .bind(epoch.0)
    .bind(source_schema)
    .bind(source_table)
    .bind(limit)
    .fetch_all(executor)
    .await?;
    ready_manifest_units_from_pg(rows)
}

/// Atomically retire a batch of zero-child schema barriers after their local schema reconciliation
/// is durable. Callers may pass an open control transaction so barrier retirement, file deletion,
/// and any actual data-file frontier advancement commit together. A barrier itself carries no raw
/// row and does not advance a data checkpoint. Every persisted identity and zero-child invariant
/// is revalidated under a parent-row lock before any status changes.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for duplicate, stale, non-ready, non-empty, or
/// otherwise changed barriers, or the underlying database error.
pub async fn complete_schema_barriers(
    executor: impl PgExecutor<'_>,
    barriers: &[StreamSchemaBarrier],
) -> Result<u64, ControlError> {
    let mut seen = BTreeSet::new();
    if let Some(barrier) = barriers
        .iter()
        .find(|barrier| barrier.final_schema_version.0 <= 0 || !seen.insert(barrier.id))
    {
        return Err(ControlError::ManifestInvariant {
            message: format!("invalid or duplicate schema barrier {}", barrier.id.0),
        });
    }

    let ids = barriers
        .iter()
        .map(|barrier| barrier.id.0)
        .collect::<Vec<_>>();
    let epochs = barriers
        .iter()
        .map(|barrier| barrier.epoch.0)
        .collect::<Vec<_>>();
    let schemas = barriers
        .iter()
        .map(|barrier| barrier.source_schema.clone())
        .collect::<Vec<_>>();
    let tables = barriers
        .iter()
        .map(|barrier| barrier.source_table.clone())
        .collect::<Vec<_>>();
    let commit_lsns = barriers
        .iter()
        .map(|barrier| {
            let raw = barrier.commit_lsn.as_u64();
            format!("{:X}/{:X}", raw >> 32, raw & u64::from(u32::MAX))
        })
        .collect::<Vec<_>>();
    let final_versions = barriers
        .iter()
        .map(|barrier| barrier.final_schema_version.0)
        .collect::<Vec<_>>();
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/complete_schema_barriers.sql"
    ))
    .bind(&ids)
    .bind(&epochs)
    .bind(&schemas)
    .bind(&tables)
    .bind(&commit_lsns)
    .bind(&final_versions)
    .fetch_one(executor)
    .await?;
    if row.try_get::<bool, _>("invalid")? {
        return Err(ControlError::ManifestInvariant {
            message: "schema barrier batch is stale, non-ready, non-empty, or changed".to_string(),
        });
    }
    let completed: i64 = row.try_get("completed_count")?;
    if usize::try_from(completed).ok() != Some(barriers.len()) {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "schema barrier completion changed {completed} rows, expected {}",
                barriers.len()
            ),
        });
    }
    u64::try_from(completed).map_err(|_| ControlError::ManifestInvariant {
        message: format!("schema barrier completion returned invalid count {completed}"),
    })
}

/// Lock the union of every stream-group parent touched by a file/barrier retirement in ascending
/// group-id order. Call this first in the open control transaction, before [`delete_claimed`] and
/// [`complete_schema_barriers`], so supersession, integrity fencing, and ordinary retirement share
/// one global parent-lock order.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for duplicate/missing input rows or parents, or the
/// underlying database error.
pub async fn lock_manifest_work_groups(
    executor: impl PgExecutor<'_>,
    file_ids: &[ManifestId],
    barriers: &[StreamSchemaBarrier],
) -> Result<(), ControlError> {
    let mut seen_files = BTreeSet::new();
    if let Some(id) = file_ids.iter().find(|id| !seen_files.insert(**id)) {
        return Err(ControlError::ManifestInvariant {
            message: format!("duplicate manifest {} in work-group lock", id.0),
        });
    }
    let mut seen_barriers = BTreeSet::new();
    if let Some(barrier) = barriers
        .iter()
        .find(|barrier| !seen_barriers.insert(barrier.id))
    {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "duplicate schema barrier {} in work-group lock",
                barrier.id.0
            ),
        });
    }
    let raw_files = file_ids.iter().map(|id| id.0).collect::<Vec<_>>();
    let raw_barriers = barriers
        .iter()
        .map(|barrier| barrier.id.0)
        .collect::<Vec<_>>();
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/lock_manifest_work_groups.sql"
    ))
    .bind(&raw_files)
    .bind(&raw_barriers)
    .fetch_one(executor)
    .await?;
    let requested_files: i64 = row.try_get("requested_file_count")?;
    let found_files: i64 = row.try_get("found_file_count")?;
    let target_groups: i64 = row.try_get("target_group_count")?;
    let locked_groups: i64 = row.try_get("locked_group_count")?;
    if requested_files != found_files || target_groups != locked_groups {
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "work-group lock found {found_files}/{requested_files} files and {locked_groups}/{target_groups} parents"
            ),
        });
    }
    Ok(())
}

/// The newest `ready` file or zero-child schema barrier's commit LSN for a table — the head of the
/// Phase-A backlog — or `None` when the queue is empty. Powers the
/// `walrus_transformer_raw_append_lag_bytes` gauge: the lag is this minus `raw_appended_lsn`. `MAX` over
/// an empty set is SQL `NULL` → `None`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the backlog query cannot reach or read control Postgres.
pub async fn max_ready_lsn_end(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<Lsn>, ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/max_ready_lsn_end.sql",
        epoch.0,
        source_schema,
        source_table,
    )
    .fetch_one(executor)
    .await?;
    Ok(row.max_lsn_end)
}

/// Return whether any table-local manifest work or corrupt active receipt still exists, regardless
/// of queue status. Unlike [`max_ready_lsn_end`], this is an emptiness proof: failed singletons,
/// incomplete ready groups, and terminal groups that incorrectly retain children all count as
/// work. Transformer replay-fence migration uses it so corruption can never masquerade as a drained
/// queue.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the authoritative manifest inventory cannot be read.
pub async fn manifest_work_exists(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<bool, ControlError> {
    Ok(sqlx::query_scalar(include_str!(
        "../sql/postgres/queries/manifest_work_exists.sql"
    ))
    .bind(epoch.0)
    .bind(source_schema)
    .bind(source_table)
    .fetch_one(executor)
    .await?)
}

/// Retire claimed rows — the queue's "done" is a `DELETE`, not a status flip. Returns the count.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the claimed-row delete cannot be executed.
pub async fn delete_claimed(
    executor: impl PgExecutor<'_>,
    ids: &[ManifestId],
) -> Result<u64, ControlError> {
    // The transparent `Type` impl carries no `PgHasArrayType`, so unwrap to `&[i64]` for the array bind.
    let raw: Vec<i64> = ids.iter().map(|id| id.0).collect();
    let row = sqlx::query(include_str!("../sql/postgres/queries/delete_claimed.sql"))
        .bind(raw.as_slice())
        .fetch_one(executor)
        .await?;
    let deleted: i64 = row.try_get("deleted_count")?;
    u64::try_from(deleted).map_err(|_| ControlError::ManifestInvariant {
        message: format!("delete_claimed returned invalid row count {deleted}"),
    })
}

/// Purge a rebuilding table's SUPERSEDED pending rows at trigger time: every non-reload row with
/// `lsn_end <= F` describes a commit covered by the post-F consistent baseline, so applying it
/// after the rebuild would only re-apply history the replacement already contains. Baseline chunk
/// files themselves carry `lsn_end = F`; the `kind` filter is what lets them survive their own
/// purge. No status filter: a dead-lettered (`failed`) pre-F file is
/// equally superseded. The exact publication and live table-ownership token are locked in the same
/// transaction as the purge. Complete protocol-v2 groups are retired as `superseded`; an
/// incomplete or semantically changed group aborts the entire operation. Idempotent (a re-run
/// deletes nothing). Returns rows purged.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for a stale publisher,
/// [`ControlError::ManifestInvariant`] for a torn group, or the underlying database error.
pub async fn delete_publication_superseded(
    pool: &PgPool,
    publication: &crate::reload::ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<u64, ControlError> {
    let mut tx = pool.begin().await?;
    crate::reload::lock_publication(&mut *tx, publication, owner_pod, fencing_token).await?;
    // Lock every candidate protocol-v2 parent before any child. This is deliberately a separate
    // statement: under READ COMMITTED, if we wait behind ordinary apply/integrity work, the purge
    // statement below receives a fresh snapshot and sees the parent's committed terminal state
    // instead of validating stale pre-wait children against a post-wait parent row.
    let group_rows = sqlx::query(include_str!(
        "../sql/postgres/queries/lock_superseded_groups.sql"
    ))
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .bind(publication.start_lsn)
    .fetch_all(&mut *tx)
    .await?;
    let group_ids = group_rows
        .iter()
        .map(|row| row.try_get::<i64, _>("id"))
        .collect::<Result<Vec<_>, _>>()?;
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/delete_superseded.sql"
    ))
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .bind(publication.start_lsn)
    .bind(&group_ids)
    .fetch_one(&mut *tx)
    .await?;
    let invalid_groups: i64 = row.try_get("invalid_groups")?;
    if invalid_groups != 0 {
        tx.rollback().await?;
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "reload {} cannot supersede {invalid_groups} incomplete or changed stream groups",
                publication.reload_id
            ),
        });
    }
    let deleted: i64 = row.try_get("deleted_count")?;
    let deleted = u64::try_from(deleted).map_err(|_| ControlError::ManifestInvariant {
        message: format!("superseded purge returned invalid row count {deleted}"),
    })?;
    tx.commit().await?;
    Ok(deleted)
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
