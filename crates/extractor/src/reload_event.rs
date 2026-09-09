//! Commit-gated source requests and table reconciliation fences.
//!
//! `public.walrus_reload_event` is append-only and published. Request rows are the causal trigger; the
//! coordinator later writes a start fence while holding the target's short `SHARE` lock and an end
//! fence after the dump has drained. Only transaction commit LSNs are authoritative.

use anyhow::Context as _;
use common::sql::SqlIdent;
use common::{Lsn, ReloadId, TupleValue};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;
use uuid::Uuid;

/// Result of attempting to append a deterministic fence row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceEmission {
    /// This call inserted the row and observed its decoded commit.
    Observed(FenceEcho),
    /// The deterministic row already exists. Its decoder/control record is the source of truth.
    AlreadyExists,
    /// The live table no longer matches the attempt's frozen registry shape.
    SchemaChanged,
}

/// One member of an all-published request, frozen into the source WAL event itself.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReloadTarget {
    /// Source schema.
    pub schema: String,
    /// Source table.
    pub table: String,
}

/// Append an idempotent request for one source table to the published control stream.
///
/// The caller supplies the UUID so it can safely retry after an ambiguous connection failure.
/// Reusing that UUID with different request data is rejected.
///
/// # Errors
///
/// Returns an error when the source write fails or the UUID already names a different request.
pub async fn request_table(
    client: &tokio_postgres::Client,
    request_id: Uuid,
    source_schema: &str,
    source_table: &str,
) -> anyhow::Result<()> {
    SqlIdent::new(source_schema).context("validate reload request schema")?;
    SqlIdent::new(source_table).context("validate reload request table")?;
    append_request(
        client,
        request_id,
        ReloadScope::Table,
        Some(source_schema),
        Some(source_table),
        &[],
    )
    .await
}

/// Append an idempotent request for an already-frozen publication inventory.
///
/// Keeping the target list in the WAL row is what makes replay deterministic: publication or
/// registry changes after this request cannot add or remove a child. The caller supplies the UUID
/// so an ambiguous retry cannot duplicate the group.
///
/// # Errors
///
/// Returns an error when a target is invalid/duplicated, the source write fails, or the UUID already
/// names different request data.
pub async fn request_all_targets(
    client: &tokio_postgres::Client,
    request_id: Uuid,
    targets: &[ReloadTarget],
) -> anyhow::Result<()> {
    validate_targets(targets)?;
    append_request(
        client,
        request_id,
        ReloadScope::AllPublished,
        None,
        None,
        targets,
    )
    .await
}

/// Freeze the source publication's current user-table inventory and append one all-table request in
/// the same source transaction.
///
/// # Errors
///
/// Returns an error when the catalog inventory cannot be read or the request cannot be committed.
pub async fn request_all_published(
    client: &mut tokio_postgres::Client,
    request_id: Uuid,
    publication: &str,
) -> anyhow::Result<Vec<ReloadTarget>> {
    let tx = client
        .transaction()
        .await
        .context("begin all-published reload request")?;
    let rows = tx
        .query(
            "SELECT schemaname, tablename
             FROM pg_publication_tables
             WHERE pubname = $1
               AND NOT (
                 schemaname = 'public'
                 AND tablename IN (
                   'walrus_heartbeat', 'walrus_ddl_audit',
                   'walrus_reload_signal', 'walrus_reload_event'
                 )
               )
             ORDER BY schemaname, tablename",
            &[&publication],
        )
        .await
        .context("freeze publication inventory for reload")?;
    let targets = rows
        .into_iter()
        .map(|row| ReloadTarget {
            schema: row.get(0),
            table: row.get(1),
        })
        .collect::<Vec<_>>();
    validate_targets(&targets)?;
    append_request(
        &tx,
        request_id,
        ReloadScope::AllPublished,
        None,
        None,
        &targets,
    )
    .await?;
    tx.commit()
        .await
        .context("commit all-published reload request")?;
    Ok(targets)
}

async fn append_request<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    request_id: Uuid,
    scope: ReloadScope,
    source_schema: Option<&str>,
    source_table: Option<&str>,
    targets: &[ReloadTarget],
) -> anyhow::Result<()> {
    let request_id_text = request_id.to_string();
    let targets = serde_json::to_string(targets).context("encode reload target inventory")?;
    let affected = client
        .execute(
            "INSERT INTO public.walrus_reload_event
             (event_id, request_id, event_kind, scope, source_schema, source_table, targets)
             VALUES ($1::text::uuid, $1::text::uuid, 'request', $2, $3, $4, $5::text::jsonb)
             ON CONFLICT (event_id) DO NOTHING",
            &[
                &request_id_text as &(dyn tokio_postgres::types::ToSql + Sync),
                &scope.as_str(),
                &source_schema,
                &source_table,
                &targets,
            ],
        )
        .await
        .context("append source reload request")?;
    if affected == 0
        && !request_row_matches(
            client,
            &request_id_text,
            scope,
            source_schema,
            source_table,
            &targets,
        )
        .await?
    {
        anyhow::bail!("reload request UUID {request_id} already names different request data");
    }
    Ok(())
}

async fn request_row_matches<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    request_id: &str,
    scope: ReloadScope,
    source_schema: Option<&str>,
    source_table: Option<&str>,
    targets_json: &str,
) -> anyhow::Result<bool> {
    let row = client
        .query_opt(
            "SELECT request_id::text = $1
                    AND event_kind = 'request'
                    AND scope = $2
                    AND source_schema IS NOT DISTINCT FROM $3
                    AND source_table IS NOT DISTINCT FROM $4
                    AND targets = $5::text::jsonb
             FROM public.walrus_reload_event
             WHERE event_id = $1::text::uuid",
            &[
                &request_id as &(dyn tokio_postgres::types::ToSql + Sync),
                &scope.as_str(),
                &source_schema,
                &source_table,
                &targets_json,
            ],
        )
        .await
        .context("inspect existing source reload request")?;
    Ok(row.is_some_and(|row| row.get::<_, bool>(0)))
}

fn validate_targets(targets: &[ReloadTarget]) -> anyhow::Result<()> {
    let mut unique = std::collections::BTreeSet::new();
    for target in targets {
        SqlIdent::new(&target.schema).context("validate reload target schema")?;
        SqlIdent::new(&target.table).context("validate reload target table")?;
        if !unique.insert((&target.schema, &target.table)) {
            anyhow::bail!(
                "duplicate reload target {}.{} in frozen inventory",
                target.schema,
                target.table
            );
        }
    }
    Ok(())
}

/// Complete immutable input for one source fence.
///
/// Keeping the source identity, frozen catalog shape, and timing policy together prevents callers
/// from accidentally shifting one positional fence argument into another.
#[derive(Debug, Clone, Copy)]
pub struct FenceSpec<'a> {
    /// Publication whose coverage must still include the complete target row.
    pub publication: &'a str,
    /// Stable source request namespace.
    pub request_id: Uuid,
    /// Control-plane attempt identity.
    pub reload_id: ReloadId,
    /// Boundary being emitted.
    pub phase: FencePhase,
    /// Exact source target and frozen structural shape.
    pub target: FenceTarget<'a>,
    /// Bounds for acquiring and observing the boundary.
    pub timeouts: FenceTimeouts,
}

/// Exact source relation guarded by a fence.
#[derive(Debug, Clone, Copy)]
pub struct FenceTarget<'a> {
    /// Source schema.
    pub schema: &'a str,
    /// Source table.
    pub table: &'a str,
    /// Full relation shape expected while the fence lock is held.
    pub expected_relation: &'a common::PgRelation,
    /// Frozen registry version represented by that shape.
    pub schema_version: common::SchemaVersionNo,
}

/// Independent time bounds for one source fence.
#[derive(Debug, Clone, Copy)]
pub struct FenceTimeouts {
    /// Maximum wait for the target-table schema fence.
    pub lock: std::time::Duration,
    /// Maximum wait for the committed fence to return through logical decoding.
    pub echo: std::time::Duration,
}

/// Emit one deterministic source fence and wait for its decoded commit.
///
/// Both fences acquire `SHARE UPDATE EXCLUSIVE` on the target before inserting the event. The table
/// lock orders the boundary against structural/membership DDL while remaining compatible with
/// ordinary DML's `ROW EXCLUSIVE` lock. The pipeline sessions' advisory guards order all publication
/// DDL across the complete F..H interval. Under those locks the exact target is revalidated for
/// membership, all four actions, and absence of row filters/column lists. A retry uses the same UUID
/// and returns [`FenceEmission::AlreadyExists`] rather than manufacturing a different boundary.
///
/// # Errors
///
/// Returns an error for invalid identifiers, source SQL failures, or a decode echo timeout.
pub async fn emit_fence(
    client: &mut tokio_postgres::Client,
    waiters: &FenceWaiters,
    spec: FenceSpec<'_>,
) -> anyhow::Result<FenceEmission> {
    let FenceSpec {
        publication,
        request_id,
        reload_id,
        phase,
        target:
            FenceTarget {
                schema: source_schema,
                table: source_table,
                expected_relation,
                schema_version,
            },
        timeouts:
            FenceTimeouts {
                lock: lock_timeout,
                echo: echo_timeout,
            },
    } = spec;
    let schema = SqlIdent::new(source_schema).context("validate reload fence schema")?;
    let table = SqlIdent::new(source_table).context("validate reload fence table")?;
    let event_id = deterministic_fence_id(request_id, reload_id, phase);
    let event_id_text = event_id.to_string();
    let request_id_text = request_id.to_string();
    let phase_text = phase.event_kind().as_str();
    let waiter = waiters.subscribe(reload_id, phase);
    if fence_row_matches(
        client,
        &event_id_text,
        request_id,
        reload_id,
        phase,
        source_schema,
        source_table,
        schema_version,
    )
    .await?
    {
        return Ok(FenceEmission::AlreadyExists);
    }
    let echo_started_at = std::time::Instant::now();
    let tx = client.transaction().await.context("begin reload fence")?;
    let timeout_ms = lock_timeout.as_millis().min(i64::MAX as u128);
    let timeout = format!("{timeout_ms}ms");
    tx.query_one("SELECT set_config('lock_timeout', $1, true)", &[&timeout])
        .await
        .context("set reload fence lock_timeout")?;
    tx.batch_execute(&format!(
        "LOCK TABLE {schema}.{table} IN SHARE UPDATE EXCLUSIVE MODE"
    ))
    .await
    .with_context(|| format!("acquire reload schema fence for {schema}.{table}"))?;

    // ALTER PUBLICATION DROP/SET TABLE needs a conflicting target-table lock. The pipeline-wide
    // advisory guard serializes every publication DDL command, including global action changes,
    // without requiring the replication role to row-lock a pg_catalog relation.
    let actions = crate::source_catalog::publication_actions(&tx, publication)
        .await
        .context("read reload publication action flags")?;
    let options = crate::source_catalog::publication_target_options(
        &tx,
        publication,
        source_schema,
        source_table,
    )
    .await
    .context("inspect reload target publication coverage under fence")?;
    let coverage = crate::source_catalog::require_publication_actions(publication, actions)
        .and_then(|()| {
            crate::source_catalog::require_full_target(
                publication,
                source_schema,
                source_table,
                options,
            )
        });
    if let Err(issue) = coverage {
        tx.rollback()
            .await
            .context("roll back publication-invalid reload fence")?;
        return Err(anyhow::Error::new(issue).context("reload fence lost full-WAL coverage"));
    }

    // The registry can lag a just-committed DDL event by a few decode frames. Comparing the live
    // catalog while the same lock that orders this fence is held closes the final-check/DDL race:
    // either the old shape is still current and the fence commits before later DDL, or this attempt
    // is rejected without writing a misleading boundary.
    let live_relation =
        crate::source_catalog::describe_source_relation(&tx, source_schema, source_table)
            .await
            .context("read live relation under reload fence")?;
    if &live_relation != expected_relation {
        if fence_row_matches(
            &tx,
            &event_id_text,
            request_id,
            reload_id,
            phase,
            source_schema,
            source_table,
            schema_version,
        )
        .await?
        {
            tx.rollback()
                .await
                .context("release duplicate reload fence")?;
            return Ok(FenceEmission::AlreadyExists);
        }
        tx.rollback()
            .await
            .context("roll back schema-stale reload fence")?;
        return Ok(FenceEmission::SchemaChanged);
    }
    let inserted = tx
        .execute(
            "INSERT INTO public.walrus_reload_event
             (event_id, request_id, reload_id, event_kind, scope, source_schema, source_table,
              schema_version)
             VALUES ($1::text::uuid, $2::text::uuid, $3, $4, 'table', $5, $6, $7)
             ON CONFLICT DO NOTHING",
            &[
                &event_id_text as &(dyn tokio_postgres::types::ToSql + Sync),
                &request_id_text,
                &reload_id.0,
                &phase_text,
                &source_schema,
                &source_table,
                &schema_version.0,
            ],
        )
        .await
        .context("insert reload fence event")?;
    tx.commit().await.context("commit reload fence event")?;
    if inserted == 0 {
        if fence_row_matches(
            client,
            &event_id_text,
            request_id,
            reload_id,
            phase,
            source_schema,
            source_table,
            schema_version,
        )
        .await?
        {
            return Ok(FenceEmission::AlreadyExists);
        }
        anyhow::bail!("reload fence event UUID {event_id} already names different event data");
    }
    let echo = tokio::time::timeout(echo_timeout, waiter)
        .await
        .context("reload fence decode echo timed out")?
        .context("reload fence waiter closed")?;
    common::metrics::record_reload_echo_wait(echo_started_at.elapsed().as_secs_f64());
    Ok(FenceEmission::Observed(echo))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the immutable fence identity is intentionally checked field-for-field"
)]
async fn fence_row_matches<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    event_id: &str,
    request_id: Uuid,
    reload_id: ReloadId,
    phase: FencePhase,
    source_schema: &str,
    source_table: &str,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<bool> {
    let row = client
        .query_opt(
            "SELECT request_id::text, reload_id, event_kind, scope, source_schema,
                    source_table, schema_version
             FROM public.walrus_reload_event WHERE event_id = $1::text::uuid",
            &[&event_id],
        )
        .await
        .context("inspect existing reload fence event")?;
    let Some(row) = row else {
        return Ok(false);
    };
    let stored_request = row
        .get::<_, String>(0)
        .parse::<Uuid>()
        .context("decode existing reload fence request UUID")?;
    let matches = stored_request == request_id
        && row.get::<_, Option<i64>>(1) == Some(reload_id.0)
        && row.get::<_, String>(2) == phase.event_kind().as_str()
        && row.get::<_, String>(3) == ReloadScope::Table.as_str()
        && row.get::<_, Option<String>>(4).as_deref() == Some(source_schema)
        && row.get::<_, Option<String>>(5).as_deref() == Some(source_table)
        && row.get::<_, Option<i64>>(6) == Some(schema_version.0);
    if !matches {
        anyhow::bail!("reload fence event UUID {event_id} already names different event data");
    }
    Ok(true)
}

fn deterministic_fence_id(request_id: Uuid, reload_id: ReloadId, phase: FencePhase) -> Uuid {
    let phase = match phase {
        FencePhase::Start => b"start".as_slice(),
        FencePhase::End => b"end".as_slice(),
    };
    // A control database can be rebuilt while this append-only source table survives, resetting
    // its bigint sequence. Namespace the local attempt number by the globally stable request UUID
    // so a future generation cannot collide with an old fence row.
    let mut name = request_id.as_bytes().to_vec();
    name.extend_from_slice(&reload_id.0.to_be_bytes());
    name.extend_from_slice(phase);
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &name)
}

/// The three append-only event families carried by `public.walrus_reload_event`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReloadEventKind {
    /// A caller asks Walrus to reconcile one table or the full publication.
    Request,
    /// The coordinator's short source write fence established the lower LSN.
    StartFence,
    /// The dump drained; decode must durably flush the table before acknowledging this fence.
    EndFence,
}

impl ReloadEventKind {
    /// Stable source-table spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::StartFence => "start_fence",
            Self::EndFence => "end_fence",
        }
    }
}

impl FromStr for ReloadEventKind {
    type Err = EventTupleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "request" => Ok(Self::Request),
            "start_fence" => Ok(Self::StartFence),
            "end_fence" => Ok(Self::EndFence),
            _ => Err(EventTupleError("event_kind")),
        }
    }
}

/// Request fan-out scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReloadScope {
    /// Reconcile the named table only.
    Table,
    /// Reconcile every currently published user table as independent children.
    AllPublished,
}

impl ReloadScope {
    /// Stable source/control spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::AllPublished => "all_published",
        }
    }
}

impl FromStr for ReloadScope {
    type Err = EventTupleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "table" => Ok(Self::Table),
            "all_published" => Ok(Self::AllPublished),
            _ => Err(EventTupleError("scope")),
        }
    }
}

/// Which boundary an exporter is waiting to observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FencePhase {
    /// Lower write fence.
    Start,
    /// Upper durability fence.
    End,
}

impl FencePhase {
    /// Source event kind for this phase.
    #[must_use]
    pub const fn event_kind(self) -> ReloadEventKind {
        match self {
            Self::Start => ReloadEventKind::StartFence,
            Self::End => ReloadEventKind::EndFence,
        }
    }
}

/// A committed fence observed through logical decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceEcho {
    /// Authoritative transaction commit LSN.
    pub commit_lsn: Lsn,
    /// Source row's pre-commit WAL position, used only as a diagnostic cross-check.
    pub embedded_lsn: Lsn,
}

/// A decoded event held until its source transaction commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingReloadEvent {
    /// Stable identity of this append-only event row.
    pub event_id: Uuid,
    /// Stable caller request identity shared by a group's children.
    pub request_id: Uuid,
    /// Control attempt identity; absent only for request events.
    pub reload_id: Option<ReloadId>,
    /// Request/start/end discriminator.
    pub kind: ReloadEventKind,
    /// One target or all published targets.
    pub scope: ReloadScope,
    /// Target schema for table-scoped events.
    pub source_schema: Option<String>,
    /// Target table for table-scoped events.
    pub source_table: Option<String>,
    /// Frozen children for an all-published request; empty for table requests and fences.
    pub targets: Vec<ReloadTarget>,
    /// Frozen structural version carried by start/end fences.
    pub schema_version: Option<common::SchemaVersionNo>,
    /// Diagnostic source-side WAL position.
    pub embedded_lsn: Lsn,
    /// Per-message xid for streamed-transaction abort handling.
    pub xid: Option<u32>,
    /// Top-level streamed transaction that owns this event. Kept separately from `xid` because
    /// StreamCommit names the top xid while an insert inside a savepoint carries its sub-xid.
    pub top_xid: Option<u32>,
}

impl PendingReloadEvent {
    /// Parse an insert tuple by column name so additive source columns do not change decoding.
    ///
    /// # Errors
    ///
    /// Returns [`EventTupleError`] naming the first absent, malformed, or inconsistent column.
    pub fn from_tuple(
        rel: &common::PgRelation,
        new: &[TupleValue],
        xid: Option<u32>,
        top_xid: Option<u32>,
    ) -> Result<Self, EventTupleError> {
        let kind: ReloadEventKind = required_field(rel, new, "event_kind")?;
        let scope: ReloadScope = required_field(rel, new, "scope")?;
        let event = Self {
            event_id: required_field(rel, new, "event_id")?,
            request_id: required_field(rel, new, "request_id")?,
            reload_id: optional_field(rel, new, "reload_id")?,
            kind,
            scope,
            source_schema: optional_field(rel, new, "source_schema")?,
            source_table: optional_field(rel, new, "source_table")?,
            targets: required_json_field(rel, new, "targets")?,
            schema_version: optional_field::<i64>(rel, new, "schema_version")?.map(Into::into),
            embedded_lsn: required_field(rel, new, "wal_insert_lsn")?,
            xid,
            top_xid,
        };
        if validate_targets(&event.targets).is_err() {
            return Err(EventTupleError("targets"));
        }
        match (
            event.kind,
            event.reload_id,
            event.scope,
            event.source_schema.as_deref(),
            event.source_table.as_deref(),
            event.targets.is_empty(),
            event.schema_version,
        ) {
            (ReloadEventKind::Request, None, ReloadScope::AllPublished, None, None, _, None) => {
                Ok(event)
            }
            (ReloadEventKind::Request, None, ReloadScope::Table, Some(_), Some(_), true, None)
            | (
                ReloadEventKind::StartFence | ReloadEventKind::EndFence,
                Some(_),
                ReloadScope::Table,
                Some(_),
                Some(_),
                true,
                Some(_),
            ) => Ok(event),
            (ReloadEventKind::Request, Some(_), _, _, _, _, _) => Err(EventTupleError("reload_id")),
            (ReloadEventKind::StartFence | ReloadEventKind::EndFence, None, _, _, _, _, _) => {
                Err(EventTupleError("reload_id"))
            }
            (ReloadEventKind::StartFence | ReloadEventKind::EndFence, _, _, _, _, _, None) => {
                Err(EventTupleError("schema_version"))
            }
            (_, _, _, _, _, false, _) => Err(EventTupleError("targets")),
            _ => Err(EventTupleError("source_schema/source_table")),
        }
    }

    /// Borrow the table target when both identifiers are present.
    #[must_use]
    pub fn target(&self) -> Option<(&str, &str)> {
        Some((
            self.source_schema.as_deref()?,
            self.source_table.as_deref()?,
        ))
    }

    /// Fence phase for start/end events.
    #[must_use]
    pub const fn fence_phase(&self) -> Option<FencePhase> {
        match self.kind {
            ReloadEventKind::Request => None,
            ReloadEventKind::StartFence => Some(FencePhase::Start),
            ReloadEventKind::EndFence => Some(FencePhase::End),
        }
    }
}

/// A commit-gated event with its authoritative commit LSN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedReloadEvent {
    /// Decoded event row.
    pub event: PendingReloadEvent,
    /// Transaction commit LSN, never the row/frame LSN.
    pub commit_lsn: Lsn,
}

/// Why an internal event tuple cannot be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("reload_event tuple missing/invalid column: {0}")]
pub struct EventTupleError(pub &'static str);

fn tuple_value<'a>(
    rel: &common::PgRelation,
    new: &'a [TupleValue],
    name: &'static str,
) -> Result<&'a TupleValue, EventTupleError> {
    let idx = rel
        .columns
        .iter()
        .position(|column| column.name == name)
        .ok_or(EventTupleError(name))?;
    new.get(idx).ok_or(EventTupleError(name))
}

fn required_field<T: FromStr>(
    rel: &common::PgRelation,
    new: &[TupleValue],
    name: &'static str,
) -> Result<T, EventTupleError> {
    let TupleValue::Text(value) = tuple_value(rel, new, name)? else {
        return Err(EventTupleError(name));
    };
    value.parse().map_err(|_| EventTupleError(name))
}

fn optional_field<T: FromStr>(
    rel: &common::PgRelation,
    new: &[TupleValue],
    name: &'static str,
) -> Result<Option<T>, EventTupleError> {
    match tuple_value(rel, new, name)? {
        TupleValue::Null => Ok(None),
        TupleValue::Text(value) => value.parse().map(Some).map_err(|_| EventTupleError(name)),
        TupleValue::UnchangedToast | TupleValue::Binary(_) => Err(EventTupleError(name)),
    }
}

fn required_json_field<T: serde::de::DeserializeOwned>(
    rel: &common::PgRelation,
    new: &[TupleValue],
    name: &'static str,
) -> Result<T, EventTupleError> {
    let TupleValue::Text(value) = tuple_value(rel, new, name)? else {
        return Err(EventTupleError(name));
    };
    serde_json::from_str(value).map_err(|_| EventTupleError(name))
}

/// Insert-to-commit buffer, including streamed savepoint abort handling.
#[derive(Debug, Default)]
pub struct PendingReloadEvents {
    pending: Vec<PendingReloadEvent>,
}

impl PendingReloadEvents {
    /// Hold an insert until the transaction's fate is known.
    pub fn push(&mut self, event: PendingReloadEvent) {
        self.pending.push(event);
    }

    /// Number of control effects owned by the currently committing ordinary transaction. This is a
    /// read-only preflight so a mixed DDL/effect commit can fail before consuming replay state.
    #[must_use]
    pub fn ordinary_effect_count(&self) -> usize {
        self.pending
            .iter()
            .filter(|event| event.xid.is_none())
            .count()
    }

    /// Number of surviving control effects owned by one streamed top-level transaction. Savepoint
    /// abort handling removes doomed entries before this preflight is queried at StreamCommit.
    #[must_use]
    pub fn stream_effect_count(&self, top_xid: u32) -> usize {
        self.pending
            .iter()
            .filter(|event| event.top_xid == Some(top_xid))
            .count()
    }

    /// Promote ordinary-transaction events at commit.
    pub fn on_commit(&mut self, commit_lsn: Lsn) -> Vec<CommittedReloadEvent> {
        promote(&mut self.pending, commit_lsn, |event| event.xid.is_none())
    }

    /// Promote surviving streamed-transaction events at stream commit.
    pub fn on_stream_commit(&mut self, top_xid: u32, commit_lsn: Lsn) -> Vec<CommittedReloadEvent> {
        promote(&mut self.pending, commit_lsn, |event| {
            event.top_xid == Some(top_xid)
        })
    }

    /// Remove events belonging to a rolled-back streamed transaction or savepoint.
    pub fn on_stream_abort(&mut self, top_xid: u32, sub_xid: u32) {
        drop(extract(&mut self.pending, |event| {
            event.top_xid == Some(top_xid) && (top_xid == sub_xid || event.xid == Some(sub_xid))
        }));
    }

    /// Whether no event is waiting for its transaction outcome.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

fn promote(
    pending: &mut Vec<PendingReloadEvent>,
    commit_lsn: Lsn,
    predicate: impl FnMut(&PendingReloadEvent) -> bool,
) -> Vec<CommittedReloadEvent> {
    extract(pending, predicate)
        .into_iter()
        .map(|event| CommittedReloadEvent { event, commit_lsn })
        .collect()
}

fn extract<T>(values: &mut Vec<T>, mut predicate: impl FnMut(&T) -> bool) -> Vec<T> {
    values.extract_if(.., |value| predicate(value)).collect()
}

type WaiterKey = (ReloadId, FencePhase);
type WaiterEntry = (u64, oneshot::Sender<FenceEcho>);
type WaiterEntries = Vec<WaiterEntry>;

/// Subscribe-before-insert registry shared by exporters and the decoder.
///
/// Every key retains all active subscribers so concurrent observers of the same deterministic
/// fence receive the one committed echo together.
#[derive(Debug, Default)]
pub struct FenceWaiters {
    // LOCK-CHOICE: every access mutates one map entry, so an RwLock provides no read concurrency.
    waiters: Mutex<HashMap<WaiterKey, WaiterEntries>>,
    next_generation: AtomicU64,
    crosscheck_violations: AtomicU64,
}

impl FenceWaiters {
    /// Subscribe before inserting the matching source fence event.
    pub fn subscribe(&self, reload_id: ReloadId, phase: FencePhase) -> FenceSubscribeGuard<'_> {
        let (tx, rx) = oneshot::channel();
        let key = (reload_id, phase);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.waiters
            .lock()
            .entry(key)
            .or_default()
            .push((generation, tx));
        FenceSubscribeGuard {
            waiters: self,
            key,
            generation,
            rx,
        }
    }

    /// Resolve a committed fence. End fences must call this only after target durability.
    pub fn resolve(&self, reload_id: ReloadId, phase: FencePhase, echo: FenceEcho) {
        if echo.embedded_lsn >= echo.commit_lsn {
            self.crosscheck_violations.fetch_add(1, Ordering::Relaxed);
            common::metrics::record_reload_crosscheck_violation();
            tracing::error!(
                %reload_id,
                ?phase,
                embedded_lsn = %echo.embedded_lsn,
                commit_lsn = %echo.commit_lsn,
                "reload fence WAL cross-check violated"
            );
        }
        let waiters = self.waiters.lock().remove(&(reload_id, phase));
        if let Some(waiters) = waiters {
            for (_, tx) in waiters {
                if tx.send(echo).is_err() {
                    tracing::debug!(%reload_id, ?phase, "reload fence resolved after waiter dropped");
                }
            }
        }
    }

    fn unsubscribe(&self, key: WaiterKey, generation: u64) {
        let mut waiters = self.waiters.lock();
        if let Entry::Occupied(mut entry) = waiters.entry(key) {
            let subscribers = entry.get_mut();
            if let Some(index) = subscribers
                .iter()
                .position(|(candidate, _)| *candidate == generation)
            {
                subscribers.swap_remove(index);
            }
            if subscribers.is_empty() {
                entry.remove();
            }
        }
    }

    /// Current subscriber count, exposed for health/tests.
    #[must_use]
    pub fn waiter_count(&self) -> usize {
        self.waiters.lock().values().map(Vec::len).sum()
    }

    /// Number of diagnostic `embedded_lsn >= commit_lsn` violations observed locally.
    #[must_use]
    pub fn crosscheck_violations(&self) -> u64 {
        self.crosscheck_violations.load(Ordering::Relaxed)
    }
}

/// RAII fence subscription; dropping it unregisters the waiter.
#[must_use = "dropping the fence subscription before awaiting it loses the decoded boundary"]
#[derive(Debug)]
pub struct FenceSubscribeGuard<'a> {
    waiters: &'a FenceWaiters,
    key: WaiterKey,
    generation: u64,
    rx: oneshot::Receiver<FenceEcho>,
}

impl std::future::Future for FenceSubscribeGuard<'_> {
    type Output = Result<FenceEcho, oneshot::error::RecvError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::future::Future::poll(std::pin::Pin::new(&mut self.get_mut().rx), cx)
    }
}

impl Drop for FenceSubscribeGuard<'_> {
    fn drop(&mut self) {
        self.waiters.unsubscribe(self.key, self.generation);
    }
}

#[cfg(test)]
#[path = "reload_event_test.rs"]
mod tests;
