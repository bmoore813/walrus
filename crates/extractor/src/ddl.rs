//! Transactional DDL capture — the extractor side of the source event-trigger tap.
//!
//! The source trigger writes a structured row to the published `public.walrus_ddl_audit` table in the SAME
//! transaction as the DDL. Ordinary pgoutput transactions are known to have committed before they are
//! emitted, but streamed transactions are visible while still open and can later abort. Consequently a
//! decoded audit row is only a **provisional** schema boundary: the extractor may use its post-change shape to
//! decode later rows in that transaction, but it does not publish the DDL manifest/registry or make the
//! version globally committed until `Commit`/`StreamCommit`. `StreamAbort` drops the complete provisional
//! state, including a rolled-back savepoint's DDL.
//!
//! `c_columns` is the correctness input. `c_ddl_text` is retained as best-effort audit context only and is
//! never replayed or parsed to determine the change.

use crate::relcache::RelationCache;
use common::{
    DdlId, EpochNo, Lsn, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo, TupleValue,
    UtcTimestamp,
};
use std::collections::HashMap;

/// The source transaction that owns a decoded DDL audit row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionScope {
    /// A normal `Begin … Commit` transaction. pgoutput carries no xid prefix on its changes.
    Ordinary,
    /// One streamed top-level transaction and the subtransaction that emitted this particular row.
    Streamed {
        /// Top-level xid named by `StreamStart`/`StreamCommit`.
        top_xid: u32,
        /// Per-message xid, used to discard a rolled-back savepoint precisely.
        sub_xid: u32,
    },
}

/// Result of observing one audit event before its transaction commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DdlObservation {
    /// New provisional version for a structural DDL; `None` for metadata-only events.
    pub structural_version: Option<SchemaVersionNo>,
    /// The source audit identity was already committed in control Postgres (WAL replay).
    pub replay: bool,
}

/// A decoded `public.walrus_ddl_audit` insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlEvent {
    /// Stable source identity (`public.walrus_ddl_audit.id`), used to make WAL replay idempotent.
    pub source_audit_id: i64,
    /// `pg_current_wal_lsn()` captured by the source trigger. Audit context, not the commit LSN.
    pub capture_lsn: Lsn,
    /// `ddl_command_end` or `sql_drop`.
    pub c_event: String,
    /// `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `COMMENT`, and so on.
    pub c_tag: String,
    /// Schema of the affected table.
    pub source_schema: String,
    /// Name of the affected table.
    pub source_table: String,
    /// Source relation OID, including the last OID of a dropped table.
    pub c_rel_oid: Option<u32>,
    /// Post-change replica identity for a surviving table.
    pub c_replica_identity: Option<ReplicaIdentity>,
    /// Structured post-change column set; an empty array is the dropped-table sentinel.
    pub c_columns: Option<serde_json::Value>,
    /// Structured dropped-object identity, when supplied by `sql_drop`.
    pub c_dropped: Option<serde_json::Value>,
    /// Complete post-change table comment; `None` means the comment is absent.
    pub c_table_comment: Option<String>,
    /// Best-effort SQL text from `current_query()`, retained for audit/debugging only.
    pub c_ddl_text: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct AuditColumn {
    name: String,
    type_oid: u32,
    type_modifier: i32,
    #[serde(default)]
    is_key: Option<bool>,
}

impl DdlEvent {
    /// Extract an event from a decoded `ddl_audit` tuple by column name.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::MissingColumn`] for a missing/invalid required scalar,
    /// [`DdlError::ReplicaIdentity`] for an invalid catalog identity code, or [`DdlError::Json`] for
    /// malformed structured payloads.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub fn from_tuple(rel: &PgRelation, values: &[TupleValue]) -> Result<Self, DdlError> {
        let text = |name: &str| -> Option<String> {
            let idx = rel.columns.iter().position(|c| c.name == name)?;
            match values.get(idx)? {
                TupleValue::Text(s) => Some(s.clone()),
                TupleValue::Null | TupleValue::UnchangedToast | TupleValue::Binary(_) => None,
            }
        };
        let required = |name: &'static str| -> Result<String, DdlError> {
            text(name).ok_or(DdlError::MissingColumn(name))
        };
        let source_audit_id = required("id")?
            .parse()
            .map_err(|_| DdlError::MissingColumn("id"))?;
        let capture_lsn = required("c_lsn")?
            .parse()
            .map_err(|_| DdlError::MissingColumn("c_lsn"))?;
        let json = |name: &str| -> Result<Option<serde_json::Value>, DdlError> {
            text(name)
                .filter(|s| !s.is_empty())
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(Into::into)
        };
        let c_rel_oid = text("c_rel_oid")
            .map(|raw| raw.parse())
            .transpose()
            .map_err(|_| DdlError::MissingColumn("c_rel_oid"))?;
        let c_replica_identity = text("c_replica_identity")
            .map(|raw| {
                raw.parse()
                    .map_err(|_| DdlError::ReplicaIdentity(raw.clone()))
            })
            .transpose()?;
        Ok(DdlEvent {
            source_audit_id,
            capture_lsn,
            c_event: text("c_event").unwrap_or_default(),
            c_tag: text("c_tag").unwrap_or_default(),
            source_schema: text("c_schema").unwrap_or_default(),
            source_table: text("c_table").unwrap_or_default(),
            c_rel_oid,
            c_replica_identity,
            c_columns: json("c_columns")?,
            c_dropped: json("c_dropped")?,
            c_table_comment: text("c_table_comment"),
            c_ddl_text: text("c_ddl_text"),
        })
    }

    /// Structural (schema version + file boundary) versus metadata-only.
    #[must_use]
    pub fn is_structural(&self) -> bool {
        !self.c_tag.eq_ignore_ascii_case("COMMENT")
    }

    /// Whether this is the `sql_drop` sentinel for a table that no longer has a catalog shape.
    #[must_use]
    pub fn is_table_drop(&self) -> bool {
        self.c_event.eq_ignore_ascii_case("sql_drop")
    }

    /// Whether the command creates a new table identity rather than changing the frozen relation.
    #[must_use]
    fn is_table_creation(&self) -> bool {
        self.c_tag.eq_ignore_ascii_case("CREATE TABLE")
            || self.c_tag.eq_ignore_ascii_case("CREATE TABLE AS")
            || self.c_tag.eq_ignore_ascii_case("SELECT INTO")
    }

    /// Build the authoritative post-change relation described by this event.
    ///
    /// The source snapshot deliberately contains a little more than [`PgColumn`] (`attnum`, nullability);
    /// serde ignores those fields here. On an upgraded source whose older trigger omitted `is_key`, the
    /// previous relation supplies it by column name. A dropped-table sentinel has no post-change
    /// relation and returns `None`; its unsupported tracked-identity change is commit-gated instead.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::Json`] when `c_columns` cannot decode to the expected column snapshot.
    pub fn relation_after(
        &self,
        previous: Option<&PgRelation>,
    ) -> Result<Option<PgRelation>, DdlError> {
        if !self.is_structural() || self.is_table_drop() {
            return Ok(None);
        }
        let Some(columns) = &self.c_columns else {
            return Ok(None);
        };
        let audit: Vec<AuditColumn> = serde_json::from_value(columns.clone())?;
        let prior_key = |name: &str| {
            previous
                .and_then(|rel| rel.columns.iter().find(|col| col.name == name))
                .is_some_and(|col| col.is_key)
        };
        let columns = audit
            .into_iter()
            .map(|col| PgColumn {
                is_key: col.is_key.unwrap_or_else(|| prior_key(&col.name)),
                name: col.name,
                type_oid: col.type_oid,
                type_modifier: col.type_modifier,
            })
            .collect();
        let Some(oid) = self.c_rel_oid.or_else(|| previous.map(|rel| rel.oid)) else {
            return Ok(None);
        };
        Ok(Some(PgRelation {
            oid,
            schema: self.source_schema.clone(),
            name: self.source_table.clone(),
            replica_identity: self
                .c_replica_identity
                .or_else(|| previous.map(|rel| rel.replica_identity))
                .unwrap_or(ReplicaIdentity::Default),
            columns,
        }))
    }
}

#[derive(Debug, Clone)]
struct PendingDdl {
    scope: TransactionScope,
    event: DdlEvent,
    version: SchemaVersionNo,
    identity_change: Option<TrackedTableIdentityChange>,
    identity_unresolved: bool,
    replay: bool,
}

#[derive(Debug, Clone)]
struct PendingRegistry {
    scope: TransactionScope,
    row: control::RegistryRow,
}

/// Validated control-plane payload for one protocol-v2 `StreamCommit`.
///
/// Preparing this value is deliberately read-only. The pending DDL/registry state remains intact
/// until the caller has atomically published this payload with every streamed data object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedStreamDdl {
    top_xid: u32,
    ddl_rows: Vec<control::DdlRow>,
    registry_rows: Vec<control::RegistryRow>,
}

/// Validated control-plane payload for one ordinary (non-streamed) transaction commit.
///
/// Preparing is read-only for the same reason as [`PreparedStreamDdl`]: a failed control
/// publication must leave the DDL provisional so WAL replay can retry the exact receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedOrdinaryDdl {
    ddl_rows: Vec<control::DdlRow>,
    registry_rows: Vec<control::RegistryRow>,
}

impl PreparedOrdinaryDdl {
    #[must_use]
    pub(crate) fn ddl_rows(&self) -> &[control::DdlRow] {
        &self.ddl_rows
    }

    #[must_use]
    pub(crate) fn registry_rows(&self) -> &[control::RegistryRow] {
        &self.registry_rows
    }

    /// Structural DDL needs the durable ordered schema barrier. COMMENT-only transactions retain
    /// the existing audit-only path and deliberately do not create a data/schema-version barrier.
    #[must_use]
    pub(crate) fn has_structural_ddl(&self) -> bool {
        self.ddl_rows
            .iter()
            .any(|row| !row.c_tag.eq_ignore_ascii_case("COMMENT"))
    }
}

impl PreparedStreamDdl {
    #[must_use]
    pub(crate) fn ddl_rows(&self) -> &[control::DdlRow] {
        &self.ddl_rows
    }

    #[must_use]
    pub(crate) fn registry_rows(&self) -> &[control::RegistryRow] {
        &self.registry_rows
    }

    /// Structural DDL must not become visible separately from other control effects owned by the
    /// same streamed source transaction. COMMENT-only transactions carry no schema barrier.
    #[must_use]
    pub(crate) fn has_structural_ddl(&self) -> bool {
        self.ddl_rows
            .iter()
            .any(|row| !row.c_tag.eq_ignore_ascii_case("COMMENT"))
    }
}

#[derive(Debug, Clone, Copy)]
enum CommitSelector {
    Ordinary,
    Streamed(u32),
}

impl CommitSelector {
    const fn matches(self, scope: TransactionScope) -> bool {
        match (self, scope) {
            (Self::Ordinary, TransactionScope::Ordinary) => true,
            (Self::Streamed(expected), TransactionScope::Streamed { top_xid, .. }) => {
                expected == top_xid
            }
            (Self::Ordinary, TransactionScope::Streamed { .. })
            | (Self::Streamed(_), TransactionScope::Ordinary) => false,
        }
    }
}

/// Tracks committed and transaction-local schema versions and atomically publishes DDL history plus
/// provisional registry rows at the matching source commit.
#[derive(Debug)]
pub struct DdlConsumer {
    epoch: EpochNo,
    committed_versions: HashMap<(String, String), SchemaVersionNo>,
    pending: Vec<PendingDdl>,
    pending_registry: Vec<PendingRegistry>,
    processed: HashMap<i64, control::DdlRow>,
}

impl DdlConsumer {
    /// A consumer for one epoch. Tables default to schema version 1 until hydrated or changed.
    #[must_use]
    pub fn new(epoch: EpochNo) -> Self {
        Self {
            epoch,
            committed_versions: HashMap::new(),
            pending: Vec::new(),
            pending_registry: Vec::new(),
            processed: HashMap::new(),
        }
    }

    /// Hydrate committed versions from the relation cache restored at extractor startup.
    pub fn hydrate_versions(&mut self, cache: &RelationCache) {
        for cached in cache {
            self.set_committed(
                &cached.relation.schema,
                &cached.relation.name,
                cached.schema_version,
            );
        }
    }

    /// Hydrate processed source audit identities and their committed versions from DDL history.
    pub fn hydrate_history(&mut self, history: Vec<control::DdlRow>) {
        for row in history {
            self.set_committed(&row.source_schema, &row.source_table, row.schema_version);
            self.processed.insert(row.source_audit_id, row);
        }
    }

    /// Highest globally committed version for a table, defaulting to 1.
    #[must_use]
    pub fn committed_version_of(&self, schema: &str, table: &str) -> SchemaVersionNo {
        find_version(&self.committed_versions, schema, table).unwrap_or(SchemaVersionNo(1))
    }

    /// Highest projected version across all currently visible pending transactions.
    ///
    /// Primarily a diagnostics/test accessor. Decode routing should use [`Self::version_for`] so one
    /// open streamed transaction never leaks its provisional version into another.
    #[must_use]
    pub fn version_of(&self, schema: &str, table: &str) -> SchemaVersionNo {
        self.pending
            .iter()
            .filter(|pending| table_matches(&pending.event, schema, table))
            .map(|pending| pending.version)
            .max()
            .unwrap_or_else(|| self.committed_version_of(schema, table))
    }

    /// Version visible inside one source transaction: committed state plus that transaction's own DDL.
    #[must_use]
    pub fn version_for(
        &self,
        scope: TransactionScope,
        schema: &str,
        table: &str,
    ) -> SchemaVersionNo {
        self.pending_version_for(scope, schema, table)
            .unwrap_or_else(|| self.committed_version_of(schema, table))
    }

    /// Resolve a pgoutput `Relation` message to its exact historical schema version.
    ///
    /// A restart can replay WAL older than the newest durable registry row. Falling back to the
    /// committed maximum would then bind old tuples to a future shape. A transaction-local DDL
    /// version wins when present; otherwise the durable DDL history is cut at the last source commit
    /// successfully processed by this decode pass and that exact registry version must match the wire
    /// shape. The XLogData `wal_start` is deliberately not used: PostgreSQL may emit zero for a
    /// Relation transport frame, so it is not a valid history cursor. An unseen relation is admitted
    /// only when neither its OID nor qualified name has any durable history (fresh bootstrap).
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::RelationVersionBinding`] when a relation conflicts with its scoped DDL
    /// version or does not match the registry version valid at `committed_through_lsn`.
    pub fn relation_version_for(
        &self,
        scope: TransactionScope,
        relation: &PgRelation,
        committed_through_lsn: Lsn,
        cache: &RelationCache,
    ) -> Result<SchemaVersionNo, DdlError> {
        let exact_versions = cache
            .iter()
            .filter(|cached| cached.relation == *relation)
            .map(|cached| cached.schema_version)
            .collect::<Vec<_>>();
        let scoped_version = self.pending_version_for(scope, &relation.schema, &relation.name);
        let expected = scoped_version.unwrap_or_else(|| {
            self.historical_version_through(&relation.schema, &relation.name, committed_through_lsn)
        });
        let expected_cached = cache.get(relation.oid, expected);
        if expected_cached
            .as_ref()
            .is_some_and(|cached| cached.relation == *relation)
        {
            return Ok(expected);
        }
        if expected_cached.is_none()
            && cache.latest_for(relation.oid).is_none()
            && cache
                .latest_for_name(&relation.schema, &relation.name)
                .is_none()
            && self.processed.values().all(|row| {
                row.source_schema != relation.schema || row.source_table != relation.name
            })
        {
            return Ok(expected);
        }
        Err(DdlError::RelationVersionBinding {
            relation_oid: relation.oid,
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            committed_through_lsn,
            expected_version: expected,
            scoped_version,
            exact_versions,
        })
    }

    /// Stage one decoded DDL event. No control-DB side effect occurs before commit.
    ///
    /// `previous_for_oid` must be the relation already tracked for `event.c_rel_oid`, or the one
    /// unique frozen qualified-name match used to resolve either a legacy null-OID event or an
    /// explicit new OID at the tracked name. A rename, schema move, drop, or recreation is recorded
    /// as provisional transaction state here, but is not rejected yet: a streamed transaction can
    /// still abort. The matching [`Self::on_commit`] or `prepare_stream_commit` returns the
    /// typed error before opening a control transaction.
    pub fn observe(
        &mut self,
        scope: TransactionScope,
        event: DdlEvent,
        previous_for_oid: Option<&PgRelation>,
    ) -> DdlObservation {
        self.observe_inner(scope, event, previous_for_oid, false)
    }

    /// Stage structural DDL whose legacy audit row omitted the relation OID and could not be tied to
    /// one frozen identity. The ambiguity is commit-gated so a streamed abort can discard it; a real
    /// commit fails closed before any control publication or source acknowledgement.
    pub(crate) fn observe_unresolved_identity(
        &mut self,
        scope: TransactionScope,
        event: DdlEvent,
    ) -> DdlObservation {
        self.observe_inner(scope, event, None, true)
    }

    fn observe_inner(
        &mut self,
        scope: TransactionScope,
        event: DdlEvent,
        previous_for_oid: Option<&PgRelation>,
        identity_unresolved: bool,
    ) -> DdlObservation {
        if let Some(existing) = self.processed.get(&event.source_audit_id).cloned() {
            self.set_committed(
                &existing.source_schema,
                &existing.source_table,
                existing.schema_version,
            );
            if !self.pending.iter().any(|pending| {
                pending.event.source_audit_id == event.source_audit_id
                    && same_transaction(pending.scope, scope)
            }) {
                self.pending.push(PendingDdl {
                    scope,
                    event: event.clone(),
                    version: existing.schema_version,
                    identity_change: None,
                    identity_unresolved: false,
                    replay: true,
                });
            }
            return DdlObservation {
                structural_version: event.is_structural().then_some(existing.schema_version),
                replay: true,
            };
        }
        let identity_unresolved = identity_unresolved
            || (event.is_structural() && event.c_rel_oid.is_none() && previous_for_oid.is_none());
        let current = self.version_for(scope, &event.source_schema, &event.source_table);
        let version = if event.is_structural() {
            SchemaVersionNo(current.0 + 1)
        } else {
            current
        };
        let structural_version = event.is_structural().then_some(version);
        let identity_change = previous_for_oid
            .and_then(|previous| TrackedTableIdentityChange::from_event(previous, &event));
        self.pending.push(PendingDdl {
            scope,
            event,
            version,
            identity_change,
            identity_unresolved,
            replay: false,
        });
        DdlObservation {
            structural_version,
            replay: false,
        }
    }

    /// Whether `version` belongs to DDL state that must be commit-gated in the current decode pass.
    /// This includes both genuinely provisional DDL and hydrated history being reconstructed for a
    /// lost-ACK replay.
    #[must_use]
    pub fn is_provisional(&self, schema: &str, table: &str, version: SchemaVersionNo) -> bool {
        self.pending.iter().any(|pending| {
            pending.event.is_structural()
                && pending.version == version
                && table_matches(&pending.event, schema, table)
        })
    }

    /// Stage a registry row in the source transaction that owns it. A later Relation message for the
    /// same structural version and transaction replaces the trigger snapshot idempotently before
    /// commit. COMMENT-only audit rows never own registry state.
    pub fn stage_registry(&mut self, scope: TransactionScope, row: control::RegistryRow) {
        let owned_by_structural_ddl = self.pending.iter().any(|pending| {
            pending.event.is_structural()
                && same_transaction(pending.scope, scope)
                && pending.version == row.schema_version
                && table_matches(&pending.event, &row.source_schema, &row.source_table)
        });
        if !owned_by_structural_ddl {
            return;
        }

        if let Some(existing) = self.pending_registry.iter_mut().find(|pending| {
            same_transaction(pending.scope, scope)
                && pending.row.epoch == row.epoch
                && pending.row.source_schema == row.source_schema
                && pending.row.source_table == row.source_table
                && pending.row.schema_version == row.schema_version
        }) {
            existing.row = row;
            return;
        }
        self.pending_registry.push(PendingRegistry { scope, row });
    }

    /// Commit the ordinary transaction's DDL and registry state with its real `Begin` xid and
    /// `Commit` LSN/timestamp.
    ///
    /// A transaction containing structural DDL uses the same atomic protocol-v2 publication
    /// receipt as a streamed transaction, with no file children. That creates one ordered
    /// zero-child schema barrier per structurally changed table. COMMENT-only transactions keep the
    /// metadata audit path and create no schema barrier.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::MixedOrdinaryDataAndStructuralDdl`] when the ordinary transaction also
    /// contains routed user-table changes: publishing its schema barrier separately from its files
    /// could expose a partial source commit. The transaction remains unacknowledged for replay.
    ///
    /// Returns [`DdlError::MixedOrdinaryReloadEffectsAndStructuralDdl`] when that transaction also
    /// carries committed reload requests or fence markers, whose control effects cannot be included
    /// in the schema publication receipt.
    ///
    /// Returns [`DdlError::TrackedTableIdentityChange`] for a committed tracked-table rename,
    /// schema move, drop, or recreation; [`DdlError::UnresolvedRelationIdentity`] when identity
    /// cannot be proven; or [`DdlError::Control`] if atomic control persistence fails.
    pub async fn on_commit(
        &mut self,
        pool: &sqlx::PgPool,
        top_xid: u32,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
        has_routed_data: bool,
        committed_reload_effects: usize,
    ) -> Result<(), DdlError> {
        let prepared = self.prepare_ordinary_commit(commit_lsn)?;
        if prepared.ddl_rows().is_empty() && prepared.registry_rows().is_empty() {
            return Ok(());
        }

        if prepared.has_structural_ddl() {
            if has_routed_data {
                return Err(DdlError::MixedOrdinaryDataAndStructuralDdl {
                    top_xid,
                    commit_lsn,
                });
            }
            if committed_reload_effects != 0 {
                return Err(DdlError::MixedOrdinaryReloadEffectsAndStructuralDdl {
                    top_xid,
                    commit_lsn,
                    committed_reload_effects,
                });
            }
            let publication = control::NewStreamCommitPublication {
                epoch: self.epoch,
                top_xid,
                commit_lsn,
                commit_ts,
                ddl_rows: prepared.ddl_rows().to_vec(),
                registry_rows: prepared.registry_rows().to_vec(),
                files: Vec::new(),
            };
            // `Published` and `AlreadyPublished` both prove the exact atomic receipt is durable.
            // A changed replay is rejected by control before any in-memory state is finalized.
            let _outcome = control::publish_stream_commit(pool, &publication).await?;
            self.finalize_ordinary_commit(prepared);
            return Ok(());
        }

        let mut tx = pool.begin().await.map_err(control::ControlError::from)?;
        for row in &prepared.ddl_rows {
            control::insert_ddl(&mut *tx, row).await?;
        }
        for row in &prepared.registry_rows {
            control::upsert_registry(&mut *tx, row).await?;
        }
        tx.commit().await.map_err(control::ControlError::from)?;
        self.finalize_ordinary_commit(prepared);
        Ok(())
    }

    /// Validate and extract the current ordinary transaction without changing in-memory state.
    /// This is intentionally retryable across a failed or ambiguously acknowledged publication.
    pub(crate) fn prepare_ordinary_commit(
        &self,
        commit_lsn: Lsn,
    ) -> Result<PreparedOrdinaryDdl, DdlError> {
        let (ddl_rows, registry_rows) =
            self.prepare_selected(commit_lsn, CommitSelector::Ordinary)?;
        Ok(PreparedOrdinaryDdl {
            ddl_rows,
            registry_rows,
        })
    }

    /// Make a previously prepared ordinary DDL payload globally visible in this process after its
    /// control transaction returned success (including an exact lost-ACK replay).
    pub(crate) fn finalize_ordinary_commit(&mut self, prepared: PreparedOrdinaryDdl) {
        self.finalize_selected(CommitSelector::Ordinary, prepared.ddl_rows);
    }

    /// Validate and extract one streamed top-level transaction's DDL/registry payload without
    /// mutating pending or committed in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::TrackedTableIdentityChange`] for a committed tracked-table rename,
    /// schema move, drop, or recreation, and [`DdlError::UnresolvedRelationIdentity`] when an audit
    /// row cannot be tied to one frozen identity. The caller must include returned rows in the same
    /// durable control transaction as streamed data, then call [`Self::finalize_stream_commit`].
    pub(crate) fn prepare_stream_commit(
        &self,
        top_xid: u32,
        commit_lsn: Lsn,
    ) -> Result<PreparedStreamDdl, DdlError> {
        let (ddl_rows, registry_rows) =
            self.prepare_selected(commit_lsn, CommitSelector::Streamed(top_xid))?;
        Ok(PreparedStreamDdl {
            top_xid,
            ddl_rows,
            registry_rows,
        })
    }

    /// Make a previously prepared streamed DDL payload globally visible in this process after the
    /// enclosing control publication returned either `Published` or `AlreadyPublished`.
    pub(crate) fn finalize_stream_commit(&mut self, prepared: PreparedStreamDdl) {
        self.finalize_selected(
            CommitSelector::Streamed(prepared.top_xid),
            prepared.ddl_rows,
        );
    }

    /// Discard DDL and provisional registry rows rolled back by `StreamAbort`.
    ///
    /// Returns the provisional `(schema, table, version)` cache entries the caller must remove.
    pub fn on_stream_abort(
        &mut self,
        top_xid: u32,
        sub_xid: u32,
    ) -> Vec<(String, String, SchemaVersionNo)> {
        let whole = top_xid == sub_xid;
        let aborts = |scope: TransactionScope| match scope {
            TransactionScope::Ordinary => false,
            TransactionScope::Streamed {
                top_xid: owner,
                sub_xid: sub,
            } => owner == top_xid && (whole || sub == sub_xid),
        };
        let removed = self
            .pending
            .extract_if(.., |pending| aborts(pending.scope))
            .collect::<Vec<_>>();
        self.pending_registry
            .retain(|pending| !aborts(pending.scope));
        removed
            .into_iter()
            .filter(|pending| pending.event.is_structural() && !pending.replay)
            .map(|pending| {
                (
                    pending.event.source_schema,
                    pending.event.source_table,
                    pending.version,
                )
            })
            .collect()
    }

    fn prepare_selected(
        &self,
        commit_lsn: Lsn,
        selector: CommitSelector,
    ) -> Result<(Vec<control::DdlRow>, Vec<control::RegistryRow>), DdlError> {
        let pending = self
            .pending
            .iter()
            .filter(|row| selector.matches(row.scope))
            .cloned()
            .collect::<Vec<_>>();
        let registry = self
            .pending_registry
            .iter()
            .filter(|row| selector.matches(row.scope))
            .map(|pending| pending.row.clone())
            .collect::<Vec<_>>();

        if let Some(unresolved) = pending.iter().find(|pending| pending.identity_unresolved) {
            return Err(DdlError::UnresolvedRelationIdentity {
                source_audit_id: unresolved.event.source_audit_id,
                schema: unresolved.event.source_schema.clone(),
                table: unresolved.event.source_table.clone(),
                c_tag: unresolved.event.c_tag.clone(),
                relation_oid: unresolved.event.c_rel_oid,
            });
        }

        // Identity is part of a transformer worker's durable routing key. Fail before any control row is
        // written; the replication Commit/StreamCommit consequently remains unacknowledged and an
        // operator cannot mistake an old canonical table for the renamed/recreated relation.
        if let Some(change) = pending
            .iter()
            .find_map(|pending| pending.identity_change.clone())
        {
            return Err(DdlError::TrackedTableIdentityChange(change));
        }

        let rows = pending
            .iter()
            .map(|pending| control::DdlRow {
                id: DdlId(0),
                epoch: self.epoch,
                source_audit_id: pending.event.source_audit_id,
                source_schema: pending.event.source_schema.clone(),
                source_table: pending.event.source_table.clone(),
                c_lsn: commit_lsn,
                c_event: pending.event.c_event.clone(),
                c_tag: pending.event.c_tag.clone(),
                schema_version: pending.version,
                c_rel_oid: pending.event.c_rel_oid,
                c_columns: pending.event.c_columns.clone(),
                c_dropped: pending.event.c_dropped.clone(),
                c_ddl_text: pending.event.c_ddl_text.clone(),
                c_table_comment: pending.event.c_table_comment.clone(),
            })
            .collect::<Vec<_>>();
        Ok((rows, registry))
    }

    fn finalize_selected(&mut self, selector: CommitSelector, rows: Vec<control::DdlRow>) {
        for row in rows {
            self.set_committed(&row.source_schema, &row.source_table, row.schema_version);
            self.processed.insert(row.source_audit_id, row);
        }
        self.pending.retain(|row| !selector.matches(row.scope));
        self.pending_registry
            .retain(|row| !selector.matches(row.scope));
    }

    fn set_committed(&mut self, schema: &str, table: &str, version: SchemaVersionNo) {
        let existing = self
            .committed_versions
            .iter_mut()
            .find(|((s, t), _)| s == schema && t == table);
        if let Some((_, current)) = existing {
            *current = (*current).max(version);
        } else {
            self.committed_versions
                .insert((schema.to_string(), table.to_string()), version);
        }
    }

    fn pending_version_for(
        &self,
        scope: TransactionScope,
        schema: &str,
        table: &str,
    ) -> Option<SchemaVersionNo> {
        self.pending
            .iter()
            .filter(|pending| {
                same_transaction(pending.scope, scope)
                    && table_matches(&pending.event, schema, table)
            })
            .map(|pending| pending.version)
            .max()
    }

    fn historical_version_through(
        &self,
        schema: &str,
        table: &str,
        committed_through_lsn: Lsn,
    ) -> SchemaVersionNo {
        self.processed
            .values()
            .filter(|row| {
                row.source_schema == schema
                    && row.source_table == table
                    && row.c_lsn <= committed_through_lsn
            })
            .max_by_key(|row| (row.c_lsn, row.schema_version))
            .map_or(SchemaVersionNo(1), |row| row.schema_version)
    }
}

/// A source identity mutation that cannot be reconciled into a worker frozen to `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedTableIdentityChange {
    /// Stable source relation OID that connected the audit event to the tracked table.
    pub relation_oid: u32,
    /// Identity already registered in Walrus.
    pub previous_schema: String,
    /// Identity already registered in Walrus.
    pub previous_table: String,
    /// Post-DDL schema, absent when the relation was dropped.
    pub new_schema: Option<String>,
    /// Post-DDL table, absent when the relation was dropped.
    pub new_table: Option<String>,
    /// Kind of unsupported identity mutation.
    pub kind: TrackedTableIdentityChangeKind,
}

impl TrackedTableIdentityChange {
    fn from_event(previous: &PgRelation, event: &DdlEvent) -> Option<Self> {
        let supplied_different_oid = event.c_rel_oid.is_some_and(|oid| oid != previous.oid);
        if supplied_different_oid
            && (previous.schema != event.source_schema || previous.name != event.source_table)
        {
            return None;
        }

        let dropped = event.is_table_drop();
        let kind = if supplied_different_oid || event.is_table_creation() {
            TrackedTableIdentityChangeKind::Recreated
        } else if dropped {
            TrackedTableIdentityChangeKind::Dropped
        } else if event.c_tag.eq_ignore_ascii_case("ALTER TABLE")
            && previous.schema != event.source_schema
        {
            TrackedTableIdentityChangeKind::SchemaMoved
        } else if event.c_tag.eq_ignore_ascii_case("ALTER TABLE")
            && previous.name != event.source_table
        {
            TrackedTableIdentityChangeKind::Renamed
        } else {
            return None;
        };

        Some(Self {
            relation_oid: previous.oid,
            previous_schema: previous.schema.clone(),
            previous_table: previous.name.clone(),
            new_schema: (!dropped).then(|| event.source_schema.clone()),
            new_table: (!dropped).then(|| event.source_table.clone()),
            kind,
        })
    }
}

impl std::fmt::Display for TrackedTableIdentityChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.new_schema, &self.new_table) {
            (Some(new_schema), Some(new_table)) => write!(
                f,
                "{}.{} (OID {}) was {} to {}.{}",
                self.previous_schema,
                self.previous_table,
                self.relation_oid,
                self.kind,
                new_schema,
                new_table
            ),
            _ => write!(
                f,
                "{}.{} (OID {}) was {}",
                self.previous_schema, self.previous_table, self.relation_oid, self.kind
            ),
        }
    }
}

/// Unsupported mutation represented by [`TrackedTableIdentityChange`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackedTableIdentityChangeKind {
    /// The relation name changed while its OID remained stable.
    Renamed,
    /// The relation moved to another schema while its OID remained stable.
    SchemaMoved,
    /// The tracked relation was dropped.
    Dropped,
    /// A different relation identity appeared at the same frozen qualified name.
    Recreated,
}

impl std::fmt::Display for TrackedTableIdentityChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Renamed => "renamed",
            Self::SchemaMoved => "moved",
            Self::Dropped => "dropped",
            Self::Recreated => "recreated",
        })
    }
}

fn find_version(
    versions: &HashMap<(String, String), SchemaVersionNo>,
    schema: &str,
    table: &str,
) -> Option<SchemaVersionNo> {
    versions
        .iter()
        .find(|((s, t), _)| s == schema && t == table)
        .map(|(_, version)| *version)
}

fn table_matches(event: &DdlEvent, schema: &str, table: &str) -> bool {
    event.source_schema == schema && event.source_table == table
}

const fn same_transaction(left: TransactionScope, right: TransactionScope) -> bool {
    match (left, right) {
        (TransactionScope::Ordinary, TransactionScope::Ordinary) => true,
        (
            TransactionScope::Streamed { top_xid: left, .. },
            TransactionScope::Streamed { top_xid: right, .. },
        ) => left == right,
        (TransactionScope::Ordinary, TransactionScope::Streamed { .. })
        | (TransactionScope::Streamed { .. }, TransactionScope::Ordinary) => false,
    }
}

/// DDL decode, structured-snapshot, and control-persistence failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DdlError {
    /// A required audit tuple column was absent or invalid.
    #[error("ddl_audit tuple missing/invalid column: {0}")]
    MissingColumn(&'static str),
    /// The source replica-identity catalog code was invalid.
    #[error("ddl_audit tuple has invalid replica identity: {0}")]
    ReplicaIdentity(String),
    /// A structured JSON payload was malformed.
    #[error("parse ddl_audit json: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed DDL transaction changed the durable identity of a tracked relation.
    #[error("tracked table identity change is unsupported: {0}")]
    TrackedTableIdentityChange(TrackedTableIdentityChange),
    /// A structural audit row could not be matched uniquely to the frozen epoch inventory. This is
    /// normally an older null-OID row, but also covers conflicting frozen same-name identities.
    #[error(
        "cannot prove relation identity for DDL audit {source_audit_id}: {c_tag} on {schema}.{table}, relation_oid={relation_oid:?}"
    )]
    UnresolvedRelationIdentity {
        /// Stable source audit-row identity.
        source_audit_id: i64,
        /// Post-command schema reported by the source trigger.
        schema: String,
        /// Post-command table reported by the source trigger.
        table: String,
        /// PostgreSQL command tag.
        c_tag: String,
        /// Supplied relation OID, absent on the legacy rows this guard primarily protects.
        relation_oid: Option<u32>,
    },
    /// An ordinary source transaction mixed structural DDL with routed data files, which cannot be
    /// published as one atomic control receipt by the ordinary batching path.
    #[error(
        "ordinary transaction xid {top_xid} at {commit_lsn} mixes structural DDL with replicated data; refusing to publish a partial source commit"
    )]
    MixedOrdinaryDataAndStructuralDdl {
        /// Top-level xid retained from the matching ordinary Begin.
        top_xid: u32,
        /// Commit LSN that must remain unacknowledged.
        commit_lsn: Lsn,
    },
    /// An ordinary source transaction mixed structural DDL with reload-event control effects,
    /// which are persisted outside the atomic schema publication receipt.
    #[error(
        "ordinary transaction xid {top_xid} at {commit_lsn} mixes structural DDL with {committed_reload_effects} committed reload effect(s); refusing to publish a partial source commit"
    )]
    MixedOrdinaryReloadEffectsAndStructuralDdl {
        /// Top-level xid retained from the matching ordinary Begin.
        top_xid: u32,
        /// Commit LSN that must remain unacknowledged.
        commit_lsn: Lsn,
        /// Number of source reload requests/fence markers committed with the DDL.
        committed_reload_effects: usize,
    },
    /// A streamed source transaction mixed structural DDL with reload-event control effects, which
    /// are not members of the atomic files-and-schema publication receipt.
    #[error(
        "streamed transaction xid {top_xid} at {commit_lsn} mixes structural DDL with {committed_reload_effects} committed reload effect(s); refusing to publish a partial source commit"
    )]
    MixedStreamedReloadEffectsAndStructuralDdl {
        /// Top-level xid named by StreamCommit.
        top_xid: u32,
        /// Commit LSN that must remain unacknowledged.
        commit_lsn: Lsn,
        /// Number of source reload requests/fence markers committed with the DDL.
        committed_reload_effects: usize,
    },
    /// A Relation message could not be assigned to one exact historical schema version without
    /// falling forward to a newer durable shape.
    #[error(
        "cannot bind relation {schema}.{table} (OID {relation_oid}) at committed decode frontier {committed_through_lsn} to expected schema version {expected_version}: scoped={scoped_version:?}, matching historical shapes={exact_versions:?}"
    )]
    RelationVersionBinding {
        /// Source relation identity carried by pgoutput.
        relation_oid: u32,
        /// Qualified source schema carried by pgoutput.
        schema: String,
        /// Qualified source table carried by pgoutput.
        table: String,
        /// Last successfully processed source commit used to cut durable DDL history.
        committed_through_lsn: Lsn,
        /// Version the transaction-local state or durable history requires at this position.
        expected_version: SchemaVersionNo,
        /// Exact transaction-local DDL version, when one exists.
        scoped_version: Option<SchemaVersionNo>,
        /// Hydrated versions whose persisted relation snapshots exactly matched the wire relation.
        exact_versions: Vec<SchemaVersionNo>,
    },
    /// The atomic control-Postgres commit failed.
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

#[cfg(test)]
#[path = "ddl_test.rs"]
mod tests;
