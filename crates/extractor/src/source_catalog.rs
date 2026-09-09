//! Source-catalog queries shared by bootstrap reconciliation and table reloads.
//!
//! These helpers describe the exact publication inventory and the relation shape pgoutput will
//! advertise. They do not read table data or open a PostgreSQL snapshot.

use anyhow::Context;
use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use std::time::Duration;

/// Session/xact advisory-lock key shared with `public.walrus_guard_publication_ddl()` in source migration
/// 0002. The bytes spell `walruspb`; changing either side would silently remove serialization.
pub const PUBLICATION_DDL_GUARD_KEY: i64 = 0x7761_6c72_7573_7062;

/// Frozen source facts that must describe one side of the same writer-drained boundary.
#[derive(Debug)]
pub struct CatalogFence {
    /// WAL insert position sampled only after every target's prior writers have committed or aborted.
    pub start_lsn: Lsn,
    /// Exact ordered target shapes held stable by the same table locks as `start_lsn`.
    pub relations: Vec<PgRelation>,
    /// Complete source comments captured under the same writer-drained table locks as `relations`.
    pub comments: Vec<RelationComments>,
}

/// One table's baseline metadata. It is kept outside [`PgRelation`] because pgoutput Relation
/// messages do not carry comments and structural wire-shape equality must remain comment-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationComments {
    /// Source PostgreSQL schema name.
    pub schema: String,
    /// Unqualified source table name.
    pub table: String,
    /// Table comment captured at the catalog fence.
    pub table_comment: Option<String>,
    /// Ordered source column names and their comments.
    pub columns: Vec<(String, Option<String>)>,
}

impl RelationComments {
    /// Add the baseline metadata to a registry row's serialized `PgRelation`. These additive JSON
    /// fields are ignored by old structural decoders and consumed by comment-aware transformers.
    ///
    /// # Errors
    ///
    /// Returns an error when the serialized relation is malformed or its logical columns do not
    /// exactly match the comment snapshot captured under the same catalog fence.
    pub fn enrich_registry(&self, relation: &mut serde_json::Value) -> anyhow::Result<()> {
        let object = relation
            .as_object_mut()
            .context("serialized registry relation is not an object")?;
        object.insert(
            "table_comment".to_string(),
            self.table_comment
                .as_ref()
                .map_or(serde_json::Value::Null, |text| {
                    serde_json::Value::String(text.clone())
                }),
        );
        let registry_columns = object
            .get_mut("columns")
            .and_then(serde_json::Value::as_array_mut)
            .context("serialized registry relation has no column array")?;
        anyhow::ensure!(
            registry_columns.len() == self.columns.len(),
            "registry/comment column count differs for {}.{}",
            self.schema,
            self.table
        );
        for (column, (expected_name, comment)) in registry_columns.iter_mut().zip(&self.columns) {
            let column = column
                .as_object_mut()
                .context("serialized registry column is not an object")?;
            let actual_name = column
                .get("name")
                .and_then(serde_json::Value::as_str)
                .context("serialized registry column has no name")?;
            anyhow::ensure!(
                actual_name == expected_name,
                "registry/comment column identity differs for {}.{}: {actual_name:?} != {expected_name:?}",
                self.schema,
                self.table
            );
            column.insert(
                "comment".to_string(),
                comment.as_ref().map_or(serde_json::Value::Null, |text| {
                    serde_json::Value::String(text.clone())
                }),
            );
        }
        Ok(())
    }
}

/// The four row-change families a PostgreSQL publication may emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationActions {
    insert: bool,
    update: bool,
    delete: bool,
    truncate: bool,
}

impl PublicationActions {
    /// Whether the publication carries every mutation the downstream mirror must reconcile.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.insert && self.update && self.delete && self.truncate
    }

    fn disabled(self) -> String {
        [
            (!self.insert).then_some("INSERT"),
            (!self.update).then_some("UPDATE"),
            (!self.delete).then_some("DELETE"),
            (!self.truncate).then_some("TRUNCATE"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(", ")
    }
}

/// Effective publication membership and any per-relation restriction on one exact target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationTargetOptions {
    published: bool,
    row_filter: bool,
    column_list: bool,
    row_level_security: bool,
    topology_stable: bool,
}

/// A publication configuration that cannot provide a lossless full-table WAL overlay.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PublicationCoverageIssue {
    /// The configured publication disappeared.
    #[error("publication {publication} does not exist")]
    MissingPublication {
        /// Configured publication name that was not found.
        publication: String,
    },
    /// At least one mutation family is disabled globally for the publication.
    #[error(
        "publication {publication} does not publish required operations: {disabled} \
         (need INSERT, UPDATE, DELETE, TRUNCATE)"
    )]
    DisabledOperations {
        /// Configured publication whose action flags are incomplete.
        publication: String,
        /// Comma-separated mutation families that are disabled.
        disabled: String,
    },
    /// The exact relation is absent from the effective publication inventory.
    #[error("table {schema}.{table} is not in publication {publication}")]
    MissingTarget {
        /// Configured publication name.
        publication: String,
        /// Source schema of the missing target.
        schema: String,
        /// Source table missing from effective membership.
        table: String,
    },
    /// A row predicate would make the baseline/WAL overlay omit matching changes.
    #[error(
        "publication {publication} applies a row filter to {schema}.{table}; \
         full-table reconciliation requires every row"
    )]
    RowFilter {
        /// Configured publication name.
        publication: String,
        /// Source schema of the filtered target.
        schema: String,
        /// Source table with a row filter.
        table: String,
    },
    /// A column list would make the baseline/WAL overlay omit part of the row shape.
    #[error(
        "publication {publication} applies a column list to {schema}.{table}; \
         full-table reconciliation requires every column"
    )]
    ColumnList {
        /// Configured publication name.
        publication: String,
        /// Source schema of the restricted target.
        schema: String,
        /// Source table with a publication column list.
        table: String,
    },
    /// The SQL snapshot could be policy-filtered even though logical decoding sees all row changes.
    #[error(
        "table {schema}.{table} in publication {publication} has row-level security enabled or \
         forced; a complete snapshot cannot be attested (fix: {remediation_sql})"
    )]
    RowLevelSecurity {
        /// Configured publication name.
        publication: String,
        /// Source schema of the protected target.
        schema: String,
        /// Source table whose snapshot can be policy-filtered.
        table: String,
        /// Safely quoted SQL that disables the unsafe policy setting.
        remediation_sql: String,
    },
    /// Table topology or indirect membership can change the visible row set without publication DDL.
    #[error(
        "table {schema}.{table} has topology-dependent membership in publication {publication}; \
         full-table reconciliation requires a plain non-inherited table published directly or \
         through FOR ALL TABLES"
    )]
    TopologyDependent {
        /// Configured publication name.
        publication: String,
        /// Source schema of the topology-dependent target.
        schema: String,
        /// Source table whose membership is not stable.
        table: String,
    },
}

/// Read the publication's global action flags without locking it.
///
/// # Errors
///
/// Returns the source query error. `Ok(None)` means the named publication does not exist.
pub async fn publication_actions<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    publication: &str,
) -> Result<Option<PublicationActions>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate
             FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?;
    Ok(row.map(|row| PublicationActions {
        insert: row.get(0),
        update: row.get(1),
        delete: row.get(2),
        truncate: row.get(3),
    }))
}

/// Try to acquire the orchestration SQL session's shared publication-DDL guard. The source event
/// trigger takes the same key exclusively for publication DDL; the raw replication session takes a
/// second shared try-lock so loss of either connection cannot silently remove the only guard.
///
/// Exporter sessions must not acquire it: they start after an operator's exclusive request can have
/// queued and could starve behind that writer. The raw replication client uses a nonblocking try-lock
/// during startup and fails the whole pipeline if it cannot join the initial guard set.
///
/// # Errors
///
/// Returns the source query error.
pub async fn try_acquire_publication_ddl_guard(
    client: &tokio_postgres::Client,
) -> Result<bool, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_lock_shared($1)",
            &[&PUBLICATION_DDL_GUARD_KEY],
        )
        .await?;
    Ok(row.get(0))
}

/// Inspect effective membership and every target option relevant to a lossless full-row overlay.
///
/// The recursive ancestor walk matters for partitions: a leaf can inherit the effective entry from
/// an explicitly-published partition root. `pg_publication_tables.rowfilter` catches the effective
/// predicate; `pg_publication_rel.prattrs` catches even an explicit list that happens to name every
/// current column and would otherwise look identical to an unrestricted `attnames` array. The
/// target's `relrowsecurity`/`relforcerowsecurity` flags are also part of coverage: pgoutput sees row
/// changes independently of SQL policies, while the snapshot COPY is policy-sensitive. Reading the
/// PostgreSQL-15 publication additions through `to_jsonb(record)` keeps the same query valid on
/// supported PostgreSQL 14 sources, where those keys are simply absent.
///
/// # Errors
///
/// Returns the source query error.
pub async fn publication_target_options<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    publication: &str,
    schema: &str,
    table: &str,
) -> Result<PublicationTargetOptions, tokio_postgres::Error> {
    let row = client
        .query_one(
            "WITH RECURSIVE target(relid) AS (
               SELECT c.oid
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = $2 AND c.relname = $3 AND c.relkind IN ('r', 'p')
             ), ancestors(relid) AS (
               SELECT relid FROM target
               UNION
               SELECT i.inhparent
               FROM pg_catalog.pg_inherits i
               JOIN ancestors a ON a.relid = i.inhrelid
             )
             SELECT
               EXISTS (
                 SELECT 1 FROM pg_catalog.pg_publication_tables pt
                 WHERE pt.pubname = $1 AND pt.schemaname = $2 AND pt.tablename = $3
               ) AS published,
               EXISTS (
                 SELECT 1 FROM pg_catalog.pg_publication_tables pt
                 WHERE pt.pubname = $1 AND pt.schemaname = $2 AND pt.tablename = $3
                   AND pg_catalog.to_jsonb(pt)->>'rowfilter' IS NOT NULL
               ) OR EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication p
                 JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid
                 JOIN ancestors a ON a.relid = pr.prrelid
                 WHERE p.pubname = $1
                   AND pg_catalog.to_jsonb(pr)->>'prqual' IS NOT NULL
               ) AS row_filter,
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication p
                 JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid
                 JOIN ancestors a ON a.relid = pr.prrelid
                 WHERE p.pubname = $1
                   AND pg_catalog.to_jsonb(pr)->>'prattrs' IS NOT NULL
               ) AS column_list,
               EXISTS (
                 SELECT 1
                 FROM target t
                 JOIN pg_catalog.pg_class c ON c.oid = t.relid
                 WHERE c.relrowsecurity OR c.relforcerowsecurity
               ) AS row_level_security,
               EXISTS (
                 SELECT 1
                 FROM target t
                 JOIN pg_catalog.pg_class c ON c.oid = t.relid
                 WHERE c.relkind = 'r'
                   AND NOT c.relispartition
                   AND NOT EXISTS (
                     SELECT 1 FROM pg_catalog.pg_inherits i
                     WHERE i.inhrelid = t.relid OR i.inhparent = t.relid
                   )
                   AND EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_publication p
                     WHERE p.pubname = $1
                       AND (
                         p.puballtables
                         OR EXISTS (
                           SELECT 1 FROM pg_catalog.pg_publication_rel pr
                           WHERE pr.prpubid = p.oid AND pr.prrelid = t.relid
                         )
                       )
                   )
               ) AS topology_stable",
            &[&publication, &schema, &table],
        )
        .await?;
    Ok(PublicationTargetOptions {
        published: row.get(0),
        row_filter: row.get(1),
        column_list: row.get(2),
        row_level_security: row.get(3),
        topology_stable: row.get(4),
    })
}

/// Require all four global publication action flags.
///
/// # Errors
///
/// Returns [`PublicationCoverageIssue::MissingPublication`] or
/// [`PublicationCoverageIssue::DisabledOperations`] for an incomplete configuration.
pub fn require_publication_actions(
    publication: &str,
    actions: Option<PublicationActions>,
) -> Result<(), PublicationCoverageIssue> {
    let Some(actions) = actions else {
        return Err(PublicationCoverageIssue::MissingPublication {
            publication: publication.to_string(),
        });
    };
    if !actions.is_complete() {
        return Err(PublicationCoverageIssue::DisabledOperations {
            publication: publication.to_string(),
            disabled: actions.disabled(),
        });
    }
    Ok(())
}

/// Require exact membership with no row/column restriction, RLS, or unstable topology.
///
/// # Errors
///
/// Returns the matching [`PublicationCoverageIssue`] when the target is absent or restricted.
pub fn require_full_target(
    publication: &str,
    schema: &str,
    table: &str,
    options: PublicationTargetOptions,
) -> Result<(), PublicationCoverageIssue> {
    if !options.published {
        return Err(PublicationCoverageIssue::MissingTarget {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    if options.row_filter {
        return Err(PublicationCoverageIssue::RowFilter {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    if options.column_list {
        return Err(PublicationCoverageIssue::ColumnList {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    if !crate::relcache::is_internal_table(schema, table) && options.row_level_security {
        let qualified = format!("{}.{}", quote_identifier(schema), quote_identifier(table));
        return Err(PublicationCoverageIssue::RowLevelSecurity {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            remediation_sql: format!(
                "ALTER TABLE {qualified} DISABLE ROW LEVEL SECURITY; \
                 ALTER TABLE {qualified} NO FORCE ROW LEVEL SECURITY"
            ),
        });
    }
    if !options.topology_stable {
        return Err(PublicationCoverageIssue::TopologyDependent {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    Ok(())
}

/// Convert a catalog `::int8` OID without wrapping negative or oversized values.
fn catalog_oid(raw: i64) -> anyhow::Result<u32> {
    u32::try_from(raw).with_context(|| format!("catalog OID {raw} is outside the u32 range"))
}

/// Every published **user** table. The four `public.walrus_*` internal tables are
/// control-plane inputs, not reconciliation targets.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if publication membership cannot be queried or decoded.
pub async fn published_user_tables<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    publication: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let rows = client
        .query(
            "SELECT schemaname, tablename FROM pg_publication_tables
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
        .context("list published user tables")?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

/// Quote a PostgreSQL identifier for the deterministic `LOCK TABLE` statement.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Capture a new generation's catalog and start LSN at one writer-drained source boundary.
///
/// The publication inventory is sorted before constructing a single `LOCK TABLE ... IN SHARE
/// MODE` statement. `SHARE` conflicts with row writers and structural DDL, so when the statement
/// returns every transaction that had already modified a target has finished, and no new target
/// writer can begin until this transaction commits. Sampling the WAL insert LSN (not the flush LSN,
/// which can trail a catalog-visible asynchronous commit) and relation shapes inside that interval
/// makes them one coherent generation boundary. A bounded lock timeout makes contention fail closed
/// instead of leaving startup hung indefinitely.
///
/// The caller must already hold the long-lived publication-DDL advisory guard; it prevents the
/// inventory itself from changing while these per-table locks are acquired.
///
/// # Errors
///
/// Returns an error if the transaction cannot acquire every target lock within `lock_timeout`, the
/// inventory changes unexpectedly, a catalog shape cannot be decoded, or the WAL LSN is invalid.
pub async fn capture_catalog_fence(
    client: &mut tokio_postgres::Client,
    publication: &str,
    lock_timeout: Duration,
) -> anyhow::Result<CatalogFence> {
    let tx = client
        .transaction()
        .await
        .context("begin source catalog-fence transaction")?;
    let timeout_ms = u64::try_from(lock_timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    tx.query_one(
        "SELECT pg_catalog.set_config('lock_timeout', $1, true)",
        &[&format!("{timeout_ms}ms")],
    )
    .await
    .context("bound source catalog-fence lock wait")?;

    let tables = published_user_tables(&tx, publication)
        .await
        .context("list targets before source catalog fence")?;
    if !tables.is_empty() {
        let targets = tables
            .iter()
            .map(|(schema, table)| {
                format!("{}.{}", quote_identifier(schema), quote_identifier(table))
            })
            .collect::<Vec<_>>()
            .join(", ");
        tx.batch_execute(&format!("LOCK TABLE {targets} IN SHARE MODE"))
            .await
            .context("drain published-table writers at source catalog fence")?;
    }

    let locked_tables = published_user_tables(&tx, publication)
        .await
        .context("verify targets under source catalog fence")?;
    anyhow::ensure!(
        locked_tables == tables,
        "publication inventory changed while acquiring source catalog fence: before={tables:?}, after={locked_tables:?}"
    );

    // Validate keys only after SHARE has frozen every target. The earlier source preflight can race
    // a DDL transaction that began before the pipeline acquired its session guard; this locked
    // catalog read is the final authority for the exact shapes about to become a generation.
    let unusable = tx
        .query_opt(
            "SELECT pt.schemaname, pt.tablename, c.relreplident::text
             FROM pg_catalog.pg_publication_tables pt
             JOIN pg_catalog.pg_namespace n ON n.nspname = pt.schemaname
             JOIN pg_catalog.pg_class c
               ON c.relnamespace = n.oid AND c.relname = pt.tablename
             WHERE pt.pubname = $1
               AND NOT (
                 pt.schemaname = 'public'
                 AND pt.tablename IN (
                   'walrus_heartbeat', 'walrus_ddl_audit',
                   'walrus_reload_signal', 'walrus_reload_event'
                 )
               )
               AND (
                 c.relreplident = 'n'
                 OR NOT EXISTS (
                   SELECT 1
                   FROM pg_catalog.pg_index i
                   WHERE i.indrelid = c.oid
                     AND i.indisprimary
                     AND i.indisvalid
                     AND i.indisready
                     AND i.indislive
                 )
               )
             ORDER BY pt.schemaname, pt.tablename
             LIMIT 1",
            &[&publication],
        )
        .await
        .context("validate source-table keys under catalog fence")?;
    if let Some(row) = unusable {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let identity: String = row.get(2);
        anyhow::bail!(
            "source catalog fence rejected {schema}.{table}: a valid, ready, live primary key and usable replica identity are required (relreplident={identity})"
        );
    }

    let lsn_text: String = tx
        .query_one("SELECT pg_catalog.pg_current_wal_insert_lsn()::text", &[])
        .await
        .context("sample source WAL position under catalog fence")?
        .get(0);
    let start_lsn = lsn_text
        .parse::<Lsn>()
        .context("parse source catalog-fence WAL position")?;
    let mut relations = Vec::with_capacity(tables.len());
    let mut comments = Vec::with_capacity(tables.len());
    for (schema, table) in &tables {
        relations.push(
            describe_source_relation(&tx, schema, table)
                .await
                .with_context(|| format!("describe {schema}.{table} under source catalog fence"))?,
        );
        comments.push(
            describe_source_comments(&tx, schema, table)
                .await
                .with_context(|| {
                    format!("describe comments for {schema}.{table} under source catalog fence")
                })?,
        );
    }
    tx.commit()
        .await
        .context("commit source catalog-fence transaction")?;
    Ok(CatalogFence {
        start_lsn,
        relations,
        comments,
    })
}

/// Read table and logical-column comments for an already-locked source relation.
async fn describe_source_comments<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    schema: &str,
    table: &str,
) -> anyhow::Result<RelationComments> {
    let head = client
        .query_one(
            "SELECT pg_catalog.obj_description(c.oid, 'pg_class')
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await?;
    let rows = client
        .query(
            "SELECT a.attname, pg_catalog.col_description(a.attrelid, a.attnum)
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await?;
    Ok(RelationComments {
        schema: schema.to_string(),
        table: table.to_string(),
        table_comment: head.get(0),
        columns: rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect(),
    })
}

/// Build a [`PgRelation`] shape from the source catalog (`pg_class`/`pg_attribute`/`pg_index`).
/// Bootstrap reconciliation and reload fencing need this shape before a streamed `Relation`
/// message necessarily arrives.
///
/// # Errors
///
/// Returns [`anyhow::Error`] when the relation is absent, catalog queries/decoding fail, an OID is
/// outside the supported integer range, or `relreplident` is invalid.
pub async fn describe_source_relation<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    schema: &str,
    table: &str,
) -> anyhow::Result<PgRelation> {
    // The relation head and its column list are independent catalog reads. Polling both futures
    // together pipelines them over one tokio-postgres connection.
    let (head, rows) = tokio::try_join!(
        async {
            client
                .query_one(
                    "SELECT c.oid::int8, c.relreplident::text
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = $1 AND c.relname = $2",
                    &[&schema, &table],
                )
                .await
                .with_context(|| format!("describe {schema}.{table}: relation not found"))
        },
        async {
            // Reproduce supported PG14–17 pgoutput's Relation-column set and key flags exactly.
            // Generated columns are absent from Relation/tuple messages on those versions, so
            // including them here would make a live catalog check disagree with the same-version
            // registry shape. Source preflight rejects PG18+ until its configurable generated-
            // column publication semantics are attested explicitly.
            // DEFAULT marks the primary key, INDEX marks only the chosen replica-identity
            // index, FULL marks every published column, and NOTHING marks none.
            client
                .query(
                    "SELECT a.attname,
                            a.atttypid::int8            AS type_oid,
                            a.atttypmod                 AS type_modifier,
                            CASE c.relreplident
                              WHEN 'f' THEN true
                              WHEN 'd' THEN COALESCE(bool_or(i.indisprimary), false)
                              WHEN 'i' THEN COALESCE(bool_or(i.indisreplident), false)
                              ELSE false
                            END AS is_key
                     FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     JOIN pg_attribute a
                         ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.attgenerated = ''
                     LEFT JOIN pg_index i
                         ON i.indrelid = c.oid
                         AND (i.indisprimary OR i.indisreplident)
                         AND i.indisvalid AND i.indisready AND i.indislive
                         AND EXISTS (
                           SELECT 1
                           FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                           WHERE k.attnum = a.attnum AND k.ord <= i.indnkeyatts
                         )
                     WHERE n.nspname = $1 AND c.relname = $2
                     GROUP BY c.relreplident, a.attname, a.atttypid, a.atttypmod, a.attnum
                     ORDER BY a.attnum",
                    &[&schema, &table],
                )
                .await
                .with_context(|| format!("describe {schema}.{table}: read columns"))
        },
    )?;
    let oid: i64 = head.get(0);
    let relreplident: String = head.get(1);

    let columns = rows
        .iter()
        .map(|r| {
            let type_oid = r.get::<_, i64>(1);
            Ok(PgColumn {
                name: r.get::<_, String>(0),
                type_oid: catalog_oid(type_oid)
                    .with_context(|| format!("describe {schema}.{table}: attribute type OID"))?,
                type_modifier: r.get::<_, i32>(2),
                is_key: r.get::<_, bool>(3),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(PgRelation {
        oid: catalog_oid(oid)
            .with_context(|| format!("describe {schema}.{table}: relation OID"))?,
        schema: schema.to_string(),
        name: table.to_string(),
        replica_identity: relreplident
            .parse::<ReplicaIdentity>()
            .with_context(|| format!("describe {schema}.{table}: replica identity"))?,
        columns,
    })
}

#[cfg(test)]
#[path = "source_catalog_test.rs"]
mod tests;
