//! Source-side preflight (§1.1, architecture "Startup & bootstrap" steps 1–3, 6).
//!
//! Assert every server-side precondition before a single byte of WAL is read: the connecting role has
//! the `REPLICATION` privilege, `wal_level = logical`, server 14–17, wal-sender headroom, the
//! publication covers `public.walrus_ddl_audit`, `public.walrus_heartbeat`, `public.walrus_reload_signal`, and
//! `public.walrus_reload_event`, and every published **user** table has schema `USAGE`, whole-table
//! `SELECT`, a privilege that permits the consistent-export locks, and a usable replica identity
//! (a PK for `DEFAULT`). Any mismatch is **terminal** — a
//! [`PreflightError`] mapped to a distinct, greppable [`common::ExitCode`] (`CrashLoopBackOff`, not
//! a silent slow failure).
//!
//! **Connection note:** `tokio-postgres` 0.7 has no API to open a `replication=database` connection
//! (and its config parser rejects the param), so the preflight runs its catalog checks over an
//! ordinary connection and asserts the `REPLICATION` privilege from `pg_roles` — a *more* reliable
//! capability check than "a superuser connect happened to succeed". The streaming replication
//! connection itself is established by [`crate::replication`]. Catalog reads use the **simple query protocol**
//! (`simple_query`); read the version from the integer `server_version_num`, never the text
//! `version()`.

use crate::config::ExtractorConfig;
use common::ReplicaIdentity;
use common::sql::{SqlIdent, SqlStrExt};
use std::collections::HashSet;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// A published table, `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// What the server reported for the two headline settings.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// `server_version_num`, e.g. `140009`. Compared numerically, never against the version string.
    pub version_num: i32,
    /// `wal_level`, which must be `logical` for logical replication to be possible at all.
    pub wal_level: String,
}

/// Strict rejects a keyless table; lenient reports it for diagnostics without granting admission.
/// Production [`ExtractorConfig::validate`](crate::config::ExtractorConfig::validate) rejects lenient mode
/// until the resulting exclusion can be bound durably to a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkMode {
    /// A keyless published table fails preflight, so the extractor refuses to start.
    Strict,
    /// Report keyless tables without returning an error. This is a low-level diagnostic mode only:
    /// it does not remove those tables from publication, catalog fencing, or logical decoding.
    Lenient,
}

/// Outcome of the per-table PK preflight.
#[derive(Debug, Default, Clone)]
pub struct PkReport {
    /// Tables whose catalog inspection found a usable replica-identity key.
    pub ok: Vec<TableId>,
    /// Keyless tables reported under [`PkMode::Lenient`]. They are not automatically excluded from
    /// replication; production configuration rejects this diagnostic-only mode. Always empty under
    /// [`PkMode::Strict`], which errors instead of reporting.
    pub quarantined: Vec<TableId>,
}

/// A terminal source-preflight mismatch. `main` maps it (via [`common::Error`]) to a distinct exit
/// code.
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PreflightError {
    /// `wal_level` is not `logical`, so no logical slot can stream. Changing it needs a restart of
    /// the source, which is why this is terminal rather than retried.
    #[error("wal_level is {found}, need 'logical'")]
    WalLevel {
        /// Actual `wal_level` reported by the source.
        found: String,
    },
    /// The source predates PG14 and cannot speak pgoutput protocol v2, which walrus requires for
    /// streamed transactions.
    #[error("server_version_num {found} < 140000 (proto v2 needs PG14+)")]
    ServerTooOld {
        /// Source `server_version_num` below the supported floor.
        found: i32,
    },
    /// PostgreSQL 18 added configurable generated-column publication semantics. Walrus's catalog
    /// and DDL shapes intentionally follow the PG14–17 pgoutput shape, which omits generated
    /// columns. Until the PG18 publication option can be attested for the full online lifetime,
    /// starting on PG18+ could silently disagree with the wire relation shape.
    #[error(
        "server_version_num {found} >= 180000; PostgreSQL 18+ generated-column logical-publication \
         semantics are not safely supported (supported source versions: PostgreSQL 14–17)"
    )]
    UnsupportedGeneratedColumnPublication {
        /// Source `server_version_num` with unsupported publication semantics.
        found: i32,
    },
    /// The source is already at its `max_replication_slots` or `max_wal_senders` limit, so creating
    /// walrus's slot would fail later, in a worse place.
    #[error("no headroom: {kind} {used}/{max}")]
    NoHeadroom {
        /// Exhausted PostgreSQL resource setting.
        kind: &'static str,
        /// Slots or senders currently in use.
        used: i32,
        /// Configured PostgreSQL maximum.
        max: i32,
    },
    /// The configured slot is not the slot owned by the durable control generation. Silently
    /// switching names would strand retained WAL and can consume a second source slot.
    #[error(
        "configured slot {configured:?} differs from current generation slot {recorded:?}; refusing to mutate replication slots"
    )]
    SlotNameDrift {
        /// Replication slot requested by current configuration.
        configured: String,
        /// Replication slot bound to the durable generation.
        recorded: String,
    },
    /// The configured publication does not exist on the source.
    #[error("publication {pub_name} does not exist")]
    PublicationMissing {
        /// Configured publication name that was not found.
        pub_name: String,
    },
    /// The publication exists but does not cover a table walrus was told to replicate. The message
    /// carries the `ALTER PUBLICATION` that fixes it, since that is the operator's next action.
    #[error(
        "publication {pub_name} missing table {schema}.{table} \
         (fix: ALTER PUBLICATION {pub_name} ADD TABLE {schema}.{table})"
    )]
    PublicationGap {
        /// Configured publication name.
        pub_name: String,
        /// Source schema of the missing table.
        schema: String,
        /// Source table absent from publication membership.
        table: String,
    },
    /// The publication exists, but its action flags or a per-table filter/column list make it an
    /// incomplete source of truth for full-table reconciliation.
    #[error(transparent)]
    PublicationCoverage(#[from] crate::source_catalog::PublicationCoverageIssue),
    /// A published table has no key, so its updates and deletes could not be applied downstream.
    /// Raised only under [`PkMode::Strict`]; diagnostic [`PkMode::Lenient`] reports it in a
    /// [`PkReport`] without admitting that configuration to production startup.
    #[error("table {schema}.{table} has no PRIMARY KEY / usable replica identity")]
    NoPrimaryKey {
        /// Source schema of the keyless table.
        schema: String,
        /// Source table without a usable replica-identity key.
        table: String,
    },
    /// The connecting role lacks `REPLICATION`, so it cannot open a replication connection.
    #[error("missing REPLICATION privilege")]
    NoReplicationPriv,
    /// Table grants alone do not let a role resolve a relation through its containing schema.
    #[error(
        "source role {role:?} has no USAGE privilege on published target schema {schema}; \
         grant schema USAGE (recommended fix: {grant_sql})"
    )]
    NoSchemaUsagePrivilege {
        /// Connected source role whose effective grants were inspected.
        role: String,
        /// Published target schema.
        schema: String,
        /// Safely quoted recommended grant for an administrator.
        grant_sql: String,
    },
    /// Consistent bootstrap/reload fences use source-table lock modes stronger than ACCESS SHARE.
    /// PostgreSQL requires a table-level UPDATE, DELETE, or TRUNCATE grant (or ownership/superuser)
    /// for those modes; SELECT and REPLICATION alone are insufficient.
    #[error(
        "source role {role:?} cannot acquire consistent-export locks on table {schema}.{table}; \
         grant a table-level UPDATE, DELETE, or TRUNCATE privilege \
         (recommended fix: {grant_sql})"
    )]
    NoTableLockPrivilege {
        /// Connected source role whose effective grants were inspected.
        role: String,
        /// Published target schema.
        schema: String,
        /// Published target table.
        table: String,
        /// Safely quoted recommended grant for an administrator.
        grant_sql: String,
    },
    /// The role has no table-level SELECT grant. Column grants are not enough for Walrus's
    /// arbitrary full-row `COPY (SELECT ...)` export and can change meaning as columns are added.
    #[error(
        "source role {role:?} cannot COPY the complete table {schema}.{table}; \
         grant table-level SELECT (recommended fix: {grant_sql})"
    )]
    NoTableSelectPrivilege {
        /// Connected source role whose effective grants were inspected.
        role: String,
        /// Published target schema.
        schema: String,
        /// Published target table.
        table: String,
        /// Safely quoted recommended grant for an administrator.
        grant_sql: String,
    },
    /// The `public.walrus_ddl_audit` tap is absent or incomplete, so schema changes would drift silently.
    /// `detail` names which half is missing (the table or an event trigger).
    #[error("DDL capture not installed: {detail} (apply migrations/source/0002_ddl_triggers.sql)")]
    DdlCaptureMissing {
        /// Missing DDL-capture object or trigger.
        detail: &'static str,
    },
    /// The `public.walrus_reload_signal` table is absent, so no chunk export could ever learn its
    /// watermark. `detail` names what was missing.
    #[error(
        "reload signal table not installed: {detail} \
         (apply migrations/source/0003_reload_signal.sql)"
    )]
    ReloadSignalMissing {
        /// Missing reload-signal contract component.
        detail: &'static str,
    },
    /// The append-only request/fence relation is absent or cannot identify its rows.
    #[error(
        "reload event table not installed: {detail} \
         (apply migrations/source/0004_reload_event.sql)"
    )]
    ReloadEventMissing {
        /// Missing reload-event contract component.
        detail: &'static str,
    },
    /// A catalog query failed on the wire. `source` keeps tokio-postgres's typed failure — SQLSTATE,
    /// severity, hint — reachable by [`source()`](std::error::Error::source)/`downcast_ref`, exactly
    /// as [`HeartbeatError`](crate::heartbeat::HeartbeatError) already does for the same client.
    ///
    /// `#[from]` rather than `#[source]`: every catalog read goes through one helper, so `?` there
    /// is the only conversion and there is nothing for a second one to be confused with.
    #[error("preflight query failed: {0}")]
    Query(#[from] tokio_postgres::Error),
    /// The catalog answered, but not with a value the preflight can read (no rows, a non-numeric
    /// setting). The assertion itself failed, so there is no underlying error to chain — and it is
    /// not a [`PreflightError::Query`]: the query worked.
    #[error("preflight catalog result unusable: {0}")]
    UnusableResult(String),
    /// A configured name cannot be rendered as a SQL identifier. `source` keeps *which* rule it
    /// broke, so a caller can branch on it instead of matching on the message. Also not a
    /// [`PreflightError::Query`]: the rejection happens before any statement reaches the server.
    #[error("invalid SQL identifier: {0}")]
    Ident(#[source] common::sql::IdentError),
}

impl From<PreflightError> for common::Error {
    /// The terminal class of a mismatch is data, never a guess — so this match is exhaustive (no
    /// `_` arm): a new variant must choose its class here, and its exit code in
    /// [`crate::exit::code_for`], instead of silently inheriting the generic ones.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(e: PreflightError) -> Self {
        match &e {
            // A keyless table has its own dedicated terminal class + exit code.
            PreflightError::NoPrimaryKey { schema, table } => common::Error::KeylessTable {
                table: format!("{schema}.{table}"),
            },
            PreflightError::WalLevel { .. }
            | PreflightError::ServerTooOld { .. }
            | PreflightError::UnsupportedGeneratedColumnPublication { .. }
            | PreflightError::NoHeadroom { .. }
            | PreflightError::SlotNameDrift { .. }
            | PreflightError::PublicationMissing { .. }
            | PreflightError::PublicationGap { .. }
            | PreflightError::PublicationCoverage(_)
            | PreflightError::NoReplicationPriv
            | PreflightError::NoSchemaUsagePrivilege { .. }
            | PreflightError::NoTableLockPrivilege { .. }
            | PreflightError::NoTableSelectPrivilege { .. }
            | PreflightError::DdlCaptureMissing { .. }
            | PreflightError::ReloadSignalMissing { .. }
            | PreflightError::ReloadEventMissing { .. }
            | PreflightError::Query(_)
            | PreflightError::UnusableResult(_)
            | PreflightError::Ident(_) => common::Error::Preflight(e.to_string()),
        }
    }
}

/// Connect to the source for the preflight catalog checks. A transport failure (server still coming
/// up) is a *transient* [`common::Error::SourceDb`]; a server-side rejection (auth/config) is a
/// *terminal* [`common::Error::Preflight`]. The `REPLICATION` privilege itself is asserted from the
/// catalog by [`SourcePreflight::assert_server_prereqs`], not inferred from the connect succeeding.
///
/// # Errors
///
/// Returns [`common::Error::SourceDb`] for a transport-level connection failure (transient), or
/// [`common::Error::Preflight`] when the server responds with a terminal authentication/configuration
/// rejection.
pub async fn connect_source(url: &str) -> Result<Client, common::Error> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| {
        if e.as_db_error().is_some() {
            // The server answered and refused (auth / bad config) — retrying won't help.
            common::Error::Preflight(format!("source connection rejected: {e}"))
        } else {
            // Transport-level (refused / timeout / DNS) — the server may still be coming up.
            common::Error::SourceDb(e.to_string())
        }
    })?;
    // Drive the connection in the background; it lives as long as `client` is held.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = ?e, "source connection closed");
        }
    });
    Ok(client)
}

/// The catalog assertions the extractor runs over the source connection before reading WAL.
#[derive(Debug)]
pub struct SourcePreflight<'a> {
    client: &'a Client,
    cfg: &'a ExtractorConfig,
}

impl<'a> SourcePreflight<'a> {
    /// Borrow a connected source client and the config whose expectations will be asserted against
    /// it. Borrows rather than owns: preflight runs once and the caller keeps using both.
    #[must_use]
    pub const fn new(client: &'a Client, cfg: &'a ExtractorConfig) -> Self {
        SourcePreflight { client, cfg }
    }

    /// The DDL-capture tap is installed: the `public.walrus_ddl_audit` table has the extractor's columns, both
    /// event-trigger functions attest the current protocol, and all three event triggers exist with
    /// the required guarded command tags. Missing/stale → terminal (schema changes would silently
    /// drift).
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::DdlCaptureMissing`] when the audit shape or a trigger/tag is absent,
    /// [`PreflightError::Query`] when a catalog query fails, or [`PreflightError::UnusableResult`]
    /// when one answers with no row to read.
    pub async fn assert_ddl_capture(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT (count(*) = 5)::text FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='walrus_ddl_audit'
                   AND column_name IN (
                     'c_columns', 'c_rel_oid', 'c_replica_identity', 'c_ddl_text',
                     'c_table_comment'
                   )",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::DdlCaptureMissing {
                detail: "public.walrus_ddl_audit table/columns absent",
            });
        }
        // Protocol 4 adds structured table/column comments on top of protocol 3's online key and
        // replica-identity guard. Trigger names and command tags cannot distinguish an older 0002.
        // The marker is stored in pg_proc.proconfig on each implementation function: it is durable,
        // and an older CREATE OR REPLACE clears it together with the guarded implementation.
        for function in [
            "public.walrus_intercept_ddl()",
            "public.walrus_guard_publication_ddl()",
        ] {
            let current = self
                .first_text(&format!(
                    "SELECT EXISTS (
                       SELECT 1
                       FROM pg_proc
                       WHERE oid = to_regprocedure('{function}')
                         AND proconfig @> ARRAY['walrus.ddl_capture_protocol=4']::text[]
                     )::text",
                ))
                .await?;
            if current != "true" {
                return Err(PreflightError::DdlCaptureMissing {
                    detail: "DDL guard protocol 4 attestation absent or stale",
                });
            }
        }
        for (name, event, function, tags) in [
            (
                "walrus_intercept_ddl",
                "ddl_command_end",
                "public.walrus_intercept_ddl()",
                "true",
            ),
            (
                "walrus_intercept_drop",
                "sql_drop",
                "public.walrus_intercept_ddl()",
                "true",
            ),
            (
                "walrus_guard_publication_ddl",
                "ddl_command_start",
                "public.walrus_guard_publication_ddl()",
                "evttags @> ARRAY['CREATE PUBLICATION', 'ALTER PUBLICATION', \
                                   'DROP PUBLICATION', 'ALTER SCHEMA']::text[]",
            ),
        ] {
            let present = self
                .first_text(&format!(
                    "SELECT EXISTS (SELECT 1 FROM pg_event_trigger
                                    WHERE evtname='{name}' AND evtevent='{event}'
                                      AND evtfoid = to_regprocedure('{function}')
                                      AND evtenabled IN ('O', 'A')
                                      AND ({tags}))::text",
                ))
                .await?;
            if present != "true" {
                return Err(PreflightError::DdlCaptureMissing {
                    detail: "event trigger missing",
                });
            }
        }
        Ok(())
    }

    /// The role has `REPLICATION`, `wal_level = logical`, `140000 ≤ server_version_num < 180000`, and free
    /// wal-sender headroom. Replication-slot headroom is intentionally deferred until slot
    /// classification: resuming a healthy slot needs no additional slot, and replacing an
    /// invalidated same-name slot frees its own capacity before recreating it.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::NoReplicationPriv`], [`PreflightError::WalLevel`],
    /// [`PreflightError::ServerTooOld`],
    /// [`PreflightError::UnsupportedGeneratedColumnPublication`], or
    /// [`PreflightError::NoHeadroom`] for a terminal prerequisite mismatch; catalog failures return
    /// [`PreflightError::Query`], and a setting that is missing or non-numeric returns
    /// [`PreflightError::UnusableResult`].
    pub async fn assert_server_prereqs(&self) -> Result<ServerInfo, PreflightError> {
        // The role must be able to start a WAL sender (rolreplication, or a superuser).
        let can_replicate = self
            .first_text(
                "SELECT (rolreplication OR rolsuper)::text FROM pg_roles WHERE rolname = current_user",
            )
            .await?;
        if can_replicate != "true" {
            return Err(PreflightError::NoReplicationPriv);
        }
        let wal_level = self.setting("wal_level").await?;
        if wal_level != "logical" {
            return Err(PreflightError::WalLevel { found: wal_level });
        }
        let version_num = self.setting_i32("server_version_num").await?;
        require_supported_server_version(version_num)?;
        self.assert_headroom(
            "wal_senders",
            "max_wal_senders",
            "SELECT count(*) FROM pg_stat_replication",
        )
        .await?;
        Ok(ServerInfo {
            version_num,
            wal_level,
        })
    }

    /// Assert capacity for a genuinely absent configured slot immediately before its creation.
    ///
    /// This is deliberately not part of [`Self::assert_server_prereqs`]. An existing healthy slot
    /// can resume at `max_replication_slots`, while an invalidated same-name slot is dropped before
    /// recreation and therefore replaces itself net-zero. Only authoritative absence needs a free
    /// slot.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::NoHeadroom`] when all replication slots are occupied, or the usual
    /// catalog query/result errors.
    pub async fn assert_slot_creation_headroom(&self) -> Result<(), PreflightError> {
        self.assert_headroom(
            "replication_slots",
            "max_replication_slots",
            "SELECT count(*) FROM pg_replication_slots",
        )
        .await
    }

    /// The reload signal table is installed with its PK. Missing → terminal, because an
    /// absent/unpublished signal table doesn't error at reload time — the echo just silently never
    /// arrives (reload H11). Publication membership is asserted (and auto-added under
    /// `manage_publication`) by [`Self::assert_publication_covers`], which treats `reload_signal`
    /// as the third walrus-internal table; this existence check runs FIRST so a missing table gets
    /// the migration-naming error, not a failed `ALTER PUBLICATION`.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::ReloadSignalMissing`] when the table or primary key is absent,
    /// [`PreflightError::Query`] when a catalog query fails, or [`PreflightError::UnusableResult`]
    /// when one answers with no row to read.
    pub async fn assert_reload_signal(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'public'
                                  AND c.relname = 'walrus_reload_signal'
                                  AND c.relkind = 'r')::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadSignalMissing {
                detail: "public.walrus_reload_signal table absent",
            });
        }
        // The PK doubles as REPLICA IDENTITY DEFAULT — all an insert-only table needs.
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'public'
                                  AND c.relname = 'walrus_reload_signal'
                                  AND i.indisprimary)::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadSignalMissing {
                detail: "public.walrus_reload_signal has no PRIMARY KEY",
            });
        }
        Ok(())
    }

    /// The append-only request/fence table exists with a primary key.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::ReloadEventMissing`] for a missing table/key and the normal query
    /// variants for catalog failures.
    pub async fn assert_reload_event(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'public'
                                  AND c.relname = 'walrus_reload_event'
                                  AND c.relkind = 'r')::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "public.walrus_reload_event table absent",
            });
        }
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'public'
                                  AND c.relname = 'walrus_reload_event'
                                  AND i.indisprimary)::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "public.walrus_reload_event has no PRIMARY KEY",
            });
        }
        if self
            .first_text(
                "SELECT (count(*) = 10)::text
                 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = 'walrus_reload_event'
                   AND column_name IN (
                     'event_id', 'request_id', 'reload_id', 'event_kind', 'scope',
                     'source_schema', 'source_table', 'targets', 'schema_version',
                     'wal_insert_lsn'
                   )",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "public.walrus_reload_event is missing required request/fence columns",
            });
        }
        if self
            .first_text(
                "SELECT EXISTS (
                   SELECT 1
                   FROM pg_trigger t
                   JOIN pg_class c ON c.oid = t.tgrelid
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = 'public' AND c.relname = 'walrus_reload_event'
                     AND t.tgname = 'reload_event_append_only'
                     AND t.tgenabled IN ('O', 'A')
                 )::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "public.walrus_reload_event append-only trigger absent or disabled",
            });
        }
        Ok(())
    }

    /// The publication emits INSERT/UPDATE/DELETE/TRUNCATE, covers the walrus-internal tables, and
    /// applies no row filters, column lists, row-level security, or topology-dependent membership
    /// to any user target (create/extend/fix global flags when `manage_publication`, else a mismatch
    /// is terminal). `pg_publication_tables` expands `FOR ALL TABLES` and partition roots;
    /// [`crate::source_catalog`] additionally inspects the underlying membership and table rows so
    /// an explicit all-current-columns list or a policy-filterable snapshot is still rejected.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::PublicationMissing`] or [`PreflightError::PublicationGap`] when
    /// automatic publication management is disabled, [`PreflightError::Query`] when inspection or
    /// an authorized create/alter statement fails, [`PreflightError::UnusableResult`] when a catalog
    /// answer cannot be read, and [`PreflightError::Ident`] when a configured publication or table
    /// name is not a legal SQL identifier.
    pub async fn assert_publication_covers(&self) -> Result<(), PreflightError> {
        let pubname = &self.cfg.extractor.publication_name;
        // Parse once, up front: both statements below that name the publication as an *identifier*
        // reuse this proven value instead of re-running `SqlIdent::new` per call site (the create
        // path plus one per missing table — up to four validations of the same string).
        let pub_ident = ident(pubname)?;
        let exists = self
            .count(&format!(
                "SELECT count(*) FROM pg_publication WHERE pubname = {}",
                pubname.to_quoted_literal()
            ))
            .await?
            > 0;
        if !exists {
            if self.cfg.extractor.manage_publication {
                self.exec(&format!(
                    "CREATE PUBLICATION {pub_ident} FOR TABLE public.walrus_heartbeat, public.walrus_ddl_audit, \
                     public.walrus_reload_signal, public.walrus_reload_event \
                     WITH (publish_via_partition_root = true)"
                ))
                .await?;
            } else {
                return Err(PreflightError::PublicationMissing {
                    pub_name: pubname.clone(),
                });
            }
        }

        let mut actions = crate::source_catalog::publication_actions(self.client, pubname).await?;
        if actions.is_some_and(|actions| !actions.is_complete())
            && self.cfg.extractor.manage_publication
        {
            self.exec(&format!(
                "ALTER PUBLICATION {pub_ident} SET \
                 (publish = 'insert, update, delete, truncate')"
            ))
            .await?;
            actions = crate::source_catalog::publication_actions(self.client, pubname).await?;
        }
        crate::source_catalog::require_publication_actions(pubname, actions)?;

        let published = self.published_tables(pubname).await?;
        for (schema, table) in [
            ("public", "walrus_heartbeat"),
            ("public", "walrus_ddl_audit"),
            ("public", "walrus_reload_signal"),
            ("public", "walrus_reload_event"),
        ] {
            let id = TableId {
                schema: schema.to_string(),
                table: table.to_string(),
            };
            if !published.contains(&id) {
                if self.cfg.extractor.manage_publication {
                    self.exec(&format!(
                        "ALTER PUBLICATION {pub_ident} ADD TABLE {}.{}",
                        ident(schema)?,
                        ident(table)?
                    ))
                    .await?;
                } else {
                    return Err(PreflightError::PublicationGap {
                        pub_name: pubname.clone(),
                        schema: schema.to_string(),
                        table: table.to_string(),
                    });
                }
            }
        }

        // Re-read after any authorized ADD TABLE above. Validate the exact effective targets the
        // decoder will see, not merely the four internal membership checks.
        let published = self.published_tables(pubname).await?;
        for id in &published {
            let options = crate::source_catalog::publication_target_options(
                self.client,
                pubname,
                &id.schema,
                &id.table,
            )
            .await?;
            crate::source_catalog::require_full_target(pubname, &id.schema, &id.table, options)?;
        }
        Ok(())
    }

    /// Every published user table's schema grants the connected role `USAGE`, and every table grants
    /// whole-table `SELECT` plus a table-level privilege that permits `LOCK TABLE ... IN SHARE
    /// [UPDATE EXCLUSIVE] MODE`. Table grants do not imply schema `USAGE`, and column-only SELECT is
    /// not sufficient for arbitrary full-row COPY. PostgreSQL does not expose a standalone LOCK
    /// grant: UPDATE is the recommended least-broad portable grant; an existing DELETE/TRUNCATE
    /// grant, table ownership, or superuser also works.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::NoSchemaUsagePrivilege`],
    /// [`PreflightError::NoTableSelectPrivilege`], or [`PreflightError::NoTableLockPrivilege`] for
    /// the first uncovered target, with a quoted remediation statement, or the normal
    /// query/identifier errors. A missing schema grant is reported before any table grant gap.
    pub async fn assert_table_lock_privileges(&self) -> Result<(), PreflightError> {
        let sql = format!(
            "SELECT current_user::text AS role_name, pt.schemaname, pt.tablename,
                    pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE')::text
                      AS can_use_schema,
                    pg_catalog.has_table_privilege(current_user, c.oid, 'SELECT')::text
                      AS can_select,
                    (pg_catalog.has_table_privilege(current_user, c.oid, 'UPDATE')
                     OR pg_catalog.has_table_privilege(current_user, c.oid, 'DELETE')
                     OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRUNCATE'))::text
                      AS can_lock
             FROM pg_publication_tables AS pt
             JOIN pg_namespace AS n ON n.nspname = pt.schemaname
             JOIN pg_class AS c ON c.relnamespace = n.oid AND c.relname = pt.tablename
             WHERE pt.pubname = {}
               AND NOT (
                 pt.schemaname = 'public'
                 AND pt.tablename IN (
                   'walrus_heartbeat', 'walrus_ddl_audit',
                   'walrus_reload_signal', 'walrus_reload_event'
                 )
               )
               AND NOT (
                 pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE')
                 AND pg_catalog.has_table_privilege(current_user, c.oid, 'SELECT')
                 AND (
                   pg_catalog.has_table_privilege(current_user, c.oid, 'UPDATE')
                   OR pg_catalog.has_table_privilege(current_user, c.oid, 'DELETE')
                   OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRUNCATE')
                 )
               )
             ORDER BY pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE'),
                      pt.schemaname, pt.tablename
             LIMIT 1",
            self.cfg.extractor.publication_name.to_quoted_literal()
        );
        for message in self.query(&sql).await? {
            let SimpleQueryMessage::Row(row) = message else {
                continue;
            };
            let role = row.get("role_name").unwrap_or_default().to_string();
            let schema = row.get("schemaname").unwrap_or_default().to_string();
            let table = row.get("tablename").unwrap_or_default().to_string();
            let schema_ident = ident(&schema)?;
            let grantee = ident(&role)?;
            if row.get("can_use_schema") != Some("true") {
                return Err(PreflightError::NoSchemaUsagePrivilege {
                    grant_sql: format!("GRANT USAGE ON SCHEMA {schema_ident} TO {grantee}"),
                    role,
                    schema,
                });
            }
            let qualified = format!("{schema_ident}.{}", ident(&table)?);
            if row.get("can_select") != Some("true") {
                return Err(PreflightError::NoTableSelectPrivilege {
                    grant_sql: format!("GRANT SELECT ON TABLE {qualified} TO {grantee}"),
                    role,
                    schema,
                    table,
                });
            }
            debug_assert_eq!(row.get("can_lock"), Some("false"));
            return Err(PreflightError::NoTableLockPrivilege {
                grant_sql: format!("GRANT UPDATE ON TABLE {qualified} TO {grantee}"),
                role,
                schema,
                table,
            });
        }
        Ok(())
    }

    /// Every published **user** table has a valid, ready, live PRIMARY KEY and a
    /// usable replica identity. The real PK is required by the snapshot export's stable keyset cursor;
    /// `NOTHING` is unusable for WAL changes even when that PK exists. Strict → terminal on the first
    /// offender; diagnostic lenient mode returns the complete report without implying that the
    /// reported tables have been excluded from replication.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::NoPrimaryKey`] for the first unusable table in strict mode, or
    /// [`PreflightError::Query`] / [`PreflightError::UnusableResult`] when publication/catalog rows
    /// cannot be read.
    pub async fn assert_tables_have_pk(&self, mode: PkMode) -> Result<PkReport, PreflightError> {
        let sql = format!(
            r#"SELECT pt.schemaname, pt.tablename, c.relreplident::text AS relreplident,
                      (EXISTS (SELECT 1 FROM pg_index i
                               WHERE i.indrelid = c.oid
                                 AND i.indisprimary
                                 AND i.indisvalid
                                 AND i.indisready
                                 AND i.indislive))::text AS has_pk
               FROM pg_publication_tables pt
               JOIN pg_namespace n ON n.nspname = pt.schemaname
               JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
               WHERE pt.pubname = {}
                 AND NOT (
                   pt.schemaname = 'public'
                   AND pt.tablename IN (
                     'walrus_heartbeat', 'walrus_ddl_audit',
                     'walrus_reload_signal', 'walrus_reload_event'
                   )
                 )"#,
            self.cfg.extractor.publication_name.to_quoted_literal()
        );
        let mut report = PkReport::default();
        for msg in self.query(&sql).await? {
            // Only `Row` carries catalog data; the command tag and row description carry none.
            let SimpleQueryMessage::Row(row) = msg else {
                continue;
            };
            let schema = row.get("schemaname").unwrap_or_default().to_string();
            let table = row.get("tablename").unwrap_or_default().to_string();
            let relreplident = row.get("relreplident").unwrap_or_default();
            // `boolean::text` renders as "true"/"false" (not "t"/"f") over the simple protocol.
            let has_pk = row.get("has_pk") == Some("true");
            // The catalog code is parsed into the shared enum rather than matched as raw text, so
            // `identity_is_usable` can be exhaustive. A code outside the catalog's four cannot
            // occur for a real `pg_class` row, and a gate that cannot classify a table's identity
            // must quarantine it rather than wave it through.
            let usable = relreplident
                .parse::<ReplicaIdentity>()
                .is_ok_and(|identity| identity_is_usable(identity, has_pk));
            let id = TableId { schema, table };
            if usable {
                report.ok.push(id);
            } else {
                match mode {
                    PkMode::Strict => {
                        return Err(PreflightError::NoPrimaryKey {
                            schema: id.schema,
                            table: id.table,
                        });
                    }
                    PkMode::Lenient => {
                        tracing::warn!(
                            schema = %id.schema, table = %id.table,
                            "ALERT: published table has no usable replica identity — reported by diagnostic lenient mode, not excluded"
                        );
                        report.quarantined.push(id);
                    }
                }
            }
        }
        Ok(report)
    }

    // ---- helpers ------------------------------------------------------------------------------

    async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>, PreflightError> {
        Ok(self.client.simple_query(sql).await?)
    }

    async fn exec(&self, sql: &str) -> Result<(), PreflightError> {
        self.query(sql).await.map(|_| ())
    }

    /// First column of the first row, as text.
    async fn first_text(&self, sql: &str) -> Result<String, PreflightError> {
        for msg in self.query(sql).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                return Ok(row.get(0).unwrap_or_default().to_string());
            }
        }
        Err(PreflightError::UnusableResult(format!(
            "no rows for `{sql}`"
        )))
    }

    async fn setting(&self, name: &str) -> Result<String, PreflightError> {
        self.first_text(&format!(
            "SELECT current_setting({})",
            name.to_quoted_literal()
        ))
        .await
    }

    async fn setting_i32(&self, name: &str) -> Result<i32, PreflightError> {
        self.setting(name).await?.trim().parse().map_err(|_| {
            PreflightError::UnusableResult(format!("setting {name} is not an integer"))
        })
    }

    async fn count(&self, sql: &str) -> Result<i32, PreflightError> {
        self.first_text(sql)
            .await?
            .trim()
            .parse()
            .map_err(|_| PreflightError::UnusableResult(format!("`{sql}` did not return a count")))
    }

    async fn assert_headroom(
        &self,
        kind: &'static str,
        max_setting: &str,
        used_sql: &str,
    ) -> Result<(), PreflightError> {
        let max = self.setting_i32(max_setting).await?;
        let used = self.count(used_sql).await?;
        if used >= max {
            return Err(PreflightError::NoHeadroom { kind, used, max });
        }
        Ok(())
    }

    async fn published_tables(&self, pubname: &str) -> Result<HashSet<TableId>, PreflightError> {
        let sql = format!(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = {}",
            pubname.to_quoted_literal()
        );
        let mut set = HashSet::new();
        for msg in self.query(&sql).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                set.insert(TableId {
                    schema: row.get("schemaname").unwrap_or_default().to_string(),
                    table: row.get("tablename").unwrap_or_default().to_string(),
                });
            }
        }
        Ok(set)
    }
}

/// Bound source compatibility explicitly. PG18's generated-column publication option changes the
/// relation/tuple contract that the PG14–17 catalog-shape code intentionally mirrors. Rejecting the
/// whole new server family is conservative but remains correct across startup and later online DDL:
/// no unknown publication option can begin emitting a column Walrus omitted from its registry.
const fn require_supported_server_version(found: i32) -> Result<(), PreflightError> {
    if found < 140_000 {
        return Err(PreflightError::ServerTooOld { found });
    }
    if found >= 180_000 {
        return Err(PreflightError::UnsupportedGeneratedColumnPublication { found });
    }
    Ok(())
}

/// Can a published table participate in the unified resumable exporter? Every supported table
/// needs a real primary key; replica identity alone cannot provide the stable keyset cursor.
///
/// Exhaustive (no `_` arm) for the same reason the `From<PreflightError>` conversion above is:
/// which identities the extractor can decode is data, never a guess, so a new [`ReplicaIdentity`]
/// variant must be classified here instead of silently inheriting "usable".
#[deny(clippy::wildcard_enum_match_arm)]
const fn identity_is_usable(identity: ReplicaIdentity, has_pk: bool) -> bool {
    match identity {
        ReplicaIdentity::Default | ReplicaIdentity::Full | ReplicaIdentity::Index => has_pk,
        ReplicaIdentity::Nothing => false,
    }
}

/// Validate a SQL identifier before its [`std::fmt::Display`] implementation quotes it.
fn ident(s: &str) -> Result<SqlIdent, PreflightError> {
    SqlIdent::new(s).map_err(PreflightError::Ident)
}

#[cfg(test)]
#[path = "preflight_test.rs"]
mod tests;
