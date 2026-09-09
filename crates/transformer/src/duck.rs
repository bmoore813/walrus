//! One source table's DuckDB execution connection. Production connections are transient, in-memory
//! DuckDB instances attached to the shared PostgreSQL-catalogued DuckLake; [`TableDb::open`] remains
//! the native-file backend used by hermetic tests and migration verification.

use crate::config::DuckLakeConfig;
use crate::duck_ext::DuckResultExt;
use crate::error::TransformerError;
use crate::plan::TablePlan;
use common::oids::{
    BOOL, BYTEA, DATE, FLOAT4, FLOAT8, INT2, INT4, INT8, JSON, JSONB, NUMERIC, TIMESTAMP,
    TIMESTAMPTZ, UUID,
};
use common::sql::SqlStrExt;
use common::{
    DdlId, EpochNo, Kind, Lsn, ManifestId, PgRelation, Redacted, ReloadId, SchemaVersionNo,
};
use duckdb::OptionalExt as _;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

const EXTENSIONS: [&str; 5] = ["json", "httpfs", "aws", "postgres", "ducklake"];

/// Stable namespace for the per-source-table UUID-v5 DuckLake schema names.
const TABLE_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x4f02_efc2_39b3_4d9d_a860_22af_7291_8cc8);

// DuckDB DDL templates (see `sql/duckdb/templates/`). Fixed structure with `{placeholder}` holes,
// rendered by `.replace(...)`; per-table column lists stay interpolated in Rust (they can't be
// static). `include_str!` paths are source-file-relative (contrast `sqlx::query_file!`).
const CREATE_MIRROR: &str = include_str!("../sql/duckdb/templates/create_mirror.sql");
const ALTER_ADD_APPLIED: &str = include_str!("../sql/duckdb/templates/alter_add_applied.sql");
const CREATE_RAW: &str = include_str!("../sql/duckdb/templates/create_raw.sql");
const CREATE_INGEST_LEDGER: &str = include_str!("../sql/duckdb/templates/create_ingest_ledger.sql");
const CREATE_USER_VIEW: &str = include_str!("../sql/duckdb/templates/create_user_view.sql");
const CREATE_META: &str = include_str!("../sql/duckdb/templates/create_meta.sql");
const CONFIGURE_S3: &str = include_str!("../sql/duckdb/templates/configure_s3.sql");
const APPEND_PARQUET: &str = include_str!("../sql/duckdb/templates/append_parquet.sql");
const WIPE_GENERATION: &str = include_str!("../sql/duckdb/templates/wipe_generation.sql");
const MIGRATE_RAW_REPLAY_FENCE: &str =
    include_str!("../sql/duckdb/templates/migrate_raw_replay_fence.sql");

const CREATE_DUCKLAKE_META: &str = r#"
CREATE TABLE IF NOT EXISTS "_walrus_meta" (k VARCHAR, v BIGINT);
INSERT INTO "_walrus_meta"
SELECT 'schema_version', {schema_version}
WHERE NOT EXISTS (SELECT 1 FROM "_walrus_meta" WHERE k = 'schema_version');
"#;

const CREATE_DUCKLAKE_LEDGER: &str = r#"
CREATE TABLE IF NOT EXISTS "_walrus_ingested_files" (
    s3_uri VARCHAR NOT NULL,
    manifest_id BIGINT NOT NULL,
    object_size BIGINT NOT NULL,
    sha256 VARCHAR NOT NULL,
    stream_group_id BIGINT
);
ALTER TABLE "_walrus_ingested_files" ADD COLUMN IF NOT EXISTS object_size BIGINT;
ALTER TABLE "_walrus_ingested_files" ADD COLUMN IF NOT EXISTS sha256 VARCHAR;
ALTER TABLE "_walrus_ingested_files" ADD COLUMN IF NOT EXISTS stream_group_id BIGINT;
"#;

const CREATE_RELOAD_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS "_walrus_reload_state" (
    reload_id BIGINT NOT NULL,
    shadow_table VARCHAR NOT NULL,
    schema_version BIGINT NOT NULL,
    start_lsn VARCHAR NOT NULL,
    final_lsn VARCHAR NOT NULL,
    publication_nonce VARCHAR NOT NULL,
    raw_appended_lsn VARCHAR NOT NULL,
    transformed_lsn VARCHAR NOT NULL,
    phase VARCHAR NOT NULL
);
ALTER TABLE "_walrus_reload_state"
  ADD COLUMN IF NOT EXISTS publication_nonce VARCHAR
  DEFAULT '00000000-0000-0000-0000-000000000000';
ALTER TABLE "_walrus_reload_state"
  ADD COLUMN IF NOT EXISTS raw_appended_lsn VARCHAR DEFAULT '0000000000000000';
ALTER TABLE "_walrus_reload_state"
  ADD COLUMN IF NOT EXISTS transformed_lsn VARCHAR DEFAULT '0000000000000000';
ALTER TABLE "_walrus_reload_state"
  ADD COLUMN IF NOT EXISTS phase VARCHAR DEFAULT 'building';
"#;

/// A durable, unpublished full-table generation being reconciled.
///
/// There is at most one row per table namespace. It lives beside the canonical mirror so a worker
/// restart can continue appending and transforming the same shadow instead of clearing partial work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReloadBuild {
    pub(crate) reload_id: ReloadId,
    pub(crate) shadow_table: String,
    pub(crate) schema_version: SchemaVersionNo,
    pub(crate) start_lsn: Lsn,
    pub(crate) final_lsn: Lsn,
    pub(crate) publication_nonce: uuid::Uuid,
    pub(crate) raw_appended_lsn: Lsn,
    pub(crate) transformed_lsn: Lsn,
    pub(crate) phase: ReloadPhase,
}

/// Durable local side of the two-database publication protocol. `Published` is retained until the
/// matching control row and canonical checkpoint commit, making the cross-database crash window
/// explicitly recoverable rather than inferred from missing shadow state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReloadPhase {
    Building,
    Published,
}

impl ReloadPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Published => "published",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BeginReload {
    Ready(Box<ReloadBuild>),
    Stale,
}

/// One immutable manifest object prepared for an atomic raw append. `original_uri` is the durable
/// control-plane identity; `verified_uri` is a private local file whose bytes were size/hash checked
/// immediately before this call.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ManifestAppend<'a> {
    pub(crate) manifest_id: ManifestId,
    pub(crate) original_uri: &'a str,
    pub(crate) verified_uri: Option<&'a str>,
    pub(crate) object_size: i64,
    pub(crate) sha256: &'a [u8],
    pub(crate) stream_group_id: Option<i64>,
    pub(crate) schema_version: SchemaVersionNo,
    pub(crate) commit_lsn_override: Option<&'a str>,
    /// Current raw-table destinations corresponding positionally to the verified Parquet columns.
    /// `None` is the identity mapping used by local single-version fixtures. Production supplies an
    /// registry-lineage mapping so historical children in an atomic protocol-v2 group can be
    /// inserted after the destination has reconciled to the group's final additive/drop schema.
    /// Ambiguous common-position name substitutions are rejected before this append path.
    pub(crate) destination_columns: Option<&'a [String]>,
    /// Production-only semantic receipt for the rows inside the verified Parquet. The public
    /// local-fixture helper leaves this absent because it has no control manifest to attest against.
    pub(crate) expectation: Option<ManifestExpectation<'a>>,
}

/// Control-plane facts every row in one immutable staged object must agree with before the object
/// can receive a durable Duck ingest receipt.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ManifestExpectation<'a> {
    pub(crate) row_count: i64,
    pub(crate) epoch: EpochNo,
    pub(crate) source_schema: &'a str,
    pub(crate) source_table: &'a str,
    /// Original Postgres columns, retained separately from the staged emit schema because one
    /// Tier-2 source column may fan out to several physical Parquet columns.
    pub(crate) source_columns: &'a [common::PgColumn],
    pub(crate) schema_version: SchemaVersionNo,
    pub(crate) kind: Kind,
    pub(crate) lsn_start: Lsn,
    pub(crate) lsn_end: Lsn,
    /// Speculative protocol-v2 spill metadata carries a pre-commit placeholder; the manifest's
    /// authoritative commit LSN is stamped during append instead of being checked against the JSON.
    pub(crate) speculative_commit_lsn: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedColumn {
    name: String,
    duckdb_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IngestReceiptState {
    Missing,
    Ingested,
}

/// Owns one table's DuckDB execution connection (mirror `<table>` + CDC log `<table>_raw`).
///
/// The owned handles are `Send`, but their shared references are not: `&T: Send` requires `T: Sync`,
/// while both types contain a `RefCell`. These compile-fail guards pin the boundary that prevents the
/// current borrowed connection API from satisfying `spawn_blocking`'s closure bound.
///
/// ```compile_fail,E0277
/// fn requires_send<T: Send>() {}
/// requires_send::<&'static duckdb::Connection>();
/// ```
///
/// ```compile_fail,E0277
/// fn requires_send<T: Send>() {}
/// requires_send::<&'static transformer::duck::TableDb>();
/// ```
#[derive(Debug)]
pub struct TableDb {
    conn: duckdb::Connection,
    backend: Backend,
    /// Expected staged-object schemas by `schema_version`. Production seeds each entry from that
    /// version's immutable registry plan; local fixture helpers can derive the current version from
    /// the destination raw table. Every Parquet is still independently DESCRIBEd and compared with
    /// this cached expectation; trusting the first file would let a later same-version object add a
    /// silently ignored column.
    /// `RefCell` provides interior mutability behind `&self`. `TableDb` is `Send + !Sync`:
    /// duckdb-rs declares `Connection: Send`, but the connection's `RefCell<InnerConnection>` and
    /// this cache's `RefCell` prevent shared access. That `!Sync` makes a future holding `&TableCtx`
    /// non-`Send`, hence one apply worker per `.duckdb` file on a `LocalSet`. Those tasks share one
    /// driver thread, so a long DuckDB call can delay sibling tables.
    /// `Arc<[StagedColumn]>` keeps reads to one indirection while preserving `TableDb: Send`. The `Rc`
    /// this `LocalSet`-confined cache would otherwise invite is declined: `Rc` is `!Send`, so it
    /// would break `assert_send::<TableDb>()` below and foreclose the owned-move redesign that
    /// note leaves open.
    parquet_cols: RefCell<HashMap<SchemaVersionNo, Arc<[StagedColumn]>>>,
    /// `true` only for a database created by a pre-ledger Walrus release. Such a raw table keeps
    /// its row-level primary key until Phase A has drained every possibly replayed manifest, then a
    /// one-time transactional CTAS removes it. Fresh databases never pay the per-row index cost.
    legacy_raw_replay_pk: Cell<bool>,
}

#[derive(Debug, Clone)]
enum Backend {
    Native,
    DuckLake(Box<DuckLakeTable>),
}

/// Names required to bridge the existing per-table SQL into one shared DuckLake catalog.
///
/// Every connection sets its default schema to `internal_schema`, so the transform can continue to
/// use the short `<table>`, `<table>_raw`, `_walrus_meta`, and `_walrus_ingested_files` names. Only
/// the compatibility view is published outside this schema.
#[derive(Debug, Clone)]
struct DuckLakeTable {
    attach_name: String,
    internal_schema: String,
    source_schema: String,
    source_table: String,
}

impl TableDb {
    /// Open (or create) the file read-write, taking DuckDB's file lock. A stale lock behind an expired
    /// lease has already been reclaimed by the caller; a *live* owner would make this fail.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if DuckDB cannot open the file or acquire its writer lock.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TransformerError> {
        let path = path.as_ref();
        let conn =
            duckdb::Connection::open(path).duck_with(|| format!("open {}", path.display()))?;
        Ok(TableDb {
            conn,
            backend: Backend::Native,
            parquet_cols: RefCell::new(HashMap::new()),
            legacy_raw_replay_pk: Cell::new(false),
        })
    }

    /// Open a transient DuckDB connection, load the pinned extensions, attach the shared DuckLake,
    /// and select this source table's isolated internal schema.
    ///
    /// The PostgreSQL URI is first stored in a temporary DuckDB secret and is never interpolated into
    /// the `ATTACH` path or an operation label, preventing driver errors from echoing credentials.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if extensions, credentials, the DuckLake attachment, or the
    /// table's namespace cannot be configured.
    pub fn open_ducklake(
        cfg: &DuckLakeConfig,
        _epoch: EpochNo,
        source_schema: &str,
        source_table: &str,
        s3: &S3Access,
    ) -> Result<Self, TransformerError> {
        let conn = attach_ducklake(cfg, s3, false)?;
        let attach = ident(&cfg.attach_name)?;

        let internal_schema = internal_schema(source_schema, source_table);
        let internal = ident(&internal_schema)?;
        let public = ident(source_schema)?;
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {attach}.{internal};\n\
             CREATE SCHEMA IF NOT EXISTS {attach}.{public};\n\
             USE {attach}.{internal};"
        ))
        .duck_with(|| format!("select DuckLake namespace for {source_schema}.{source_table}"))?;

        Ok(Self {
            conn,
            backend: Backend::DuckLake(Box::new(DuckLakeTable {
                attach_name: cfg.attach_name.clone(),
                internal_schema,
                source_schema: source_schema.to_string(),
                source_table: source_table.to_string(),
            })),
            parquet_cols: RefCell::new(HashMap::new()),
            legacy_raw_replay_pk: Cell::new(false),
        })
    }

    /// Whether this connection writes to the shared DuckLake rather than a native test database.
    #[must_use]
    pub const fn is_ducklake(&self) -> bool {
        matches!(self.backend, Backend::DuckLake(_))
    }

    /// `CREATE TABLE IF NOT EXISTS` for BOTH the mirror `<table>` and the CDC log `<table>_raw`
    /// (a heap; file-level replay is fenced by `_walrus_ingested_files`), the user-facing
    /// `<table>_current` view, and a `_walrus_meta` row seeding this table's `schema_version`
    /// (the DDL-reconcile watermark).
    /// The seed is `ON CONFLICT DO NOTHING`, so an EXISTING `.duckdb` keeps its persisted, already-
    /// reconciled version across restarts — the additive DDL applier ([`crate::ddl`]) advances it.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::ManifestInvariant`] if a column cannot be emitted bare, or
    /// [`TransformerError::Duck`] if DuckDB cannot create or reconcile the planned tables and view.
    pub fn ensure_tables(
        &self,
        rel: &PgRelation,
        schema_version: SchemaVersionNo,
    ) -> Result<(), TransformerError> {
        self.ensure_tables_planned(&crate::plan::TablePlan::tier1(rel)?, schema_version)
    }

    /// As [`TableDb::ensure_tables`], but from a full [`TablePlan`] — the mirror carries the recombined
    /// target types and `<table>_raw` the verbatim emit columns (Tier-2 decomposition). The
    /// Tier-1 plan produces exactly the scalar shape `ensure_tables` always built.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if any mirror, raw-table, view, or metadata DDL statement fails.
    pub(crate) fn ensure_tables_planned(
        &self,
        plan: &TablePlan,
        schema_version: SchemaVersionNo,
    ) -> Result<(), TransformerError> {
        let generation = self.generation_sql(plan);
        let table = &plan.table;
        // The per-table DDL-reconcile watermark. Seeded once; the applier advances it.
        let meta = if self.is_ducklake() {
            CREATE_DUCKLAKE_META.replace("{schema_version}", &schema_version.to_string())
        } else {
            CREATE_META.replace("{schema_version}", &schema_version.to_string())
        };
        let ledger = if self.is_ducklake() {
            CREATE_DUCKLAKE_LEDGER
        } else {
            CREATE_INGEST_LEDGER
        };

        self.conn
            .execute_batch(&format!(
                "{generation} {meta} {ledger} {CREATE_RELOAD_STATE}"
            ))
            .duck_with(|| format!("ensure tables for {table}"))?;
        // `CREATE TABLE IF NOT EXISTS` intentionally leaves an upgraded raw table's old primary
        // key intact. Phase A uses it as a compatibility fence until all potentially pre-appended
        // manifests have drained, then calls `migrate_legacy_replay_fence` exactly once.
        self.legacy_raw_replay_pk
            .set(!self.is_ducklake() && self.raw_has_primary_key(table)?);
        self.publish_current_view()?;
        Ok(())
    }

    /// Render one physical mirror/raw/view generation. Reload shadows share the canonical metadata
    /// and ingest ledger, so neither is part of this fragment.
    fn generation_sql(&self, plan: &TablePlan) -> String {
        let cols: Vec<String> = plan
            .mirror_cols
            .iter()
            .map(|c| format!("{} {}", c.name, c.duckdb_type))
            .collect();
        let keys: Vec<String> = plan
            .mirror_cols
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.clone())
            .collect();
        let raw_cols: Vec<String> = plan
            .raw_cols
            .iter()
            .map(|c| format!("{} {}", c.name, c.duckdb_type))
            .collect();
        let table = &plan.table;

        // The mirror: current row per key, plus two HIDDEN guard columns (§7, ⚠ extends architecture.md)
        // recording the `(commit_lsn, lsn)` tuple that last shaped each row — the per-PK max-applied guard
        // that makes a stale straddle winner a no-op. Seeded from the low sentinel `0/0`; a pre-3.7 mirror
        // gains them via `ALTER … IF NOT EXISTS`, which back-fills existing rows with that sentinel (a
        // too-low seed just means the first real event wins — which is correct).
        let primary_key = if self.is_ducklake() || keys.is_empty() {
            String::new()
        } else {
            format!(", PRIMARY KEY ({})", keys.join(", "))
        };
        let mirror = CREATE_MIRROR
            .replace("{table}", table)
            .replace("{cols}", &cols.join(", "))
            .replace("{primary_key}", &primary_key);
        // Idempotent back-fill for a mirror created before the applied-LSN guard (compose resume).
        let applied_cols = ALTER_ADD_APPLIED.replace("{table}", table);
        // The user-facing projection: the mirror WITHOUT the internal guard columns (DoD §7 "hidden from
        // user projections"). Users read `<table>_current`; `_applied_*` never leak. Recreated by the DDL
        // applier after any structural change (a `SELECT *` view binds its columns at creation time).
        let user_view = user_view_sql(table);
        // The CDC log: every change verbatim (the emit columns), with the intact
        // `walrus_extractor_meta` JSON plus four promoted columns. It is deliberately a HEAP: a
        // per-row composite primary key made each append build an ART index even though replay is a
        // per-file event. `_walrus_ingested_files` is the much smaller idempotency fence.
        let raw = CREATE_RAW
            .replace("{table}", table)
            .replace("{raw_cols}", &raw_cols.join(", "));

        format!("{mirror} {applied_cols} {raw} {user_view}")
    }

    /// Configure DuckDB's bundled httpfs for the S3/MinIO staging bucket — **once per connection**, so
    /// `read_parquet('s3://…')` then needs no per-call credentials. For MinIO the endpoint is
    /// `host:port` (no scheme), path-style, TLS off.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] when DuckDB rejects the httpfs/S3 configuration statements.
    pub fn configure_s3(&self, s3: &S3Access) -> Result<(), TransformerError> {
        if self.is_ducklake() {
            // `open_ducklake` creates a modern secret before ATTACH so both staging reads and
            // DuckLake-owned writes use the same refreshable credential source.
            return Ok(());
        }
        let esc = common::sql::sql_literal;
        let use_ssl = if s3.use_ssl { "true" } else { "false" };
        let sql = CONFIGURE_S3
            .replace("{region}", &esc(&s3.region))
            .replace("{endpoint}", &esc(&s3.endpoint))
            .replace("{use_ssl}", use_ssl)
            .replace("{access_key}", &esc(&s3.access_key_id))
            .replace("{secret_key}", &esc(s3.secret_access_key.expose()));
        self.conn.execute_batch(&sql).duck("configure S3")
    }

    /// Inspect the durable ingest receipt for a manifest object without reopening the object.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Ident`] if the Parquet schema contains a column name that is not safe
    /// as a bare snake_case SQL column, or [`TransformerError::Duck`] if the schema cannot be inspected
    /// or its rows cannot be appended into the raw table.
    pub(crate) fn ingest_receipt_state(
        &self,
        file: &ManifestAppend<'_>,
    ) -> Result<IngestReceiptState, TransformerError> {
        ingest_receipt_state_on(&self.conn, file)
    }

    /// Append one singleton or one complete protocol-v2 per-table stream group. Every raw INSERT
    /// and every fingerprint-bearing ledger receipt commits in one Duck transaction. Exact receipt
    /// replays are no-ops; partial groups and identity/fingerprint collisions are terminal.
    pub(crate) fn append_manifest_unit(
        &self,
        table: &str,
        files: &[ManifestAppend<'_>],
    ) -> Result<u64, TransformerError> {
        let Some(first) = files.first() else {
            return Err(TransformerError::ManifestInvariant {
                message: "cannot append an empty manifest unit".to_string(),
            });
        };
        if files.len() > 1 && first.stream_group_id.is_none()
            || files
                .iter()
                .any(|file| file.stream_group_id != first.stream_group_id)
        {
            return Err(TransformerError::ManifestInvariant {
                message: "an atomic append unit mixed stream groups or singleton files".to_string(),
            });
        }
        let on_conflict = if self.legacy_raw_replay_pk.get() {
            " ON CONFLICT DO NOTHING"
        } else {
            ""
        };
        self.in_txn("append manifest unit", |conn| {
            let states = files
                .iter()
                .map(|file| ingest_receipt_state_on(conn, file))
                .collect::<Result<Vec<_>, _>>()?;
            if states
                .iter()
                .all(|state| *state == IngestReceiptState::Ingested)
            {
                return Ok(0);
            }
            if states.contains(&IngestReceiptState::Ingested) {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "stream group {:?} has a partial durable ingest receipt",
                        first.stream_group_id
                    ),
                });
            }

            let mut appended = 0_u64;
            for file in files {
                let verified_uri =
                    file.verified_uri
                        .ok_or_else(|| TransformerError::ManifestInvariant {
                            message: format!(
                                "manifest {} is missing its verified local object",
                                file.manifest_id
                            ),
                        })?;
                let uri = common::sql::sql_literal(verified_uri);
                let file_cols =
                    self.columns_for(table, &uri, file.original_uri, file.schema_version)?;
                let destination_cols = file
                    .destination_columns
                    .unwrap_or_else(|| file_cols.as_ref());
                if destination_cols.len() != file_cols.len() {
                    return Err(TransformerError::ManifestInvariant {
                        message: format!(
                            "manifest {} maps {} staged columns onto {} raw destinations",
                            file.manifest_id,
                            file_cols.len(),
                            destination_cols.len()
                        ),
                    });
                }
                if let Some(expectation) = file.expectation {
                    validate_manifest_rows(conn, &uri, file.original_uri, expectation)?;
                }
                let bare_columns = |columns: &[String]| {
                    for column in columns {
                        common::sql::SqlColumn::new(column).map_err(|source| {
                            TransformerError::Ident {
                                uri: file.original_uri.to_string(),
                                source,
                            }
                        })?;
                    }
                    Ok::<_, TransformerError>(columns.join(", "))
                };
                let source_columns = bare_columns(&file_cols)?;
                let destination_columns = bare_columns(destination_cols)?;
                let mut unique_destinations = HashSet::with_capacity(destination_cols.len());
                if destination_cols
                    .iter()
                    .any(|column| !unique_destinations.insert(column.as_str()))
                {
                    return Err(TransformerError::ManifestInvariant {
                        message: format!(
                            "manifest {} maps multiple staged columns onto one raw destination",
                            file.manifest_id
                        ),
                    });
                }
                let commit_lsn_expr = match file.commit_lsn_override {
                    Some(lsn) => lsn.to_quoted_literal(),
                    None => {
                        "json_extract_string(walrus_extractor_meta, '$.commit_lsn')".to_string()
                    }
                };
                let sql = APPEND_PARQUET
                    .replace("{table}", table)
                    .replace("{destination_columns}", &destination_columns)
                    .replace("{source_columns}", &source_columns)
                    .replace("{commit_lsn_expr}", &commit_lsn_expr)
                    .replace("{uri}", &uri)
                    .replace("{on_conflict}", on_conflict);
                let n =
                    conn.execute(&sql, [])
                        .map_err(|source| TransformerError::ObjectIntegrity {
                            uri: file.original_uri.to_string(),
                            reason: format!(
                                "verified Parquet could not be appended to {table}_raw: {source}"
                            ),
                        })?;
                if !self.legacy_raw_replay_pk.get()
                    && let Some(expectation) = file.expectation
                    && i64::try_from(n).ok() != Some(expectation.row_count)
                {
                    return Err(TransformerError::ObjectIntegrity {
                        uri: file.original_uri.to_string(),
                        reason: format!(
                            "manifest receipt records {} rows but DuckDB appended {n}",
                            expectation.row_count
                        ),
                    });
                }
                appended = appended.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
                conn.execute(
                    "INSERT INTO \"_walrus_ingested_files\" \
                     (s3_uri, manifest_id, object_size, sha256, stream_group_id) \
                     VALUES (?, ?, ?, ?, ?)",
                    duckdb::params![
                        file.original_uri,
                        file.manifest_id.0,
                        file.object_size,
                        hex::encode(file.sha256),
                        file.stream_group_id,
                    ],
                )
                .duck_with(|| format!("record ingest receipt for {}", file.original_uri))?;
            }
            Ok(appended)
        })
    }

    /// Compatibility helper for local tests/benchmarks. Production Phase A uses
    /// `append_manifest_unit` with control-provided fingerprints and verified temp files.
    ///
    /// # Errors
    ///
    /// Returns an error when the local fixture cannot be read, its schema is invalid, its receipt
    /// conflicts with an existing identity, or its rows and receipt cannot commit atomically.
    #[expect(
        clippy::disallowed_methods,
        reason = "this synchronous fixture/benchmark helper runs beside blocking DuckDB work; production downloads asynchronously before this layer"
    )]
    pub fn append_parquet(
        &self,
        table: &str,
        manifest_id: ManifestId,
        uri: &str,
        schema_version: SchemaVersionNo,
        commit_lsn_override: Option<&str>,
    ) -> Result<u64, TransformerError> {
        let existing: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT s3_uri, manifest_id FROM \"_walrus_ingested_files\" \
                 WHERE s3_uri = ? OR manifest_id = ? LIMIT 1",
                duckdb::params![uri, manifest_id.0],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .duck("read local-fixture ingest receipt")?;
        if let Some((stored_uri, stored_id)) = existing {
            if stored_uri == uri && stored_id == manifest_id.0 {
                return Ok(0);
            }
            return Err(TransformerError::ManifestInvariant {
                message: format!(
                    "manifest {manifest_id} / URI {uri} conflicts with a local-fixture receipt"
                ),
            });
        }
        use std::io::Read as _;

        let mut fixture = std::fs::File::open(uri).map_err(|source| TransformerError::File {
            op: "read local Parquet fixture",
            path: uri.to_string(),
            source,
        })?;
        let mut bytes = Vec::new();
        fixture
            .read_to_end(&mut bytes)
            .map_err(|source| TransformerError::File {
                op: "read local Parquet fixture",
                path: uri.to_string(),
                source,
            })?;
        use sha2::Digest as _;
        let digest = sha2::Sha256::digest(&bytes);
        let object_size =
            i64::try_from(bytes.len()).map_err(|_| TransformerError::ManifestInvariant {
                message: format!("local fixture {uri} is larger than bigint"),
            })?;
        self.append_manifest_unit(
            table,
            &[ManifestAppend {
                manifest_id,
                original_uri: uri,
                verified_uri: Some(uri),
                object_size,
                sha256: digest.as_slice(),
                stream_group_id: None,
                schema_version,
                commit_lsn_override,
                destination_columns: None,
                expectation: None,
            }],
        )
    }

    /// Whether this upgraded database still carries the old per-row replay index.
    #[must_use]
    pub(crate) const fn has_legacy_replay_fence(&self) -> bool {
        self.legacy_raw_replay_pk.get()
    }

    /// Replace an upgraded raw table with an identical heap after Phase A has proved there are no
    /// pending manifests that might have been appended by the old implementation. The CTAS
    /// replacement is transactional, so a crash leaves either the indexed old table or the complete
    /// heap, never a partial copy. Returns whether a migration ran.
    pub(crate) fn migrate_legacy_replay_fence(
        &self,
        table: &str,
    ) -> Result<bool, TransformerError> {
        if !self.legacy_raw_replay_pk.get() {
            return Ok(false);
        }
        if !self.raw_has_primary_key(table)? {
            self.legacy_raw_replay_pk.set(false);
            return Ok(false);
        }
        self.in_txn("migrate raw replay fence", |conn| {
            conn.execute_batch(&MIGRATE_RAW_REPLAY_FENCE.replace("{table}", table))
                .duck_with(|| format!("remove legacy replay primary key from {table}_raw"))
        })?;
        self.legacy_raw_replay_pk.set(false);
        Ok(true)
    }

    fn raw_has_primary_key(&self, table: &str) -> Result<bool, TransformerError> {
        self.conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM duckdb_constraints() \
                 WHERE table_name = ? AND constraint_type = 'PRIMARY KEY')",
                [format!("{table}_raw")],
                |row| row.get(0),
            )
            .duck_with(|| format!("inspect replay constraint on {table}_raw"))
    }

    /// Verify one Parquet's complete column name/type sequence against its registry-bound staged
    /// schema. The expected sequence is cached by schema version, but `uri` is DESCRIBEd every call.
    fn columns_for(
        &self,
        table: &str,
        uri: &str,
        original_uri: &str,
        schema_version: SchemaVersionNo,
    ) -> Result<Arc<[String]>, TransformerError> {
        // Release the shared borrow before a miss runs DESCRIBE on the destination table.
        let cached = { self.parquet_cols.borrow().get(&schema_version).cloned() };
        let expected = match cached {
            Some(expected) => expected,
            None => {
                let expected: Arc<[StagedColumn]> = self.expected_staged_columns(table)?.into();
                self.parquet_cols
                    .borrow_mut()
                    .insert(schema_version, Arc::clone(&expected));
                expected
            }
        };
        let actual =
            self.parquet_columns(uri)
                .map_err(|error| TransformerError::ObjectIntegrity {
                    uri: original_uri.to_string(),
                    reason: format!("verified object is not a readable Parquet file: {error}"),
                })?;
        if actual.as_slice() != expected.as_ref() {
            return Err(TransformerError::ObjectIntegrity {
                uri: original_uri.to_string(),
                reason: format!(
                    "Parquet schema {:?} does not equal expected {:?}",
                    actual, expected
                ),
            });
        }
        Ok(actual
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>()
            .into())
    }

    /// Cache the exact staged schema represented by one immutable registry plan.
    ///
    /// A protocol-v2 stream group can contain files on both sides of one or more transactional DDL
    /// boundaries. Phase A must reconcile the destination to the group's newest version before its
    /// atomic append, so the current raw table is an additive superset and cannot authoritatively
    /// describe an older child. The versioned registry plan can: it preserves both source order and
    /// Tier-2 emit expansion. Types are canonicalized through DuckDB before comparison so aliases
    /// such as `REAL` and `FLOAT` do not manufacture a schema mismatch.
    pub(crate) fn cache_staged_schema(
        &self,
        schema_version: SchemaVersionNo,
        plan: &TablePlan,
    ) -> Result<(), TransformerError> {
        const RESERVED: [&str; 5] = [
            "walrus_extractor_meta",
            "_walrus_op",
            "_walrus_commit_lsn",
            "_walrus_lsn",
            "_walrus_extractor_processed_at",
        ];
        let mut staged_names = HashSet::with_capacity(plan.raw_cols.len());
        for column in &plan.raw_cols {
            if RESERVED.contains(&column.name.as_str())
                || !staged_names.insert(column.name.as_str())
            {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "registry schema {schema_version} contains duplicate or reserved staged column {:?}",
                        column.name
                    ),
                });
            }
        }

        let mut projections = plan
            .raw_cols
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "CAST(NULL AS {}) AS _walrus_schema_{index}",
                    column.duckdb_type
                )
            })
            .collect::<Vec<_>>();
        projections.push("CAST(NULL AS VARCHAR) AS _walrus_schema_meta".to_string());
        let mut stmt = self
            .conn
            .prepare(&format!("DESCRIBE SELECT {}", projections.join(", ")))
            .duck_with(|| format!("canonicalize registry schema {schema_version}"))?;
        let canonical_types = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .duck_with(|| format!("canonicalize registry schema {schema_version}"))?
            .collect::<Result<Vec<_>, _>>()
            .duck_with(|| format!("canonicalize registry schema {schema_version}"))?;
        if canonical_types.len() != plan.raw_cols.len() + 1 {
            return Err(TransformerError::ManifestInvariant {
                message: format!(
                    "registry schema {schema_version} canonicalized to an unexpected column count"
                ),
            });
        }
        let mut expected = plan
            .raw_cols
            .iter()
            .zip(&canonical_types)
            .map(|(column, duckdb_type)| StagedColumn {
                name: column.name.clone(),
                duckdb_type: duckdb_type.clone(),
            })
            .collect::<Vec<_>>();
        expected.push(StagedColumn {
            name: "walrus_extractor_meta".to_string(),
            duckdb_type: canonical_types.last().cloned().ok_or_else(|| {
                TransformerError::ManifestInvariant {
                    message: format!(
                        "registry schema {schema_version} lost its staged metadata column"
                    ),
                }
            })?,
        });
        let expected: Arc<[StagedColumn]> = expected.into();

        match self.parquet_cols.borrow_mut().entry(schema_version) {
            Entry::Occupied(occupied) => {
                if occupied.get().as_ref() != expected.as_ref() {
                    return Err(TransformerError::ManifestInvariant {
                        message: format!(
                            "registry schema {schema_version} conflicts with its cached staged schema"
                        ),
                    });
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(expected);
            }
        }
        Ok(())
    }

    /// Number of distinct `schema_version`s whose column list is cached; exposed only to tests.
    #[cfg(test)]
    pub fn cached_schema_versions(&self) -> usize {
        self.parquet_cols.borrow().len()
    }

    /// The exact columns of a staged Parquet file, in file order (source columns + metadata).
    fn parquet_columns(&self, uri: &str) -> Result<Vec<StagedColumn>, TransformerError> {
        let mut stmt = self
            .conn
            .prepare(&format!("DESCRIBE SELECT * FROM read_parquet('{uri}')"))
            .duck_with(|| format!("describe {uri}"))?;
        let cols = stmt
            .query_map([], |row| {
                Ok(StagedColumn {
                    name: row.get(0)?,
                    duckdb_type: row.get(1)?,
                })
            })
            .duck_with(|| format!("describe {uri}"))?
            .collect::<Result<Vec<_>, _>>()
            .duck_with(|| format!("describe {uri}"))?;
        Ok(cols)
    }

    fn expected_staged_columns(&self, table: &str) -> Result<Vec<StagedColumn>, TransformerError> {
        let raw = format!("{table}_raw");
        let mut stmt = self
            .conn
            .prepare(&format!("DESCRIBE SELECT * FROM \"{raw}\""))
            .duck_with(|| format!("describe expected staged schema for {raw}"))?;
        let mut columns = stmt
            .query_map([], |row| {
                Ok(StagedColumn {
                    name: row.get(0)?,
                    duckdb_type: row.get(1)?,
                })
            })
            .duck_with(|| format!("describe expected staged schema for {raw}"))?
            .collect::<Result<Vec<_>, _>>()
            .duck_with(|| format!("describe expected staged schema for {raw}"))?;
        const PROMOTED: [&str; 4] = [
            "_walrus_op",
            "_walrus_commit_lsn",
            "_walrus_lsn",
            "_walrus_extractor_processed_at",
        ];
        if columns.len() < PROMOTED.len() + 1
            || !columns
                .iter()
                .rev()
                .take(PROMOTED.len())
                .map(|column| column.name.as_str())
                .eq(PROMOTED.iter().rev().copied())
        {
            return Err(TransformerError::ManifestInvariant {
                message: format!("{raw} does not end with the four promoted Walrus columns"),
            });
        }
        columns.truncate(columns.len() - PROMOTED.len());
        if columns.last().map(|column| column.name.as_str()) != Some("walrus_extractor_meta") {
            return Err(TransformerError::ManifestInvariant {
                message: format!("{raw} has no trailing walrus_extractor_meta staged column"),
            });
        }
        Ok(columns)
    }

    /// The `.duckdb` connection used by transform, compaction, and schema-reconciliation callers.
    ///
    /// Handing out the borrow does nothing on its own — the caller still has to run something
    /// through it — so a discarded call is a no-op. `clippy::must_use_candidate` cannot say so: the
    /// `parquet_cols` `RefCell` behind `&self` reads to that lint as a mutable — therefore
    /// side-effecting — argument, so the attribute is spelled out.
    #[must_use]
    pub const fn conn(&self) -> &duckdb::Connection {
        &self.conn
    }

    /// Run `f` inside one DuckDB transaction. Commits on success and attempts a best-effort rollback
    /// on error, so callers cannot forget the unhappy path. `what` names the transaction in the
    /// begin/commit error contexts and rollback warning.
    ///
    /// Bounded [`FnOnce`] because the body runs exactly once and may consume what it captured. This
    /// is the weakest bound that works, accepting move-consuming closures too. There is no `Send`
    /// bound: the body runs inline on the owning worker's `LocalSet` and never crosses a task or
    /// thread boundary.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if beginning or committing the transaction fails, or returns
    /// the body's error unchanged after attempting to roll back.
    pub fn in_txn<T>(
        &self,
        what: &str,
        f: impl FnOnce(&duckdb::Connection) -> Result<T, TransformerError>,
    ) -> Result<T, TransformerError> {
        self.conn
            .execute_batch("BEGIN TRANSACTION;")
            .duck_with(|| format!("begin {what} txn"))?;
        match f(&self.conn) {
            Ok(value) => {
                self.conn
                    .execute_batch("COMMIT;")
                    .duck_with(|| format!("commit {what} txn"))?;
                Ok(value)
            }
            Err(error) => {
                if let Err(rollback) = self.conn.execute_batch("ROLLBACK;") {
                    tracing::warn!(
                        error = ?rollback,
                        transaction = what,
                        "transaction rollback failed; connection may be wedged"
                    );
                }
                Err(error)
            }
        }
    }

    /// This table's currently-reconciled `schema_version` (the `_walrus_meta` watermark). Persisted in
    /// the `.duckdb` file, so a restart resumes at the exact version its columns are already at.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the metadata watermark cannot be read.
    pub fn schema_version(&self) -> Result<SchemaVersionNo, TransformerError> {
        let version = self
            .conn
            .query_row(
                "SELECT v FROM \"_walrus_meta\" WHERE k = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .duck("read schema_version")?;
        Ok(SchemaVersionNo(version))
    }

    /// The persisted watermark read **before** the seeding `ensure_tables*`, or `None` when this
    /// `.duckdb` has none yet (a brand-new file has no `_walrus_meta` at all). Only bootstrap needs
    /// this: it asks what version a file is already at while deciding whether to reconcile it
    /// forward, so it meets both a fresh and a resumed file. Every later caller runs after the seed
    /// and uses the probe-free [`TableDb::schema_version`], which Phase A reads per file.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the metadata table probe or the watermark read fails — a
    /// real DuckDB fault, kept distinct from the absent watermark `Ok(None)` reports.
    pub fn stored_schema_version(&self) -> Result<Option<SchemaVersionNo>, TransformerError> {
        // A brand-new file has no `_walrus_meta` yet — probe first, exactly as `built_epoch` does.
        if !self.has_metadata_table()? {
            return Ok(None);
        }
        // `max(v)` yields one row with NULL (→ `None`) when the key is absent, as in `built_epoch`.
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .duck("read stored schema_version")?;
        Ok(v.map(SchemaVersionNo))
    }

    /// Advance the reconcile watermark after the additive DDL for `v` has been applied to both tables.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if updating the persisted schema watermark fails.
    pub fn set_schema_version(&self, version: SchemaVersionNo) -> Result<(), TransformerError> {
        self.conn
            .execute(
                "UPDATE \"_walrus_meta\" SET v = ? WHERE k = 'schema_version'",
                duckdb::params![version.0],
            )
            .duck("set schema_version")?;
        Ok(())
    }

    /// Highest durable DDL row whose complete comment snapshot has been applied to the mirror.
    /// `None` also covers mirrors created before comment replication was introduced.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the local metadata watermark cannot be read.
    pub(crate) fn applied_comment_ddl_id(&self) -> Result<Option<DdlId>, TransformerError> {
        let value: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'comment_ddl_id'",
                [],
                |row| row.get(0),
            )
            .duck("read comment DDL watermark")?;
        Ok(value.map(DdlId))
    }

    /// Invalidate the comment watermark after an operation that physically replaces the mirror.
    /// The next metadata reconcile reapplies the authoritative registry/DDL snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the local metadata watermark cannot be removed.
    pub(crate) fn clear_comment_ddl_id(&self) -> Result<(), TransformerError> {
        self.conn
            .execute(
                "DELETE FROM \"_walrus_meta\" WHERE k = 'comment_ddl_id'",
                [],
            )
            .duck("clear comment DDL watermark")?;
        Ok(())
    }

    /// The **epoch (generation)** this `.duckdb` was last built for (`_walrus_meta['epoch']`), or `None`
    /// if never stamped (a brand-new file — no `_walrus_meta` yet — or a pre-4.6 file). A value below the
    /// control-plane epoch means the mirror + raw hold a **retired generation** (total-restart, §1.8) and
    /// must be wiped before the new generation's full-table reconciliation publishes.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the metadata table probe or epoch read fails.
    pub fn built_epoch(&self) -> Result<Option<EpochNo>, TransformerError> {
        // A brand-new file has no `_walrus_meta` yet — probe first so this never errors on it.
        if !self.has_metadata_table()? {
            return Ok(None);
        }
        // `max(v)` yields one row with NULL (→ `None`) when the 'epoch' key is absent (pre-4.6 file).
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'epoch'",
                [],
                |r| r.get(0),
            )
            .duck("read built epoch")?;
        Ok(v.map(EpochNo))
    }

    /// Stamp the generation this `.duckdb` is now built for (`_walrus_meta['epoch']`). Upserts, so it both
    /// records a fresh file's epoch and re-stamps a rebuilt file's new epoch.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the epoch upsert fails.
    pub fn set_built_epoch(&self, epoch: EpochNo) -> Result<(), TransformerError> {
        if self.is_ducklake() {
            return self.replace_meta("epoch", epoch.0);
        }
        self.conn
            .execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('epoch', ?) \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                duckdb::params![epoch.0],
            )
            .duck("set built epoch")?;
        Ok(())
    }

    /// The highest `reload_id` this `.duckdb` has rebuilt for — the H8 idempotency latch.
    /// `None` when the latch was never set, so the first attempt of any kind triggers a rebuild; a
    /// caller can no longer confuse "never rebuilt" with a real id. `max(v)` yields NULL (→ `None`)
    /// when the key is missing, mirroring [`TableDb::built_epoch`]'s probe-free read.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the reload latch cannot be read.
    pub fn recorded_reload_id(&self) -> Result<Option<ReloadId>, TransformerError> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'reload_id'",
                [],
                |r| r.get(0),
            )
            .duck("read recorded reload_id")?;
        Ok(v.map(ReloadId))
    }

    /// Latch the reload generation this `.duckdb` is now rebuilt for. With a monotonic bigserial
    /// id, "latest wins" (H9) is this upsert plus a numeric compare at the trigger.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the reload-id upsert fails.
    pub fn set_recorded_reload_id(&self, reload_id: ReloadId) -> Result<(), TransformerError> {
        if self.is_ducklake() {
            return self.replace_meta("reload_id", reload_id.0);
        }
        self.conn
            .execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('reload_id', ?) \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                duckdb::params![reload_id.0],
            )
            .duck("set recorded reload_id")?;
        Ok(())
    }

    fn has_metadata_table(&self) -> Result<bool, TransformerError> {
        let count: i64 = match &self.backend {
            Backend::Native => self
                .conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_name = '_walrus_meta'",
                    [],
                    |row| row.get(0),
                )
                .duck("probe _walrus_meta")?,
            Backend::DuckLake(names) => self
                .conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_catalog = ? AND table_schema = ? \
                       AND table_name = '_walrus_meta'",
                    duckdb::params![names.attach_name, names.internal_schema],
                    |row| row.get(0),
                )
                .duck("probe DuckLake _walrus_meta")?,
        };
        Ok(count > 0)
    }

    /// Read the durable unpublished reload generation, if one exists.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] for a metadata read failure or [`TransformerError::LsnParse`] when
    /// persisted boundary text is corrupt.
    pub(crate) fn reload_build(&self) -> Result<Option<ReloadBuild>, TransformerError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT reload_id, shadow_table, schema_version, start_lsn, final_lsn, \
                        publication_nonce, raw_appended_lsn, transformed_lsn, phase \
                 FROM \"_walrus_reload_state\" ORDER BY reload_id DESC LIMIT 1",
            )
            .duck("prepare reload build read")?;
        let mut rows = stmt.query([]).duck("read reload build")?;
        let Some(row) = rows.next().duck("step reload build")? else {
            return Ok(None);
        };
        let start_hex: String = row.get(3).duck("read reload start_lsn")?;
        let final_hex: String = row.get(4).duck("read reload final_lsn")?;
        let publication_nonce: String = row.get(5).duck("read reload publication nonce")?;
        let raw_hex: String = row.get(6).duck("read reload raw frontier")?;
        let transformed_hex: String = row.get(7).duck("read reload transformed frontier")?;
        let phase: String = row.get(8).duck("read reload phase")?;
        let reload_id = ReloadId(row.get(0).duck("read reload_id")?);
        let shadow_table: String = row.get(1).duck("read reload shadow table")?;
        let expected_shadow = format!("__walrus_reload_{}", reload_id.0);
        if shadow_table != expected_shadow {
            return Err(TransformerError::Internal(format!(
                "reload {reload_id} state names unsafe shadow table {shadow_table:?}; expected {expected_shadow:?}"
            )));
        }
        Ok(Some(ReloadBuild {
            reload_id,
            shadow_table,
            schema_version: SchemaVersionNo(row.get(2).duck("read reload schema version")?),
            start_lsn: start_hex
                .parse()
                .map_err(|source| TransformerError::LsnParse {
                    field: "reload start_lsn",
                    source,
                })?,
            final_lsn: final_hex
                .parse()
                .map_err(|source| TransformerError::LsnParse {
                    field: "reload final_lsn",
                    source,
                })?,
            publication_nonce: publication_nonce.parse().map_err(|_| {
                TransformerError::Internal(format!(
                    "reload state contains invalid publication nonce {publication_nonce:?}"
                ))
            })?,
            raw_appended_lsn: raw_hex
                .parse()
                .map_err(|source| TransformerError::LsnParse {
                    field: "reload raw_appended_lsn",
                    source,
                })?,
            transformed_lsn: transformed_hex.parse().map_err(|source| {
                TransformerError::LsnParse {
                    field: "reload transformed_lsn",
                    source,
                }
            })?,
            phase: match phase.as_str() {
                "building" => ReloadPhase::Building,
                "published" => ReloadPhase::Published,
                _ => {
                    return Err(TransformerError::Internal(format!(
                        "reload state contains invalid phase {phase:?}"
                    )));
                }
            },
        }))
    }

    /// Create (or resume) a hidden full-table generation without changing the live projection.
    ///
    /// The physical name is deterministic from `reload_id`. Repeating the exact begin after a crash
    /// returns the existing generation without clearing it. A newer attempt discards only the older
    /// unpublished shadow; an attempt at or below the published latch is stale.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] when shadow DDL/state persistence fails, or
    /// [`TransformerError::Internal`] when an existing attempt's immutable boundaries changed.
    pub(crate) fn begin_reload_shadow(
        &self,
        plan: &TablePlan,
        schema_version: SchemaVersionNo,
        reload_id: ReloadId,
        start_lsn: Lsn,
        final_lsn: Lsn,
        publication_nonce: uuid::Uuid,
    ) -> Result<BeginReload, TransformerError> {
        if self
            .recorded_reload_id()?
            .is_some_and(|published| reload_id <= published)
        {
            return Ok(BeginReload::Stale);
        }
        if let Some(existing) = self.reload_build()? {
            if reload_id < existing.reload_id {
                return Ok(BeginReload::Stale);
            }
            if reload_id == existing.reload_id {
                if existing.schema_version != schema_version
                    || existing.start_lsn != start_lsn
                    || existing.final_lsn != final_lsn
                    || existing.publication_nonce != publication_nonce
                {
                    return Err(TransformerError::Internal(format!(
                        "reload {reload_id} marker/shape changed while its shadow was being built"
                    )));
                }
                return Ok(BeginReload::Ready(Box::new(existing)));
            }
        }

        let shadow_table = format!("__walrus_reload_{}", reload_id.0);
        let build = ReloadBuild {
            reload_id,
            shadow_table: shadow_table.clone(),
            schema_version,
            start_lsn,
            final_lsn,
            publication_nonce,
            raw_appended_lsn: start_lsn,
            transformed_lsn: start_lsn,
            phase: ReloadPhase::Building,
        };
        let shadow_plan = plan.for_table(shadow_table.as_str());
        let create_shadow = self.generation_sql(&shadow_plan);
        let prior = self.reload_build()?;
        self.in_txn("begin reload shadow", |conn| {
            if let Some(prior) = &prior {
                conn.execute_batch(&drop_generation_sql(&prior.shadow_table))
                    .duck_with(|| {
                        format!("drop superseded reload shadow {}", prior.shadow_table)
                    })?;
            }
            // This also cleans up a residue left by an old implementation/manual repair. In the
            // normal path the state row and generation were created in this same transaction.
            conn.execute_batch(&drop_generation_sql(&shadow_table))
                .duck_with(|| format!("drop stale reload shadow {shadow_table}"))?;
            conn.execute_batch(&create_shadow)
                .duck_with(|| format!("create reload shadow {shadow_table}"))?;
            conn.execute("DELETE FROM \"_walrus_reload_state\"", [])
                .duck("clear prior reload state")?;
            conn.execute(
                "INSERT INTO \"_walrus_reload_state\" \
                 (reload_id, shadow_table, schema_version, start_lsn, final_lsn, \
                  publication_nonce, raw_appended_lsn, transformed_lsn, phase) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    reload_id.0,
                    shadow_table,
                    schema_version.0,
                    start_lsn.to_string(),
                    final_lsn.to_string(),
                    publication_nonce.to_string(),
                    start_lsn.to_string(),
                    start_lsn.to_string(),
                    ReloadPhase::Building.as_str(),
                ],
            )
            .duck("record reload shadow")?;
            Ok(())
        })?;
        Ok(BeginReload::Ready(Box::new(build)))
    }

    /// Advance the hidden generation's raw frontier after its Duck append transaction commits.
    /// This is deliberately local: the canonical control checkpoint remains frozen until publish.
    pub(crate) fn advance_reload_raw(
        &self,
        reload_id: ReloadId,
        publication_nonce: uuid::Uuid,
        lsn: Lsn,
    ) -> Result<(), TransformerError> {
        let changed = self
            .conn
            .execute(
                "UPDATE \"_walrus_reload_state\" SET raw_appended_lsn = ? \
                 WHERE reload_id = ? AND publication_nonce = ? AND phase = 'building' \
                   AND raw_appended_lsn <= ? AND final_lsn >= ?",
                duckdb::params![
                    lsn.to_string(),
                    reload_id.0,
                    publication_nonce.to_string(),
                    lsn.to_string(),
                    lsn.to_string(),
                ],
            )
            .duck("advance reload raw frontier")?;
        if changed != 1 {
            return Err(TransformerError::Internal(format!(
                "reload {reload_id} raw frontier advance was not owned by the building receipt"
            )));
        }
        Ok(())
    }

    /// Advance the hidden generation's transform frontier after its mirror transaction commits.
    pub(crate) fn advance_reload_transformed(
        &self,
        reload_id: ReloadId,
        publication_nonce: uuid::Uuid,
        lsn: Lsn,
    ) -> Result<(), TransformerError> {
        let changed = self
            .conn
            .execute(
                "UPDATE \"_walrus_reload_state\" SET transformed_lsn = ? \
                 WHERE reload_id = ? AND publication_nonce = ? AND phase = 'building' \
                   AND transformed_lsn <= ? AND raw_appended_lsn >= ?",
                duckdb::params![
                    lsn.to_string(),
                    reload_id.0,
                    publication_nonce.to_string(),
                    lsn.to_string(),
                    lsn.to_string(),
                ],
            )
            .duck("advance reload transformed frontier")?;
        if changed != 1 {
            return Err(TransformerError::Internal(format!(
                "reload {reload_id} transformed frontier advance exceeded raw or lost ownership"
            )));
        }
        Ok(())
    }

    /// Seal an otherwise-empty tail at H after control-pg proves no manifest in any status remains
    /// through that boundary. The shadow's local frontiers become the durable precondition to swap.
    pub(crate) fn seal_reload_at_h(
        &self,
        reload_id: ReloadId,
        publication_nonce: uuid::Uuid,
        final_lsn: Lsn,
    ) -> Result<(), TransformerError> {
        let changed = self
            .conn
            .execute(
                "UPDATE \"_walrus_reload_state\" \
                 SET raw_appended_lsn = ?, transformed_lsn = ? \
                 WHERE reload_id = ? AND publication_nonce = ? AND phase = 'building' \
                   AND final_lsn = ? AND raw_appended_lsn <= ? AND transformed_lsn <= ?",
                duckdb::params![
                    final_lsn.to_string(),
                    final_lsn.to_string(),
                    reload_id.0,
                    publication_nonce.to_string(),
                    final_lsn.to_string(),
                    final_lsn.to_string(),
                    final_lsn.to_string(),
                ],
            )
            .duck("seal reload frontiers at H")?;
        if changed != 1 {
            return Err(TransformerError::Internal(format!(
                "reload {reload_id} could not seal its building receipt at H {final_lsn}"
            )));
        }
        Ok(())
    }

    /// Atomically replace the canonical generation with a fully reconciled shadow.
    ///
    /// The public DuckLake view continues to name the canonical internal view. That view, the live
    /// tables, the shadow tables, the schema watermark, and the reload latch change in one DuckDB
    /// transaction, so readers observe either the old complete generation or the new complete one.
    /// An already-published retry is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] when the transactional swap fails, or
    /// [`TransformerError::Internal`] when the requested attempt does not own the current shadow.
    pub(crate) fn publish_reload_shadow(
        &self,
        canonical_table: &str,
        reload_id: ReloadId,
    ) -> Result<bool, TransformerError> {
        if self
            .recorded_reload_id()?
            .is_some_and(|published| reload_id <= published)
        {
            return Ok(false);
        }
        let build = self.reload_build()?.ok_or_else(|| {
            TransformerError::Internal(format!(
                "cannot publish reload {reload_id}: no durable shadow generation"
            ))
        })?;
        if build.reload_id != reload_id {
            return Err(TransformerError::Internal(format!(
                "cannot publish reload {reload_id}: shadow belongs to reload {}",
                build.reload_id
            )));
        }
        if build.phase != ReloadPhase::Building
            || build.raw_appended_lsn != build.final_lsn
            || build.transformed_lsn != build.final_lsn
        {
            return Err(TransformerError::Internal(format!(
                "cannot publish reload {reload_id}: local building receipt is not sealed at H {}",
                build.final_lsn
            )));
        }

        let canonical_view = format!("{canonical_table}_current");
        let canonical_raw = format!("{canonical_table}_raw");
        let shadow_view = format!("{}_current", build.shadow_table);
        let shadow_raw = format!("{}_raw", build.shadow_table);
        let recreate_view = user_view_sql(canonical_table);
        let (drop_public_view, recreate_public_view) = match &self.backend {
            Backend::Native => (String::new(), String::new()),
            Backend::DuckLake(names) => {
                let catalog = ident(&names.attach_name)?;
                let source_schema = ident(&names.source_schema)?;
                let internal_schema = ident(&names.internal_schema)?;
                let public_view = ident(&format!("{}_current", names.source_table))?;
                let internal_view = ident(&format!("{canonical_table}_current"))?;
                (
                    format!("DROP VIEW IF EXISTS {catalog}.{source_schema}.{public_view};"),
                    format!(
                        "CREATE VIEW {catalog}.{source_schema}.{public_view} AS \
                         SELECT * FROM {catalog}.{internal_schema}.{internal_view};"
                    ),
                )
            }
        };
        self.in_txn("publish reload shadow", |conn| {
            conn.execute_batch(&format!(
                "{drop_public_view} \
                 DROP VIEW IF EXISTS \"{canonical_view}\"; \
                 DROP VIEW IF EXISTS \"{shadow_view}\"; \
                 DROP TABLE IF EXISTS \"{canonical_table}\"; \
                 DROP TABLE IF EXISTS \"{canonical_raw}\"; \
                 ALTER TABLE \"{}\" RENAME TO \"{canonical_table}\"; \
                 ALTER TABLE \"{shadow_raw}\" RENAME TO \"{canonical_raw}\"; \
                 {recreate_view} \
                 {recreate_public_view}",
                build.shadow_table,
            ))
            .duck_with(|| format!("swap reload {reload_id} into {canonical_table}"))?;
            conn.execute(
                "DELETE FROM \"_walrus_meta\" WHERE k IN ('schema_version', 'reload_id', 'comment_ddl_id')",
                [],
            )
            .duck("clear cutover metadata")?;
            conn.execute(
                "INSERT INTO \"_walrus_meta\" (k, v) \
                 VALUES ('schema_version', ?), ('reload_id', ?)",
                duckdb::params![build.schema_version.0, reload_id.0],
            )
            .duck("record reload cutover")?;
            conn.execute(
                "UPDATE \"_walrus_reload_state\" SET phase = 'published' \
                 WHERE reload_id = ? AND publication_nonce = ? AND phase = 'building'",
                duckdb::params![reload_id.0, build.publication_nonce.to_string()],
            )
            .duck("record durable published reload receipt")?;
            Ok(())
        })?;
        self.legacy_raw_replay_pk.set(false);
        Ok(true)
    }

    /// Clear the durable Duck-side receipt only after control-pg is `complete` with the same nonce.
    /// A missing receipt is an idempotent no-op; a different receipt is never removed.
    pub(crate) fn clear_reload_publication(
        &self,
        reload_id: ReloadId,
        publication_nonce: uuid::Uuid,
    ) -> Result<bool, TransformerError> {
        let deleted = self
            .conn
            .execute(
                "DELETE FROM \"_walrus_reload_state\" \
                 WHERE reload_id = ? AND publication_nonce = ? AND phase = 'published'",
                duckdb::params![reload_id.0, publication_nonce.to_string()],
            )
            .duck("clear completed reload publication receipt")?;
        Ok(deleted == 1)
    }

    /// Drop an exact unpublished reload generation after control Postgres has fenced that attempt.
    /// A missing build, a different nonce/id, or an already-published receipt is an idempotent
    /// no-op: none of those identities authorizes deleting the local generation.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if the transactional shadow cleanup fails.
    pub(crate) fn abandon_reload_build(
        &self,
        reload_id: ReloadId,
        publication_nonce: uuid::Uuid,
    ) -> Result<bool, TransformerError> {
        let Some(build) = self.reload_build()? else {
            return Ok(false);
        };
        if build.reload_id != reload_id
            || build.publication_nonce != publication_nonce
            || build.phase != ReloadPhase::Building
        {
            return Ok(false);
        }
        self.in_txn("abandon reload build", |conn| {
            conn.execute_batch(&drop_generation_sql(&build.shadow_table))
                .duck_with(|| format!("drop abandoned reload shadow {}", build.shadow_table))?;
            let deleted = conn
                .execute(
                    "DELETE FROM \"_walrus_reload_state\" \
                     WHERE reload_id = ? AND publication_nonce = ? AND phase = 'building'",
                    duckdb::params![reload_id.0, publication_nonce.to_string()],
                )
                .duck("delete abandoned reload state")?;
            if deleted != 1 {
                return Err(TransformerError::Internal(format!(
                    "reload {reload_id} changed while abandoning its local building receipt"
                )));
            }
            Ok(())
        })?;
        Ok(true)
    }

    /// Wipe a retired generation from this `.duckdb` (total-restart, §1.8): drop the user view, the mirror,
    /// the CDC log, and `_walrus_meta`. The caller then re-runs `ensure_tables*` to recreate them empty, so
    /// the new generation's full-table reconciliation repopulates and publishes `<table>`
    /// from scratch (both watermarks reset — the new epoch's `transformer_checkpoint` is a fresh `0/0`).
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if DuckDB cannot drop the retired generation's objects.
    pub fn wipe_generation(&self, table: &str) -> Result<(), TransformerError> {
        let shadow = if self.has_reload_state_table()? {
            self.reload_build()?.map(|build| build.shadow_table)
        } else {
            None
        };
        self.unpublish_current_view()?;
        if let Some(shadow) = shadow {
            self.conn
                .execute_batch(&drop_generation_sql(&shadow))
                .duck_with(|| format!("wipe reload shadow {shadow}"))?;
        }
        self.conn
            .execute_batch(&WIPE_GENERATION.replace("{table}", table))
            .duck_with(|| format!("wipe generation for {table}"))?;
        self.legacy_raw_replay_pk.set(false);
        Ok(())
    }

    fn has_reload_state_table(&self) -> Result<bool, TransformerError> {
        let count: i64 = match &self.backend {
            Backend::Native => self
                .conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_name = '_walrus_reload_state'",
                    [],
                    |row| row.get(0),
                )
                .duck("probe _walrus_reload_state")?,
            Backend::DuckLake(names) => self
                .conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_catalog = ? AND table_schema = ? \
                       AND table_name = '_walrus_reload_state'",
                    duckdb::params![names.attach_name, names.internal_schema],
                    |row| row.get(0),
                )
                .duck("probe DuckLake _walrus_reload_state")?,
        };
        Ok(count > 0)
    }

    /// Re-publish the stable source-schema read contract after creating or evolving the internal
    /// mirror view. Native test databases have no shared catalog and therefore need no second view.
    pub(crate) fn publish_current_view(&self) -> Result<(), TransformerError> {
        let Backend::DuckLake(names) = &self.backend else {
            return Ok(());
        };
        let catalog = ident(&names.attach_name)?;
        let source_schema = ident(&names.source_schema)?;
        let internal_schema = ident(&names.internal_schema)?;
        let public_view = ident(&format!("{}_current", names.source_table))?;
        let internal_view = ident(&format!("{}_current", names.source_table))?;
        self.conn
            .execute_batch(&format!(
                "CREATE OR REPLACE VIEW {catalog}.{source_schema}.{public_view} AS \
                 SELECT * FROM {catalog}.{internal_schema}.{internal_view};"
            ))
            .duck_with(|| {
                format!(
                    "publish DuckLake view {}.{}_current",
                    names.source_schema, names.source_table
                )
            })
    }

    pub(crate) fn unpublish_current_view(&self) -> Result<(), TransformerError> {
        let Backend::DuckLake(names) = &self.backend else {
            return Ok(());
        };
        self.conn
            .execute_batch(&format!(
                "DROP VIEW IF EXISTS {}.{}.{};",
                ident(&names.attach_name)?,
                ident(&names.source_schema)?,
                ident(&format!("{}_current", names.source_table))?
            ))
            .duck_with(|| {
                format!(
                    "unpublish DuckLake view {}.{}_current",
                    names.source_schema, names.source_table
                )
            })
    }

    fn replace_meta(&self, key: &str, value: i64) -> Result<(), TransformerError> {
        self.in_txn("replace DuckLake table metadata", |conn| {
            conn.execute("DELETE FROM \"_walrus_meta\" WHERE k = ?", [key])
                .duck_with(|| format!("delete old {key} metadata"))?;
            conn.execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES (?, ?)",
                duckdb::params![key, value],
            )
            .duck_with(|| format!("insert {key} metadata"))?;
            Ok(())
        })
    }

    /// Compact this table's newly written files and rewrite delete-heavy files. Snapshot expiration
    /// is deliberately catalog-level and runs in the singleton maintenance loop instead.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::Duck`] if any DuckLake maintenance procedure fails.
    pub fn maintain_files(&self, table: &str) -> Result<(), TransformerError> {
        let Backend::DuckLake(names) = &self.backend else {
            return Ok(());
        };
        let catalog = names.attach_name.to_quoted_literal();
        let schema = names.internal_schema.to_quoted_literal();
        let mirror = table.to_quoted_literal();
        let raw = format!("{table}_raw").to_quoted_literal();
        self.conn
            .execute_batch(&format!(
                "CALL ducklake_merge_adjacent_files({catalog}, {mirror}, schema => {schema});\n\
                 CALL ducklake_merge_adjacent_files({catalog}, {raw}, schema => {schema});\n\
                 CALL ducklake_rewrite_data_files({catalog}, {mirror}, schema => {schema});\n\
                 CALL ducklake_rewrite_data_files({catalog}, {raw}, schema => {schema});"
            ))
            .duck_with(|| format!("maintain DuckLake files for {table}"))
    }
}

/// Install the exact set of dynamic extensions required by the production transformer into `directory`.
/// This is invoked by the image build (`walrus-transformer --install-duckdb-extensions …`), not by the
/// normal service lifecycle.
///
/// # Errors
///
/// Returns [`TransformerError::File`] if `directory` cannot be created, or [`TransformerError::Duck`] if an
/// extension cannot be installed there.
pub fn install_extensions(directory: &Path) -> Result<(), TransformerError> {
    std::fs::create_dir_all(directory).map_err(|source| TransformerError::File {
        op: "create extension directory",
        path: directory.display().to_string(),
        source,
    })?;
    let conn = duckdb::Connection::open_in_memory().duck("open extension installer")?;
    let directory = directory.to_string_lossy().to_quoted_literal();
    conn.execute_batch(&format!("SET extension_directory = {directory};"))
        .duck("set extension install directory")?;
    for extension in EXTENSIONS {
        conn.execute_batch(&format!("INSTALL {extension};"))
            .duck_with(|| format!("install DuckDB extension {extension}"))?;
    }
    Ok(())
}

fn configure_extensions(
    conn: &duckdb::Connection,
    cfg: &DuckLakeConfig,
) -> Result<(), TransformerError> {
    if let Some(directory) = &cfg.extension_directory {
        conn.execute_batch(&format!(
            "SET extension_directory = {};",
            directory.to_quoted_literal()
        ))
        .duck("set extension directory")?;
    }
    if cfg.install_extensions {
        for extension in EXTENSIONS {
            conn.execute_batch(&format!("INSTALL {extension};"))
                .duck_with(|| format!("install DuckDB extension {extension}"))?;
        }
    }
    for extension in EXTENSIONS {
        conn.execute_batch(&format!("LOAD {extension};"))
            .duck_with(|| format!("load DuckDB extension {extension}"))?;
    }
    // Once every required artifact is resident, prohibit a typo or optional code path from fetching
    // and executing a new extension at runtime.
    conn.execute_batch(
        "SET autoinstall_known_extensions = false; SET autoload_known_extensions = false;",
    )
    .duck("disable runtime extension downloads")
}

fn attach_ducklake(
    cfg: &DuckLakeConfig,
    s3: &S3Access,
    automatic_migration: bool,
) -> Result<duckdb::Connection, TransformerError> {
    let conn = duckdb::Connection::open_in_memory().duck("open transient DuckDB")?;
    configure_extensions(&conn, cfg)?;
    configure_s3_secret(&conn, s3)?;

    let attach = ident(&cfg.attach_name)?;
    let metadata_schema = cfg.metadata_schema.to_quoted_literal();
    let data_path = cfg.data_path.to_quoted_literal();
    let catalog_uri = cfg.catalog_url.expose().to_quoted_literal();
    let migration = if automatic_migration {
        ", AUTOMATIC_MIGRATION true"
    } else {
        ""
    };
    let create_if_not_exists = if automatic_migration { "true" } else { "false" };
    conn.execute_batch(&format!(
        "CREATE OR REPLACE SECRET walrus_catalog (TYPE postgres, URI {catalog_uri});\n\
         ATTACH 'ducklake:postgres:' AS {attach} (META_SECRET 'walrus_catalog', \
             METADATA_SCHEMA {metadata_schema}, META_SCHEMA {metadata_schema}, DATA_PATH {data_path}, \
             DATA_INLINING_ROW_LIMIT 0, CREATE_IF_NOT_EXISTS {create_if_not_exists}, \
             OVERRIDE_DATA_PATH false{migration});"
    ))
    .duck("attach DuckLake catalog")?;
    Ok(conn)
}

/// Run catalog-wide retention and physical cleanup. Callers serialize this to shard zero; all
/// thresholds are explicit so upgrading DuckLake cannot silently change the storage policy.
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if attachment, snapshot expiration, or file cleanup fails.
pub fn maintain_catalog(cfg: &DuckLakeConfig, s3: &S3Access) -> Result<(), TransformerError> {
    let conn = attach_ducklake(cfg, s3, false)?;
    let catalog = cfg.attach_name.to_quoted_literal();
    let snapshot_seconds = cfg.snapshot_retention.as_secs();
    let cleanup_seconds = cfg.cleanup_grace.as_secs();
    conn.execute_batch(&format!(
        "CALL ducklake_expire_snapshots({catalog}, \
             older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{snapshot_seconds} seconds');\n\
         CALL ducklake_cleanup_old_files({catalog}, \
             older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{cleanup_seconds} seconds');\n\
         CALL ducklake_delete_orphaned_files({catalog}, \
             older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{cleanup_seconds} seconds');"
    ))
    .duck("run DuckLake catalog maintenance")
}

/// Explicitly create or migrate the DuckLake catalog with the pinned extension version. Normal
/// service startup never enables automatic migration.
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if the catalog cannot be attached, created, migrated, or verified.
pub fn migrate_catalog(cfg: &DuckLakeConfig, s3: &S3Access) -> Result<(), TransformerError> {
    let conn = attach_ducklake(cfg, s3, true)?;
    conn.execute_batch("SELECT 1;")
        .duck("verify migrated DuckLake catalog")
}

fn configure_s3_secret(conn: &duckdb::Connection, s3: &S3Access) -> Result<(), TransformerError> {
    let region = s3.region.to_quoted_literal();
    let sql = if s3.endpoint.is_empty() && s3.access_key_id.is_empty() {
        format!(
            "CREATE OR REPLACE SECRET walrus_s3 (TYPE s3, PROVIDER credential_chain, \
             REGION {region}, REFRESH auto);"
        )
    } else {
        let endpoint = s3.endpoint.to_quoted_literal();
        let key = s3.access_key_id.to_quoted_literal();
        let secret = s3.secret_access_key.expose().to_quoted_literal();
        let ssl = if s3.use_ssl { "true" } else { "false" };
        format!(
            "CREATE OR REPLACE SECRET walrus_s3 (TYPE s3, PROVIDER config, KEY_ID {key}, \
             SECRET {secret}, REGION {region}, ENDPOINT {endpoint}, URL_STYLE 'path', \
             USE_SSL {ssl});"
        )
    };
    conn.execute_batch(&sql).duck("configure S3 secret")
}

fn internal_schema(source_schema: &str, source_table: &str) -> String {
    let id = table_uuid(source_schema, source_table);
    format!("_walrus_{}", id.simple())
}

fn table_uuid(source_schema: &str, source_table: &str) -> uuid::Uuid {
    let key = format!("{source_schema}\0{source_table}");
    uuid::Uuid::new_v5(&TABLE_NAMESPACE, key.as_bytes())
}

/// Stable deterministic shard for a registered source table.
#[must_use]
pub fn table_shard(
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    shard_count: NonZeroU32,
) -> u32 {
    let table = format!("{}\0{source_schema}\0{source_table}", epoch.0);
    let score = |shard| {
        let candidate = format!("{table}\0{shard}");
        u128::from_be_bytes(uuid::Uuid::new_v5(&TABLE_NAMESPACE, candidate.as_bytes()).into_bytes())
    };
    let mut best = (0, score(0));
    for shard in 1..shard_count.get() {
        let candidate = score(shard);
        if candidate > best.1 {
            best = (shard, candidate);
        }
    }
    best.0
}

fn ident(raw: &str) -> Result<common::sql::SqlIdent, TransformerError> {
    common::sql::SqlIdent::new(raw).map_err(|source| {
        TransformerError::Internal(format!("DuckLake identifier {raw:?}: {source}"))
    })
}

/// The user-facing `<table>_current` view: the mirror minus the hidden `_applied_*` guard columns
/// (§7). A `SELECT *` view binds its column list at creation, so the DDL applier ([`crate::ddl`])
/// re-runs this after any structural change to pick up added/renamed columns.
pub(crate) fn user_view_sql(table: &str) -> String {
    CREATE_USER_VIEW.replace("{table}", table)
}

fn ingest_receipt_state_on(
    conn: &duckdb::Connection,
    file: &ManifestAppend<'_>,
) -> Result<IngestReceiptState, TransformerError> {
    let mut stmt = conn
        .prepare(
            "SELECT s3_uri, manifest_id, object_size, sha256, stream_group_id \
             FROM \"_walrus_ingested_files\" WHERE s3_uri = ? OR manifest_id = ?",
        )
        .duck("prepare ingest receipt lookup")?;
    let mut rows = stmt
        .query(duckdb::params![file.original_uri, file.manifest_id.0])
        .duck_with(|| format!("read ingest receipt for {}", file.original_uri))?;
    let Some(row) = rows.next().duck("step ingest receipt")? else {
        return Ok(IngestReceiptState::Missing);
    };
    let stored_uri: String = row.get(0).duck("read receipt URI")?;
    let stored_id: i64 = row.get(1).duck("read receipt manifest id")?;
    let stored_size: Option<i64> = row.get(2).duck("read receipt object size")?;
    let stored_sha: Option<String> = row.get(3).duck("read receipt SHA-256")?;
    let stored_group: Option<i64> = row.get(4).duck("read receipt stream group")?;
    if rows
        .next()
        .duck("check duplicate ingest receipts")?
        .is_some()
    {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "manifest {} / URI {} matched multiple ingest receipts",
                file.manifest_id, file.original_uri
            ),
        });
    }
    let expected_sha = hex::encode(file.sha256);
    if stored_uri != file.original_uri
        || stored_id != file.manifest_id.0
        || stored_size != Some(file.object_size)
        || stored_sha.as_deref() != Some(expected_sha.as_str())
        || stored_group != file.stream_group_id
    {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "manifest {} replay metadata does not match its durable ingest receipt",
                file.manifest_id
            ),
        });
    }
    Ok(IngestReceiptState::Ingested)
}

fn validate_manifest_rows(
    conn: &duckdb::Connection,
    uri: &str,
    original_uri: &str,
    expectation: ManifestExpectation<'_>,
) -> Result<(), TransformerError> {
    let poison = |reason: String| TransformerError::ObjectIntegrity {
        uri: original_uri.to_string(),
        reason,
    };
    let mut statement = conn
        .prepare(&format!(
            "SELECT walrus_extractor_meta FROM read_parquet('{uri}')"
        ))
        .map_err(|source| {
            poison(format!(
                "cannot inspect verified Parquet metadata: {source}"
            ))
        })?;
    let mut rows = statement
        .query([])
        .map_err(|source| poison(format!("cannot scan verified Parquet metadata: {source}")))?;
    let mut row_count = 0_i64;
    let mut batch_id: Option<String> = None;
    let mut extractor_instance: Option<String> = None;
    while let Some(row) = rows
        .next()
        .map_err(|source| poison(format!("cannot read verified Parquet metadata: {source}")))?
    {
        row_count = row_count
            .checked_add(1)
            .ok_or_else(|| poison("Parquet row count overflowed bigint".to_string()))?;
        let raw = row
            .get::<_, Option<String>>(0)
            .map_err(|source| poison(format!("cannot decode walrus_extractor_meta: {source}")))?
            .ok_or_else(|| poison(format!("row {row_count} has NULL walrus_extractor_meta")))?;
        let meta: common::ExtractorMeta = serde_json::from_str(&raw).map_err(|source| {
            poison(format!(
                "row {row_count} has invalid walrus_extractor_meta: {source}"
            ))
        })?;
        if meta.epoch != expectation.epoch
            || meta.schema_version != expectation.schema_version
            || meta.source_schema != expectation.source_schema
            || meta.source_table != expectation.source_table
            || meta.kind != expectation.kind
        {
            return Err(poison(format!(
                "row {row_count} metadata identity does not match epoch/table/schema-version/kind receipt"
            )));
        }
        if meta.batch_id.is_empty() || meta.extractor_instance.is_empty() {
            return Err(poison(format!(
                "row {row_count} metadata has an empty batch_id or extractor_instance"
            )));
        }
        match &batch_id {
            Some(expected) if expected != &meta.batch_id => {
                return Err(poison(format!(
                    "row {row_count} changes batch_id inside one immutable object"
                )));
            }
            None => batch_id = Some(meta.batch_id.clone()),
            _ => {}
        }
        match &extractor_instance {
            Some(expected) if expected != &meta.extractor_instance => {
                return Err(poison(format!(
                    "row {row_count} changes extractor_instance inside one immutable object"
                )));
            }
            None => extractor_instance = Some(meta.extractor_instance.clone()),
            _ => {}
        }
        for (index, column) in meta.unchanged_toast.iter().enumerate() {
            if meta.op != common::Op::Update
                || column == "walrus_extractor_meta"
                || !expectation
                    .source_columns
                    .iter()
                    .any(|candidate| !candidate.is_key && candidate.name == *column)
                || meta.unchanged_toast[..index].contains(column)
            {
                return Err(poison(format!(
                    "row {row_count} has an invalid unchanged_toast column {column:?}"
                )));
            }
        }
        if expectation.kind != common::Kind::Stream && meta.op != common::Op::Insert {
            return Err(poison(format!(
                "row {row_count} in a snapshot/reload object is not an insert image"
            )));
        }
        let lsn_invalid = if expectation.speculative_commit_lsn {
            // Spill bytes were written before commit. Their placeholder is the transaction's
            // begin LSN (the manifest start), while row-frame LSNs must still precede the real
            // StreamCommit LSN stored as the manifest end.
            expectation.lsn_start > expectation.lsn_end
                || meta.commit_lsn != expectation.lsn_start
                || meta.lsn < expectation.lsn_start
                || meta.lsn > expectation.lsn_end
        } else if expectation.kind != common::Kind::Stream {
            expectation.lsn_start != expectation.lsn_end
                || meta.commit_lsn != expectation.lsn_start
                || meta.lsn != expectation.lsn_start
        } else {
            meta.commit_lsn < expectation.lsn_start
                || meta.commit_lsn > expectation.lsn_end
                || meta.lsn > meta.commit_lsn
        };
        if lsn_invalid {
            return Err(poison(format!(
                "row {row_count} WAL/commit LSN lies outside manifest [{}, {}]",
                expectation.lsn_start, expectation.lsn_end
            )));
        }
    }
    if row_count != expectation.row_count {
        return Err(poison(format!(
            "manifest receipt records {} rows but Parquet contains {row_count}",
            expectation.row_count
        )));
    }
    Ok(())
}

fn drop_generation_sql(table: &str) -> String {
    format!(
        "DROP VIEW IF EXISTS \"{table}_current\"; \
         DROP TABLE IF EXISTS \"{table}\"; \
         DROP TABLE IF EXISTS \"{table}_raw\";"
    )
}

/// DuckDB S3/httpfs credentials for reading the staging bucket.
///
/// Only the secret half is [`Redacted`]: an access key id is an identifier that already appears in
/// bucket policies and audit trails, while `secret_access_key` *is* the credential — and this
/// struct derives `Debug`, which would otherwise put it one `?s3` away from a log line.
#[derive(Debug, Clone)]
pub struct S3Access {
    /// Host:port DuckDB's httpfs extension should talk to (MinIO in dev, S3 in production).
    pub endpoint: String,
    /// Region used to sign requests.
    pub region: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key. Held only long enough to configure the connection; the wrapper is what
    /// keeps "never logged" a property of the type rather than a promise in a doc comment.
    pub secret_access_key: Redacted<String>,
    /// Whether to reach the endpoint over TLS. `false` for a plain-HTTP local MinIO.
    pub use_ssl: bool,
}

/// Map a Postgres type OID to a DuckDB column type. Unknown types fall back to `VARCHAR` (the transformer
/// stages *text*-format tuples; the exact numeric/temporal fidelity is refined as the transform lands).
pub(crate) const fn duck_type(oid: u32) -> &'static str {
    match oid {
        INT2 => "SMALLINT",
        INT4 => "INTEGER",
        INT8 => "BIGINT",
        BOOL => "BOOLEAN",
        FLOAT4 => "REAL",
        FLOAT8 => "DOUBLE",
        NUMERIC => "DECIMAL(38,10)",
        DATE => "DATE",
        TIMESTAMP => "TIMESTAMP",
        TIMESTAMPTZ => "TIMESTAMP WITH TIME ZONE",
        UUID => "UUID",
        JSON | JSONB => "JSON",
        BYTEA => "BLOB",
        _ => "VARCHAR", // text, varchar, enums, and everything else
    }
}

// Compile-time proof of the transformer's threading model. This closure is type-checked but never run or
// code-generated. The compiler derives every bound; no manual auto-trait implementation is needed.
const _: fn() = || {
    const fn assert_send<T: Send + ?Sized>() {}
    const fn assert_sync<T: Sync + ?Sized>() {}

    // Moves into `TableCtx` and then into the worker future.
    assert_send::<TableDb>();
    // The one value handed from a local worker to `tokio::spawn` during compaction drain.
    assert_send::<duckdb::InterruptHandle>();
    assert_sync::<duckdb::InterruptHandle>();
    // Shared by the health server and every worker.
    assert_send::<Arc<crate::health::TransformerState>>();
    assert_sync::<Arc<crate::health::TransformerState>>();

    // Pin walrus's own source of `!Sync`; DuckDB independently keeps the overall type `!Sync`.
    const fn cache_refcell(
        db: &TableDb,
    ) -> &RefCell<HashMap<SchemaVersionNo, Arc<[StagedColumn]>>> {
        &db.parquet_cols
    }
    let _cache_refcell_fn = cache_refcell;
};

// Negative assertion: while the overall `TableDb` is not `Sync`, only the `()` impl applies and `_`
// resolves. If every source of `!Sync` is removed, both impls apply and this line is ambiguous. This
// is `static_assertions::assert_not_impl_all!` hand-rolled to avoid a direct dependency.
const _: fn() = || {
    trait AmbiguousIfSync<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

    let _some_item = <TableDb as AmbiguousIfSync<_>>::some_item;
};

#[cfg(test)]
#[path = "duck_test.rs"]
mod tests;
