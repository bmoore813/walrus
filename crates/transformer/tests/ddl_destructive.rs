#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]

//! Destructive DDL apply (transformer §5.7) — where mirror and raw **diverge**. Three hermetic tests
//! (in-memory / temp-file `TableDb`) prove the per-taxonomy behaviour; the `#[ignore]` compose test
//! proves a lossy-cast failure quarantines the table and degrades `/ready`.
//!
//!   cargo test -p transformer --test ddl_destructive              # hermetic
//!   cargo test -p transformer --test ddl_destructive -- --ignored # + compose (quarantine)

mod support;

use common::{DdlId, EpochNo, Lsn, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo};
use std::sync::Arc;
use std::time::Duration;
use transformer::ddl::{DestructiveChange, apply_destructive, retire_file};
use transformer::duck::{S3Access, TableDb};
use transformer::error::TransformerError;
use transformer::health::TransformerState;
use transformer::phase_a::{TableCtx, run_phase_a};
use transformer::phase_b::run_phase_b;

fn col(name: &str, oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    }
}

fn rel(name: &str, columns: Vec<PgColumn>) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: name.into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

fn mem(rel: &PgRelation) -> TableDb {
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables(rel, common::SchemaVersionNo(1)).unwrap();
    db
}

fn columns_of(conn: &duckdb::Connection, name: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )
        .unwrap();
    let rows = stmt.query_map([name], |r| r.get::<_, String>(0)).unwrap();
    rows.map(Result::unwrap).collect()
}

fn data_type_of(conn: &duckdb::Connection, table: &str, column: &str) -> String {
    conn.query_row(
        "SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
        [table, column],
        |r| r.get::<_, String>(0),
    )
    .unwrap()
}

fn table_exists(conn: &duckdb::Connection, name: &str) -> bool {
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
            |r| r.get(0),
        )
        .unwrap();
    n > 0
}

/// A scratch directory for one test's `.duckdb` file. The returned guard deletes it on drop — even
/// when an assertion panics, which a trailing `remove_dir_all` would skip.
fn tmpdir(name: &str) -> tempfile::TempDir {
    let prefix = format!("walrus-transformer-ddld-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

// ---- DROP COLUMN: physical on the mirror, retained-nullable on raw. ----
#[test]
fn drop_column_physical_on_mirror_retained_nullable_on_raw() {
    let db = mem(&rel(
        "orders",
        vec![col("id", 23, true), col("x", 25, false)],
    ));
    apply_destructive(
        &db,
        "orders",
        &[DestructiveChange::DropColumn { name: "x".into() }],
    )
    .unwrap();

    assert!(
        !columns_of(db.conn(), "orders").iter().any(|c| c == "x"),
        "mirror physically drops the column"
    );
    assert!(
        columns_of(db.conn(), "orders_raw").iter().any(|c| c == "x"),
        "raw RETAINS the column (verbatim history)"
    );
    // The retained raw column is nullable — a post-drop verbatim row omitting it reads NULL.
    db.conn()
        .execute(
            "INSERT INTO orders_raw (id, walrus_extractor_meta, _walrus_op, \
             _walrus_commit_lsn, _walrus_lsn, _walrus_extractor_processed_at) \
             VALUES (1, '{}', 'i', 'A', 'B', 'C')",
            [],
        )
        .unwrap();
    let x: Option<String> = db
        .conn()
        .query_row("SELECT x FROM orders_raw WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(x, None, "post-drop file NULL-fills the retained raw column");
    // The user view refreshed — no dropped column.
    assert!(
        !columns_of(db.conn(), "orders_current")
            .iter()
            .any(|c| c == "x")
    );
}

// ---- Lossy ALTER TYPE: raw widens to VARCHAR (history preserved), never re-cast. ----
#[test]
fn lossy_type_change_widens_raw_to_varchar_without_recasting() {
    // Mirror value fits the narrowed type (42 → SMALLINT); raw holds a BIG value (99999) that would
    // OVERFLOW a smallint cast — proving raw is widened to VARCHAR and never re-cast.
    let db = mem(&rel(
        "orders",
        vec![col("id", 23, true), col("n", 23, false)],
    )); // n int4
    db.conn()
        .execute("INSERT INTO orders (id, n) VALUES (1, 42)", [])
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO orders_raw (id, n, walrus_extractor_meta, _walrus_op, \
             _walrus_commit_lsn, _walrus_lsn, _walrus_extractor_processed_at) \
             VALUES (1, 99999, '{}', 'i', 'A', 'B', 'C')",
            [],
        )
        .unwrap();

    apply_destructive(
        &db,
        "orders",
        &[DestructiveChange::LossyType {
            name: "n".into(),
            new: col("n", 21, false), // int4 → int2 (narrowing / lossy)
        }],
    )
    .unwrap();

    // Raw widened to VARCHAR; the big value survives as text (never re-cast → no overflow).
    assert_eq!(data_type_of(db.conn(), "orders_raw", "n"), "VARCHAR");
    let raw_n: String = db
        .conn()
        .query_row("SELECT n FROM orders_raw WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(raw_n, "99999", "raw value preserved as text, not re-cast");

    // Mirror cast succeeded in place (42 fits SMALLINT).
    assert_eq!(data_type_of(db.conn(), "orders", "n"), "SMALLINT");
    let m_n: i16 = db
        .conn()
        .query_row("SELECT n FROM orders WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(m_n, 42);
}

// ---- DROP TABLE: retire both DuckDB tables + the .duckdb file (idempotent). ----
#[tokio::test]
async fn drop_table_retires_both_tables_and_the_file() {
    let dir = tmpdir("drop");
    let path = dir.path().join("orders.duckdb");
    {
        let db = TableDb::open(&path).unwrap();
        db.ensure_tables(
            &rel(
                "orders",
                vec![col("id", 23, true), col("status", 25, false)],
            ),
            common::SchemaVersionNo(1),
        )
        .unwrap();
        apply_destructive(
            &db,
            "orders",
            &[DestructiveChange::DropTable {
                name: "orders".into(),
            }],
        )
        .unwrap();
        assert!(!table_exists(db.conn(), "orders"), "mirror retired");
        assert!(!table_exists(db.conn(), "orders_raw"), "CDC log retired");
    } // drop the connection → release the DuckDB file lock

    assert!(path.exists(), "file present until explicitly retired");
    retire_file(&path).await.unwrap();
    assert!(!path.exists(), "the .duckdb file is retired");
    // Idempotent — a crash-mid-retire re-run is a no-op. Spelled as a bare `&str` here (the call
    // above passes `&PathBuf`) because `retire_file` takes a borrowed path view, not one exact type.
    retire_file(path.to_str().unwrap()).await.unwrap();
    assert!(!path.exists(), "the retire re-run left the file absent");
}

// ---- Compose: a lossy cast that fails quarantines the table and degrades /ready. ----

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn s3() -> S3Access {
    S3Access {
        endpoint: "localhost:9000".into(),
        region: "us-east-1".into(),
        access_key_id: "minioadmin".into(),
        secret_access_key: "minioadmin".into(),
        use_ssl: false,
    }
}

async fn publish_lossy_schema_barrier(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    commit_lsn: Lsn,
    relation: &PgRelation,
) {
    let columns = serde_json::to_value(relation).unwrap();
    assert_eq!(
        control::publish_stream_commit(
            pool,
            &control::NewStreamCommitPublication {
                epoch,
                top_xid: 8_301,
                commit_lsn,
                commit_ts: "2026-09-02T12:05:00Z".parse().unwrap(),
                ddl_rows: vec![control::DdlRow {
                    id: DdlId(0),
                    epoch,
                    source_audit_id: 39_002_001,
                    source_schema: "public".into(),
                    source_table: "orders".into(),
                    c_lsn: commit_lsn,
                    c_event: "ddl_command_end".into(),
                    c_tag: "ALTER TABLE".into(),
                    schema_version: SchemaVersionNo(2),
                    c_rel_oid: Some(relation.oid),
                    c_columns: Some(columns.clone()),
                    c_dropped: None,
                    c_ddl_text: Some("ALTER TABLE orders ALTER COLUMN n TYPE smallint".into()),
                    c_table_comment: None,
                }],
                registry_rows: vec![control::RegistryRow {
                    epoch,
                    source_schema: "public".into(),
                    source_table: "orders".into(),
                    schema_version: SchemaVersionNo(2),
                    descriptors: Vec::new(),
                    columns,
                }],
                files: Vec::new(),
            },
        )
        .await
        .unwrap(),
        control::PublishStreamOutcome::Published
    );
}

/// Write an (id, n, walrus_extractor_meta) Parquet fixture to S3.
fn write_fixture(
    epoch: EpochNo,
    tag: &str,
    batch_no: u64,
    schema_version: common::SchemaVersionNo,
    id: i64,
    n: i64,
    commit_lsn: &str,
) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    let epoch_bits = u64::try_from(epoch.0).unwrap();
    let batch_id =
        uuid::Uuid::from_u128((u128::from(epoch_bits) << 64) | u128::from(batch_no)).to_string();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
        a.region,
        a.endpoint,
        a.access_key_id,
        a.secret_access_key.expose()
    ))
    .unwrap();
    w.execute_batch("CREATE TABLE fixture (id INTEGER, n INTEGER, walrus_extractor_meta VARCHAR);")
        .unwrap();
    let metadata = serde_json::to_string(&support::extractor_meta(
        epoch,
        &batch_id,
        schema_version,
        "public",
        "orders",
        common::Kind::Stream,
        "i",
        commit_lsn,
        commit_lsn,
    ))
    .unwrap();
    w.execute(
        "INSERT INTO fixture VALUES (?, ?, ?)",
        duckdb::params![id, n, metadata],
    )
    .unwrap();
    let uri = format!("s3://walrus/{epoch}/public/orders/{tag}-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn lossy_cast_failure_quarantines_the_table_and_alerts() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_900_001);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    support::cleanup_epoch(&pool, epoch).await;
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT pg_catalog.set_config(
             'walrus.schema_registry_maintenance', '1-delete', true
           ) AS protocol
         )
         DELETE FROM public.walrus_schema_registry
         WHERE epoch = $1 AND (SELECT protocol = '1-delete' FROM authorized)",
    )
    .bind(epoch.0)
    .execute(&pool)
    .await
    .unwrap();
    control::insert_epoch(
        &pool,
        epoch,
        "walrus_slot",
        "0/0".parse().unwrap(),
        control::ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap();

    // v1: n is int4. Seed a v1 file with a value (99999) that will NOT fit the later int2 narrowing.
    let orders_v1 = rel("orders", vec![col("id", 23, true), col("n", 23, false)]);
    control::upsert_registry(
        &pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            schema_version: common::SchemaVersionNo(1),
            descriptors: Vec::new(),
            columns: serde_json::to_value(&orders_v1).unwrap(),
        },
    )
    .await
    .unwrap();
    let v1 = write_fixture(epoch, "v1", 1, common::SchemaVersionNo(1), 1, 99999, "0/64");
    let (v1_object_size, v1_sha256) = support::fingerprint(&v1).await;
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: v1,
            kind: control::ManifestKind::Stream,
            row_count: 1,
            object_size: v1_object_size,
            sha256: v1_sha256,
            lsn_start: "0/64".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: common::SchemaVersionNo(1),
            reload_id: None,
        },
    )
    .await
    .unwrap();

    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders_v1, common::SchemaVersionNo(1))
        .unwrap();
    db.configure_s3(&s3()).unwrap();
    let state = TransformerState::new();
    let (owner_pod, fencing_token) = support::acquire_table(&pool, epoch, "public", "orders").await;
    let ctx = TableCtx {
        pool,
        epoch,
        epoch_rx: transformer::epoch::fixed_epoch_watch(epoch),
        owner_pod,
        fencing_token,
        store: support::store(),
        staging_bucket: "walrus".into(),
        schema: "public".into(),
        table: "orders".into(),
        series: "public.orders".into(),
        rel: orders_v1,
        db,
        state: Arc::clone(&state),
        max_files: std::num::NonZeroI64::new(100).unwrap(),
        max_integrity_resnapshots: 1,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    };

    // Process v1 fully so the mirror holds n=99999 BEFORE the lossy DDL reconcile runs.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    state.mark_ready();
    assert!(ctx.state.is_ready(), "ready after bootstrap+first cycle");

    // v2: n narrows to int2 (lossy). A v2 file triggers the reconcile before it is appended.
    let orders_v2 = rel("orders", vec![col("id", 23, true), col("n", 21, false)]);
    control::upsert_registry(
        &ctx.pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            schema_version: common::SchemaVersionNo(2),
            descriptors: Vec::new(),
            columns: serde_json::to_value(&orders_v2).unwrap(),
        },
    )
    .await
    .unwrap();
    let v2 = write_fixture(epoch, "v2", 2, common::SchemaVersionNo(2), 2, 5, "0/C8");
    let (v2_object_size, v2_sha256) = support::fingerprint(&v2).await;
    control::insert_ready(
        &ctx.pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: v2,
            kind: control::ManifestKind::Stream,
            row_count: 1,
            object_size: v2_object_size,
            sha256: v2_sha256,
            lsn_start: "0/C8".parse().unwrap(),
            lsn_end: "0/C8".parse().unwrap(),
            schema_version: common::SchemaVersionNo(2),
            reload_id: None,
        },
    )
    .await
    .unwrap();

    // The lossy int4→int2 cast on the mirror (which holds 99999) overflows → quarantine.
    let result = run_phase_a(&ctx).await;
    assert!(
        matches!(result, Err(TransformerError::Quarantine { .. })),
        "a failed lossy cast quarantines the table: {result:?}"
    );
    assert!(
        ctx.state.is_quarantined(),
        "quarantine latched on the state"
    );
    assert!(!ctx.state.is_ready(), "/ready degrades on quarantine");

    // raw was widened to VARCHAR (history preserved); the mirror value was NOT destroyed.
    let raw_type: String = ctx
        .db
        .conn()
        .query_row(
            "SELECT data_type FROM information_schema.columns WHERE table_name='orders_raw' AND column_name='n'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw_type, "VARCHAR", "raw widened to VARCHAR, not re-cast");
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn lossy_schema_only_barrier_stays_ready_when_reconciliation_quarantines() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(3_900_002);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    support::cleanup_epoch(&pool, epoch).await;
    let mut cleanup = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance', '2-delete', true)")
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.schema_registry_maintenance', '1-delete', true)")
        .execute(&mut *cleanup)
        .await
        .unwrap();
    for table in ["ddl_manifest", "schema_registry"] {
        sqlx::query(&format!(
            "DELETE FROM public.walrus_{table} WHERE epoch = $1"
        ))
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    }
    cleanup.commit().await.unwrap();
    control::insert_epoch(
        &pool,
        epoch,
        "walrus_slot",
        Lsn::ZERO,
        control::ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap();

    let v1 = rel("orders", vec![col("id", 23, true), col("n", 23, false)]);
    control::upsert_registry(
        &pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            schema_version: SchemaVersionNo(1),
            descriptors: Vec::new(),
            columns: serde_json::to_value(&v1).unwrap(),
        },
    )
    .await
    .unwrap();
    let v2 = rel("orders", vec![col("id", 23, true), col("n", 21, false)]);
    publish_lossy_schema_barrier(&pool, epoch, Lsn::new(0x300), &v2).await;

    let dir = tmpdir("lossy-schema-barrier");
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&v1, SchemaVersionNo(1)).unwrap();
    db.conn()
        .execute("INSERT INTO orders (id, n) VALUES (1, 99999)", [])
        .unwrap();
    let state = TransformerState::new();
    let (owner_pod, fencing_token) = support::acquire_table(&pool, epoch, "public", "orders").await;
    let ctx = TableCtx {
        pool,
        epoch,
        epoch_rx: transformer::epoch::fixed_epoch_watch(epoch),
        owner_pod,
        fencing_token,
        store: support::store(),
        staging_bucket: "walrus".into(),
        schema: "public".into(),
        table: "orders".into(),
        series: "public.orders".into(),
        rel: v1,
        db,
        state: Arc::clone(&state),
        max_files: std::num::NonZeroI64::new(100).unwrap(),
        max_integrity_resnapshots: 1,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    };

    let result = run_phase_a(&ctx).await;
    assert!(
        matches!(result, Err(TransformerError::Quarantine { .. })),
        "lossy zero-file schema reconciliation must quarantine: {result:?}"
    );
    assert!(state.is_quarantined());
    assert_eq!(ctx.db.schema_version().unwrap(), SchemaVersionNo(1));
    assert!(matches!(
        control::claim_ready_units(&ctx.pool, epoch, "public", "orders", 10)
            .await
            .unwrap()
            .as_slice(),
        [control::ReadyManifestUnit::SchemaBarrier(_)]
    ));
    let checkpoint = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (checkpoint.raw_appended_lsn, checkpoint.transformed_lsn),
        (Lsn::ZERO, Lsn::ZERO),
        "a rejected schema-only commit neither retires nor advances data"
    );
}
