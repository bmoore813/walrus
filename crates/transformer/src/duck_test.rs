use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};

fn requires_send<T: Send>() {}

/// The exact closure and result bounds imposed by `tokio::task::spawn_blocking`. This is
/// intentionally instantiated only with an owned legal closure: current DuckDB work borrows a
/// `TableDb`, whose negative shared-reference bounds are guarded by `TableDb`'s compile-fail docs.
fn requires_spawn_blocking_bounds<F, R>(_f: F)
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
}

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

#[test]
fn mirror_template_lists_every_wide_primary_key_column() {
    let mut columns = (1..=32)
        .map(|index| PgColumn {
            name: format!("key_{index:02}"),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        })
        .collect::<Vec<_>>();
    columns.push(PgColumn {
        name: "payload".into(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    });
    let relation = PgRelation {
        oid: 43,
        schema: "public".into(),
        name: "wide_keys".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    };
    let db = TableDb::open(":memory:").unwrap();
    let sql = db.generation_sql(&crate::plan::TablePlan::tier1(&relation).unwrap());
    let expected = (1..=32)
        .map(|index| format!("key_{index:02}"))
        .collect::<Vec<_>>()
        .join(", ");

    assert!(
        sql.contains(&format!("PRIMARY KEY ({expected})")),
        "the mirror DDL must contain the complete composite key"
    );
}

/// `S3Access` derives `Debug` and is carried by every worker, so a stray `?s3` in a diagnostic
/// would ship the bucket credential to the log aggregator. The endpoint, region and key id stay
/// legible — they are identifiers an operator needs; only the secret half is withheld.
#[test]
fn debug_renders_the_bucket_secret_but_not_its_neighbours() {
    let access = S3Access {
        endpoint: "minio:9000".to_string(),
        region: "eu-west-2".to_string(),
        access_key_id: "AKIAEXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI".into(),
        use_ssl: false,
    };

    let rendered = format!("{access:?}");

    assert!(!rendered.contains("wJalrXUtnFEMI"), "{rendered}");
    assert!(rendered.contains("AKIAEXAMPLE"), "{rendered}");
    assert!(rendered.contains(common::REDACTED), "{rendered}");
}

#[test]
fn in_txn_commits_on_ok() {
    let db = TableDb::open(":memory:").unwrap();

    db.in_txn("probe", |conn| {
        conn.execute_batch("CREATE TABLE txn_probe (id INTEGER); INSERT INTO txn_probe VALUES (1);")
            .duck("commit probe body")
    })
    .unwrap();

    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM txn_probe", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "an Ok transaction body is committed");
}

#[test]
fn in_txn_rolls_back_on_err() {
    let db = TableDb::open(":memory:").unwrap();
    db.conn()
        .execute_batch("CREATE TABLE txn_probe (id INTEGER);")
        .unwrap();

    let error = db
        .in_txn("probe", |conn| {
            conn.execute_batch("INSERT INTO txn_probe VALUES (1);")
                .duck("rollback probe body")?;
            Err::<(), TransformerError>(TransformerError::Quarantine {
                table: "public.orders".into(),
                reason: "identity sentinel".into(),
            })
        })
        .unwrap_err();

    match error {
        TransformerError::Quarantine { table, reason } => {
            assert_eq!(table, "public.orders");
            assert_eq!(reason, "identity sentinel");
        }
        other => panic!("transaction changed the body error: {other:?}"),
    }
    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM txn_probe", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0, "an Err transaction body is rolled back");

    db.in_txn("following", |conn| {
        conn.execute_batch("INSERT INTO txn_probe VALUES (2);")
            .duck("following transaction body")
    })
    .unwrap();
    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM txn_probe", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "rollback leaves the connection transaction-ready");
}

#[test]
fn in_txn_accepts_a_move_consuming_body() {
    let db = TableDb::open(":memory:").unwrap();
    let sql = String::from("CREATE TABLE t_once (a INTEGER);");

    db.in_txn("once", move |conn| {
        conn.execute_batch(&sql).duck("once body")?;
        drop(sql);
        Ok(())
    })
    .unwrap();
}

/// Write a local `(id, status, walrus_extractor_meta)` Parquet whose rows carry `commit_lsn = placeholder`
/// — mimicking a speculative spill written before its txn's commit LSN was known.
fn write_local_fixture(dir: &Path, name: &str, ids: (i64, i64), placeholder: &str) -> String {
    let path = dir.join(name);
    let uri = path.to_string_lossy().replace('\'', "''");
    let w = duckdb::Connection::open_in_memory().unwrap();
    let meta = |lsn: &str| {
        format!(
            "{{\"op\":\"Insert\",\"commit_lsn\":\"{placeholder}\",\"lsn\":\"{lsn}\",\
                  \"extractor_processed_at\":\"2026-07-08T12:00:0{lsn}Z\"}}"
        )
    };
    w.execute_batch(&format!(
        "CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR); \
             INSERT INTO fixture VALUES \
               ({}, 'a', '{}'), ({}, 'b', '{}'); \
             COPY fixture TO '{uri}' (FORMAT PARQUET);",
        ids.0,
        meta("1"),
        ids.1,
        meta("2"),
    ))
    .unwrap();
    uri
}

fn attested_meta(source_table: &str) -> common::ExtractorMeta {
    common::ExtractorMeta {
        op: common::Op::Insert,
        lsn: common::Lsn::new(0x10),
        commit_lsn: common::Lsn::new(0x20),
        commit_ts: "2026-07-08T12:00:00Z".parse().unwrap(),
        xid: 7,
        epoch: common::EpochNo(1),
        batch_id: "batch-1".to_string(),
        schema_version: common::SchemaVersionNo(1),
        source_schema: "public".to_string(),
        source_table: source_table.to_string(),
        kind: common::Kind::Stream,
        unchanged_toast: Box::default(),
        extractor_instance: "extractor-0".to_string(),
        extractor_processed_at: "2026-07-08T12:00:01Z".parse().unwrap(),
    }
}

fn write_attested_metas(dir: &Path, name: &str, metas: &[common::ExtractorMeta]) -> String {
    let path = dir.join(name);
    let uri = path.to_string_lossy().replace('\'', "''");
    let writer = duckdb::Connection::open_in_memory().unwrap();
    writer
        .execute_batch(
            "CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR);",
        )
        .unwrap();
    for (index, meta) in metas.iter().enumerate() {
        let id = i32::try_from(index + 1).unwrap();
        let status = format!("row-{id}");
        let encoded = serde_json::to_string(meta).unwrap();
        writer
            .execute(
                "INSERT INTO fixture VALUES (?, ?, ?)",
                duckdb::params![id, status, encoded],
            )
            .unwrap();
    }
    writer
        .execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

fn write_attested_fixture(dir: &Path, name: &str, source_table: &str) -> String {
    write_attested_metas(dir, name, &[attested_meta(source_table)])
}

fn validate_attested_fixture(
    dir: &Path,
    name: &str,
    metas: &[common::ExtractorMeta],
    expectation: ManifestExpectation<'_>,
) -> Result<(), TransformerError> {
    let uri = write_attested_metas(dir, name, metas);
    let reader = duckdb::Connection::open_in_memory().unwrap();
    validate_manifest_rows(&reader, &uri, "s3://walrus/attested.parquet", expectation)
}

fn assert_integrity_reason(result: Result<(), TransformerError>, needle: &str) {
    match result {
        Err(TransformerError::ObjectIntegrity { reason, .. }) => {
            assert!(
                reason.contains(needle),
                "{reason:?} does not contain {needle:?}"
            );
        }
        other => panic!("expected object-integrity error containing {needle:?}, got {other:?}"),
    }
}

fn manifest_expectation(row_count: i64) -> ManifestExpectation<'static> {
    ManifestExpectation {
        row_count,
        epoch: common::EpochNo(1),
        source_schema: "public",
        source_table: "orders",
        source_columns: &[],
        schema_version: common::SchemaVersionNo(1),
        kind: common::Kind::Stream,
        lsn_start: common::Lsn::new(0x10),
        lsn_end: common::Lsn::new(0x20),
        speculative_commit_lsn: false,
    }
}

fn commit_lsns(db: &TableDb, ids: (i64, i64)) -> Vec<String> {
    let mut stmt = db
        .conn
        .prepare("SELECT _walrus_commit_lsn FROM orders_raw WHERE id IN (?, ?) ORDER BY id")
        .unwrap();
    stmt.query_map([ids.0, ids.1], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn raw_rows(db: &TableDb) -> i64 {
    db.conn
        .query_row("SELECT count(*) FROM orders_raw", [], |row| row.get(0))
        .unwrap()
}

fn ingest_markers(db: &TableDb) -> i64 {
    db.conn
        .query_row(
            "SELECT count(*) FROM \"_walrus_ingested_files\"",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

/// `TableDb` is `Send`. The apply worker moves one into `TableCtx` and then into a
/// `spawn_local` future; this test exercises the bound by moving the database across an OS thread.
#[test]
fn table_db_moves_across_a_thread() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(7))
        .unwrap();

    let handle = std::thread::spawn(move || db.schema_version().unwrap());
    let version = handle.join().unwrap();
    assert_eq!(
        version,
        common::SchemaVersionNo(7),
        "the schema version survives the thread move"
    );
}

/// Bootstrap reads the watermark BEFORE the seeding `ensure_tables*`, so "no watermark yet" must be
/// a value and not an error the caller has to paper over with a fallback version — a papered-over
/// read failure would answer with the registry version the pending-rebuild branch exists to avoid.
#[test]
fn stored_schema_version_reports_absence_without_an_error() {
    let db = TableDb::open(":memory:").unwrap();
    assert_eq!(
        db.stored_schema_version().unwrap(),
        None,
        "a brand-new file has no _walrus_meta to read"
    );

    db.ensure_tables(&orders(), common::SchemaVersionNo(4))
        .unwrap();
    assert_eq!(
        db.stored_schema_version().unwrap(),
        Some(common::SchemaVersionNo(4)),
        "the seeded watermark reads back"
    );
}

#[test]
fn owned_duckdb_handles_meet_the_blocking_pool_send_bound() {
    requires_send::<duckdb::Connection>();
    requires_send::<TableDb>();
    requires_spawn_blocking_bounds(|| 1_i64);
}

#[test]
fn reload_shadow_resumes_without_clearing_and_stays_hidden_until_publish() {
    let db = TableDb::open(":memory:").unwrap();
    let rel = orders();
    let plan = crate::plan::TablePlan::tier1(&rel).unwrap();
    let version = common::SchemaVersionNo(3);
    let reload_id = common::ReloadId(41);
    let publication_nonce = uuid::Uuid::from_u128(41);
    let start = "0/100".parse().unwrap();
    let end = "0/180".parse().unwrap();
    db.ensure_tables_planned(&plan, version).unwrap();
    db.conn()
        .execute(
            "INSERT INTO orders (id, status) VALUES (1, 'live'), (99, 'phantom')",
            [],
        )
        .unwrap();

    let BeginReload::Ready(build) = db
        .begin_reload_shadow(&plan, version, reload_id, start, end, publication_nonce)
        .unwrap()
    else {
        panic!("a new reload must create a shadow");
    };
    db.conn()
        .execute(
            &format!(
                "INSERT INTO \"{}\" (id, status) VALUES (1, 'dump'), (2, 'new')",
                build.shadow_table
            ),
            [],
        )
        .unwrap();

    let live: Vec<(i64, String)> = db
        .conn()
        .prepare("SELECT id, status FROM orders_current ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        live,
        vec![(1, "live".into()), (99, "phantom".into())],
        "an in-progress export never leaks into or clears the live view"
    );

    let resumed = db
        .begin_reload_shadow(&plan, version, reload_id, start, end, publication_nonce)
        .unwrap();
    assert_eq!(resumed, BeginReload::Ready(build.clone()));
    let shadow_rows: i64 = db
        .conn()
        .query_row(
            &format!("SELECT count(*) FROM \"{}\"", build.shadow_table),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        shadow_rows, 2,
        "a crash-redo resumes the deterministic shadow instead of clearing it"
    );

    db.seal_reload_at_h(reload_id, publication_nonce, end)
        .unwrap();
    assert!(db.publish_reload_shadow("orders", reload_id).unwrap());
    let published: Vec<(i64, String)> = db
        .conn()
        .prepare("SELECT id, status FROM orders_current ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(published, vec![(1, "dump".into()), (2, "new".into())]);
    assert_eq!(db.recorded_reload_id().unwrap(), Some(reload_id));
    let receipt = db.reload_build().unwrap().unwrap();
    assert_eq!(receipt.phase, super::ReloadPhase::Published);
    assert_eq!(receipt.publication_nonce, publication_nonce);
    assert!(
        !db.publish_reload_shadow("orders", reload_id).unwrap(),
        "a cutover retry is an idempotent no-op"
    );
    assert!(
        db.clear_reload_publication(reload_id, publication_nonce)
            .unwrap()
    );
    assert_eq!(db.reload_build().unwrap(), None);
}

#[test]
fn publishing_an_empty_shadow_clears_phantoms() {
    let db = TableDb::open(":memory:").unwrap();
    let rel = orders();
    let plan = crate::plan::TablePlan::tier1(&rel).unwrap();
    let version = common::SchemaVersionNo(1);
    let reload_id = common::ReloadId(42);
    let publication_nonce = uuid::Uuid::from_u128(42);
    let end = "0/240".parse().unwrap();
    db.ensure_tables_planned(&plan, version).unwrap();
    db.conn()
        .execute("INSERT INTO orders (id, status) VALUES (99, 'phantom')", [])
        .unwrap();

    db.begin_reload_shadow(
        &plan,
        version,
        reload_id,
        "0/200".parse().unwrap(),
        end,
        publication_nonce,
    )
    .unwrap();
    db.seal_reload_at_h(reload_id, publication_nonce, end)
        .unwrap();
    assert!(db.publish_reload_shadow("orders", reload_id).unwrap());

    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM orders_current", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        rows, 0,
        "the marker-delimited empty generation is data: it removes every phantom"
    );
}

#[test]
fn abandoning_a_reload_requires_the_exact_unpublished_identity() {
    let db = TableDb::open(":memory:").unwrap();
    let plan = crate::plan::TablePlan::tier1(&orders()).unwrap();
    let version = common::SchemaVersionNo(1);
    let reload_id = common::ReloadId(51);
    let nonce = uuid::Uuid::from_u128(51);
    let start = "0/300".parse().unwrap();
    let end = "0/340".parse().unwrap();
    db.ensure_tables_planned(&plan, version).unwrap();
    let BeginReload::Ready(build) = db
        .begin_reload_shadow(&plan, version, reload_id, start, end, nonce)
        .unwrap()
    else {
        panic!("fresh reload must create a shadow");
    };

    assert!(
        !db.abandon_reload_build(reload_id, uuid::Uuid::from_u128(52))
            .unwrap(),
        "a different publication nonce has no deletion authority"
    );
    assert!(db.reload_build().unwrap().is_some());
    assert!(db.abandon_reload_build(reload_id, nonce).unwrap());
    assert_eq!(db.reload_build().unwrap(), None);
    let shadow_raw = format!("{}_raw", build.shadow_table);
    let shadow_tables: i64 = db
        .conn()
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name IN (?, ?)",
            duckdb::params![build.shadow_table, shadow_raw],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(shadow_tables, 0);

    let BeginReload::Ready(_) = db
        .begin_reload_shadow(&plan, version, reload_id, start, end, nonce)
        .unwrap()
    else {
        panic!("abandoned reload can be rebuilt for this test");
    };
    db.seal_reload_at_h(reload_id, nonce, end).unwrap();
    assert!(db.publish_reload_shadow("orders", reload_id).unwrap());
    assert!(
        !db.abandon_reload_build(reload_id, nonce).unwrap(),
        "a published receipt is never removable by the building cleanup path"
    );
    assert_eq!(
        db.reload_build().unwrap().unwrap().phase,
        super::ReloadPhase::Published
    );
}

/// A `spill` file's per-row `commit_lsn` placeholder is overridden by the file's `lsn_end`
/// (the real commit LSN), while a non-spill file appends the per-row value verbatim.
#[test]
fn spill_override_stamps_lsn_end_but_verbatim_otherwise() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    // Local Parquet + JSON extraction need the json extension (no S3 here → configure_s3 is not called).
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();

    // A spill file: rows carry the placeholder `0000000000000064`, but the file committed at `…00C8`.
    let placeholder = "0000000000000064";
    let lsn_end = "00000000000000C8";
    let spill = write_local_fixture(dir.path(), "spill.parquet", (1, 2), placeholder);
    let n = db
        .append_parquet(
            "orders",
            common::ManifestId(1),
            &spill,
            common::SchemaVersionNo(1),
            Some(lsn_end),
        )
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        commit_lsns(&db, (1, 2)),
        vec![lsn_end, lsn_end],
        "a spill file's rows are stamped with the file's lsn_end, not the placeholder"
    );

    // A non-spill (verbatim) file: the per-row placeholder is preserved.
    let batch = write_local_fixture(dir.path(), "batch.parquet", (3, 4), placeholder);
    let n = db
        .append_parquet(
            "orders",
            common::ManifestId(2),
            &batch,
            common::SchemaVersionNo(1),
            None,
        )
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        commit_lsns(&db, (3, 4)),
        vec![placeholder, placeholder],
        "a non-spill file keeps its verbatim per-row commit_lsn"
    );

    // both files are schema_version 1 → the column list is DESCRIBEd once, cached, reused.
    assert_eq!(
        db.cached_schema_versions(),
        1,
        "two v1 files → one cached introspection, not per-file"
    );
}

#[allow(
    clippy::disallowed_methods,
    reason = "synchronous unit-test helper fingerprints a local temporary Parquet fixture"
)]
fn fixture_fingerprint(path: &str) -> (i64, Vec<u8>) {
    use sha2::Digest as _;
    let bytes = std::fs::read(path).unwrap();
    (
        i64::try_from(bytes.len()).unwrap(),
        sha2::Sha256::digest(bytes).to_vec(),
    )
}

#[test]
fn attested_parquet_metadata_and_row_count_are_checked_before_receipt_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("attested.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    let parquet = write_attested_fixture(dir.path(), "attested.parquet", "orders");
    let (size, sha) = fixture_fingerprint(&parquet);
    let file = ManifestAppend {
        manifest_id: common::ManifestId(450),
        original_uri: "s3://walrus/attested.parquet",
        verified_uri: Some(&parquet),
        object_size: size,
        sha256: &sha,
        stream_group_id: None,
        schema_version: common::SchemaVersionNo(1),
        commit_lsn_override: None,
        destination_columns: None,
        expectation: Some(manifest_expectation(1)),
    };
    assert_eq!(db.append_manifest_unit("orders", &[file]).unwrap(), 1);
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (1, 1));

    let wrong_table = write_attested_fixture(dir.path(), "wrong-table.parquet", "other");
    let (wrong_size, wrong_sha) = fixture_fingerprint(&wrong_table);
    let wrong = ManifestAppend {
        manifest_id: common::ManifestId(451),
        original_uri: "s3://walrus/wrong-table.parquet",
        verified_uri: Some(&wrong_table),
        object_size: wrong_size,
        sha256: &wrong_sha,
        expectation: Some(manifest_expectation(1)),
        ..file
    };
    assert!(matches!(
        db.append_manifest_unit("orders", &[wrong]),
        Err(crate::error::TransformerError::ObjectIntegrity { .. })
    ));
    assert_eq!(
        (raw_rows(&db), ingest_markers(&db)),
        (1, 1),
        "invalid metadata rolls back both raw rows and the ingest receipt"
    );
}

#[test]
fn attested_row_count_mismatch_rolls_back_the_atomic_append() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("row-count.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    let parquet = write_attested_fixture(dir.path(), "one-row.parquet", "orders");
    let (size, sha) = fixture_fingerprint(&parquet);
    let file = ManifestAppend {
        manifest_id: common::ManifestId(460),
        original_uri: "s3://walrus/one-row.parquet",
        verified_uri: Some(&parquet),
        object_size: size,
        sha256: &sha,
        stream_group_id: None,
        schema_version: common::SchemaVersionNo(1),
        commit_lsn_override: None,
        destination_columns: None,
        expectation: Some(manifest_expectation(2)),
    };
    assert!(matches!(
        db.append_manifest_unit("orders", &[file]),
        Err(crate::error::TransformerError::ObjectIntegrity { .. })
    ));
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (0, 0));
}

#[test]
fn unchanged_toast_is_limited_to_distinct_non_key_source_columns_on_updates() {
    let dir = tempfile::tempdir().unwrap();
    let relation = orders();
    let expectation = ManifestExpectation {
        source_columns: &relation.columns,
        ..manifest_expectation(1)
    };
    let mut valid = attested_meta("orders");
    valid.op = common::Op::Update;
    valid.unchanged_toast = vec!["status".to_string()].into_boxed_slice();
    validate_attested_fixture(
        dir.path(),
        "toast-valid.parquet",
        &[valid.clone()],
        expectation,
    )
    .unwrap();

    let invalid = [
        ("toast-insert.parquet", {
            let mut meta = valid.clone();
            meta.op = common::Op::Insert;
            meta
        }),
        ("toast-key.parquet", {
            let mut meta = valid.clone();
            meta.unchanged_toast = vec!["id".to_string()].into_boxed_slice();
            meta
        }),
        ("toast-unknown.parquet", {
            let mut meta = valid.clone();
            meta.unchanged_toast = vec!["missing".to_string()].into_boxed_slice();
            meta
        }),
        ("toast-meta.parquet", {
            let mut meta = valid.clone();
            meta.unchanged_toast = vec!["walrus_extractor_meta".to_string()].into_boxed_slice();
            meta
        }),
        ("toast-duplicate.parquet", {
            let mut meta = valid;
            meta.unchanged_toast =
                vec!["status".to_string(), "status".to_string()].into_boxed_slice();
            meta
        }),
    ];
    for (name, meta) in invalid {
        assert_integrity_reason(
            validate_attested_fixture(dir.path(), name, &[meta], expectation),
            "invalid unchanged_toast column",
        );
    }
}

#[test]
fn unchanged_toast_is_validated_against_the_manifest_versions_source_relation() {
    let dir = tempfile::tempdir().unwrap();
    let v1 = orders();
    let mut v2 = v1.clone();
    v2.columns.push(PgColumn {
        name: "note".to_string(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    });
    let mut meta = attested_meta("orders");
    meta.op = common::Op::Update;
    meta.schema_version = common::SchemaVersionNo(2);
    meta.unchanged_toast = vec!["note".to_string()].into_boxed_slice();

    let v2_expectation = ManifestExpectation {
        source_columns: &v2.columns,
        schema_version: common::SchemaVersionNo(2),
        ..manifest_expectation(1)
    };
    validate_attested_fixture(
        dir.path(),
        "toast-v2-valid.parquet",
        &[meta.clone()],
        v2_expectation,
    )
    .unwrap();

    let incorrectly_bound_to_v1 = ManifestExpectation {
        source_columns: &v1.columns,
        ..v2_expectation
    };
    assert_integrity_reason(
        validate_attested_fixture(
            dir.path(),
            "toast-v2-against-v1.parquet",
            &[meta],
            incorrectly_bound_to_v1,
        ),
        "invalid unchanged_toast column",
    );
}

#[test]
fn snapshot_and_reload_rows_are_exact_fence_insert_images() {
    let dir = tempfile::tempdir().unwrap();
    let relation = orders();
    let fence = common::Lsn::new(0x30);
    for (name, kind) in [
        ("snapshot-valid.parquet", common::Kind::Snapshot),
        ("reload-valid.parquet", common::Kind::Reload),
    ] {
        let mut meta = attested_meta("orders");
        meta.kind = kind;
        meta.lsn = fence;
        meta.commit_lsn = fence;
        let expectation = ManifestExpectation {
            kind,
            lsn_start: fence,
            lsn_end: fence,
            source_columns: &relation.columns,
            ..manifest_expectation(1)
        };
        validate_attested_fixture(dir.path(), name, &[meta], expectation).unwrap();
    }

    let reload_expectation = ManifestExpectation {
        kind: common::Kind::Reload,
        lsn_start: fence,
        lsn_end: fence,
        source_columns: &relation.columns,
        ..manifest_expectation(1)
    };
    let mut update = attested_meta("orders");
    update.kind = common::Kind::Reload;
    update.op = common::Op::Update;
    update.lsn = fence;
    update.commit_lsn = fence;
    assert_integrity_reason(
        validate_attested_fixture(
            dir.path(),
            "reload-update.parquet",
            &[update],
            reload_expectation,
        ),
        "not an insert image",
    );

    let mut wrong_kind = attested_meta("orders");
    wrong_kind.lsn = fence;
    wrong_kind.commit_lsn = fence;
    assert_integrity_reason(
        validate_attested_fixture(
            dir.path(),
            "reload-wrong-kind.parquet",
            &[wrong_kind],
            reload_expectation,
        ),
        "metadata identity does not match",
    );

    let ranged_expectation = ManifestExpectation {
        lsn_end: common::Lsn::new(fence.as_u64() + 1),
        ..reload_expectation
    };
    let mut ranged = attested_meta("orders");
    ranged.kind = common::Kind::Reload;
    ranged.lsn = fence;
    ranged.commit_lsn = fence;
    assert_integrity_reason(
        validate_attested_fixture(
            dir.path(),
            "reload-range.parquet",
            &[ranged],
            ranged_expectation,
        ),
        "lies outside manifest",
    );
}

#[test]
fn batch_and_extractor_identity_are_nonempty_and_stable_within_each_object() {
    let dir = tempfile::tempdir().unwrap();
    let relation = orders();
    let expectation = ManifestExpectation {
        row_count: 2,
        source_columns: &relation.columns,
        ..manifest_expectation(2)
    };
    let first = attested_meta("orders");
    validate_attested_fixture(
        dir.path(),
        "identity-valid.parquet",
        &[first.clone(), first.clone()],
        expectation,
    )
    .unwrap();

    let mut changed_batch = first.clone();
    changed_batch.batch_id = "batch-2".to_string();
    assert_integrity_reason(
        validate_attested_fixture(
            dir.path(),
            "identity-batch-change.parquet",
            &[first.clone(), changed_batch],
            expectation,
        ),
        "changes batch_id",
    );

    let mut changed_extractor = first.clone();
    changed_extractor.extractor_instance = "extractor-1".to_string();
    assert_integrity_reason(
        validate_attested_fixture(
            dir.path(),
            "identity-extractor-change.parquet",
            &[first.clone(), changed_extractor],
            expectation,
        ),
        "changes extractor_instance",
    );

    for (name, mut meta) in [
        ("identity-empty-batch.parquet", first.clone()),
        ("identity-empty-extractor.parquet", first),
    ] {
        if name.contains("batch") {
            meta.batch_id.clear();
        } else {
            meta.extractor_instance.clear();
        }
        assert_integrity_reason(
            validate_attested_fixture(dir.path(), name, &[meta.clone(), meta], expectation),
            "empty batch_id or extractor_instance",
        );
    }
}

#[test]
fn speculative_spill_rows_use_the_begin_placeholder_and_stay_inside_the_txn_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let relation = orders();
    let begin = common::Lsn::new(0x40);
    let commit = common::Lsn::new(0x60);
    let expectation = ManifestExpectation {
        source_columns: &relation.columns,
        lsn_start: begin,
        lsn_end: commit,
        speculative_commit_lsn: true,
        ..manifest_expectation(1)
    };
    let mut valid = attested_meta("orders");
    valid.commit_lsn = begin;
    valid.lsn = common::Lsn::new(0x50);
    validate_attested_fixture(
        dir.path(),
        "spill-valid.parquet",
        &[valid.clone()],
        expectation,
    )
    .unwrap();

    let invalid = [
        ("spill-placeholder.parquet", {
            let mut meta = valid.clone();
            meta.commit_lsn = common::Lsn::new(begin.as_u64() + 1);
            meta
        }),
        ("spill-before-begin.parquet", {
            let mut meta = valid.clone();
            meta.lsn = common::Lsn::new(begin.as_u64() - 1);
            meta
        }),
        ("spill-after-commit.parquet", {
            let mut meta = valid;
            meta.lsn = common::Lsn::new(commit.as_u64() + 1);
            meta
        }),
    ];
    for (name, meta) in invalid {
        assert_integrity_reason(
            validate_attested_fixture(dir.path(), name, &[meta], expectation),
            "lies outside manifest",
        );
    }
}

#[test]
fn complete_stream_group_appends_rows_and_receipts_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("group.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    let first = write_local_fixture(dir.path(), "group-0.parquet", (1, 2), "1");
    let second = write_local_fixture(dir.path(), "group-1.parquet", (3, 4), "1");
    let (first_size, first_sha) = fixture_fingerprint(&first);
    let (second_size, second_sha) = fixture_fingerprint(&second);
    let files = [
        ManifestAppend {
            manifest_id: common::ManifestId(501),
            original_uri: "s3://walrus/group-0.parquet",
            verified_uri: Some(&first),
            object_size: first_size,
            sha256: &first_sha,
            stream_group_id: Some(77),
            schema_version: common::SchemaVersionNo(1),
            commit_lsn_override: None,
            destination_columns: None,
            expectation: None,
        },
        ManifestAppend {
            manifest_id: common::ManifestId(502),
            original_uri: "s3://walrus/group-1.parquet",
            verified_uri: Some(&second),
            object_size: second_size,
            sha256: &second_sha,
            stream_group_id: Some(77),
            schema_version: common::SchemaVersionNo(1),
            commit_lsn_override: None,
            destination_columns: None,
            expectation: None,
        },
    ];
    assert_eq!(db.append_manifest_unit("orders", &files).unwrap(), 4);
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (4, 2));
    assert_eq!(db.append_manifest_unit("orders", &files).unwrap(), 0);
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (4, 2));
}

#[test]
fn second_stream_group_file_failure_rolls_back_first_file_and_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("group-rollback.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    let first = write_local_fixture(dir.path(), "good.parquet", (1, 2), "1");
    let missing = dir
        .path()
        .join("missing.parquet")
        .to_string_lossy()
        .into_owned();
    let (first_size, first_sha) = fixture_fingerprint(&first);
    let files = [
        ManifestAppend {
            manifest_id: common::ManifestId(601),
            original_uri: "s3://walrus/good.parquet",
            verified_uri: Some(&first),
            object_size: first_size,
            sha256: &first_sha,
            stream_group_id: Some(88),
            schema_version: common::SchemaVersionNo(1),
            commit_lsn_override: None,
            destination_columns: None,
            expectation: None,
        },
        ManifestAppend {
            manifest_id: common::ManifestId(602),
            original_uri: "s3://walrus/missing.parquet",
            verified_uri: Some(&missing),
            object_size: 1,
            sha256: &[9_u8; 32],
            stream_group_id: Some(88),
            schema_version: common::SchemaVersionNo(1),
            commit_lsn_override: None,
            destination_columns: None,
            expectation: None,
        },
    ];
    assert!(db.append_manifest_unit("orders", &files).is_err());
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (0, 0));
}

#[test]
fn replay_metadata_mismatch_never_reuses_an_ingest_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("receipt-mismatch.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    let parquet = write_local_fixture(dir.path(), "receipt.parquet", (1, 2), "1");
    let (size, sha) = fixture_fingerprint(&parquet);
    let file = ManifestAppend {
        manifest_id: common::ManifestId(701),
        original_uri: "s3://walrus/receipt.parquet",
        verified_uri: Some(&parquet),
        object_size: size,
        sha256: &sha,
        stream_group_id: None,
        schema_version: common::SchemaVersionNo(1),
        commit_lsn_override: None,
        destination_columns: None,
        expectation: None,
    };
    db.append_manifest_unit("orders", &[file]).unwrap();
    let wrong_sha = [4_u8; 32];
    let changed = ManifestAppend {
        verified_uri: None,
        sha256: &wrong_sha,
        ..file
    };
    assert!(matches!(
        db.ingest_receipt_state(&changed),
        Err(crate::error::TransformerError::ManifestInvariant { .. })
    ));
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "synchronous unit test removes its local fixture to prove replay does not reopen it"
)]
fn fresh_raw_is_a_heap_and_uri_replay_skips_the_parquet_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();

    assert!(!db.raw_has_primary_key("orders").unwrap());
    assert!(!db.has_legacy_replay_fence());
    let parquet = write_local_fixture(dir.path(), "once.parquet", (1, 2), "1");
    assert_eq!(
        db.append_parquet(
            "orders",
            common::ManifestId(101),
            &parquet,
            common::SchemaVersionNo(1),
            None,
        )
        .unwrap(),
        2
    );
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (2, 1));

    // An exact receipt retry does not reopen the object. Manifest id and URI are both immutable;
    // reusing either with different metadata is rejected by the production unit API.
    std::fs::remove_file(dir.path().join("once.parquet")).unwrap();
    assert_eq!(
        db.append_parquet(
            "orders",
            common::ManifestId(101),
            &parquet,
            common::SchemaVersionNo(1),
            None,
        )
        .unwrap(),
        0
    );
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (2, 1));
}

#[test]
fn marker_failure_rolls_back_the_raw_append() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    let parquet = write_local_fixture(dir.path(), "rollback.parquet", (1, 2), "1");

    // Force the SECOND statement in append_parquet (the marker insert) to fail. If the file append
    // and marker were separate auto-commits, the two raw rows would leak through this failure.
    db.conn
        .execute_batch(
            "DROP TABLE \"_walrus_ingested_files\"; \
             CREATE TABLE \"_walrus_ingested_files\" (s3_uri VARCHAR PRIMARY KEY, \
                 manifest_id BIGINT NOT NULL CHECK (manifest_id > 0), \
                 object_size BIGINT, sha256 VARCHAR, stream_group_id BIGINT);",
        )
        .unwrap();
    let error = db
        .append_parquet(
            "orders",
            common::ManifestId(-1),
            &parquet,
            common::SchemaVersionNo(1),
            None,
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("record ingest receipt"),
        "{error:?}"
    );
    assert_eq!(
        (raw_rows(&db), ingest_markers(&db)),
        (0, 0),
        "the marker failure rolls the raw insert back with it"
    );
}

#[test]
fn legacy_primary_key_absorbs_the_upgrade_replay_then_migrates_losslessly() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    // Pre-ledger on-disk shape. `ensure_tables` must recognize it without removing its only replay
    // fence before a possibly pre-appended control row has had a chance to replay.
    db.conn
        .execute_batch(
            "CREATE TABLE orders_raw (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR, \
                 _walrus_op VARCHAR, _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR, \
                 _walrus_extractor_processed_at VARCHAR, \
                 PRIMARY KEY (id, _walrus_extractor_processed_at, _walrus_lsn));",
        )
        .unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();
    assert!(db.has_legacy_replay_fence());

    let parquet = write_local_fixture(dir.path(), "legacy.parquet", (1, 2), "1");
    let uri = common::sql::sql_literal(&parquet);
    // Simulate the old transformer having committed this file just before crashing, with the manifest
    // row still ready in Postgres and no new-ledger marker yet.
    db.conn
        .execute_batch(&format!(
            "INSERT INTO orders_raw \
                 SELECT id, status, walrus_extractor_meta, \
                    json_extract_string(walrus_extractor_meta, '$.op'), \
                    json_extract_string(walrus_extractor_meta, '$.commit_lsn'), \
                    json_extract_string(walrus_extractor_meta, '$.lsn'), \
                    json_extract_string(walrus_extractor_meta, '$.extractor_processed_at') \
                 FROM read_parquet('{uri}') ON CONFLICT DO NOTHING;"
        ))
        .unwrap();
    assert_eq!(raw_rows(&db), 2);

    assert_eq!(
        db.append_parquet(
            "orders",
            common::ManifestId(201),
            &parquet,
            common::SchemaVersionNo(1),
            None,
        )
        .unwrap(),
        0,
        "the compatibility PK absorbs a pre-ledger crash replay"
    );
    assert_eq!((raw_rows(&db), ingest_markers(&db)), (2, 1));

    assert!(db.migrate_legacy_replay_fence("orders").unwrap());
    assert!(!db.has_legacy_replay_fence());
    assert!(!db.raw_has_primary_key("orders").unwrap());
    assert_eq!(
        (raw_rows(&db), ingest_markers(&db)),
        (2, 1),
        "the transactional CTAS preserves both data and replay markers"
    );
    assert!(!db.migrate_legacy_replay_fence("orders").unwrap());
}

#[test]
fn expected_schema_is_cached_but_every_parquet_is_still_verified() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("cache.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    let parquet = write_local_fixture(dir.path(), "columns.parquet", (1, 2), "0");

    let initial = db
        .columns_for("orders", &parquet, &parquet, common::SchemaVersionNo(1))
        .unwrap();
    let hit = db
        .columns_for("orders", &parquet, &parquet, common::SchemaVersionNo(1))
        .unwrap();
    assert_eq!(initial, hit);

    let miss = db
        .columns_for("orders", &parquet, &parquet, common::SchemaVersionNo(2))
        .unwrap();
    assert_eq!(&*miss, &*initial);
    assert_eq!(
        db.cached_schema_versions(),
        2,
        "the miss can mutably insert after the hit borrow ends"
    );

    let extra = dir.path().join("extra.parquet");
    let extra_uri = extra.to_string_lossy().replace('\'', "''");
    let writer = duckdb::Connection::open_in_memory().unwrap();
    writer
        .execute_batch(&format!(
            "CREATE TABLE fixture (id INTEGER, status VARCHAR, unexpected VARCHAR, \
             walrus_extractor_meta VARCHAR); \
             INSERT INTO fixture VALUES (1, 'a', 'must-not-be-dropped', '{{}}'); \
             COPY fixture TO '{extra_uri}' (FORMAT PARQUET);"
        ))
        .unwrap();
    assert!(matches!(
        db.columns_for(
            "orders",
            &extra_uri,
            "s3://walrus/extra.parquet",
            common::SchemaVersionNo(1),
        ),
        Err(crate::error::TransformerError::ObjectIntegrity { .. })
    ));
}

#[test]
fn registry_schemas_validate_a_mixed_version_group_against_an_additive_raw_superset() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("additive.duckdb")).unwrap();
    let v1 = orders();
    let mut v2 = v1.clone();
    v2.columns.push(PgColumn {
        name: "note".to_string(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    });
    db.ensure_tables(&v1, common::SchemaVersionNo(1)).unwrap();

    // Reproduce the post-reconcile/restart shape: DuckDB appends an additive source column after
    // Walrus's promoted raw columns, while no historical schema expectation is cached yet.
    db.conn
        .execute_batch("ALTER TABLE orders_raw ADD COLUMN note VARCHAR;")
        .unwrap();
    db.cache_staged_schema(
        common::SchemaVersionNo(1),
        &crate::plan::TablePlan::tier1(&v1).unwrap(),
    )
    .unwrap();
    db.cache_staged_schema(
        common::SchemaVersionNo(2),
        &crate::plan::TablePlan::tier1(&v2).unwrap(),
    )
    .unwrap();

    let before = dir.path().join("before.parquet");
    let before_uri = before.to_string_lossy().replace('\'', "''");
    let after = dir.path().join("after.parquet");
    let after_uri = after.to_string_lossy().replace('\'', "''");
    let writer = duckdb::Connection::open_in_memory().unwrap();
    writer
        .execute_batch(&format!(
            "CREATE TABLE before (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR); \
             INSERT INTO before VALUES (1, 'before-ddl', '{{}}'); \
             COPY before TO '{before_uri}' (FORMAT PARQUET); \
             CREATE TABLE after (id INTEGER, status VARCHAR, note VARCHAR, \
             walrus_extractor_meta VARCHAR); \
             INSERT INTO after VALUES (2, 'after-ddl', 'present', '{{}}'); \
             COPY after TO '{after_uri}' (FORMAT PARQUET);"
        ))
        .unwrap();

    let (before_size, before_sha) = fixture_fingerprint(&before_uri);
    let (after_size, after_sha) = fixture_fingerprint(&after_uri);
    let files = [
        ManifestAppend {
            manifest_id: common::ManifestId(701),
            original_uri: &before_uri,
            verified_uri: Some(&before_uri),
            object_size: before_size,
            sha256: &before_sha,
            stream_group_id: Some(77),
            schema_version: common::SchemaVersionNo(1),
            commit_lsn_override: None,
            destination_columns: None,
            expectation: None,
        },
        ManifestAppend {
            manifest_id: common::ManifestId(702),
            original_uri: &after_uri,
            verified_uri: Some(&after_uri),
            object_size: after_size,
            sha256: &after_sha,
            stream_group_id: Some(77),
            schema_version: common::SchemaVersionNo(2),
            commit_lsn_override: None,
            destination_columns: None,
            expectation: None,
        },
    ];
    assert_eq!(db.append_manifest_unit("orders", &files).unwrap(), 2);
    let rows = db
        .conn
        .prepare("SELECT id, note FROM orders_raw ORDER BY id")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, None), (2, Some("present".to_string()))],
        "the historical child NULL-fills the additive raw column and the newer child preserves it"
    );
    assert_eq!(db.cached_schema_versions(), 2);
}

#[test]
fn explicit_destination_mapping_can_atomically_append_differently_named_file_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("rename.duckdb")).unwrap();
    let v1 = orders();
    let mut v2 = v1.clone();
    v2.columns[1].name = "state".to_string();
    db.ensure_tables(&v1, common::SchemaVersionNo(1)).unwrap();
    db.conn
        .execute_batch("ALTER TABLE orders_raw RENAME COLUMN status TO state;")
        .unwrap();
    db.cache_staged_schema(
        common::SchemaVersionNo(1),
        &crate::plan::TablePlan::tier1(&v1).unwrap(),
    )
    .unwrap();
    db.cache_staged_schema(
        common::SchemaVersionNo(2),
        &crate::plan::TablePlan::tier1(&v2).unwrap(),
    )
    .unwrap();

    let before = dir.path().join("rename-before.parquet");
    let before_uri = before.to_string_lossy().replace('\'', "''");
    let after = dir.path().join("rename-after.parquet");
    let after_uri = after.to_string_lossy().replace('\'', "''");
    let writer = duckdb::Connection::open_in_memory().unwrap();
    writer
        .execute_batch(&format!(
            "CREATE TABLE before (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR); \
             INSERT INTO before VALUES (1, 'old-name', '{{}}'); \
             COPY before TO '{before_uri}' (FORMAT PARQUET); \
             CREATE TABLE after (id INTEGER, state VARCHAR, walrus_extractor_meta VARCHAR); \
             INSERT INTO after VALUES (2, 'new-name', '{{}}'); \
             COPY after TO '{after_uri}' (FORMAT PARQUET);"
        ))
        .unwrap();
    let (before_size, before_sha) = fixture_fingerprint(&before_uri);
    let (after_size, after_sha) = fixture_fingerprint(&after_uri);
    let v1_destinations = vec![
        "id".to_string(),
        "state".to_string(),
        "walrus_extractor_meta".to_string(),
    ];
    let v2_destinations = v1_destinations.clone();
    let files = [
        ManifestAppend {
            manifest_id: common::ManifestId(711),
            original_uri: &before_uri,
            verified_uri: Some(&before_uri),
            object_size: before_size,
            sha256: &before_sha,
            stream_group_id: Some(78),
            schema_version: common::SchemaVersionNo(1),
            commit_lsn_override: None,
            destination_columns: Some(&v1_destinations),
            expectation: None,
        },
        ManifestAppend {
            manifest_id: common::ManifestId(712),
            original_uri: &after_uri,
            verified_uri: Some(&after_uri),
            object_size: after_size,
            sha256: &after_sha,
            stream_group_id: Some(78),
            schema_version: common::SchemaVersionNo(2),
            commit_lsn_override: None,
            destination_columns: Some(&v2_destinations),
            expectation: None,
        },
    ];
    assert_eq!(db.append_manifest_unit("orders", &files).unwrap(), 2);
    let states = db
        .conn
        .prepare("SELECT state FROM orders_raw ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(states, ["old-name", "new-name"]);
}

#[test]
fn table_sharding_is_deterministic_balanced_and_minimally_disruptive() {
    let epoch = common::EpochNo(42);
    let shards3 = std::num::NonZeroU32::new(3).unwrap();
    let shards4 = std::num::NonZeroU32::new(4).unwrap();
    let first = super::table_shard(epoch, "public", "orders", shards4);
    assert_eq!(
        first,
        super::table_shard(epoch, "public", "orders", shards4)
    );

    let mut counts = [0_u32; 4];
    for i in 0..2_000 {
        let table = format!("table_{i}");
        let old = super::table_shard(epoch, "public", &table, shards3);
        let new = super::table_shard(epoch, "public", &table, shards4);
        if new != old {
            assert_eq!(new, 3, "adding one rendezvous node only moves rows to it");
        }
        counts[usize::try_from(new).unwrap()] += 1;
    }
    assert!(
        counts.iter().all(|count| *count > 350),
        "every shard receives a useful share: {counts:?}"
    );
}
