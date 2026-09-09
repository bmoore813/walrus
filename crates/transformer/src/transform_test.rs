use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity, Tier, TypeDescriptor, TypeMeta, oids};

fn relation() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
    }
}

#[test]
fn an_all_pk_table_self_assigns_its_key_in_the_matched_update() {
    let transform = TransformSql::from_relation(&relation()).unwrap();

    let sql = transform.render(Lsn::ZERO, None);
    assert!(sql.contains("id = s.id"), "got {sql}");
}

#[test]
fn a_column_less_relation_renders_instead_of_panicking() {
    // `CREATE TABLE t ()` is legal in Postgres, so a published relation can arrive with no columns
    // at all — neither key nor non-key. Rendering must still produce a string; DuckDB then rejects
    // the degenerate MERGE as a classified error rather than the apply loop aborting.
    let empty = PgRelation {
        columns: Vec::new(),
        ..relation()
    };
    let transform = TransformSql::from_relation(&empty).unwrap();

    let sql = transform.render(Lsn::ZERO, None);
    let stamps_only = "_applied_commit_lsn = s._walrus_commit_lsn";
    assert!(sql.contains(stamps_only), "got {sql}");
}

#[test]
fn the_wipe_and_tuple_bound_render_only_for_a_present_boundary() {
    // The truncate boundary is ONE `Option<TruncateBoundary>`: absent renders neither the wipe nor
    // the window predicate; present renders both from the SAME pair, so there is no half-resolved
    // state in between for `render` to second-guess. (`> ('` is unique to the tuple bound — the
    // per-PK guard compares against `t._applied_*`, never a quoted literal.)
    let transform = TransformSql::from_relation(&relation()).unwrap();
    let wipe = r#"DELETE FROM "orders";"#;
    let bound = "> ('0000000000000064', '0000000000000065')";

    let absent = transform.render(Lsn::ZERO, None);
    assert!(!absent.contains(wipe), "got {absent}");
    assert!(!absent.contains("> ('"), "got {absent}");

    let boundary = TruncateBoundary {
        ct: Lsn::new(0x64),
        lt: Lsn::new(0x65),
    };
    let present = transform.render(Lsn::ZERO, Some(boundary));
    assert!(present.contains(wipe), "got {present}");
    assert!(present.contains(bound), "got {present}");
}

#[test]
fn the_prune_target_is_the_typed_cdc_log_not_the_mirror() {
    // `compaction::prune_raw` DELETEs from whatever this names, so handing it the mirror would erase
    // every current row. The name comes back tagged `Raw`, which makes that swap a type error rather
    // than a review question — and it is the only suffix derivation on the path.
    let transform = TransformSql::from_relation(&relation()).unwrap();

    let raw: DuckTable<Raw> = transform.to_raw();
    assert_eq!(transform.table(), "orders");
    assert_eq!(raw.as_str(), "orders_raw");
}

#[test]
fn a_broken_raw_table_is_an_error_not_an_absent_truncate_boundary() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let transform = TransformSql::from_relation(&relation()).unwrap();

    let err = transform.latest_truncate(&conn, Lsn::ZERO).unwrap_err();
    assert!(matches!(err, TransformerError::Duck { .. }), "got {err:?}");
}

#[derive(Clone, Copy, Debug)]
enum TransformPath {
    Incremental,
    DuckLake,
    Rebuild,
}

impl TransformPath {
    fn apply(self, conn: &duckdb::Connection, transform: &TransformSql) {
        self.apply_after(conn, transform, Lsn::ZERO);
    }

    fn apply_after(self, conn: &duckdb::Connection, transform: &TransformSql, after: Lsn) {
        let sql = match self {
            Self::Incremental => transform.render(after, None),
            Self::DuckLake => transform.render_ducklake(after, None),
            Self::Rebuild => transform.render_rebuild(None),
        };
        conn.execute_batch(&sql).unwrap();
    }
}

fn lifecycle_db() -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE lifecycle (
             id INTEGER PRIMARY KEY, body VARCHAR,
             _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
             _applied_lsn VARCHAR DEFAULT '0000000000000000');
         CREATE TABLE lifecycle_raw (
             id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
             _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);",
    )
    .unwrap();
    conn
}

fn seed_lifecycle_change(
    conn: &duckdb::Connection,
    id: i32,
    op: char,
    commit_lsn: u64,
    row_lsn: u64,
    body: Option<&str>,
) {
    conn.execute(
        "INSERT INTO lifecycle_raw VALUES (?, ?, '{}', ?, ?, ?)",
        duckdb::params![
            id,
            body,
            op.to_string(),
            hex_lsn(commit_lsn),
            hex_lsn(row_lsn),
        ],
    )
    .unwrap();
}

fn lifecycle_rows(conn: &duckdb::Connection) -> Vec<(i32, String, String, String)> {
    let mut statement = conn
        .prepare(
            "SELECT id, body, _applied_commit_lsn, _applied_lsn
             FROM lifecycle ORDER BY id",
        )
        .unwrap();
    statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap()
}

#[test]
fn delete_insert_lifecycle_matrix_uses_the_latest_row_lsn_on_every_transform_path() {
    let rel = relation_with("lifecycle", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [
        TransformPath::Incremental,
        TransformPath::DuckLake,
        TransformPath::Rebuild,
    ] {
        let conn = lifecycle_db();
        conn.execute_batch(
            "INSERT INTO lifecycle (id, body) VALUES
                 (1, 'old'),
                 (3, 'old'),
                 (5, 'old');",
        )
        .unwrap();

        // Every change shares one commit LSN, while physical insertion order is intentionally
        // scrambled. Only the per-key row LSN may decide each lifecycle's final state.
        for (id, op, row_lsn, body) in [
            (6, 'i', 55, Some("C")),
            (1, 'i', 20, Some("replacement")),
            (2, 'd', 21, None),
            (5, 'i', 24, Some("A")),
            (3, 'd', 12, None),
            (1, 'd', 30, None),
            (6, 'd', 25, None),
            (4, 'i', 13, Some("A")),
            (5, 'd', 54, None),
            (2, 'i', 11, Some("A")),
            (3, 'i', 22, Some("fresh")),
            (1, 'd', 10, None),
            (6, 'd', 45, None),
            (5, 'd', 14, None),
            (4, 'd', 23, None),
            (2, 'i', 31, Some("B")),
            (5, 'i', 44, Some("B")),
            (5, 'd', 34, None),
            (6, 'i', 15, Some("A")),
            (6, 'i', 35, Some("B")),
        ] {
            seed_lifecycle_change(&conn, id, op, 100, row_lsn, body);
        }

        path.apply(&conn, &transform);
        let expected = vec![
            (2, "B".to_string(), hex_lsn(100), hex_lsn(31)),
            (3, "fresh".to_string(), hex_lsn(100), hex_lsn(22)),
            (6, "C".to_string(), hex_lsn(100), hex_lsn(55)),
        ];
        assert_eq!(
            lifecycle_rows(&conn),
            expected,
            "wrong lifecycle winners on {path:?}"
        );

        path.apply(&conn, &transform);
        assert_eq!(
            lifecycle_rows(&conn),
            expected,
            "replaying the same tail changed the result on {path:?}"
        );
    }
}

#[test]
fn final_delete_at_an_already_applied_commit_lsn_is_not_skipped() {
    let rel = relation_with("lifecycle", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::DuckLake] {
        let conn = lifecycle_db();
        conn.execute("INSERT INTO lifecycle (id, body) VALUES (7, 'old')", [])
            .unwrap();
        seed_lifecycle_change(&conn, 7, 'd', 100, 10, None);
        seed_lifecycle_change(&conn, 7, 'i', 100, 20, Some("replacement"));

        path.apply(&conn, &transform);
        assert_eq!(
            lifecycle_rows(&conn),
            vec![(7, "replacement".to_string(), hex_lsn(100), hex_lsn(20))],
            "the intermediate insert should win on {path:?}"
        );

        // Simulate a same-commit tail straddling two transformer passes. The inclusive commit-LSN floor
        // must re-examine the earlier rows, and the later row LSN must promote the final tombstone.
        seed_lifecycle_change(&conn, 7, 'd', 100, 30, None);
        path.apply_after(&conn, &transform, Lsn::new(100));
        assert!(
            lifecycle_rows(&conn).is_empty(),
            "the final same-commit delete was skipped on {path:?}"
        );
    }
}

fn descriptor(column: &str, oid: u32, duckdb: &str, emit: &[&str]) -> TypeDescriptor {
    TypeDescriptor {
        column: column.into(),
        pg_type_oid: oid,
        pg_type: column.into(),
        tier: Tier::Two,
        arrow: "test".into(),
        duckdb: duckdb.into(),
        emit: emit.iter().map(ToString::to_string).collect(),
        recombine: None,
        meta: TypeMeta::default(),
    }
}

fn relation_with(table: &str, value: &str, value_oid: u32) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: table.into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: oids::INT4,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: value.into(),
                type_oid: value_oid,
                type_modifier: -1,
                is_key: false,
            },
        ],
    }
}

fn hex_lsn(value: u64) -> String {
    Lsn::new(value).to_string()
}

#[test]
fn representative_transform_renderings_match_reviewed_snapshots() {
    let scalar = TransformSql::from_relation(&relation()).unwrap();
    insta::assert_snapshot!(
        "scalar_incremental_transform",
        scalar.render(Lsn::ZERO, None)
    );

    let toast = TransformSql::from_relation(&relation_with("notes", "body", oids::TEXT)).unwrap();
    insta::assert_snapshot!(
        "toast_ducklake_transform_with_truncate",
        toast.render_ducklake(
            Lsn::new(0x2A),
            Some(TruncateBoundary {
                ct: Lsn::new(0x30),
                lt: Lsn::new(0x31),
            }),
        )
    );

    let interval_relation = relation_with("events", "elapsed", oids::INTERVAL);
    let elapsed = descriptor(
        "elapsed",
        oids::INTERVAL,
        "INTERVAL",
        &[
            "elapsed_months:INT32",
            "elapsed_days:INT32",
            "elapsed_micros:INT64",
        ],
    );
    let interval_plan =
        crate::plan::TablePlan::from_registry(&interval_relation, &[elapsed]).unwrap();
    let interval = TransformSql::from_plan(&interval_plan);
    insta::assert_snapshot!(
        "interval_full_rebuild_transform",
        interval.render_rebuild(None)
    );
}

#[test]
fn tier2_recombine_resolves_the_source_sentinel_in_incremental_and_rebuild_paths() {
    let rel = relation_with("events", "elapsed", oids::INTERVAL);
    let elapsed = descriptor(
        "elapsed",
        oids::INTERVAL,
        "INTERVAL",
        &[
            "elapsed_months:INT32",
            "elapsed_days:INT32",
            "elapsed_micros:INT64",
        ],
    );
    let plan = crate::plan::TablePlan::from_registry(&rel, &[elapsed]).unwrap();
    let transform = TransformSql::from_plan(&plan);

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE events (
                 id INTEGER PRIMARY KEY, elapsed INTERVAL,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE events_raw (
                 id INTEGER, elapsed_months INTEGER, elapsed_days INTEGER, elapsed_micros BIGINT,
                 walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);",
        )
        .unwrap();
        // Deliberately wrong fallback: successful resolution must use the earlier raw setter.
        conn.execute(
            "INSERT INTO events (id, elapsed) VALUES (1, to_days(99))",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO events_raw VALUES
                 (1, 2, 3, 4, '{}', 'i', ?, ?),
                 (1, NULL, NULL, NULL, '{\"unchanged_toast\":[\"elapsed\"]}', 'u', ?, ?)",
            duckdb::params![hex_lsn(100), hex_lsn(1), hex_lsn(100), hex_lsn(2)],
        )
        .unwrap();

        path.apply(&conn, &transform);

        let resolved: bool = conn
            .query_row(
                "SELECT elapsed = to_months(2) + to_days(3) + to_microseconds(4)
                 FROM events WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(resolved, "Tier-2 recombine failed on selected path");
    }
}

#[test]
fn timetz_recombine_backscan_uses_the_original_source_name_in_both_paths() {
    let rel = relation_with("schedule", "at", oids::TIMETZ);
    let at = descriptor(
        "at",
        oids::TIMETZ,
        "TIME WITH TIME ZONE",
        &["at_micros:INT64", "at_offset:INT32"],
    );
    let plan = crate::plan::TablePlan::from_registry(&rel, &[at]).unwrap();
    let transform = TransformSql::from_plan(&plan);
    let prior = "list_value(make_timetz(r.at_micros, r.at_offset))";

    for sql in [
        transform.render(Lsn::ZERO, None),
        transform.render_rebuild(None),
    ] {
        assert!(sql.contains(prior), "got {sql}");
        assert!(sql.contains(r#"LIKE '%"at"%'"#), "got {sql}");
        assert!(!sql.contains(r#"LIKE '%"at_micros"%'"#), "got {sql}");
    }
}

#[test]
fn tier2_flat_siblings_resolve_the_source_sentinel_in_incremental_and_rebuild_paths() {
    let rel = relation_with("ranges", "span", oids::INT4RANGE);
    let span = descriptor(
        "span",
        oids::INT4RANGE,
        "IGNORED",
        &[
            "span_lower:INT32",
            "span_upper:INT32",
            "span_lower_inc:BOOLEAN",
            "span_upper_inc:BOOLEAN",
            "span_empty:BOOLEAN",
        ],
    );
    let plan = crate::plan::TablePlan::from_registry(&rel, &[span]).unwrap();
    let transform = TransformSql::from_plan(&plan);

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE ranges (
                 id INTEGER PRIMARY KEY, span_lower INTEGER, span_upper INTEGER,
                 span_lower_inc BOOLEAN, span_upper_inc BOOLEAN, span_empty BOOLEAN,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE ranges_raw (
                 id INTEGER, span_lower INTEGER, span_upper INTEGER,
                 span_lower_inc BOOLEAN, span_upper_inc BOOLEAN, span_empty BOOLEAN,
                 walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);
             INSERT INTO ranges VALUES
                 (1, 90, 99, FALSE, FALSE, TRUE, '0000000000000000', '0000000000000000');",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO ranges_raw VALUES
                 (1, 10, 20, TRUE, FALSE, FALSE, '{}', 'i', ?, ?),
                 (1, NULL, NULL, NULL, NULL, NULL,
                  '{\"unchanged_toast\":[\"span\"]}', 'u', ?, ?)",
            duckdb::params![hex_lsn(100), hex_lsn(1), hex_lsn(100), hex_lsn(2)],
        )
        .unwrap();

        path.apply(&conn, &transform);

        let values: (i32, i32, bool, bool, bool) = conn
            .query_row(
                "SELECT span_lower, span_upper, span_lower_inc, span_upper_inc, span_empty
                 FROM ranges WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(values, (10, 20, true, false, false));
    }
}

#[test]
fn a_found_real_null_wins_over_the_mirror_fallback_in_both_paths() {
    let rel = relation_with("notes", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                 id INTEGER PRIMARY KEY, body VARCHAR,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE notes_raw (
                 id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);
             INSERT INTO notes (id, body) VALUES (1, 'stale mirror value');",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO notes_raw VALUES
                 (1, NULL, '{}', 'u', ?, ?),
                 (1, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?)",
            duckdb::params![hex_lsn(100), hex_lsn(1), hex_lsn(100), hex_lsn(2)],
        )
        .unwrap();

        path.apply(&conn, &transform);

        let is_null: bool = conn
            .query_row("SELECT body IS NULL FROM notes WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(is_null, "a found real NULL was replaced by the fallback");
    }
}

#[test]
fn key_change_unchanged_toast_resolves_through_the_paired_old_key_delete() {
    let rel = relation_with("keyed_notes", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE keyed_notes (
                 id INTEGER PRIMARY KEY, body VARCHAR,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE keyed_notes_raw (
                 id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);
             INSERT INTO keyed_notes (id, body) VALUES (1, 'inherited');",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO keyed_notes_raw VALUES
                 (1, 'inherited', '{}', 'i', ?, ?),
                 (1, NULL, '{}', 'd', ?, ?),
                 (2, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?)",
            duckdb::params![
                hex_lsn(90),
                hex_lsn(1),
                hex_lsn(100),
                hex_lsn(2),
                hex_lsn(100),
                hex_lsn(2),
            ],
        )
        .unwrap();

        path.apply(&conn, &transform);

        let rows: i64 = conn
            .query_row("SELECT count(*) FROM keyed_notes", [], |row| row.get(0))
            .unwrap();
        let body: String = conn
            .query_row("SELECT body FROM keyed_notes WHERE id = 2", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1, "the synthetic delete must remove the old key");
        assert_eq!(body, "inherited");
    }
}

#[test]
fn key_change_unchanged_toast_falls_back_to_the_old_key_mirror_after_raw_pruning() {
    let rel = relation_with("pruned_notes", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        for expected in [Some("mirror-only value"), None] {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE pruned_notes (
                     id INTEGER PRIMARY KEY, body VARCHAR,
                     _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                     _applied_lsn VARCHAR DEFAULT '0000000000000000');
                 CREATE TABLE pruned_notes_raw (
                     id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                     _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pruned_notes (id, body) VALUES (1, ?)",
                [expected],
            )
            .unwrap();
            // The historical setter for id=1 has been compacted away. Only the paired key move
            // remains in raw, so resolution must address the current mirror by OLD key.
            conn.execute(
                "INSERT INTO pruned_notes_raw VALUES
                     (1, NULL, '{}', 'd', ?, ?),
                     (2, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?)",
                duckdb::params![hex_lsn(100), hex_lsn(1), hex_lsn(100), hex_lsn(1)],
            )
            .unwrap();

            path.apply(&conn, &transform);

            let body: Option<String> = conn
                .query_row("SELECT body FROM pruned_notes WHERE id = 2", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(body.as_deref(), expected);
        }
    }
}

#[test]
fn chained_pruned_key_moves_carry_the_value_across_transform_cycles() {
    let rel = relation_with("moving_notes", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE moving_notes (
                 id INTEGER PRIMARY KEY, body VARCHAR,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE moving_notes_raw (
                 id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);
             INSERT INTO moving_notes (id, body) VALUES (1, 'crosses both moves');",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO moving_notes_raw VALUES
                 (1, NULL, '{}', 'd', ?, ?),
                 (2, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?)",
            duckdb::params![hex_lsn(100), hex_lsn(1), hex_lsn(100), hex_lsn(1)],
        )
        .unwrap();
        path.apply(&conn, &transform);

        // Compact all evidence of A→B, then move B→C with another unchanged marker. The only
        // surviving setter is the current B mirror row produced by the previous cycle.
        conn.execute("DELETE FROM moving_notes_raw", []).unwrap();
        conn.execute(
            "INSERT INTO moving_notes_raw VALUES
                 (2, NULL, '{}', 'd', ?, ?),
                 (3, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?)",
            duckdb::params![hex_lsn(200), hex_lsn(1), hex_lsn(200), hex_lsn(1)],
        )
        .unwrap();
        path.apply(&conn, &transform);

        let result: (i64, String) = conn
            .query_row(
                "SELECT count(*), any_value(body) FROM moving_notes WHERE id = 3",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(result, (1, "crosses both moves".to_string()));
    }
}

#[test]
fn chained_key_moves_in_one_tail_walk_back_to_the_mirror_value() {
    let rel = relation_with("tail_moves", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [
        TransformPath::Incremental,
        TransformPath::DuckLake,
        TransformPath::Rebuild,
    ] {
        for expected in [Some("crosses the whole tail"), None] {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE tail_moves (
                     id INTEGER PRIMARY KEY, body VARCHAR,
                     _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                     _applied_lsn VARCHAR DEFAULT '0000000000000000');
                 CREATE TABLE tail_moves_raw (
                     id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                     _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO tail_moves (id, body) VALUES (1, ?)",
                [expected],
            )
            .unwrap();
            // Both key moves are in the same untransformed tail and all historical setters have
            // already been pruned. C inherits from B, which itself inherited from the mirror's A;
            // a one-hop old-key fallback cannot recover this value.
            conn.execute(
                "INSERT INTO tail_moves_raw VALUES
                     (1, NULL, '{}', 'd', ?, ?),
                     (2, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?),
                     (2, NULL, '{}', 'd', ?, ?),
                     (3, NULL, '{\"unchanged_toast\":[\"body\"]}', 'u', ?, ?)",
                duckdb::params![
                    hex_lsn(100),
                    hex_lsn(1),
                    hex_lsn(100),
                    hex_lsn(1),
                    hex_lsn(200),
                    hex_lsn(1),
                    hex_lsn(200),
                    hex_lsn(1),
                ],
            )
            .unwrap();

            path.apply(&conn, &transform);

            let rows: i64 = conn
                .query_row("SELECT count(*) FROM tail_moves", [], |row| row.get(0))
                .unwrap();
            let body: Option<String> = conn
                .query_row("SELECT body FROM tail_moves WHERE id = 3", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(rows, 1, "old keys survived on {path:?}");
            assert_eq!(body.as_deref(), expected, "wrong value on {path:?}");
        }
    }
}

#[test]
fn a_recombined_identity_partitions_incremental_raw_rows_by_its_emit_expression() {
    let rel = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "interval_keys".into(),
        replica_identity: ReplicaIdentity::Index,
        columns: vec![
            PgColumn {
                name: "elapsed".into(),
                type_oid: oids::INTERVAL,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "body".into(),
                type_oid: oids::TEXT,
                type_modifier: -1,
                is_key: false,
            },
        ],
    };
    let elapsed = descriptor(
        "elapsed",
        oids::INTERVAL,
        "INTERVAL",
        &[
            "elapsed_months:INT32",
            "elapsed_days:INT32",
            "elapsed_micros:INT64",
        ],
    );
    let plan = crate::plan::TablePlan::from_registry(&rel, &[elapsed]).unwrap();
    let transform = TransformSql::from_plan(&plan);

    for sql in [
        transform.render(Lsn::ZERO, None),
        transform.render_ducklake(Lsn::ZERO, None),
    ] {
        assert!(
            sql.contains(
                "PARTITION BY to_months(raw_winner.elapsed_months) + \
                 to_days(raw_winner.elapsed_days) + \
                 to_microseconds(raw_winner.elapsed_micros)"
            ),
            "got {sql}"
        );
        assert!(!sql.contains("PARTITION BY elapsed"), "got {sql}");
    }
}

#[test]
fn nullable_full_identity_components_match_without_leaving_a_ghost() {
    let rel = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "full_notes".into(),
        replica_identity: ReplicaIdentity::Full,
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: oids::INT4,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "nullable_identity".into(),
                type_oid: oids::TEXT,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "body".into(),
                type_oid: oids::TEXT,
                type_modifier: -1,
                is_key: true,
            },
        ],
    };
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        // No DuckDB PK here: PostgreSQL FULL identity components may be nullable even though the
        // source table also has a separate real primary key.
        conn.execute_batch(
            "CREATE TABLE full_notes (
                 id INTEGER, nullable_identity VARCHAR, body VARCHAR,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE full_notes_raw (
                 id INTEGER, nullable_identity VARCHAR, body VARCHAR,
                 walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);
             INSERT INTO full_notes (id, nullable_identity, body) VALUES (1, NULL, 'keep me');",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO full_notes_raw VALUES
                 (1, NULL, 'keep me', '{}', 'u', ?, ?)",
            duckdb::params![hex_lsn(100), hex_lsn(1)],
        )
        .unwrap();

        path.apply(&conn, &transform);

        let result: (i64, Option<String>) = conn
            .query_row(
                "SELECT count(*), any_value(body) FROM full_notes",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(result, (1, Some("keep me".to_string())));
    }
}

#[test]
fn an_exact_tuple_tie_prefers_the_non_delete_winner() {
    let rel = relation_with("tie_notes", "body", oids::TEXT);
    let transform = TransformSql::from_relation(&rel).unwrap();

    for path in [TransformPath::Incremental, TransformPath::Rebuild] {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE tie_notes (
                 id INTEGER PRIMARY KEY, body VARCHAR,
                 _applied_commit_lsn VARCHAR DEFAULT '0000000000000000',
                 _applied_lsn VARCHAR DEFAULT '0000000000000000');
             CREATE TABLE tie_notes_raw (
                 id INTEGER, body VARCHAR, walrus_extractor_meta VARCHAR, _walrus_op VARCHAR,
                 _walrus_commit_lsn VARCHAR, _walrus_lsn VARCHAR);",
        )
        .unwrap();
        // Put the delete first so physical input order cannot accidentally provide the desired tie-break.
        conn.execute(
            "INSERT INTO tie_notes_raw VALUES
                 (1, NULL, '{}', 'd', ?, ?),
                 (1, 'survives', '{}', 'u', ?, ?)",
            duckdb::params![hex_lsn(100), hex_lsn(1), hex_lsn(100), hex_lsn(1)],
        )
        .unwrap();

        path.apply(&conn, &transform);

        let body: String = conn
            .query_row("SELECT body FROM tie_notes WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(body, "survives");
    }
}
