use super::*;
use common::{PgColumn, ReplicaIdentity, Tier, TypeDescriptor, TypeMeta, oids};

fn relation(columns: Vec<PgColumn>) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

fn column(name: &str, type_oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid,
        type_modifier: -1,
        is_key,
    }
}

fn range_descriptor(column: &str) -> TypeDescriptor {
    TypeDescriptor {
        column: column.to_string(),
        pg_type_oid: oids::INT4RANGE,
        pg_type: "int4range".to_string(),
        tier: Tier::Two,
        arrow: "Struct/Decomposed".to_string(),
        duckdb: "IGNORED".to_string(),
        emit: [
            "lower:INT32",
            "upper:INT32",
            "lower_inc:BOOLEAN",
            "upper_inc:BOOLEAN",
            "empty:BOOLEAN",
        ]
        .map(|suffix| format!("{column}_{suffix}"))
        .into(),
        recombine: None,
        meta: TypeMeta::default(),
    }
}

fn registry(
    version: i64,
    relation: PgRelation,
    descriptors: Vec<TypeDescriptor>,
) -> RegistryVersion {
    RegistryVersion::new(
        SchemaVersion {
            version: SchemaVersionNo(version),
            relation,
        },
        descriptors,
    )
    .unwrap()
}

fn columns_of(db: &TableDb, table: &str) -> Vec<String> {
    let mut stmt = db
        .conn()
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )
        .unwrap();
    stmt.query_map([table], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn table_comment(db: &TableDb, table: &str) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT comment FROM duckdb_tables() WHERE table_name = ?",
            [table],
            |row| row.get(0),
        )
        .unwrap()
}

fn column_comment(db: &TableDb, table: &str, column: &str) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT comment FROM duckdb_columns() WHERE table_name = ? AND column_name = ?",
            [table, column],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn representative_additive_ddl_matches_the_reviewed_snapshot() {
    let changes = vec![
        AdditiveChange::AddColumn(column("note", oids::TEXT, false)),
        AdditiveChange::RenameColumn {
            position: 1,
            from: "status".to_string(),
            to: "state".to_string(),
        },
        AdditiveChange::WidenColumn {
            position: 0,
            name: "id".to_string(),
            new: column("id", oids::INT8, true),
        },
        AdditiveChange::RenameTable {
            from: "orders".to_string(),
            to: "archived_orders".to_string(),
        },
        AdditiveChange::Comment {
            target: CommentTarget::Table,
            text: Some("customer's archived orders".to_string()),
        },
    ];

    let sql = render_additive_inner("orders", &changes, None).unwrap();

    insta::assert_snapshot!("additive_ddl_plan", sql);
}

#[test]
fn structured_comment_snapshot_creates_updates_and_removes_mirror_metadata() {
    let registry = registry(
        1,
        relation(vec![
            column("id", oids::INT4, true),
            column("span", oids::INT4RANGE, false),
        ]),
        vec![range_descriptor("span")],
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&registry.shape.relation, &registry.descriptors).unwrap(),
        registry.shape.version,
    )
    .unwrap();

    let created = CommentSnapshot {
        table: Some("customer's orders".into()),
        columns: vec![
            ("id".into(), None),
            ("span".into(), Some("window v1".into())),
        ],
    };
    apply_comment_snapshot(db.conn(), "orders", &created, &registry).unwrap();
    assert_eq!(
        table_comment(&db, "orders").as_deref(),
        Some("customer's orders")
    );
    for sibling in [
        "span_lower",
        "span_upper",
        "span_lower_inc",
        "span_upper_inc",
        "span_empty",
    ] {
        assert_eq!(
            column_comment(&db, "orders", sibling).as_deref(),
            Some("window v1")
        );
    }
    assert_eq!(
        table_comment(&db, "orders_raw"),
        None,
        "raw table is metadata-free"
    );

    let updated = CommentSnapshot {
        table: Some("orders v2".into()),
        columns: vec![
            ("id".into(), None),
            ("span".into(), Some("window v2".into())),
        ],
    };
    apply_comment_snapshot(db.conn(), "orders", &updated, &registry).unwrap();
    assert_eq!(table_comment(&db, "orders").as_deref(), Some("orders v2"));
    assert_eq!(
        column_comment(&db, "orders", "span_lower").as_deref(),
        Some("window v2")
    );

    let removed = CommentSnapshot {
        table: None,
        columns: vec![("id".into(), None), ("span".into(), None)],
    };
    apply_comment_snapshot(db.conn(), "orders", &removed, &registry).unwrap();
    assert_eq!(table_comment(&db, "orders"), None);
    assert_eq!(column_comment(&db, "orders", "span_lower"), None);
}

#[test]
fn registry_version_requires_complete_descriptor_identity() {
    let shape = SchemaVersion {
        version: SchemaVersionNo(1),
        relation: relation(vec![
            column("id", oids::INT4, true),
            column("span", oids::INT4RANGE, false),
        ]),
    };
    let descriptor = range_descriptor("span");

    assert!(matches!(
        RegistryVersion::new(
            shape.clone(),
            vec![descriptor.clone(), descriptor.clone()]
        ),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("duplicate descriptors")
    ));

    let mut unknown = descriptor.clone();
    unknown.column = "missing".to_string();
    assert!(matches!(
        RegistryVersion::new(shape.clone(), vec![unknown]),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("descriptor for unknown column")
    ));

    let mut wrong_oid = descriptor;
    wrong_oid.pg_type_oid = oids::INT4;
    assert!(matches!(
        RegistryVersion::new(shape, vec![wrong_oid]),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("has type OID")
    ));
}

#[test]
fn registry_reconcile_requires_both_structural_snapshots() {
    let old = registry(
        1,
        relation(vec![column("id", oids::INT4, true)]),
        Vec::new(),
    );
    let new = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("note", oids::TEXT, false),
        ]),
        Vec::new(),
    );

    assert!(matches!(
        require_registry_pair(SchemaVersionNo(1), SchemaVersionNo(2), None, Some(new)),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("missing source version 1")
    ));
    assert!(matches!(
        require_registry_pair(SchemaVersionNo(1), SchemaVersionNo(2), Some(old), None),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("missing destination version 2")
    ));
}

#[test]
fn registry_reconcile_renames_every_tier2_sibling_on_mirror_and_raw() {
    let old = registry(
        1,
        relation(vec![
            column("id", oids::INT4, true),
            column("span", oids::INT4RANGE, false),
        ]),
        vec![range_descriptor("span")],
    );
    let new = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("time_window", oids::INT4RANGE, false),
        ]),
        vec![range_descriptor("time_window")],
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&old.shape.relation, &old.descriptors).unwrap(),
        old.shape.version,
    )
    .unwrap();
    db.conn()
        .execute(
            "INSERT INTO orders_raw \
             (id, span_lower, span_upper, span_lower_inc, span_upper_inc, span_empty) \
             VALUES (1, 2, 9, true, false, false)",
            [],
        )
        .unwrap();

    let diff = diff(&old.shape, &new.shape).unwrap();
    db.in_txn("test Tier-2 rename", |conn| {
        apply_additive_registry(conn, "orders", &diff.additive, &old, &new)
    })
    .unwrap();

    for table in ["orders", "orders_raw"] {
        let columns = columns_of(&db, table);
        for suffix in ["lower", "upper", "lower_inc", "upper_inc", "empty"] {
            assert!(
                columns.contains(&format!("time_window_{suffix}")),
                "{table} is missing renamed Tier-2 sibling {suffix}: {columns:?}"
            );
            assert!(!columns.contains(&format!("span_{suffix}")));
        }
    }
    let values: (i32, i32, bool, bool, bool) = db
        .conn()
        .query_row(
            "SELECT time_window_lower, time_window_upper, time_window_lower_inc, \
                    time_window_upper_inc, time_window_empty \
             FROM orders_raw WHERE id = 1",
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
    assert_eq!(values, (2, 9, true, false, false));
}

#[test]
fn registry_reconcile_adds_tier2_physical_shape_and_drops_only_its_mirror() {
    let old = registry(
        1,
        relation(vec![column("id", oids::INT4, true)]),
        Vec::new(),
    );
    let added = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("span", oids::INT4RANGE, false),
        ]),
        vec![range_descriptor("span")],
    );
    let dropped = registry(
        3,
        relation(vec![column("id", oids::INT4, true)]),
        Vec::new(),
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&old.shape.relation, &old.descriptors).unwrap(),
        old.shape.version,
    )
    .unwrap();

    let add = diff(&old.shape, &added.shape).unwrap();
    apply_additive_registry(db.conn(), "orders", &add.additive, &old, &added).unwrap();
    for table in ["orders", "orders_raw"] {
        let columns = columns_of(&db, table);
        assert!(columns.contains(&"span_lower".to_string()), "{columns:?}");
        assert!(columns.contains(&"span_empty".to_string()), "{columns:?}");
        assert!(!columns.contains(&"span".to_string()), "{columns:?}");
    }

    let drop = diff(&added.shape, &dropped.shape).unwrap();
    apply_destructive_registry(&db, "orders", &drop.destructive, &added, &dropped).unwrap();
    let mirror = columns_of(&db, "orders");
    let raw = columns_of(&db, "orders_raw");
    assert!(!mirror.iter().any(|name| name.starts_with("span_")));
    assert!(raw.contains(&"span_lower".to_string()), "{raw:?}");
    assert!(raw.contains(&"span_empty".to_string()), "{raw:?}");
}

#[test]
fn registry_reconcile_quarantines_non_scalar_lossy_shape_before_mutation() {
    let old = registry(
        1,
        relation(vec![
            column("id", oids::INT4, true),
            column("span", oids::INT4RANGE, false),
        ]),
        vec![range_descriptor("span")],
    );
    let new = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("span", oids::INT4, false),
        ]),
        Vec::new(),
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&old.shape.relation, &old.descriptors).unwrap(),
        old.shape.version,
    )
    .unwrap();
    let before_mirror = columns_of(&db, "orders");
    let before_raw = columns_of(&db, "orders_raw");
    let diff = diff(&old.shape, &new.shape).unwrap();

    assert!(matches!(
        apply_destructive_registry(&db, "orders", &diff.destructive, &old, &new),
        Err(TransformerError::Quarantine { reason, .. })
            if reason.contains("non-1:1 physical shape")
    ));
    assert_eq!(columns_of(&db, "orders"), before_mirror);
    assert_eq!(columns_of(&db, "orders_raw"), before_raw);
}

#[test]
fn registry_reconcile_rejects_drop_then_name_reuse_before_aliasing_raw_history() {
    let v1 = registry(
        1,
        relation(vec![
            column("id", oids::INT4, true),
            column("retired", oids::TEXT, false),
            column("live", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let v2 = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("live", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let readded = registry(
        3,
        relation(vec![
            column("id", oids::INT4, true),
            column("live", oids::TEXT, false),
            column("retired", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let renamed = registry(
        3,
        relation(vec![
            column("id", oids::INT4, true),
            column("retired", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&v1.shape.relation, &v1.descriptors).unwrap(),
        v1.shape.version,
    )
    .unwrap();
    let drop = diff(&v1.shape, &v2.shape).unwrap();
    apply_destructive_registry(&db, "orders", &drop.destructive, &v1, &v2).unwrap();
    assert!(columns_of(&db, "orders_raw").contains(&"retired".to_string()));

    let readd_diff = diff(&v2.shape, &readded.shape).unwrap();
    assert!(matches!(
        validate_registry_step(&db, "orders", &readd_diff, &v2, &readded),
        Err(TransformerError::Quarantine { reason, .. })
            if reason.contains("reuses retained raw column name(s) retired")
    ));
    assert!(matches!(
        validate_retained_raw_names(&db, "orders", &v2, &renamed),
        Err(TransformerError::Quarantine { reason, .. })
            if reason.contains("reuses retained raw column name(s) retired")
    ));
    assert_eq!(
        columns_of(&db, "orders"),
        ["id", "live", "_applied_commit_lsn", "_applied_lsn"]
    );
    assert!(columns_of(&db, "orders_raw").contains(&"retired".to_string()));
}

#[test]
fn shrink_diff_rejects_a_renamed_or_changed_survivor() {
    let old = SchemaVersion {
        version: SchemaVersionNo(1),
        relation: relation(vec![
            column("id", oids::INT4, true),
            column("first", oids::TEXT, false),
            column("second", oids::TEXT, false),
        ]),
    };
    let renamed_while_dropping = SchemaVersion {
        version: SchemaVersionNo(2),
        relation: relation(vec![
            column("id", oids::INT4, true),
            column("renamed", oids::TEXT, false),
        ]),
    };
    assert!(matches!(
        diff(&old, &renamed_while_dropping),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("not an ordered old column")
    ));

    let changed_while_dropping = SchemaVersion {
        version: SchemaVersionNo(2),
        relation: relation(vec![
            column("id", oids::INT4, true),
            column("second", oids::INT4, false),
        ]),
    };
    assert!(matches!(
        diff(&old, &changed_while_dropping),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("changes surviving column")
    ));
}

#[test]
fn registry_step_rejects_mixed_additive_and_destructive_before_mutation() {
    let old = registry(
        1,
        relation(vec![
            column("id", oids::INT4, true),
            column("value", oids::INT4, false),
        ]),
        Vec::new(),
    );
    let new = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("value", oids::TEXT, false),
            column("note", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&old.shape.relation, &old.descriptors).unwrap(),
        old.shape.version,
    )
    .unwrap();
    let before_mirror = columns_of(&db, "orders");
    let before_raw = columns_of(&db, "orders_raw");
    let diff = diff(&old.shape, &new.shape).unwrap();
    assert!(!diff.additive.is_empty() && !diff.destructive.is_empty());

    assert!(matches!(
        validate_registry_step(&db, "orders", &diff, &old, &new),
        Err(TransformerError::Quarantine { reason, .. })
            if reason.contains("mixes additive and destructive")
    ));
    assert_eq!(columns_of(&db, "orders"), before_mirror);
    assert_eq!(columns_of(&db, "orders_raw"), before_raw);
}

#[test]
fn registry_step_rejects_ambiguous_name_substitution_before_mutation() {
    let old = registry(
        1,
        relation(vec![
            column("id", oids::INT4, true),
            column("status", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let new = registry(
        2,
        relation(vec![
            column("id", oids::INT4, true),
            column("state", oids::TEXT, false),
        ]),
        Vec::new(),
    );
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&old.shape.relation, &old.descriptors).unwrap(),
        old.shape.version,
    )
    .unwrap();
    let before_mirror = columns_of(&db, "orders");
    let before_raw = columns_of(&db, "orders_raw");
    let diff = diff(&old.shape, &new.shape).unwrap();

    assert!(matches!(
        validate_registry_step(&db, "orders", &diff, &old, &new),
        Err(TransformerError::Quarantine { reason, .. })
            if reason.contains("genuine RENAME and same-statement DROP+ADD")
    ));
    assert_eq!(columns_of(&db, "orders"), before_mirror);
    assert_eq!(columns_of(&db, "orders_raw"), before_raw);
}

#[test]
fn registry_step_rejects_unexplained_descriptor_emit_drift() {
    let rel = relation(vec![
        column("id", oids::INT4, true),
        column("span", oids::INT4RANGE, false),
    ]);
    let old = registry(1, rel.clone(), vec![range_descriptor("span")]);
    let mut changed_descriptor = range_descriptor("span");
    changed_descriptor.emit[0] = "span_different_lower:INT32".to_string();
    let new = registry(2, rel, vec![changed_descriptor]);
    let db = TableDb::open(":memory:").unwrap();
    db.ensure_tables_planned(
        &TablePlan::from_registry(&old.shape.relation, &old.descriptors).unwrap(),
        old.shape.version,
    )
    .unwrap();
    let before_mirror = columns_of(&db, "orders");
    let before_raw = columns_of(&db, "orders_raw");
    let diff = diff(&old.shape, &new.shape).unwrap();
    assert!(diff.additive.is_empty() && diff.destructive.is_empty());

    assert!(matches!(
        validate_registry_step(&db, "orders", &diff, &old, &new),
        Err(TransformerError::Quarantine { reason, .. })
            if reason.contains("changes physical emit plan at unchanged position")
    ));
    assert_eq!(columns_of(&db, "orders"), before_mirror);
    assert_eq!(columns_of(&db, "orders_raw"), before_raw);
}
