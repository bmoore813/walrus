use super::*;

#[test]
fn out_of_range_catalog_oid_is_reported_not_wrapped() {
    let raw = i64::from(u32::MAX) + 1;
    let err = catalog_oid(raw).unwrap_err();
    let message = format!("{err:#}");
    assert!(
        message.contains(&raw.to_string()),
        "error includes {raw}: {message}"
    );
}

#[test]
fn publication_actions_require_all_four_mutation_families() {
    let complete = PublicationActions {
        insert: true,
        update: true,
        delete: true,
        truncate: true,
    };
    assert!(require_publication_actions("walrus_pub", Some(complete)).is_ok());

    let incomplete = PublicationActions {
        delete: false,
        ..complete
    };
    let err = require_publication_actions("walrus_pub", Some(incomplete)).unwrap_err();
    assert!(matches!(
        err,
        PublicationCoverageIssue::DisabledOperations { .. }
    ));
    assert!(err.to_string().contains("DELETE"));
}

#[test]
fn target_restrictions_are_rejected_independently() {
    let full = PublicationTargetOptions {
        published: true,
        row_filter: false,
        column_list: false,
        row_level_security: false,
        topology_stable: true,
    };
    assert!(require_full_target("walrus_pub", "public", "orders", full).is_ok());

    let filtered = PublicationTargetOptions {
        row_filter: true,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", filtered),
        Err(PublicationCoverageIssue::RowFilter { .. })
    ));

    let projected = PublicationTargetOptions {
        column_list: true,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", projected),
        Err(PublicationCoverageIssue::ColumnList { .. })
    ));

    let policy_filtered = PublicationTargetOptions {
        row_level_security: true,
        ..full
    };
    let err = require_full_target("walrus_pub", "odd\"schema", "order lines", policy_filtered)
        .unwrap_err();
    assert!(matches!(
        &err,
        PublicationCoverageIssue::RowLevelSecurity { .. }
    ));
    assert!(
        err.to_string()
            .contains("ALTER TABLE \"odd\"\"schema\".\"order lines\" DISABLE ROW LEVEL SECURITY"),
        "RLS rejection carries safely quoted remediation: {err}"
    );

    let absent = PublicationTargetOptions {
        published: false,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", absent),
        Err(PublicationCoverageIssue::MissingTarget { .. })
    ));

    let topology_dependent = PublicationTargetOptions {
        topology_stable: false,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", topology_dependent),
        Err(PublicationCoverageIssue::TopologyDependent { .. })
    ));
}

#[test]
fn advisory_key_matches_the_source_migration_literal() {
    assert_eq!(PUBLICATION_DDL_GUARD_KEY, 8_602_276_002_106_929_250);
}

#[test]
fn catalog_fence_lock_identifiers_are_quoted_not_interpolated_raw() {
    assert_eq!(quote_identifier("ordinary"), "\"ordinary\"");
    assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
    assert_eq!(
        quote_identifier("public; DROP TABLE orders"),
        "\"public; DROP TABLE orders\""
    );
}

#[test]
fn baseline_comments_enrich_registry_json_without_changing_relation_shape() {
    let comments = RelationComments {
        schema: "public".into(),
        table: "orders".into(),
        table_comment: Some("customer orders".into()),
        columns: vec![("id".into(), None), ("status".into(), Some("state".into()))],
    };
    let relation = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "status".into(),
                type_oid: 25,
                type_modifier: -1,
                is_key: false,
            },
        ],
    };
    let mut json = serde_json::to_value(&relation).unwrap();
    comments.enrich_registry(&mut json).unwrap();

    assert_eq!(json["table_comment"], "customer orders");
    assert_eq!(json["columns"][0]["comment"], serde_json::Value::Null);
    assert_eq!(json["columns"][1]["comment"], "state");
    assert_eq!(
        serde_json::from_value::<PgRelation>(json).unwrap(),
        relation
    );
}
