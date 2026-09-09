use super::*;
use common::{PgColumn, ReplicaIdentity};

fn composite_rel() -> PgRelation {
    let col = |name: &str, is_key: bool| PgColumn {
        name: name.to_string(),
        type_oid: 25,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 1,
        schema: "public".to_string(),
        name: "customers".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("region", true), col("id", true), col("name", false)],
    }
}

#[test]
fn coordinator_and_workers_pin_canonical_postgres_text_output() {
    let clauses = [
        "SET LOCAL DateStyle = 'ISO, YMD';",
        "SET LOCAL IntervalStyle = 'postgres';",
        "SET LOCAL bytea_output = 'hex';",
        "SET LOCAL extra_float_digits = 3;",
        "SET LOCAL TimeZone = 'UTC';",
    ];
    assert_eq!(
        CANONICAL_COPY_GUCS_SQL,
        clauses.join(" "),
        "the reusable GUC block is the exact pg-to-arrow text contract"
    );

    let setups = [
        ("coordinator", coordinator_snapshot_setup_sql()),
        (
            "importing worker",
            snapshot_worker_setup_sql("00000003-0000001B-1", &composite_rel()).unwrap(),
        ),
    ];
    for (role, setup) in setups {
        for clause in clauses {
            assert_eq!(
                setup.matches(clause).count(),
                1,
                "{role} must apply canonical clause {clause:?} exactly once: {setup}"
            );
        }
    }
}

#[test]
fn full_copy_is_binary_streaming_and_has_no_sort_or_limit() {
    let sql = copy_sql(&composite_rel(), ScanRange::Full).unwrap();
    assert_eq!(
        sql,
        "COPY (SELECT _src.region::text, _src.id::text, _src.name::text \
         FROM ONLY \"public\".\"customers\" AS _src) TO STDOUT WITH (FORMAT BINARY)"
    );
    assert!(!sql.contains("ORDER BY"));
    assert!(!sql.contains("LIMIT"));
}

#[test]
fn bounded_copy_uses_a_half_open_ctid_page_interval() {
    let sql = copy_sql(
        &composite_rel(),
        ScanRange::Blocks {
            start: 8,
            end: Some(13),
        },
    )
    .unwrap();
    assert!(
        sql.contains("_src.ctid >= '(8,0)'::tid AND _src.ctid < '(13,0)'::tid"),
        "half-open physical interval: {sql}"
    );
}

#[test]
fn final_copy_range_is_open_ended() {
    let sql = copy_sql(
        &composite_rel(),
        ScanRange::Blocks {
            start: 99,
            end: None,
        },
    )
    .unwrap();
    assert!(sql.contains("WHERE _src.ctid >= '(99,0)'::tid"));
    assert!(!sql.contains("ctid <"));
}

#[test]
fn ctid_planner_covers_every_block_once_and_caps_tasks() {
    for (blocks, workers) in [(1, 8), (3, 1), (17, 2), (10_001, 7)] {
        let ranges = plan_ctid_ranges(blocks, NonZeroUsize::new(workers).unwrap());
        assert!(ranges.len() <= workers * RANGE_TASKS_PER_WORKER);
        let mut expected_start = 0;
        for (index, range) in ranges.iter().enumerate() {
            let ScanRange::Blocks { start, end } = *range else {
                panic!("heap planner returned a full-table range")
            };
            assert_eq!(start, expected_start, "ranges are contiguous");
            if index + 1 == ranges.len() {
                assert_eq!(end, None, "the final range is defensive/open-ended");
                expected_start = blocks;
            } else {
                let end = end.expect("only the final range is open-ended");
                assert!(end > start);
                expected_start = end;
            }
        }
        assert_eq!(expected_start, blocks);
    }
}

#[test]
fn router_memory_is_split_across_the_configured_copy_ceiling() {
    let total = NonZeroU64::new(64 * 1024 * 1024).unwrap();
    assert_eq!(
        router_batch_bytes(total, NonZeroUsize::new(16).unwrap()).get(),
        4 * 1024 * 1024
    );
    assert_eq!(
        router_batch_bytes(total, NonZeroUsize::new(2).unwrap()).get(),
        ROUTER_BATCH_BYTES_MAX,
        "one router never exceeds the fixed internal cap"
    );
    assert_eq!(
        router_batch_bytes(NonZeroU64::MIN, NonZeroUsize::new(8).unwrap()).get(),
        1,
        "a tiny diagnostic ceiling still makes forward progress"
    );
}

#[test]
fn reload_memory_admission_is_capped_by_configured_streams() {
    let configured_streams = NonZeroUsize::new(3).unwrap();
    let limit = reload_memory_worker_limit(
        NonZeroU64::new(512 * 1024 * 1024).unwrap(),
        configured_streams,
    );
    assert_eq!(limit, configured_streams);
    assert_eq!(ReloadWorkerAdmission::new(limit).available_permits(), 3);
}

#[test]
fn reload_memory_admission_uses_budget_floor_but_always_makes_progress() {
    let configured_streams = NonZeroUsize::new(8).unwrap();
    let budget_limited = reload_memory_worker_limit(
        NonZeroU64::new(100 * 1024 * 1024).unwrap(),
        configured_streams,
    );
    assert_eq!(budget_limited.get(), 3, "floor(100 MiB / 32 MiB)");
    assert_eq!(
        ReloadWorkerAdmission::new(budget_limited).available_permits(),
        3
    );

    let minimum = reload_memory_worker_limit(NonZeroU64::MIN, configured_streams);
    assert_eq!(minimum.get(), 1, "a tiny budget cannot deadlock reloads");
    assert_eq!(ReloadWorkerAdmission::new(minimum).available_permits(), 1);
}

#[test]
fn reload_memory_admission_clamps_counts_above_tokios_semaphore_limit() {
    let Some(over_limit) = Semaphore::MAX_PERMITS.checked_add(1) else {
        return;
    };
    let Some(over_limit) = NonZeroUsize::new(over_limit) else {
        return;
    };

    let admission = ReloadWorkerAdmission::new(over_limit);

    assert_eq!(admission.available_permits(), Semaphore::MAX_PERMITS);
}

#[test]
fn parquet_metadata_has_a_fixed_column_chunk_bound() {
    for columns in [1, 2, 127, 4_096, 8_192] {
        let row_groups = max_row_groups_per_object(columns);
        assert!(row_groups >= 1);
        assert!(
            columns.saturating_mul(row_groups) <= MAX_PARQUET_COLUMN_CHUNKS_PER_OBJECT.max(columns),
            "metadata bound for {columns} columns"
        );
    }
    assert_eq!(max_row_groups_per_object(0), 1);
}

#[test]
fn copy_sql_rejects_a_column_that_would_need_identifier_quotes() {
    let rel = PgRelation {
        schema: "odd\"schema".into(),
        name: "customer\"table".into(),
        columns: vec![PgColumn {
            name: "customer\"id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
        ..composite_rel()
    };

    let error = copy_sql(&rel, ScanRange::Full).unwrap_err();
    assert!(error.to_string().contains("validate reload COPY column"));
}

#[test]
fn copy_planning_does_not_depend_on_primary_key_order() {
    let mut rel = composite_rel();
    rel.replica_identity = ReplicaIdentity::Full;
    for column in &mut rel.columns {
        column.is_key = false;
    }
    assert!(copy_sql(&rel, ScanRange::Full).is_ok());
}

#[test]
fn schema_bump_after_snapshot_interrupts_with_new_version() {
    // A structural bump past the frozen version restarts; equal (metadata-only DDL never bumps
    // the registry) and a stale backwards read do not.
    assert_eq!(
        version_changed(common::SchemaVersionNo(1), Some(common::SchemaVersionNo(2))),
        Some(common::SchemaVersionNo(2)),
        "1 → 2 restarts"
    );
    assert_eq!(
        version_changed(common::SchemaVersionNo(1), Some(common::SchemaVersionNo(1))),
        None,
        "metadata-only: no restart"
    );
    assert_eq!(
        version_changed(common::SchemaVersionNo(2), Some(common::SchemaVersionNo(1))),
        None,
        "never restart backwards"
    );
    assert_eq!(
        version_changed(common::SchemaVersionNo(1), None),
        None,
        "no registry row: no restart"
    );
}

#[test]
fn durable_end_recovery_requires_exact_f_h_schema_and_request_identity() {
    let rel = composite_rel();
    let reload_id = ReloadId(91);
    let epoch = EpochNo(7);
    let schema_version = SchemaVersionNo(3);
    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let request_id = Uuid::from_u128(0x1234);
    let row = control::ReloadRow {
        reload_id,
        epoch,
        source_schema: rel.schema.clone(),
        source_table: rel.name.clone(),
        flavor: control::ReloadFlavor::Reload,
        source_request_id: Some(request_id),
        parent_request_id: None,
        scope: control::ReloadScope::Table,
        status: control::ReloadStatus::Exporting,
        chunk_no: 1,
        cursor_pk: Some(serde_json::json!(["42"])),
        start_lsn: Some(f),
        first_lsn: Some(f),
        final_lsn: None,
        schema_version: Some(schema_version),
        restart_count: 0,
        lease_holder: Some("extractor-a".to_string()),
        exporter_generation: 1,
        has_export_plan: true,
        error: None,
    };
    let markers = vec![
        control::ReloadMarkerRow {
            reload_id,
            kind: control::ReloadMarkerKind::Baseline,
            lsn: f,
            schema_version,
        },
        control::ReloadMarkerRow {
            reload_id,
            kind: control::ReloadMarkerKind::End,
            lsn: h,
            schema_version,
        },
    ];

    assert_eq!(
        validate_durable_end(&row, &markers, epoch, &rel, schema_version, f, request_id,).unwrap(),
        Some(h)
    );
    assert!(
        validate_durable_end(
            &row,
            &markers,
            epoch,
            &rel,
            schema_version,
            f,
            Uuid::from_u128(0x9999),
        )
        .unwrap_err()
        .to_string()
        .contains("request identity")
    );

    let mut missing_namespace = row.clone();
    missing_namespace.source_request_id = None;
    assert!(
        validate_durable_end(
            &missing_namespace,
            &markers,
            epoch,
            &rel,
            schema_version,
            f,
            request_id,
        )
        .unwrap_err()
        .to_string()
        .contains("no source-fence request namespace")
    );

    let mut wrong_baseline = markers;
    wrong_baseline[0].lsn = "0/101".parse().unwrap();
    assert!(
        validate_durable_end(
            &row,
            &wrong_baseline,
            epoch,
            &rel,
            schema_version,
            f,
            request_id,
        )
        .unwrap_err()
        .to_string()
        .contains("baseline marker")
    );
}

#[test]
fn restart_cap_zero_means_first_ddl_fails_the_reload() {
    // The controller consults the same pure cap check the control-layer restart uses.
    assert!(
        control::reload::restart_would_exceed_cap(0, 0),
        "cap 0 ⇒ the first mid-export DDL fails the reload"
    );
    assert!(
        !control::reload::restart_would_exceed_cap(0, 3),
        "with headroom the first DDL restarts instead"
    );
}
