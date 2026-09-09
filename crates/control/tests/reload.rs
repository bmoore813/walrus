#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the `table_reload` state machine.
//!
//! Same discipline as the manifest tests: tests normally run inside a rolled-back transaction and
//! namespace rows by a unique `epoch`, so runs are isolated and idempotent. The immediate
//! pool-reuse regression commits deliberately and deletes its epoch before/after. Statements that
//! provoke a real SQL error (the duplicate-request unique violation) run under a nested savepoint,
//! because a failed statement aborts the enclosing Postgres transaction.
#![cfg(feature = "integration")]

use common::{EpochNo, FailureClass, Lsn, ManifestId, ReloadId, SchemaVersionNo, UtcTimestamp};
use control::reload::{
    self, ExportRangePlan, ExportSnapshot, ExporterLease, ReloadFenceIdentity, ReloadFlavor,
    ReloadMarkerKind, ReloadScope, ReloadStatus, SourceReloadRequest,
};
use control::{
    ControlError, ManifestRow, NewManifestFile, NewStreamCommitPublication, PublishStreamOutcome,
    acquire_lease, claim_ready, connect, delete_claimed, ensure_checkpoint, insert_ready,
    publish_stream_commit, run_migrations,
};
use sqlx::Connection;
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};
use std::fmt::Write as _;
use std::time::Duration;
use uuid::Uuid;

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn pool() -> PgPool {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

fn exporter(reload_id: ReloadId, holder: &str, generation: i64) -> ExporterLease {
    ExporterLease {
        reload_id,
        holder: holder.to_string(),
        generation,
    }
}

/// A staged reload chunk file: `kind='reload'` carrying its `reload_id` (stamped `lsn = L_i`).
fn chunk_file(epoch: EpochNo, table: &str, reload_id: ReloadId, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/reload-{reload_id}-{lsn_end}.parquet"),
        kind: control::ManifestKind::Reload,
        row_count: 1,
        object_size: 1,
        sha256: vec![0; 32],
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: Some(reload_id),
    }
}

/// `lease_expiry` as a comparable number — the model omits the column by design (every time
/// comparison lives in SQL), so tests that care probe it directly.
async fn expiry_epoch(ex: impl sqlx::PgExecutor<'_>, reload_id: ReloadId) -> f64 {
    sqlx::query_scalar::<_, f64>(
        "SELECT extract(epoch FROM lease_expiry)::float8
         FROM public.walrus_table_reload WHERE reload_id = $1",
    )
    .bind(reload_id.0)
    .fetch_one(ex)
    .await
    .unwrap()
}

async fn assert_protocol_deauthorized(ex: impl sqlx::PgExecutor<'_>, setting: &str) {
    let value: Option<String> = sqlx::query_scalar("SELECT pg_catalog.current_setting($1, true)")
        .bind(setting)
        .fetch_one(ex)
        .await
        .unwrap();
    assert!(
        value.as_deref().unwrap_or_default().is_empty(),
        "protocol capability {setting} leaked with value {value:?}"
    );
}

async fn prime_protocol(ex: impl sqlx::PgExecutor<'_>, setting: &str) {
    let value: String = sqlx::query_scalar("SELECT pg_catalog.set_config($1, '2', true)")
        .bind(setting)
        .fetch_one(ex)
        .await
        .unwrap();
    assert_eq!(value, "2");
}

async fn assert_reload_evidence_rejected(
    conn: &mut PgConnection,
    reload_id: ReloadId,
    statement: &str,
    expected_constraint: &str,
) {
    let mut savepoint = conn.begin().await.unwrap();
    let error = sqlx::query(statement)
        .bind(reload_id.0)
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some(expected_constraint),
        "unexpected database guard for statement: {statement}"
    );
    savepoint.rollback().await.unwrap();
}

/// Complete an ordinary test attempt through the same explicit F/baseline/H protocol used by the
/// exporter. Tests that exercise invalid or mismatched markers call the lower-level functions
/// directly instead.
async fn finish_fenced(conn: &mut PgConnection, reload_id: ReloadId, h: Lsn) {
    let row = reload::get(&mut *conn, reload_id).await.unwrap().unwrap();
    let schema_version = row.schema_version.unwrap_or(SchemaVersionNo(1));
    let f = row.start_lsn.or(row.first_lsn).unwrap_or(h);
    let holder = row.lease_holder.clone().unwrap();
    let lease = row.exporter_lease(&holder).unwrap();
    let identity = ReloadFenceIdentity {
        request_id: row.source_request_id.or(row.parent_request_id),
        source_schema: &row.source_schema,
        source_table: &row.source_table,
        schema_version,
    };
    reload::record_start_fence(&mut *conn, reload_id, f, identity)
        .await
        .unwrap();
    if !row.has_export_plan {
        assert!(
            row.cursor_pk.is_none(),
            "a protocol-v2 fixture cannot seal legacy keyset progress"
        );
        begin_full_scan_plan(conn, &lease, f, schema_version).await;
    }
    let (file_count, row_count): (i64, i64) = sqlx::query_as(
        "SELECT count(*)::bigint, COALESCE(sum(row_count), 0)::bigint
         FROM public.walrus_file_manifest WHERE reload_id = $1",
    )
    .bind(reload_id.0)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let persisted_files = reload::get(&mut *conn, reload_id)
        .await
        .unwrap()
        .unwrap()
        .chunk_no;
    assert!(persisted_files <= file_count);
    for _ in persisted_files..file_count {
        reload::record_exported_file(&mut *conn, &lease, f, schema_version)
            .await
            .unwrap();
    }
    let range_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_table_reload_export_range WHERE reload_id = $1",
    )
    .bind(reload_id.0)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    if range_count == 1 {
        reload::record_export_range(&mut *conn, &lease, 0, file_count, row_count)
            .await
            .unwrap();
    }
    reload::seal_export(conn, &lease, f, schema_version)
        .await
        .unwrap();
    reload::record_end_marker(&mut *conn, reload_id, h, identity)
        .await
        .unwrap();
    reload::complete_export(&mut *conn, &lease, h)
        .await
        .unwrap();
}

async fn begin_full_scan_plan(
    conn: &mut PgConnection,
    lease: &ExporterLease,
    f: Lsn,
    schema_version: SchemaVersionNo,
) {
    let snapshot = format!("1:{}:", lease.reload_id.0 + 2);
    reload::begin_export_plan(
        conn,
        lease,
        f,
        schema_version,
        ExportSnapshot {
            identity: &snapshot,
            xmin: 1,
            xmax: lease.reload_id.0 + 2,
        },
        &[ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn protocol_capabilities_do_not_escape_a_caller_owned_transaction() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let missing = ReloadId(i64::MAX - 10);
    let lease = exporter(missing, "missing-exporter", 1);
    let fence = ReloadFenceIdentity {
        request_id: None,
        source_schema: "public",
        source_table: "missing_table",
        schema_version: SchemaVersionNo(1),
    };
    let f = Lsn::new(0x100);

    prime_protocol(&mut *tx, "walrus.reload_marker_protocol").await;
    reload::record_start_fence(&mut *tx, missing, f, fence)
        .await
        .unwrap_err();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_marker_protocol").await;

    prime_protocol(&mut *tx, "walrus.reload_marker_protocol").await;
    reload::record_end_marker(&mut *tx, missing, f, fence)
        .await
        .unwrap_err();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_marker_protocol").await;

    prime_protocol(&mut *tx, "walrus.reload_export_plan_protocol").await;
    reload::record_export_range(&mut *tx, &lease, 0, 0, 0)
        .await
        .unwrap_err();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_export_plan_protocol").await;

    prime_protocol(&mut *tx, "walrus.reload_export_complete_protocol").await;
    reload::complete_export(&mut *tx, &lease, f)
        .await
        .unwrap_err();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_export_complete_protocol").await;

    prime_protocol(&mut *tx, "walrus.manifest_delete_protocol").await;
    assert_eq!(delete_claimed(&mut *tx, &[]).await.unwrap(), 0);
    assert_protocol_deauthorized(&mut *tx, "walrus.manifest_delete_protocol").await;

    prime_protocol(&mut *tx, "walrus.manifest_delete_protocol").await;
    let deleted_count: i64 =
        sqlx::query_scalar(include_str!("../sql/postgres/queries/fail_purge_files.sql"))
            .bind(missing.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(deleted_count, 0);
    assert_protocol_deauthorized(&mut *tx, "walrus.manifest_delete_protocol").await;

    prime_protocol(&mut *tx, "walrus.reload_publication_adopt_protocol").await;
    assert!(
        reload::claim_publication(
            &mut *tx,
            EpochNo(i64::MAX - 10),
            "public",
            "missing_table",
            "missing-owner",
            1,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_publication_adopt_protocol").await;

    tx.rollback().await.unwrap();
}

/// Build a zero-file attempt with a valid plan of `range_count` rows. Tests below then replace the
/// plan through the explicit maintenance tripwire to model corrupted durable evidence without
/// disabling the production triggers.
async fn begin_empty_range_plan_fixture(
    conn: &mut PgConnection,
    epoch: EpochNo,
    table: &str,
    range_count: usize,
) -> (ReloadId, ExporterLease, Lsn, SchemaVersionNo) {
    assert!(range_count > 0);
    let id = reload::request(&mut *conn, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    let claimed = reload::claim_requested(&mut *conn, epoch, "extractor-plan-attestation", 600, 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].reload_id, id);
    let lease = claimed[0]
        .exporter_lease("extractor-plan-attestation")
        .unwrap();
    let f = Lsn::new(0x100);
    let schema_version = SchemaVersionNo(1);
    reload::record_start_fence(
        &mut *conn,
        id,
        f,
        ReloadFenceIdentity {
            request_id: claimed[0].parent_request_id,
            source_schema: "public",
            source_table: table,
            schema_version,
        },
    )
    .await
    .unwrap();

    let seed = if range_count == 1 {
        vec![ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }]
    } else {
        (0..range_count)
            .map(|index| {
                let range_no = i64::try_from(index).unwrap();
                ExportRangePlan {
                    range_no,
                    full_scan: false,
                    start_block: Some(range_no * 64),
                    end_block: (index + 1 < range_count).then_some((range_no + 1) * 64),
                }
            })
            .collect()
    };
    let snapshot = format!("{}:{}:", id.0, id.0 + 1);
    reload::begin_export_plan(
        conn,
        &lease,
        f,
        schema_version,
        ExportSnapshot {
            identity: &snapshot,
            xmin: id.0,
            xmax: id.0 + 1,
        },
        &seed,
    )
    .await
    .unwrap();
    (id, lease, f, schema_version)
}

async fn replace_empty_range_plan_fixture(
    conn: &mut PgConnection,
    lease: &ExporterLease,
    ranges: &[ExportRangePlan],
) {
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance', '2-delete', true)")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_reload_export_range WHERE reload_id = $1")
        .bind(lease.reload_id.0)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance', '', true)")
        .execute(&mut *conn)
        .await
        .unwrap();
    for range in ranges {
        sqlx::query("SELECT set_config('walrus.reload_export_plan_protocol', '2', true)")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO public.walrus_table_reload_export_range
               (reload_id, exporter_generation, range_no, full_scan, start_block, end_block)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(lease.reload_id.0)
        .bind(lease.generation)
        .bind(range.range_no)
        .bind(range.full_scan)
        .bind(range.start_block)
        .bind(range.end_block)
        .execute(&mut *conn)
        .await
        .unwrap();
        reload::record_export_range(&mut *conn, lease, range.range_no, 0, 0)
            .await
            .unwrap();
    }
}

async fn export_plan_is_attested(conn: &mut PgConnection, lease: &ExporterLease) -> bool {
    sqlx::query_scalar(
        "SELECT public.walrus_reload_export_plan_is_attested(
           reload_id, exporter_generation, export_range_count)
         FROM public.walrus_table_reload WHERE reload_id = $1",
    )
    .bind(lease.reload_id.0)
    .fetch_one(conn)
    .await
    .unwrap()
}

#[tokio::test]
async fn reload_boundaries_headers_and_markers_are_durable_immutable_evidence() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_011);
    let mut fabricated = tx.begin().await.unwrap();
    let error = sqlx::query(
        "INSERT INTO public.walrus_table_reload
           (epoch, source_schema, source_table, flavor, status,
            lease_holder, lease_expiry, exporter_generation, start_lsn, schema_version)
         VALUES ($1, 'public', 'fabricated_evidence', 'reload', 'exporting',
                 'extractor-a', now() + interval '1 minute', 1, '0/100', 1)",
    )
    .bind(epoch.0)
    .execute(&mut *fabricated)
    .await
    .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("table_reload_initial_evidence_pristine")
    );
    fabricated.rollback().await.unwrap();

    let mut fabricated_terminal = tx.begin().await.unwrap();
    let error = sqlx::query(
        "INSERT INTO public.walrus_table_reload
           (epoch, source_schema, source_table, flavor, status)
         VALUES ($1, 'public', 'fabricated_terminal', 'reload', 'failed')",
    )
    .bind(epoch.0)
    .execute(&mut *fabricated_terminal)
    .await
    .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint),
        Some("table_reload_initial_evidence_pristine")
    );
    fabricated_terminal.rollback().await.unwrap();

    let reload_id = reload::request(
        &mut *tx,
        epoch,
        "public",
        "immutable_evidence",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let row = reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease = row.exporter_lease("extractor-a").unwrap();
    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let schema_version = SchemaVersionNo(1);
    let identity = ReloadFenceIdentity {
        request_id: row.parent_request_id,
        source_schema: "public",
        source_table: "immutable_evidence",
        schema_version,
    };

    assert_reload_evidence_rejected(
        &mut tx,
        reload_id,
        "UPDATE public.walrus_table_reload
         SET start_lsn = '0/100', schema_version = 1
         WHERE reload_id = $1",
        "table_reload_start_fence_protocol_v2",
    )
    .await;
    reload::record_start_fence(&mut *tx, reload_id, f, identity)
        .await
        .unwrap();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_marker_protocol").await;

    for (statement, constraint) in [
        (
            "UPDATE public.walrus_table_reload SET source_table = 'rewritten' WHERE reload_id = $1",
            "table_reload_request_identity_immutable",
        ),
        (
            "UPDATE public.walrus_table_reload SET start_lsn = '0/101' WHERE reload_id = $1",
            "table_reload_start_lsn_immutable",
        ),
        (
            "UPDATE public.walrus_table_reload SET schema_version = 2 WHERE reload_id = $1",
            "table_reload_schema_version_immutable",
        ),
        (
            "UPDATE public.walrus_table_reload SET first_lsn = '0/101' WHERE reload_id = $1",
            "table_reload_first_lsn_matches_start",
        ),
        (
            "UPDATE public.walrus_table_reload_marker SET lsn = '0/101'
             WHERE reload_id = $1 AND marker_kind = 'baseline'",
            "table_reload_marker_immutable",
        ),
        (
            "DELETE FROM public.walrus_table_reload_marker
             WHERE reload_id = $1 AND marker_kind = 'baseline'",
            "table_reload_marker_removal",
        ),
        (
            "INSERT INTO public.walrus_table_reload_marker
               (reload_id, marker_kind, lsn, schema_version)
             VALUES ($1, 'end', '0/200', 1)",
            "table_reload_marker_insert_protocol_v2",
        ),
    ] {
        assert_reload_evidence_rejected(&mut tx, reload_id, statement, constraint).await;
    }

    begin_full_scan_plan(&mut tx, &lease, f, schema_version).await;
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_export_plan_protocol").await;
    assert_reload_evidence_rejected(
        &mut tx,
        reload_id,
        "UPDATE public.walrus_table_reload SET export_snapshot_xmax = export_snapshot_xmax + 1
         WHERE reload_id = $1",
        "table_reload_snapshot_header_immutable",
    )
    .await;

    reload::record_export_range(&mut *tx, &lease, 0, 0, 0)
        .await
        .unwrap();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_export_plan_protocol").await;
    reload::seal_export(&mut tx, &lease, f, schema_version)
        .await
        .unwrap();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_export_seal_protocol").await;
    for (statement, constraint) in [
        (
            "UPDATE public.walrus_table_reload SET export_row_count = export_row_count + 1
             WHERE reload_id = $1",
            "table_reload_export_seal_immutable",
        ),
        (
            "UPDATE public.walrus_table_reload SET chunk_no = chunk_no + 1 WHERE reload_id = $1",
            "table_reload_sealed_progress_immutable",
        ),
    ] {
        assert_reload_evidence_rejected(&mut tx, reload_id, statement, constraint).await;
    }

    reload::record_end_marker(&mut *tx, reload_id, h, identity)
        .await
        .unwrap();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_marker_protocol").await;
    reload::complete_export(&mut *tx, &lease, h).await.unwrap();
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_export_complete_protocol").await;
    assert_reload_evidence_rejected(
        &mut tx,
        reload_id,
        "UPDATE public.walrus_table_reload SET final_lsn = '0/201' WHERE reload_id = $1",
        "table_reload_final_lsn_immutable",
    )
    .await;

    // Explicit administrative teardown remains possible, but only inside its transaction-local
    // maintenance capability. Roll the deletion back so the enclosing lifecycle remains intact.
    let mut maintenance = tx.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *maintenance)
        .await
        .unwrap();
    let deleted = sqlx::query("DELETE FROM public.walrus_table_reload_marker WHERE reload_id = $1")
        .bind(reload_id.0)
        .execute(&mut *maintenance)
        .await
        .unwrap();
    assert_eq!(deleted.rows_affected(), 2);
    maintenance.rollback().await.unwrap();

    reload::record_start_fence(&mut *tx, reload_id, f, identity)
        .await
        .expect("exact baseline replay remains valid after export completion");
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_marker_protocol").await;
    reload::record_end_marker(&mut *tx, reload_id, h, identity)
        .await
        .expect("exact end-marker replay remains valid after export completion");
    assert_protocol_deauthorized(&mut *tx, "walrus.reload_marker_protocol").await;
    tx.rollback().await.unwrap();
}

/// Exercise the transformer's fenced publication path for a marker-complete attempt and return the
/// manifest rows it retired through H.
async fn publish_fenced(
    conn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    epoch: EpochNo,
    table: &str,
    reload_id: ReloadId,
) -> Vec<ManifestRow> {
    let owner = "transformer-test";
    let lease = acquire_lease(&mut **conn, epoch, "public", table, owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut **conn, epoch, "public", table)
        .await
        .unwrap();
    let publication = reload::claim_publication(
        &mut **conn,
        epoch,
        "public",
        table,
        owner,
        lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(publication.reload_id, reload_id);
    assert_protocol_deauthorized(&mut **conn, "walrus.reload_publication_adopt_protocol").await;
    let claimed =
        reload::claim_publication_ready(&mut **conn, &publication, owner, lease.fencing_token, 100)
            .await
            .unwrap();
    let ids = claimed.iter().map(|row| row.id).collect::<Vec<_>>();
    assert_eq!(
        delete_claimed(&mut **conn, &ids).await.unwrap(),
        ids.len() as u64
    );
    assert_protocol_deauthorized(&mut **conn, "walrus.manifest_delete_protocol").await;
    assert!(
        reload::seal_publication_if_drained(conn, &publication, owner, lease.fencing_token)
            .await
            .unwrap()
    );
    assert_protocol_deauthorized(&mut **conn, "walrus.manifest_seal_protocol").await;
    assert!(
        reload::finish_publication(&mut **conn, &publication, owner, lease.fencing_token)
            .await
            .unwrap()
    );
    claimed
}

#[tokio::test]
async fn logical_export_rejection_is_rolled_back_before_connection_reuse() {
    let dsn = control_dsn();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let observer = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .unwrap();
    let epoch = EpochNo(910_017);
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch.0)
    .execute(&pool)
    .await
    .unwrap();

    let predecessor_id = reload::request(
        &pool,
        epoch,
        "public",
        "rollback_predecessor",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let successor_id = reload::request(
        &pool,
        epoch,
        "public",
        "rollback_successor",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    reload::claim_requested(&pool, epoch, "extractor-a", 60, 2)
        .await
        .unwrap();
    let predecessor = reload::get(&pool, predecessor_id).await.unwrap().unwrap();
    let successor = reload::get(&pool, successor_id).await.unwrap().unwrap();
    let predecessor_lease = predecessor.exporter_lease("extractor-a").unwrap();
    let predecessor_f = Lsn::new(0x100);
    let version = SchemaVersionNo(1);
    reload::record_start_fence(
        &pool,
        predecessor_id,
        predecessor_f,
        ReloadFenceIdentity {
            request_id: predecessor.parent_request_id,
            source_schema: &predecessor.source_schema,
            source_table: &predecessor.source_table,
            schema_version: version,
        },
    )
    .await
    .unwrap();

    let stale_lease = ExporterLease {
        generation: predecessor_lease.generation.checked_add(1).unwrap(),
        ..predecessor_lease
    };
    let mut conn = pool.acquire().await.unwrap();
    let err = reload::begin_export_plan(
        &mut conn,
        &stale_lease,
        predecessor_f,
        version,
        ExportSnapshot {
            identity: "10:20:",
            xmin: 10,
            xmax: 20,
        },
        &[ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }],
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // Reuse the exact backend immediately. The rejected plan's rollback must reach
    // `ReadyForQuery` before this statement, otherwise F remains trapped in its old transaction.
    let successor_f = Lsn::new(0x200);
    reload::record_start_fence(
        &mut *conn,
        successor_id,
        successor_f,
        ReloadFenceIdentity {
            request_id: successor.parent_request_id,
            source_schema: &successor.source_schema,
            source_table: &successor.source_table,
            schema_version: version,
        },
    )
    .await
    .unwrap();
    let visible = reload::get(&observer, successor_id).await.unwrap().unwrap();
    assert_eq!(
        visible.start_lsn,
        Some(successor_f),
        "an independent connection sees the successor fence before backend reuse returns"
    );

    drop(conn);
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(epoch.0)
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn seal_attests_the_exact_physical_range_partition_in_postgres() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_019);

    let invalid_plans = [
        (
            "gap",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: Some(64),
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(65),
                    end_block: None,
                },
            ],
        ),
        (
            "overlap",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: Some(64),
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(63),
                    end_block: None,
                },
            ],
        ),
        (
            "swapped_order",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: false,
                    start_block: Some(64),
                    end_block: Some(128),
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: None,
                },
            ],
        ),
        (
            "early_open_end",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: None,
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(64),
                    end_block: None,
                },
            ],
        ),
        (
            "finite_final_end",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: Some(64),
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(64),
                    end_block: Some(128),
                },
            ],
        ),
        (
            "mixed_full_scan",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: true,
                    start_block: None,
                    end_block: None,
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: None,
                },
            ],
        ),
    ];

    for (case_index, (case_name, ranges)) in invalid_plans.into_iter().enumerate() {
        let table = format!("seal_plan_invalid_{case_index}_{case_name}");
        let (id, lease, f, version) =
            begin_empty_range_plan_fixture(&mut tx, epoch, &table, ranges.len()).await;
        replace_empty_range_plan_fixture(&mut tx, &lease, &ranges).await;
        assert!(
            !export_plan_is_attested(&mut tx, &lease).await,
            "{case_name} must fail the database plan proof"
        );

        if case_index == 0 {
            let mut bypass = Connection::begin(&mut *tx).await.unwrap();
            sqlx::query("SELECT set_config('walrus.reload_export_seal_protocol', '2', true)")
                .execute(&mut *bypass)
                .await
                .unwrap();
            let error = sqlx::query(
                "UPDATE public.walrus_table_reload
                 SET export_sealed_at = now(), export_file_count = 0, export_row_count = 0
                 WHERE reload_id = $1",
            )
            .bind(id.0)
            .execute(&mut *bypass)
            .await
            .unwrap_err();
            assert_eq!(
                error
                    .as_database_error()
                    .and_then(sqlx::error::DatabaseError::constraint),
                Some("table_reload_export_plan_attestation"),
                "even a direct protocol-GUC update must not bypass plan attestation"
            );
            bypass.rollback().await.unwrap();
        }

        assert!(
            matches!(
                reload::seal_export(&mut tx, &lease, f, version)
                    .await
                    .unwrap_err(),
                ControlError::ReloadTransition { .. }
            ),
            "{case_name} must not receive an export seal"
        );
    }

    let valid_plans = [
        (
            "full_scan",
            vec![ExportRangePlan {
                range_no: 0,
                full_scan: true,
                start_block: None,
                end_block: None,
            }],
        ),
        (
            "single_ctid_range",
            vec![ExportRangePlan {
                range_no: 0,
                full_scan: false,
                start_block: Some(0),
                end_block: None,
            }],
        ),
        (
            "contiguous_ctid_ranges",
            vec![
                ExportRangePlan {
                    range_no: 0,
                    full_scan: false,
                    start_block: Some(0),
                    end_block: Some(64),
                },
                ExportRangePlan {
                    range_no: 1,
                    full_scan: false,
                    start_block: Some(64),
                    end_block: Some(128),
                },
                ExportRangePlan {
                    range_no: 2,
                    full_scan: false,
                    start_block: Some(128),
                    end_block: None,
                },
            ],
        ),
    ];
    for (case_index, (case_name, ranges)) in valid_plans.into_iter().enumerate() {
        let table = format!("seal_plan_valid_{case_index}_{case_name}");
        let (_id, lease, f, version) =
            begin_empty_range_plan_fixture(&mut tx, epoch, &table, ranges.len()).await;
        replace_empty_range_plan_fixture(&mut tx, &lease, &ranges).await;
        assert!(
            export_plan_is_attested(&mut tx, &lease).await,
            "{case_name} must satisfy the database plan proof"
        );
        let seal = reload::seal_export(&mut tx, &lease, f, version)
            .await
            .unwrap();
        assert_eq!((seal.file_count, seal.row_count), (0, 0));
    }

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn parallel_export_seals_only_the_exact_durable_plan() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_010);
    let id = reload::request(
        &mut *tx,
        epoch,
        "public",
        "sealed_parallel",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let claimed = reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    let lease = claimed[0].exporter_lease("extractor-a").unwrap();
    let request_id = claimed[0].parent_request_id.unwrap();
    let f = Lsn::new(0x100);
    let h = Lsn::new(0x200);
    let version = SchemaVersionNo(1);
    let fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "sealed_parallel",
        schema_version: version,
    };
    reload::record_start_fence(&mut *tx, id, f, fence)
        .await
        .unwrap();
    let ranges = [
        ExportRangePlan {
            range_no: 0,
            full_scan: false,
            start_block: Some(0),
            end_block: Some(128),
        },
        ExportRangePlan {
            range_no: 1,
            full_scan: false,
            start_block: Some(128),
            end_block: None,
        },
    ];
    let snapshot = ExportSnapshot {
        identity: "10:20:",
        xmin: 10,
        xmax: 20,
    };
    reload::begin_export_plan(&mut tx, &lease, f, version, snapshot.clone(), &ranges)
        .await
        .unwrap();
    reload::begin_export_plan(&mut tx, &lease, f, version, snapshot, &ranges)
        .await
        .expect("the same generation can replay its exact durable plan");
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(
            "UPDATE public.walrus_table_reload_export_range
             SET end_block = 64
             WHERE reload_id = $1 AND range_no = 0",
        )
        .bind(id.0)
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("table_reload_export_range_semantics_immutable")
        );
        savepoint.rollback().await.unwrap();
    }
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(
            "DELETE FROM public.walrus_table_reload_export_range
             WHERE reload_id = $1 AND range_no = 1",
        )
        .bind(id.0)
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("table_reload_export_range_removal")
        );
        savepoint.rollback().await.unwrap();
    }
    let changed = [
        ExportRangePlan {
            end_block: Some(64),
            ..ranges[0]
        },
        ranges[1],
    ];
    assert!(matches!(
        reload::begin_export_plan(
            &mut tx,
            &lease,
            f,
            version,
            ExportSnapshot {
                identity: "10:20:",
                xmin: 10,
                xmax: 20,
            },
            &changed,
        )
        .await
        .unwrap_err(),
        ControlError::ReloadTransition { .. }
    ));

    for range_no in [1_i64, 0] {
        let mut file = chunk_file(epoch, "sealed_parallel", id, "0/100");
        write!(file.s3_uri, "-range-{range_no}").unwrap();
        insert_ready(&mut *tx, &file).await.unwrap();
        reload::record_exported_file(&mut *tx, &lease, f, version)
            .await
            .unwrap();
        reload::record_export_range(&mut *tx, &lease, range_no, 1, 1)
            .await
            .unwrap();
        if range_no == 1 {
            assert!(matches!(
                reload::seal_export(&mut tx, &lease, f, version)
                    .await
                    .unwrap_err(),
                ControlError::ReloadTransition { .. }
            ));
        }
    }
    assert!(matches!(
        reload::record_export_range(&mut *tx, &lease, 0, 2, 1)
            .await
            .unwrap_err(),
        ControlError::ReloadTransition { .. }
    ));
    let seal = reload::seal_export(&mut tx, &lease, f, version)
        .await
        .unwrap();
    assert_eq!((seal.file_count, seal.row_count), (2, 2));
    assert_eq!(
        reload::seal_export(&mut tx, &lease, f, version)
            .await
            .unwrap(),
        seal,
        "the exact seal receipt is idempotent"
    );
    assert!(matches!(
        reload::record_exported_file(&mut *tx, &lease, f, version)
            .await
            .unwrap_err(),
        ControlError::ReloadTransition { .. }
    ));
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let mut late = chunk_file(epoch, "sealed_parallel", id, "0/100");
        late.s3_uri.push_str("-late-after-seal");
        assert!(matches!(
            insert_ready(&mut *savepoint, &late).await.unwrap_err(),
            ControlError::CheckViolation { .. }
        ));
        savepoint.rollback().await.unwrap();
    }

    reload::record_end_marker(&mut *tx, id, h, fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, &lease, h).await.unwrap();
    let completed = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(completed.status, ReloadStatus::ExportComplete);
    assert_eq!(completed.chunk_no, 2);
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(
            "UPDATE public.walrus_table_reload SET status = 'complete' WHERE reload_id = $1",
        )
        .bind(id.0)
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("table_reload_v2_completion_guard")
        );
        savepoint.rollback().await.unwrap();
    }
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn superseded_stream_group_keeps_an_exact_lost_ack_replay_receipt() {
    let pool = pool().await;
    let epoch_bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8].try_into().unwrap();
    let epoch = EpochNo(i64::from_be_bytes(epoch_bytes) & i64::MAX);
    let table = "superseded_group";
    let commit_lsn = Lsn::new(0x50);
    let mut first = stream_file(epoch, table, "0/50");
    first.s3_uri.push_str("-a");
    let mut second = first.clone();
    second.s3_uri.pop();
    second.s3_uri.push('b');
    let publication = NewStreamCommitPublication {
        epoch,
        top_xid: 901,
        commit_lsn,
        commit_ts: "2026-09-02T12:00:00Z".parse::<UtcTimestamp>().unwrap(),
        ddl_rows: Vec::new(),
        registry_rows: Vec::new(),
        files: vec![first, second],
    };
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::Published
    );

    let reload_id = reload::request(&pool, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    let claimed = reload::claim_requested(&pool, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    let request_id = claimed[0].parent_request_id.unwrap();
    let f = Lsn::new(0x100);
    let h = Lsn::new(0x200);
    reload::record_start_fence(
        &pool,
        reload_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(request_id),
            source_schema: "public",
            source_table: table,
            schema_version: SchemaVersionNo(1),
        },
    )
    .await
    .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    finish_fenced(&mut conn, reload_id, h).await;
    drop(conn);

    let owner = "transformer-supersede";
    let table_lease = acquire_lease(&pool, epoch, "public", table, owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&pool, epoch, "public", table)
        .await
        .unwrap();
    let reload_publication = reload::claim_publication(
        &pool,
        epoch,
        "public",
        table,
        owner,
        table_lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        reload::claim_publication_ready_units(
            &pool,
            &reload_publication,
            owner,
            table_lease.fencing_token + 1,
            100,
        )
        .await,
        Err(ControlError::ReloadTransition { .. })
    ));
    assert!(matches!(
        reload::claim_publication_ready(
            &pool,
            &reload_publication,
            owner,
            table_lease.fencing_token + 1,
            100,
        )
        .await,
        Err(ControlError::ReloadTransition { .. })
    ));
    sqlx::query(
        "UPDATE public.walrus_table_ownership
         SET lease_expiry = statement_timestamp() - interval '1 second'
         WHERE epoch=$1 AND source_schema='public' AND source_table=$2",
    )
    .bind(epoch.0)
    .bind(table)
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        reload::claim_publication_ready_units(
            &pool,
            &reload_publication,
            owner,
            table_lease.fencing_token,
            100,
        )
        .await,
        Err(ControlError::ReloadTransition { .. })
    ));
    sqlx::query(
        "UPDATE public.walrus_table_ownership
         SET lease_expiry = statement_timestamp() + interval '60 seconds'
         WHERE epoch=$1 AND source_schema='public' AND source_table=$2",
    )
    .bind(epoch.0)
    .bind(table)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        control::delete_publication_superseded(
            &pool,
            &reload_publication,
            owner,
            table_lease.fencing_token,
        )
        .await
        .unwrap(),
        2
    );
    let (status, children): (String, i64) = sqlx::query_as(
        "SELECT g.status, count(m.id)::bigint
         FROM public.walrus_stream_manifest_group g
         LEFT JOIN public.walrus_file_manifest m ON m.stream_group_id = g.id
         WHERE g.epoch = $1 GROUP BY g.id",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((status.as_str(), children), ("superseded", 0));
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished
    );
    let mut changed = publication.clone();
    changed.files[0].row_count += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed).await,
        Err(ControlError::StreamPublicationConflict { .. })
    ));

    let mut seal_tx = pool.begin().await.unwrap();
    assert!(
        reload::seal_publication_if_drained(
            &mut seal_tx,
            &reload_publication,
            owner,
            table_lease.fencing_token,
        )
        .await
        .unwrap()
    );
    seal_tx.commit().await.unwrap();
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished,
        "an exact lost-ACK replay remains valid after its table is sealed"
    );
    let mut late = publication.clone();
    late.top_xid += 1;
    late.commit_lsn = Lsn::new(0x80);
    late.files[0].lsn_start = late.commit_lsn;
    late.files[0].lsn_end = late.commit_lsn;
    late.files[0].s3_uri.push_str("-fresh-after-seal");
    late.files.truncate(1);
    assert!(matches!(
        publish_stream_commit(&pool, &late).await,
        Err(ControlError::ManifestInvariant { .. })
    ));
    assert!(
        reload::finish_publication(&pool, &reload_publication, owner, table_lease.fencing_token,)
            .await
            .unwrap()
    );
    let mut cleanup = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_manifest_group WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_txn_publication WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_manifest_publication_fence WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_transformer_checkpoint WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_reload WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_ownership WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
}

#[tokio::test]
async fn superseded_purge_refreshes_snapshot_after_waiting_on_an_applied_parent() {
    let pool = pool().await;
    let epoch_bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8].try_into().unwrap();
    let epoch = EpochNo(i64::from_be_bytes(epoch_bytes) & i64::MAX);
    let table = "supersede_lock_wait";
    let mut first = stream_file(epoch, table, "0/50");
    first.s3_uri.push_str("-a");
    let mut second = first.clone();
    second.s3_uri.pop();
    second.s3_uri.push('b');
    publish_stream_commit(
        &pool,
        &NewStreamCommitPublication {
            epoch,
            top_xid: 902,
            commit_lsn: Lsn::new(0x50),
            commit_ts: "2026-09-02T12:01:00Z".parse::<UtcTimestamp>().unwrap(),
            ddl_rows: Vec::new(),
            registry_rows: Vec::new(),
            files: vec![first, second],
        },
    )
    .await
    .unwrap();

    let reload_id = reload::request(&pool, epoch, "public", table, ReloadFlavor::Reload)
        .await
        .unwrap();
    let claimed = reload::claim_requested(&pool, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    let request_id = claimed[0].parent_request_id.unwrap();
    let f = Lsn::new(0x100);
    let h = Lsn::new(0x200);
    reload::record_start_fence(
        &pool,
        reload_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(request_id),
            source_schema: "public",
            source_table: table,
            schema_version: SchemaVersionNo(1),
        },
    )
    .await
    .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    finish_fenced(&mut conn, reload_id, h).await;
    drop(conn);

    let owner = "transformer-supersede-lock";
    let table_lease = acquire_lease(&pool, epoch, "public", table, owner, 60)
        .await
        .unwrap()
        .unwrap();
    let reload_publication = reload::claim_publication(
        &pool,
        epoch,
        "public",
        table,
        owner,
        table_lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    let child_ids = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM public.walrus_file_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch.0)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(ManifestId)
    .collect::<Vec<_>>();

    // Model ordinary apply winning the parent lock. The reload purge starts its statement while
    // this transaction still sees the group ready, then must refresh its READ COMMITTED snapshot
    // after the winner atomically marks the parent applied and removes every child.
    let mut apply_tx = pool.begin().await.unwrap();
    control::lock_manifest_work_groups(&mut *apply_tx, &child_ids, &[])
        .await
        .unwrap();
    let purge = control::delete_publication_superseded(
        &pool,
        &reload_publication,
        owner,
        table_lease.fencing_token,
    );
    tokio::pin!(purge);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), purge.as_mut())
            .await
            .is_err(),
        "the purge should wait behind the already-locked stream parent"
    );
    assert_eq!(
        delete_claimed(&mut *apply_tx, &child_ids).await.unwrap(),
        child_ids.len() as u64
    );
    apply_tx.commit().await.unwrap();
    assert_eq!(
        purge.await.unwrap(),
        0,
        "the fresh post-wait snapshot treats the applied childless parent as an idempotent no-op"
    );

    let (status, children): (String, i64) = sqlx::query_as(
        "SELECT g.status, count(m.id)::bigint
         FROM public.walrus_stream_manifest_group g
         LEFT JOIN public.walrus_file_manifest m ON m.stream_group_id = g.id
         WHERE g.epoch = $1 GROUP BY g.id",
    )
    .bind(epoch.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((status.as_str(), children), ("applied", 0));

    let mut cleanup = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_manifest_group WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_stream_txn_publication WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_manifest_publication_fence WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_reload WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM public.walrus_table_ownership WHERE epoch = $1")
        .bind(epoch.0)
        .execute(&mut *cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
}

#[tokio::test]
async fn adopted_generation_cannot_reuse_a_connection_local_exported_snapshot() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_011);
    let id = reload::request(
        &mut *tx,
        epoch,
        "public",
        "lost_snapshot",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let first = reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let first_lease = first.exporter_lease("extractor-a").unwrap();
    let f = Lsn::new(0x100);
    let version = SchemaVersionNo(1);
    reload::record_start_fence(
        &mut *tx,
        id,
        f,
        ReloadFenceIdentity {
            request_id: first.parent_request_id,
            source_schema: "public",
            source_table: "lost_snapshot",
            schema_version: version,
        },
    )
    .await
    .unwrap();
    let range = [ExportRangePlan {
        range_no: 0,
        full_scan: true,
        start_block: None,
        end_block: None,
    }];
    reload::begin_export_plan(
        &mut tx,
        &first_lease,
        f,
        version,
        ExportSnapshot {
            identity: "30:40:",
            xmin: 30,
            xmax: 40,
        },
        &range,
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE public.walrus_table_reload
         SET lease_expiry = now() - interval '1 second'
         WHERE reload_id = $1",
    )
    .bind(id.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    let adopted = reload::adopt_resumable(&mut *tx, epoch, "extractor-b", 60, 1, false)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let adopted_lease = adopted.exporter_lease("extractor-b").unwrap();
    assert!(adopted.has_export_plan);
    assert!(adopted_lease.generation > first_lease.generation);
    assert!(matches!(
        reload::begin_export_plan(
            &mut tx,
            &adopted_lease,
            f,
            version,
            ExportSnapshot {
                identity: "30:40:",
                xmin: 30,
                xmax: 40,
            },
            &range,
        )
        .await
        .unwrap_err(),
        ControlError::ReloadTransition { .. }
    ));
    assert!(matches!(
        reload::record_export_range(&mut *tx, &first_lease, 0, 0, 0)
            .await
            .unwrap_err(),
        ControlError::ReloadTransition { .. }
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn parallel_file_progress_is_atomic_out_of_order_and_mode_safe() {
    const FILES: i64 = 8;
    let pool = pool().await;
    // This test needs independent committed transactions to exercise concurrent row locking. A
    // random positive epoch keeps parallel/rerun debris isolated; all owned rows are removed below.
    let epoch_bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8].try_into().unwrap();
    let epoch = EpochNo(i64::from_be_bytes(epoch_bytes) & i64::MAX);
    let id = reload::request(
        &pool,
        epoch,
        "public",
        "parallel_files",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let claimed = reload::claim_requested(&pool, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    let request_id = claimed[0].parent_request_id.unwrap();
    let f: Lsn = "0/100".parse().unwrap();
    let schema_version = SchemaVersionNo(1);
    reload::record_start_fence(
        &pool,
        id,
        f,
        ReloadFenceIdentity {
            request_id: Some(request_id),
            source_schema: "public",
            source_table: "parallel_files",
            schema_version,
        },
    )
    .await
    .unwrap();
    let mut plan_conn = pool.acquire().await.unwrap();
    begin_full_scan_plan(
        &mut plan_conn,
        &exporter(id, "extractor-a", 1),
        f,
        schema_version,
    )
    .await;
    drop(plan_conn);

    // Model workers completing physical ranges in reverse order. Every worker commits its manifest
    // and the shared file counter together, while Postgres assigns a unique count under the row lock.
    let mut workers = Vec::new();
    for worker in 0..FILES {
        let pool = pool.clone();
        workers.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(
                u64::try_from(FILES - worker).unwrap(),
            ))
            .await;
            let mut tx = pool.begin().await.unwrap();
            let mut file = chunk_file(epoch, "parallel_files", id, "0/100");
            write!(file.s3_uri, "-{worker}").unwrap();
            insert_ready(&mut *tx, &file).await.unwrap();
            let assigned = reload::record_exported_file(
                &mut *tx,
                &exporter(id, "extractor-a", 1),
                f,
                schema_version,
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            assigned
        }));
    }
    let mut assigned = Vec::new();
    for worker in workers {
        assigned.push(worker.await.unwrap());
    }
    assigned.sort_unstable();
    assert_eq!(assigned, (1..=FILES).collect::<Vec<_>>());

    let row = reload::get(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.chunk_no, FILES);
    assert_eq!(
        row.cursor_pk, None,
        "parallel progress never creates a PK cursor"
    );
    assert_eq!(row.first_lsn, Some(f));
    let manifests: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_file_manifest WHERE reload_id = $1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(manifests, FILES);

    for (wrong_f, wrong_schema) in [
        ("0/200".parse().unwrap(), schema_version),
        (f, SchemaVersionNo(2)),
    ] {
        let err = reload::record_exported_file(
            &pool,
            &exporter(id, "extractor-a", 1),
            wrong_f,
            wrong_schema,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ControlError::ReloadTransition { .. }));
    }
    assert_eq!(
        reload::get(&pool, id).await.unwrap().unwrap().chunk_no,
        FILES
    );

    // A manifest and counter advance rolled back together leave neither half visible.
    let mut rolled_back = pool.begin().await.unwrap();
    let mut file = chunk_file(epoch, "parallel_files", id, "0/100");
    file.s3_uri.push_str("-rollback");
    insert_ready(&mut *rolled_back, &file).await.unwrap();
    assert_eq!(
        reload::record_exported_file(
            &mut *rolled_back,
            &exporter(id, "extractor-a", 1),
            f,
            schema_version,
        )
        .await
        .unwrap(),
        FILES + 1
    );
    rolled_back.rollback().await.unwrap();
    assert_eq!(
        reload::get(&pool, id).await.unwrap().unwrap().chunk_no,
        FILES
    );
    let manifests_after_rollback: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_file_manifest WHERE reload_id = $1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(manifests_after_rollback, FILES);

    // Neither rolling direction may combine legacy keyset progress with file-count progress.
    let err = reload::advance_cursor(
        &pool,
        id,
        FILES + 1,
        &serde_json::json!([42]),
        f,
        schema_version,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    let legacy_id = reload::request(
        &pool,
        epoch,
        "public",
        "legacy_cursor",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let legacy = reload::claim_requested(&pool, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    let legacy_request_id = legacy[0].parent_request_id.unwrap();
    reload::record_start_fence(
        &pool,
        legacy_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(legacy_request_id),
            source_schema: "public",
            source_table: "legacy_cursor",
            schema_version,
        },
    )
    .await
    .unwrap();
    let err = reload::advance_cursor(
        &pool,
        legacy_id,
        1,
        &serde_json::json!([42]),
        f,
        schema_version,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    sqlx::query("WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM public.walrus_file_manifest WHERE reload_id = $1 AND (SELECT protocol='2' FROM authorized)")
        .bind(id.0)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE reload_id = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(id.0)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "WITH authorized AS MATERIALIZED (
           SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
         )
         DELETE FROM public.walrus_table_reload
         WHERE reload_id = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
    )
    .bind(legacy_id.0)
    .execute(&pool)
    .await
    .unwrap();
}

/// An ordinary stream file — `reload_id` stays NULL, exactly like every pre-6.1 row.
fn stream_file(epoch: EpochNo, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{lsn_end}.parquet"),
        kind: control::ManifestKind::Stream,
        row_count: 1,
        object_size: 1,
        sha256: vec![0; 32],
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: None,
    }
}

#[tokio::test]
async fn full_status_walk_and_duplicate_request_rejected() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_001);

    let id = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();

    // A duplicate request hits the `walrus_table_reload_one_live` partial unique index and surfaces as
    // the TYPED already-in-progress error — probed under a savepoint, since the unique violation
    // aborts its (sub)transaction.
    {
        let mut sp = Connection::begin(&mut *tx).await.unwrap();
        let err = reload::request(&mut *sp, epoch, "public", "orders", ReloadFlavor::Reload)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ControlError::ReloadInProgress { ref schema, ref table }
                if schema == "public" && table == "orders"),
            "expected the typed ReloadInProgress, got: {err:?}"
        );
        assert!(
            err.is_terminal(),
            "retrying a duplicate request never helps"
        );
        sp.rollback().await.unwrap();
    }

    // A reload on a DIFFERENT table is untouched by the index.
    let other = reload::request(&mut *tx, epoch, "public", "customers", ReloadFlavor::Resync)
        .await
        .unwrap();
    assert!(other > id, "bigserial: monotonic ids");

    // The pause engages at REQUEST time, not claim time, for both persisted spellings.
    let rebuilds = reload::active_rebuilds(&mut *tx, epoch).await.unwrap();
    assert_eq!(
        rebuilds.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![id, other],
        "reload and resync are both active rebuilds while requested"
    );
    assert_eq!(rebuilds[0].status, ReloadStatus::Requested);
    assert_eq!(rebuilds[1].status, ReloadStatus::Requested);
    assert_eq!(rebuilds[1].flavor, ReloadFlavor::Resync);

    // Claim honors the batch cap and hands out the OLDEST request first.
    let claimed = reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "limit=1 claims exactly one row");
    let orders = &claimed[0];
    assert_eq!(orders.reload_id, id, "oldest reload_id first");
    assert_eq!(orders.status, ReloadStatus::Exporting);
    assert_eq!(orders.flavor, ReloadFlavor::Reload);
    assert_eq!(orders.lease_holder.as_deref(), Some("extractor-a"));
    assert_eq!(orders.chunk_no, 0);
    assert_eq!(orders.first_lsn, None);
    assert_eq!(orders.source_request_id, None);
    let orders_fence_request_id = orders
        .parent_request_id
        .expect("direct requests persist a private source-fence namespace");

    // The claim SET a real lease: expiry sits in the future. The model omits lease_expiry by
    // design (no clock in Rust), so probe it with SQL — the transformer's shutdown test's idiom.
    let now_epoch: f64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::float8")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let exp_claim = expiry_epoch(&mut *tx, id).await;
    assert!(
        exp_claim > now_epoch,
        "claim set lease_expiry in the future"
    );

    // The one_live index guards the WHOLE non-terminal breadth, not just `requested`: a
    // duplicate request against the now-`exporting` row is rejected identically.
    {
        let mut sp = Connection::begin(&mut *tx).await.unwrap();
        let err = reload::request(&mut *sp, epoch, "public", "orders", ReloadFlavor::Reload)
            .await
            .unwrap_err();
        assert!(matches!(err, ControlError::ReloadInProgress { .. }));
        sp.rollback().await.unwrap();
    }

    // The second requested row (the resync) is still there; a cap above the queue drains it.
    let rest = reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        rest.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![other]
    );
    assert_eq!(rest[0].flavor, ReloadFlavor::Resync);
    let other_fence_request_id = rest[0]
        .parent_request_id
        .expect("every direct request persists a source-fence namespace");
    assert_ne!(
        other_fence_request_id, orders_fence_request_id,
        "direct requests must not derive their durable namespace from a reusable bigint id"
    );

    // Nothing left in `requested`: a latecomer gets an empty Vec, not an error. (The
    // cross-connection SKIP LOCKED race is exercised in
    // `concurrent_claimers_partition_the_queue_via_skip_locked` below.)
    let raced = reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
        .await
        .unwrap();
    assert!(raced.is_empty());

    // The holder renews — and the lease observably extends (same frozen now(), bigger ttl);
    // a phantom does not.
    assert!(
        reload::renew_lease(&mut *tx, &exporter(id, "extractor-a", 1), 3600)
            .await
            .unwrap()
    );
    let exp_renewed = expiry_epoch(&mut *tx, id).await;
    assert!(
        exp_renewed > exp_claim + 3000.0,
        "renew pushed lease_expiry out by the new ttl"
    );
    assert!(
        !reload::renew_lease(&mut *tx, &exporter(id, "extractor-zombie", 1), 60)
            .await
            .unwrap()
    );

    // Freeze F + schema before any chunk. Every baseline chunk must carry that exact F; a stale
    // producer cannot smuggle a per-chunk Lᵢ from a different source snapshot into this attempt.
    let l1: Lsn = "0/100".parse().unwrap();
    let l2: Lsn = "0/200".parse().unwrap();
    reload::record_start_fence(
        &mut *tx,
        id,
        l1,
        ReloadFenceIdentity {
            request_id: Some(orders_fence_request_id),
            source_schema: "public",
            source_table: "orders",
            schema_version: SchemaVersionNo(7),
        },
    )
    .await
    .unwrap();
    let lease = reload::get(&mut *tx, id)
        .await
        .unwrap()
        .unwrap()
        .exporter_lease("extractor-a")
        .unwrap();
    let err = reload::record_exported_file(&mut *tx, &lease, l2, SchemaVersionNo(7))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // A mismatched schema_version is ASSERTED, not swallowed: every attempt is single-schema by
    // construction (H9), so version 9 mid-attempt means the export engine missed a DDL restart.
    let err = reload::record_exported_file(&mut *tx, &lease, l1, SchemaVersionNo(9))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.chunk_no, 0, "the rejected mismatch advanced nothing");
    assert_eq!(row.cursor_pk, None);
    assert_eq!(row.first_lsn, None);
    assert_eq!(
        row.schema_version,
        Some(SchemaVersionNo(7)),
        "schema_version is frozen on chunk 1"
    );

    // exporting → export_complete records the final watermark H…
    let h: Lsn = "0/300".parse().unwrap();
    finish_fenced(&mut tx, id, h).await;
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::ExportComplete);
    assert_eq!(row.final_lsn, Some(h));
    assert_eq!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![id, other],
        "export_complete keeps the orders pause active alongside the exporting resync alias"
    );

    // …but the one_live index still guards `export_complete` (non-terminal): no new request yet.
    {
        let mut sp = Connection::begin(&mut *tx).await.unwrap();
        let err = reload::request(&mut *sp, epoch, "public", "orders", ReloadFlavor::Reload)
            .await
            .unwrap_err();
        assert!(matches!(err, ControlError::ReloadInProgress { .. }));
        sp.rollback().await.unwrap();
    }

    let bypass = reload::complete(&mut *tx, id).await.unwrap_err();
    assert!(
        matches!(bypass, ControlError::ReloadTransition { reload_id, .. } if reload_id == id),
        "the legacy single-row transition cannot complete a protocol-v2 generation"
    );
    ensure_checkpoint(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    control::advance_raw_appended(&mut *tx, epoch, "public", "orders", h)
        .await
        .unwrap();
    control::advance_transformed(&mut *tx, epoch, "public", "orders", h)
        .await
        .unwrap();
    assert!(
        reload::complete_reached(&mut *tx, epoch, "public", "orders")
            .await
            .unwrap()
            .is_empty(),
        "even a checkpoint at H cannot bypass hidden-generation publication"
    );
    assert_eq!(
        reload::get(&mut *tx, id).await.unwrap().unwrap().status,
        ReloadStatus::ExportComplete
    );

    // …and the transformer publishes the hidden generation. Terminal ⇒ the table is requestable again.
    let _ = publish_fenced(&mut tx, epoch, "orders", id).await;
    let again = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(
        again > id,
        "a fresh attempt gets a fresh, larger reload_id (latest wins = max)"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_request_is_idempotent_per_table_and_supports_fanout() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_006);
    let source_request_id = Uuid::from_u128(0x100);
    let parent_request_id = Some(Uuid::from_u128(0x200));

    let orders = SourceReloadRequest {
        epoch,
        source_request_id,
        parent_request_id,
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let orders_id = reload::request_from_source(&mut *tx, &orders)
        .await
        .unwrap();
    let replayed_id = reload::request_from_source(&mut *tx, &orders)
        .await
        .unwrap();
    assert_eq!(
        replayed_id, orders_id,
        "WAL redelivery must return the original attempt"
    );

    let customers = SourceReloadRequest {
        source_table: "customers",
        ..orders
    };
    let customers_id = reload::request_from_source(&mut *tx, &customers)
        .await
        .unwrap();
    assert_ne!(
        customers_id, orders_id,
        "one all-published event fans out to distinct table attempts"
    );

    let row = reload::get(&mut *tx, orders_id).await.unwrap().unwrap();
    assert_eq!(row.source_request_id, Some(source_request_id));
    assert_eq!(row.parent_request_id, parent_request_id);
    assert_eq!(row.scope, ReloadScope::AllPublished);

    let changed_payload = SourceReloadRequest {
        scope: ReloadScope::Table,
        ..orders
    };
    let err = reload::request_from_source(&mut *tx, &changed_payload)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ControlError::SourceRequestConflict { request_id, ref schema, ref table }
            if request_id == source_request_id && schema == "public" && table == "orders"
    ));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_restart_successors_keep_the_source_uuid_when_parent_differs() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_016);

    for (table, source_request_id, parent_request_id, pristine) in [
        (
            "source_parent_ddl",
            Uuid::from_u128(0x1601),
            Uuid::from_u128(0x2601),
            false,
        ),
        (
            "source_parent_pristine",
            Uuid::from_u128(0x1602),
            Uuid::from_u128(0x2602),
            true,
        ),
    ] {
        let request = SourceReloadRequest {
            epoch,
            source_request_id,
            parent_request_id: Some(parent_request_id),
            scope: ReloadScope::Table,
            source_schema: "public",
            source_table: table,
            flavor: ReloadFlavor::Reload,
        };
        let old_id = reload::request_from_source(&mut *tx, &request)
            .await
            .unwrap();
        let claimed = reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|row| row.reload_id).collect::<Vec<_>>(),
            vec![old_id]
        );
        let old = reload::get(&mut *tx, old_id).await.unwrap().unwrap();

        let successor_id = if pristine {
            reload::restart_pristine_adoption(&mut tx, &old)
                .await
                .unwrap()
        } else {
            reload::restart_for_ddl(&mut tx, &old, SchemaVersionNo(2), 3)
                .await
                .unwrap()
                .expect("DDL restart is below the cap")
        };
        let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
        assert_eq!(successor.source_request_id, None);
        assert_eq!(
            successor.parent_request_id,
            Some(source_request_id),
            "a successor carries the original source event UUID as its fence namespace"
        );
        assert_ne!(
            successor.parent_request_id,
            Some(parent_request_id),
            "the correlation parent must not replace the source event identity"
        );

        let wrong_identity = ReloadFenceIdentity {
            request_id: Some(parent_request_id),
            source_schema: "public",
            source_table: table,
            schema_version: SchemaVersionNo(2),
        };
        let error =
            reload::record_start_fence(&mut *tx, successor_id, Lsn::new(0x100), wrong_identity)
                .await
                .unwrap_err();
        assert!(matches!(error, ControlError::ReloadTransition { .. }));

        reload::record_start_fence(
            &mut *tx,
            successor_id,
            Lsn::new(0x100),
            ReloadFenceIdentity {
                request_id: Some(source_request_id),
                ..wrong_identity
            },
        )
        .await
        .unwrap();
    }

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_requests_queue_fifo_until_the_current_attempt_is_terminal() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_008);

    let first_request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0x801),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let first = reload::request_from_source(&mut *tx, &first_request)
        .await
        .unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![first]
    );

    // Unlike direct requests, a new source UUID is accepted while this table is exporting.
    let second_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0x802),
        ..first_request
    };
    let second = reload::request_from_source(&mut *tx, &second_request)
        .await
        .unwrap();
    assert!(second > first);
    assert!(
        reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "the next source request must wait while the first is exporting"
    );

    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let first_fence = ReloadFenceIdentity {
        request_id: Some(first_request.source_request_id),
        source_schema: first_request.source_schema,
        source_table: first_request.source_table,
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, first, f, first_fence)
        .await
        .unwrap();
    finish_fenced(&mut tx, first, h).await;

    // A third request is also durable while the current attempt waits on transformer cutover.
    let third_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0x803),
        ..first_request
    };
    let third = reload::request_from_source(&mut *tx, &third_request)
        .await
        .unwrap();
    assert!(third > second);
    assert!(
        reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "export_complete remains active until the transformer publishes it"
    );

    let _ = publish_fenced(&mut tx, epoch, "orders", first).await;
    let claimed = reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        claimed.iter().map(|row| row.reload_id).collect::<Vec<_>>(),
        vec![second],
        "only the oldest queued request for a table may claim"
    );
    assert_eq!(
        reload::get(&mut *tx, third).await.unwrap().unwrap().status,
        ReloadStatus::Requested
    );

    // A failed active attempt is terminal too, so the next FIFO entry may start.
    reload::fail(&mut tx, second, "test failure").await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "extractor-c", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![third]
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn current_export_complete_cuts_over_despite_a_later_source_request() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_009);
    let manifest = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/180"))
        .await
        .unwrap();

    let current_request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0x901),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let current = reload::request_from_source(&mut *tx, &current_request)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    let later_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0x902),
        ..current_request
    };
    let later = reload::request_from_source(&mut *tx, &later_request)
        .await
        .unwrap();

    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the exporting attempt and queued request keep Phase A paused"
    );

    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let current_fence = ReloadFenceIdentity {
        request_id: Some(current_request.source_request_id),
        source_schema: current_request.source_schema,
        source_table: current_request.source_table,
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, current, f, current_fence)
        .await
        .unwrap();
    finish_fenced(&mut tx, current, h).await;

    assert!(
        reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "the later request cannot start before the current cutover completes"
    );
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "export_complete keeps the generic claim paused despite the queued request"
    );

    let published = publish_fenced(&mut tx, epoch, "orders", current).await;
    assert_eq!(
        published.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![manifest],
        "the publication-specific claim drains the current attempt through H"
    );
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![later]
    );
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "once the queued request starts, it owns the normal pause"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn data_free_markers_drive_an_empty_resync_alias_rebuild() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_007);
    let request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0x300),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "empty_table",
        flavor: ReloadFlavor::Resync,
    };
    let reload_id = reload::request_from_source(&mut *tx, &request)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();

    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let schema_version = SchemaVersionNo(9);
    let fence = ReloadFenceIdentity {
        request_id: Some(request.source_request_id),
        source_schema: request.source_schema,
        source_table: request.source_table,
        schema_version,
    };

    let err = reload::complete_export(&mut *tx, &exporter(reload_id, "extractor-a", 1), h)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    let err = reload::record_start_fence(
        &mut *tx,
        reload_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(Uuid::from_u128(0xdead)),
            ..fence
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ControlError::ReloadTransition { .. }),
        "an old source request must not attach its fence to a reused bigint reload_id"
    );

    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .unwrap();
    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .unwrap();
    let row = reload::get(&mut *tx, reload_id).await.unwrap().unwrap();
    assert_eq!(row.start_lsn, Some(f));
    assert_eq!(row.first_lsn, None, "no data chunk was needed");
    assert_eq!(row.chunk_no, 0, "the empty export wrote no data file");
    assert_eq!(row.schema_version, Some(schema_version));
    assert_eq!(
        reload::reload_supersede_floor(&mut *tx, epoch, "public", "empty_table")
            .await
            .unwrap(),
        Some(f),
        "the explicit fence, not a first file, is authoritative"
    );

    let lease = row.exporter_lease("extractor-a").unwrap();
    begin_full_scan_plan(&mut tx, &lease, f, schema_version).await;
    reload::record_export_range(&mut *tx, &lease, 0, 0, 0)
        .await
        .unwrap();
    reload::seal_export(&mut tx, &lease, f, schema_version)
        .await
        .unwrap();

    let err = reload::record_end_marker(&mut *tx, reload_id, "0/FF".parse().unwrap(), fence)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::record_end_marker(
        &mut *tx,
        reload_id,
        h,
        ReloadFenceIdentity {
            schema_version: SchemaVersionNo(schema_version.0 + 1),
            ..fence
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .unwrap();
    let markers = reload::read_markers(&mut *tx, reload_id).await.unwrap();
    assert_eq!(markers.len(), 2);
    assert_eq!(markers[0].kind, ReloadMarkerKind::Baseline);
    assert_eq!(markers[0].lsn, f);
    assert_eq!(markers[0].schema_version, schema_version);
    assert_eq!(markers[1].kind, ReloadMarkerKind::End);
    assert_eq!(markers[1].lsn, h);
    assert!(
        reload::ready_rebuild(&mut *tx, epoch, "public", "empty_table")
            .await
            .unwrap()
            .is_none(),
        "durable markers do not skip the exporting → export_complete transition"
    );

    reload::complete_export(&mut *tx, &lease, h).await.unwrap();
    let row = reload::get(&mut *tx, reload_id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::ExportComplete);
    assert_eq!(row.final_lsn, Some(h));
    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .expect("an exact start-event replay remains idempotent after the status flip");
    reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .expect("an exact end-event replay remains idempotent after the status flip");
    let ready = reload::ready_rebuild(&mut *tx, epoch, "public", "empty_table")
        .await
        .unwrap()
        .expect("marker-only empty reload is discoverable without a manifest file");
    assert_eq!(ready.reload_id, reload_id);
    assert_eq!(ready.chunk_no, 0);
    assert_eq!(ready.flavor, ReloadFlavor::Resync);
    let _ = publish_fenced(&mut tx, epoch, "empty_table", reload_id).await;
    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .expect("an exact start-event replay remains idempotent after completion");

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn wrong_state_transition_changes_zero_rows() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_002);

    let id = reload::request(&mut *tx, epoch, "public", "t", ReloadFlavor::Reload)
        .await
        .unwrap();
    let h: Lsn = "0/300".parse().unwrap();

    // Every jump out of `requested` that isn't a claim is illegal — the guarded UPDATE matches
    // zero rows and errors, and the row is provably untouched. (No savepoints needed: a
    // zero-row UPDATE is not a SQL error, so the transaction stays healthy.)
    let err = reload::complete_export(&mut *tx, &exporter(id, "extractor-a", 1), h)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { reload_id, .. } if reload_id == id));
    let err = reload::complete(&mut *tx, id).await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::advance_cursor(
        &mut *tx,
        id,
        1,
        &serde_json::json!([1]),
        h,
        SchemaVersionNo(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::record_exported_file(
        &mut *tx,
        &exporter(id, "extractor-a", 1),
        h,
        SchemaVersionNo(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::fail(&mut tx, id, "nope").await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        ReloadStatus::Requested,
        "illegal jumps changed nothing"
    );
    assert_eq!(row.error, None);

    // Claim it, then try to skip export_complete: exporting → complete is equally illegal.
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();
    let err = reload::complete(&mut *tx, id).await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // An out-of-order cursor advance (chunk 2 before chunk 1) is a loud error too.
    let err = reload::advance_cursor(
        &mut *tx,
        id,
        2,
        &serde_json::json!([1]),
        h,
        SchemaVersionNo(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // Walk through fenced publication to terminal, then confirm terminal states reject everything.
    finish_fenced(&mut tx, id, h).await;
    let _ = publish_fenced(&mut tx, epoch, "t", id).await;
    let err = reload::fail(&mut tx, id, "too late").await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::record_exported_file(
        &mut *tx,
        &exporter(id, "extractor-a", 1),
        h,
        SchemaVersionNo(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::Complete);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn release_claim_returns_the_row_to_the_queue() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_005);

    let id = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();

    // A phantom can't release someone else's claim; releasing a `requested` row is a no-op too.
    assert!(
        !reload::release_claim(&mut *tx, &exporter(id, "extractor-zombie", 1))
            .await
            .unwrap()
    );

    // The claimant releases: back to `requested`, lease cleared, immediately re-claimable — the
    // controller's un-claim path for infra failures between claim and exporter spawn.
    assert!(
        reload::release_claim(&mut *tx, &exporter(id, "extractor-a", 1))
            .await
            .unwrap()
    );
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::Requested);
    assert_eq!(row.lease_holder, None);
    assert!(
        !reload::release_claim(&mut *tx, &exporter(id, "extractor-a", 1))
            .await
            .unwrap()
    );

    let reclaimed = reload::claim_requested(&mut *tx, epoch, "extractor-b", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        reclaimed.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![id],
        "a released claim is re-claimable"
    );
    let fence_request_id = reclaimed[0]
        .parent_request_id
        .expect("direct requests persist a private source-fence namespace");

    reload::record_start_fence(
        &mut *tx,
        id,
        Lsn::new(100),
        control::ReloadFenceIdentity {
            request_id: Some(fence_request_id),
            source_schema: "public",
            source_table: "orders",
            schema_version: SchemaVersionNo(1),
        },
    )
    .await
    .unwrap();
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(
            "UPDATE public.walrus_table_reload
             SET status = 'requested', lease_holder = NULL, lease_expiry = NULL
             WHERE reload_id = $1",
        )
        .bind(id.0)
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("table_reload_status_transition")
        );
        savepoint.rollback().await.unwrap();
    }
    assert!(
        !reload::release_claim(&mut *tx, &exporter(id, "extractor-b", 2))
            .await
            .unwrap(),
        "a fenced attempt must keep its snapshot ownership semantics"
    );
    assert_eq!(
        reload::get(&mut *tx, id).await.unwrap().unwrap().status,
        ReloadStatus::Exporting
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn exporter_lease_liveness_uses_statement_time_inside_a_long_transaction() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_012);

    let id = reload::request(
        &mut *tx,
        epoch,
        "public",
        "statement_clock",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let claimed = reload::claim_requested(&mut *tx, epoch, "extractor-a", 1, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease = claimed.exporter_lease("extractor-a").unwrap();

    // `now()` remains frozen at this transaction's start. Every lease decision must instead see
    // the clock at its own statement boundary after this short lease has elapsed.
    sqlx::query("SELECT pg_sleep(1.1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        !reload::renew_lease(&mut *tx, &lease, 60).await.unwrap(),
        "renewal cannot resurrect an exporter lease that expired during the transaction"
    );
    assert!(
        reload::stuck_exporting(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .any(|(reload_id, _)| *reload_id == id),
        "expiry diagnostics must use the per-statement clock too"
    );

    let adopted = reload::adopt_resumable(&mut *tx, epoch, "extractor-b", 60, 1, false)
        .await
        .unwrap();
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].reload_id, id);
    assert_eq!(adopted[0].exporter_generation, lease.generation + 1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn fail_purges_this_reloads_manifest_rows_only() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_003);

    // Two live reloads on different tables, both exporting.
    let r1 = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    let r2 = reload::request(&mut *tx, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
        .await
        .unwrap();

    for (reload_id, table, f) in [
        (r1, "orders", Lsn::new(0x10)),
        (r2, "customers", Lsn::new(0x10)),
    ] {
        let row = reload::get(&mut *tx, reload_id).await.unwrap().unwrap();
        reload::record_start_fence(
            &mut *tx,
            reload_id,
            f,
            ReloadFenceIdentity {
                request_id: row.parent_request_id,
                source_schema: "public",
                source_table: table,
                schema_version: SchemaVersionNo(1),
            },
        )
        .await
        .unwrap();
        let lease = row.exporter_lease("extractor-a").unwrap();
        begin_full_scan_plan(&mut tx, &lease, f, SchemaVersionNo(1)).await;
    }

    // Staged chunk files for both reloads, plus an ordinary stream file (reload_id IS NULL).
    let first_chunk = chunk_file(epoch, "orders", r1, "0/10");
    insert_ready(&mut *tx, &first_chunk).await.unwrap();
    let mut second_chunk = first_chunk.clone();
    second_chunk.s3_uri.push_str("-second");
    insert_ready(&mut *tx, &second_chunk).await.unwrap();
    let keep_chunk = insert_ready(&mut *tx, &chunk_file(epoch, "customers", r2, "0/10"))
        .await
        .unwrap();
    let keep_stream = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/30"))
        .await
        .unwrap();

    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let error = sqlx::query(
            "UPDATE public.walrus_table_reload
             SET status = 'failed', error = 'unsafe direct failure'
             WHERE reload_id = $1",
        )
        .bind(r1.0)
        .execute(&mut *savepoint)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("table_reload_failure_requires_purge")
        );
        savepoint.rollback().await.unwrap();
    }

    reload::fail(
        &mut tx,
        r1,
        "echo timeout: is public.walrus_reload_signal published?",
    )
    .await
    .unwrap();

    // The failed reload is terminal with its reason recorded…
    let row = reload::get(&mut *tx, r1).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::Failed);
    assert!(row.error.as_deref().unwrap().contains("echo timeout"));

    // …its chunk files are GONE (purged in the same transaction as the flip)…
    let orders_left = claim_ready(&mut *tx, epoch, "public", "orders", 100)
        .await
        .unwrap();
    assert_eq!(
        orders_left.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![keep_stream],
        "only the stream file survives for orders"
    );
    assert_eq!(
        orders_left[0].reload_id, None,
        "stream rows never carry a reload_id"
    );

    // …and the OTHER reload's chunk file is untouched. Its reload is still `exporting`, and the
    // generic claim remains paused through `export_complete` and `publishing`; only its fenced
    // publication claim may retire the chunk.
    let h: Lsn = "0/500".parse().unwrap();
    finish_fenced(&mut tx, r2, h).await;
    assert!(
        claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()
            .is_empty()
    );
    let customers_left = publish_fenced(&mut tx, epoch, "customers", r2).await;
    assert_eq!(
        customers_left.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![keep_chunk]
    );
    assert_eq!(customers_left[0].kind, control::ManifestKind::Reload);
    assert_eq!(customers_left[0].reload_id, Some(r2));

    // A failed reload is terminal: the table is immediately requestable again.
    let r3 = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(r3 > r1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn adoption_batch_limit_is_an_exact_one_shot_boundary() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_015);
    let first = reload::request(
        &mut *tx,
        epoch,
        "public",
        "adopt_limit_first",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let second = reload::request(
        &mut *tx,
        epoch,
        "public",
        "adopt_limit_second",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 10)
            .await
            .unwrap()
            .len(),
        2
    );
    sqlx::query(
        "UPDATE public.walrus_table_reload
         SET lease_expiry = statement_timestamp() - interval '1 second'
         WHERE reload_id IN ($1, $2)",
    )
    .bind(first.0)
    .bind(second.0)
    .execute(&mut *tx)
    .await
    .unwrap();

    let adopted = reload::adopt_resumable(&mut *tx, epoch, "extractor-b", 60, 1, false)
        .await
        .unwrap();
    assert_eq!(
        adopted.iter().map(|row| row.reload_id).collect::<Vec<_>>(),
        vec![first],
        "LIMIT 1 is evaluated once rather than being rescanned by UPDATE"
    );
    let remainder = reload::adopt_resumable(&mut *tx, epoch, "extractor-b", 60, 10, false)
        .await
        .unwrap();
    assert_eq!(
        remainder
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![second]
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn pristine_adoption_restart_refreshes_identity_without_spending_budget() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_014);

    for (table, start_lsn) in [
        ("adopted_pre_f", None),
        ("adopted_pre_chunk", Some(Lsn::new(0x100))),
    ] {
        let old_id = reload::request(&mut *tx, epoch, "public", table, ReloadFlavor::Reload)
            .await
            .unwrap();
        reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
            .await
            .unwrap();
        let claimed = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
        let request_id = claimed
            .parent_request_id
            .expect("direct requests persist a private source-fence namespace");
        if let Some(f) = start_lsn {
            reload::record_start_fence(
                &mut *tx,
                old_id,
                f,
                ReloadFenceIdentity {
                    request_id: Some(request_id),
                    source_schema: "public",
                    source_table: table,
                    schema_version: SchemaVersionNo(4),
                },
            )
            .await
            .unwrap();
        }
        let old = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
        assert_eq!((old.chunk_no, old.cursor_pk.as_ref()), (0, None));

        let successor_id = reload::restart_pristine_adoption(&mut tx, &old)
            .await
            .unwrap();
        let failed = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
        assert_eq!(failed.status, ReloadStatus::Failed);
        assert!(
            failed
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("adopted pristine attempt")
        );

        let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
        assert_ne!(successor.reload_id, old.reload_id);
        assert_eq!(successor.parent_request_id, Some(request_id));
        assert_eq!(successor.status, ReloadStatus::Exporting);
        assert_eq!(
            successor.restart_count, old.restart_count,
            "pre-F and pre-chunk adoption must not consume the bounded restart budget"
        );
        assert_eq!((successor.chunk_no, successor.cursor_pk), (0, None));
        assert_eq!(
            successor.start_lsn, None,
            "the successor establishes fresh F"
        );
        assert_eq!(successor.final_lsn, None);
        assert_eq!(successor.schema_version, None);
        assert_eq!(successor.lease_holder.as_deref(), Some("extractor-a"));
    }

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn lost_snapshot_restart_purges_predecessor_and_creates_fresh_successor() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_013);

    let old_id = reload::request(
        &mut *tx,
        epoch,
        "public",
        "snapshot_lost",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    reload::claim_requested(&mut *tx, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    let f: Lsn = "0/100".parse().unwrap();
    let schema_version = SchemaVersionNo(4);
    let fence_request_id = reload::get(&mut *tx, old_id)
        .await
        .unwrap()
        .unwrap()
        .parent_request_id
        .expect("direct requests persist a private source-fence namespace");
    reload::record_start_fence(
        &mut *tx,
        old_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(fence_request_id),
            source_schema: "public",
            source_table: "snapshot_lost",
            schema_version,
        },
    )
    .await
    .unwrap();
    let old_lease = reload::get(&mut *tx, old_id)
        .await
        .unwrap()
        .unwrap()
        .exporter_lease("extractor-a")
        .unwrap();
    begin_full_scan_plan(&mut tx, &old_lease, f, schema_version).await;
    let mut old_chunk = chunk_file(epoch, "snapshot_lost", old_id, "0/100");
    old_chunk.schema_version = schema_version;
    insert_ready(&mut *tx, &old_chunk).await.unwrap();
    reload::record_exported_file(&mut *tx, &old_lease, f, schema_version)
        .await
        .unwrap();
    let old = reload::get(&mut *tx, old_id).await.unwrap().unwrap();

    let successor_id = reload::restart_for_lost_snapshot(&mut tx, &old, 3)
        .await
        .unwrap()
        .expect("the first snapshot-loss restart is below the cap");

    let failed = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
    assert_eq!(failed.status, ReloadStatus::Failed);
    assert!(
        failed
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("lost its source snapshot ownership")
    );
    let old_files: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_file_manifest WHERE reload_id = $1")
            .bind(old_id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        old_files, 0,
        "the predecessor's staged chunk is purged atomically"
    );

    let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
    assert_eq!(successor.status, ReloadStatus::Exporting);
    assert_eq!(successor.restart_count, 1);
    assert_eq!(successor.chunk_no, 0);
    assert_eq!(successor.cursor_pk, None);
    assert_eq!(
        successor.start_lsn, None,
        "the successor establishes fresh F"
    );
    assert_eq!(successor.first_lsn, None);
    assert_eq!(successor.final_lsn, None);
    assert_eq!(successor.schema_version, None);
    assert_eq!(successor.lease_holder.as_deref(), Some("extractor-a"));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn concurrent_claimers_partition_the_queue_via_skip_locked() {
    let pool = pool().await;
    let epoch = EpochNo(910_004);

    // SKIP LOCKED is only observable ACROSS transactions, so this test needs committed fixtures
    // (unlike the rolled-back-txn discipline above). Clean up leftovers from any crashed prior
    // run first — a stale non-terminal row would trip the one_live index — and again at the end.
    let cleanup = || async {
        sqlx::query(
            "WITH authorized AS MATERIALIZED (
               SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
             )
             DELETE FROM public.walrus_table_reload
             WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
        )
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
    };
    cleanup().await;
    let r1 = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    let r2 = reload::request(&pool, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();

    // Claimer A locks the oldest request and HOLDS its transaction open…
    let mut tx_a = pool.begin().await.unwrap();
    let a = reload::claim_requested(&mut *tx_a, epoch, "extractor-a", 60, 1)
        .await
        .unwrap();
    assert_eq!(a.iter().map(|r| r.reload_id).collect::<Vec<_>>(), vec![r1]);

    // …and claimer B, on a separate connection, neither blocks nor double-claims: FOR UPDATE
    // SKIP LOCKED steps over A's locked (still-uncommitted) row and hands B only the other one.
    let mut tx_b = pool.begin().await.unwrap();
    let b = reload::claim_requested(&mut *tx_b, epoch, "extractor-b", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        b.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![r2],
        "B must skip A's locked row — overlap here means a double export"
    );

    tx_a.rollback().await.unwrap();
    tx_b.rollback().await.unwrap();
    cleanup().await;
}

#[tokio::test]
async fn concurrent_claimer_cannot_skip_a_locked_fifo_head_for_the_same_table() {
    let pool = pool().await;
    let epoch = EpochNo(910_010);

    // This lock-order property needs committed fixtures and two connections. As in the general
    // SKIP LOCKED test, clean this test-owned epoch before and after in case an earlier run died.
    let cleanup = || async {
        sqlx::query(
            "WITH authorized AS MATERIALIZED (
               SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true) AS protocol
             )
             DELETE FROM public.walrus_table_reload
             WHERE epoch = $1 AND (SELECT protocol = '2-delete' FROM authorized)",
        )
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
    };
    cleanup().await;
    let first_request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0xA01),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let first = reload::request_from_source(&pool, &first_request)
        .await
        .unwrap();
    let second_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0xA02),
        ..first_request
    };
    reload::request_from_source(&pool, &second_request)
        .await
        .unwrap();

    let mut tx_a = pool.begin().await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx_a, epoch, "extractor-a", 60, 1)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![first]
    );

    // A's status flip is uncommitted. B skips A's row lock but must not overtake it by claiming
    // the next source UUID for the same table.
    let mut tx_b = pool.begin().await.unwrap();
    assert!(
        reload::claim_requested(&mut *tx_b, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "SKIP LOCKED must not turn a per-table FIFO into concurrent exports"
    );

    tx_a.rollback().await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx_b, epoch, "extractor-b", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![first],
        "after the head lock is released, the head — not its successor — is claimable"
    );

    tx_b.rollback().await.unwrap();
    cleanup().await;
}
