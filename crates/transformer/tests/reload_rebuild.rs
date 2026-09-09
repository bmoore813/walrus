#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! The rebuild trigger against compose (`#[ignore]` — needs control PG + MinIO). The first
//! claimed `kind='reload'` file with `reload_id >` the `_walrus_meta` latch replaces both tables
//! at the file's schema_version, clears the quarantine, purges superseded pending rows, sets the
//! latch, and then ordinary Phase A/B replays chunks + post-`W` stream files in `(lsn_end, id)`
//! order — converging the mirror to the source (phantoms dead, mid-export updates win, deletes
//! no-op through the MERGE's guard). Stale-id files retire unapplied (latest wins, H9).
//!
//!   cargo test -p transformer --test reload_rebuild -- --ignored --test-threads=1

mod support;

use common::{EpochNo, Lsn, PgColumn, PgRelation, ReplicaIdentity};
use control::reload::{self, ReloadFenceIdentity, ReloadFlavor};
use std::time::Duration;
use transformer::duck::{S3Access, TableDb};
use transformer::health::TransformerState;
use transformer::phase_a::{TableCtx, run_phase_a};
use transformer::phase_b::run_phase_b;

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

/// A scratch directory for one test's `.duckdb` file. The returned guard deletes it on drop — even
/// when an assertion panics, which a trailing `remove_dir_all` would skip.
fn tmpdir(name: &str) -> tempfile::TempDir {
    let prefix = format!("walrus-transformer-rr-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

/// Write a `(id, status, op, commit_lsn, lsn)` Parquet to MinIO with the complete provenance
/// document emitted by the production extractor. Ops use the wire values (`i`/`u`/`d`); LSNs are the
/// sortable 16-hex text the extractor emits.
fn write_rows_v(
    epoch: EpochNo,
    name: &str,
    kind: common::Kind,
    schema_version: common::SchemaVersionNo,
    rows: &[(i32, &str, &str, &str, &str)],
) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}'; \
         CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_extractor_meta VARCHAR);",
        a.region,
        a.endpoint,
        a.access_key_id,
        a.secret_access_key.expose()
    ))
    .unwrap();
    for (id, status, op, commit, lsn) in rows {
        let meta = support::extractor_meta(
            epoch,
            &format!("reload-rebuild-{}-{name}", epoch.0),
            schema_version,
            "public",
            "orders",
            kind,
            op,
            commit,
            lsn,
        );
        w.execute(
            "INSERT INTO fixture VALUES (?, ?, ?)",
            duckdb::params![id, status, serde_json::to_string(&meta).unwrap()],
        )
        .unwrap();
    }
    let uri = format!("s3://walrus/{epoch}/public/orders/{name}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

fn write_rows(
    epoch: EpochNo,
    name: &str,
    kind: common::Kind,
    rows: &[(i32, &str, &str, &str, &str)],
) -> String {
    write_rows_v(epoch, name, kind, common::SchemaVersionNo(1), rows)
}

async fn seed_file(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    uri: &str,
    kind: &str,
    lsn_end: &str,
    reload_id: Option<common::ReloadId>,
) -> i64 {
    seed_file_v(
        pool,
        epoch,
        uri,
        kind,
        lsn_end,
        reload_id,
        common::SchemaVersionNo(1),
    )
    .await
}

/// Like [`seed_file`] but at an explicit `schema_version` — a file at a version NEWER than the
/// transformer's would trigger a reconcile (the skip path).
async fn seed_file_v(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    uri: &str,
    kind: &str,
    lsn_end: &str,
    reload_id: Option<common::ReloadId>,
    schema_version: common::SchemaVersionNo,
) -> i64 {
    let (object_size, sha256) = support::fingerprint(uri).await;
    let row_count = support::parquet_row_count(uri);
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri.into(),
            kind: kind.parse::<control::ManifestKind>().unwrap(),
            row_count,
            object_size,
            sha256,
            lsn_start: lsn_end.parse().unwrap(),
            lsn_end: lsn_end.parse().unwrap(),
            schema_version,
            reload_id,
        },
    )
    .await
    .unwrap()
    .0
}

/// Fresh control state + an owned `TableCtx` (DuckDB in a temp dir).
async fn setup(epoch: EpochNo) -> (TableCtx, tempfile::TempDir) {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    support::cleanup_epoch(&pool, epoch).await;
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
    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.configure_s3(&s3()).unwrap();
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
        rel: orders(),
        db,
        state: TransformerState::new(),
        max_files: std::num::NonZeroI64::new(100).unwrap(),
        max_integrity_resnapshots: 1,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    };
    (ctx, dir)
}

#[derive(Debug)]
struct PlannedReload {
    reload_id: common::ReloadId,
    lease: control::ExporterLease,
    request_id: uuid::Uuid,
    start_lsn: Lsn,
    final_lsn: Lsn,
}

/// Open the same one-range exported-snapshot plan production uses. Call [`finish_reload`] only
/// after every reload manifest belonging to this fixture has been inserted.
async fn planned_reload(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    l1: &str,
    h: &str,
    flavor: ReloadFlavor,
) -> PlannedReload {
    let id = reload::request(pool, epoch, "public", "orders", flavor)
        .await
        .unwrap();
    reload::claim_requested(pool, epoch, "extractor-t", 60, 10)
        .await
        .unwrap();
    let request_id = reload::get(pool, id)
        .await
        .unwrap()
        .unwrap()
        .parent_request_id
        .expect("direct reload has a durable fence namespace");
    let start_lsn = l1.parse::<Lsn>().unwrap();
    let final_lsn = h.parse::<Lsn>().unwrap();
    let fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: common::SchemaVersionNo(1),
    };
    reload::record_start_fence(pool, id, start_lsn, fence)
        .await
        .unwrap();
    let row = reload::get(pool, id).await.unwrap().unwrap();
    let lease = row.exporter_lease("extractor-t").unwrap();
    let mut conn = pool.acquire().await.unwrap();
    reload::begin_export_plan(
        &mut conn,
        &lease,
        start_lsn,
        common::SchemaVersionNo(1),
        control::ExportSnapshot {
            identity: &format!("{}:{}:", id.0, id.0 + 1),
            xmin: id.0,
            xmax: id.0 + 1,
        },
        &[control::ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }],
    )
    .await
    .unwrap();
    PlannedReload {
        reload_id: id,
        lease,
        request_id,
        start_lsn,
        final_lsn,
    }
}

async fn finish_reload(pool: &sqlx::PgPool, export: &PlannedReload) {
    let (file_count, row_count): (i64, i64) = sqlx::query_as(
        "SELECT count(*)::bigint, COALESCE(sum(row_count), 0)::bigint
         FROM public.walrus_file_manifest WHERE reload_id = $1",
    )
    .bind(export.reload_id.0)
    .fetch_one(pool)
    .await
    .unwrap();
    for _ in 0..file_count {
        reload::record_exported_file(
            pool,
            &export.lease,
            export.start_lsn,
            common::SchemaVersionNo(1),
        )
        .await
        .unwrap();
    }
    reload::record_export_range(pool, &export.lease, 0, file_count, row_count)
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    reload::seal_export(
        &mut conn,
        &export.lease,
        export.start_lsn,
        common::SchemaVersionNo(1),
    )
    .await
    .unwrap();
    let fence = ReloadFenceIdentity {
        request_id: Some(export.request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: common::SchemaVersionNo(1),
    };
    reload::record_end_marker(pool, export.reload_id, export.final_lsn, fence)
        .await
        .unwrap();
    reload::complete_export(pool, &export.lease, export.final_lsn)
        .await
        .unwrap();
}

fn mirror_status(ctx: &TableCtx, id: i32) -> Option<String> {
    ctx.db
        .conn()
        .query_row("SELECT status FROM orders WHERE id = ?", [id], |r| r.get(0))
        .ok()
}

fn mirror_count(ctx: &TableCtx) -> i64 {
    ctx.db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap()
}

#[test]
fn fixture_metadata_is_complete_protocol_v2_provenance() {
    let meta = support::extractor_meta(
        EpochNo(670_000),
        "fixture-batch",
        common::SchemaVersionNo(3),
        "public",
        "orders",
        common::Kind::Reload,
        "i",
        "0000000000000100",
        "00000000000000ff",
    );
    let json = serde_json::to_value(&meta).unwrap();

    for key in [
        "op",
        "lsn",
        "commit_lsn",
        "commit_ts",
        "xid",
        "epoch",
        "batch_id",
        "schema_version",
        "source_schema",
        "source_table",
        "kind",
        "extractor_instance",
        "extractor_processed_at",
    ] {
        assert!(json.get(key).is_some(), "fixture metadata omitted {key}");
    }
    assert_eq!(json["op"], "i");
    assert_eq!(json["kind"], "reload");
    assert!(json.get("unchanged_toast").is_none());
    assert!(meta.lsn <= meta.commit_lsn);
    assert_eq!(
        serde_json::from_value::<common::ExtractorMeta>(json).unwrap(),
        meta
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn rebuild_converges_mirror_to_source_and_kills_phantoms() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_001);
    let (ctx, _dir) = setup(epoch).await;

    // The OLD world: ids 1,2 streamed normally; then a phantom drifts into the mirror directly.
    let old = write_rows(
        epoch,
        "old",
        common::Kind::Stream,
        &[
            (1, "old", "i", "0000000000000050", "0000000000000050"),
            (2, "b", "i", "0000000000000050", "0000000000000050"),
        ],
    );
    seed_file(&ctx.pool, epoch, &old, "stream", "0/50", None).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(mirror_count(&ctx), 2);
    ctx.db
        .conn()
        .execute_batch("INSERT INTO orders (id, status) VALUES (9999, 'ghost')")
        .unwrap();

    // The reload: chunks stamped L1 = 0/100 carry the source truth {1:'snap', 2:'b', 3:'c'};
    // a mid-export stream file at 0/200 updates 1 → 'newest' and deletes 2.
    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/200", ReloadFlavor::Reload).await;
    let reload_id = export.reload_id;
    let chunk = write_rows(
        epoch,
        "chunk1",
        common::Kind::Reload,
        &[
            (1, "snap", "i", "0000000000000100", "0000000000000100"),
            (2, "b", "i", "0000000000000100", "0000000000000100"),
            (3, "c", "i", "0000000000000100", "0000000000000100"),
        ],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    let post_w = write_rows(
        epoch,
        "postw",
        common::Kind::Stream,
        &[
            (1, "newest", "u", "0000000000000200", "0000000000000200"),
            (2, "", "d", "0000000000000200", "0000000000000200"),
        ],
    );
    seed_file(&ctx.pool, epoch, &post_w, "stream", "0/200", None).await;
    finish_reload(&ctx.pool, &export).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // Convergence: the phantom is dead (the clear), the mid-export update wins (chunk stamp L_i
    // loses dedup to 0/200), the mid-export delete holds (its winner is 'd' — and a delete for a
    // row the rebuilt mirror never saw would no-op through NOT MATCHED AND op='d').
    assert_eq!(
        mirror_status(&ctx, 9999),
        None,
        "phantom killed by the clear"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("newest"));
    assert_eq!(mirror_status(&ctx, 2), None, "mid-export delete holds");
    assert_eq!(mirror_status(&ctx, 3).as_deref(), Some("c"));
    assert_eq!(mirror_count(&ctx), 2);
    assert_eq!(ctx.db.recorded_reload_id().unwrap(), Some(reload_id));
    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cp.transformed_lsn,
        "0/200".parse().unwrap(),
        "monotonic, no rewind"
    );

    // A subsequent transformer tick is an idempotent no-op. Protocol v2 deliberately forbids
    // re-publishing a reload object after the export was sealed; crash recovery resumes the
    // durable publication receipt instead of manufacturing a late manifest.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(
        mirror_status(&ctx, 1).as_deref(),
        Some("newest"),
        "no re-clear, no regression"
    );
    assert_eq!(mirror_status(&ctx, 2), None);
    assert_eq!(mirror_count(&ctx), 2);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn superseded_rows_are_purged_and_their_content_discarded() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_002);
    let (ctx, _dir) = setup(epoch).await;

    // One claim batch holds the whole story: a pre-`W` stream file (sorts first), the chunk, a
    // post-`W` stream file. The pre-`W` file is applied into the OLD raw before the chunk fires
    // the trigger — the wasted-but-harmless path — and the trigger then (a) PURGES its manifest
    // row (claimed rows are only deleted at end-of-batch, so it is still visible to the purge)
    // and (b) discards its content with the old raw. The chunk re-covers its commit (C ≤ L₁).
    let pre_w = write_rows(
        epoch,
        "prew",
        common::Kind::Stream,
        &[(
            8,
            "discarded-by-the-clear",
            "i",
            "0000000000000060",
            "0000000000000060",
        )],
    );
    let pre_w_id = seed_file(&ctx.pool, epoch, &pre_w, "stream", "0/60", None).await;

    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/200", ReloadFlavor::Reload).await;
    let reload_id = export.reload_id;
    let chunk = write_rows(
        epoch,
        "chunk1",
        common::Kind::Reload,
        &[(1, "snap", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    let post_w = write_rows(
        epoch,
        "postw",
        common::Kind::Stream,
        &[(9, "survives", "i", "0000000000000200", "0000000000000200")],
    );
    seed_file(&ctx.pool, epoch, &post_w, "stream", "0/200", None).await;
    finish_reload(&ctx.pool, &export).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    let pre_w_gone: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_file_manifest WHERE id = $1")
            .bind(pre_w_id)
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(
        pre_w_gone, 0,
        "the superseded row left the queue at trigger time"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("snap"));
    assert_eq!(
        mirror_status(&ctx, 9).as_deref(),
        Some("survives"),
        "post-`W` applies after the chunks"
    );
    assert_eq!(
        mirror_status(&ctx, 8),
        None,
        "pre-`W` content is discarded with the old raw — the chunks re-cover its commit"
    );
    assert_eq!(mirror_count(&ctx), 2);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn delete_superseded_prunes_by_kind_and_lsn() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_005);
    let (ctx, _dir) = setup(epoch).await;

    // The contract itself (the loop's safety net for LATE-arriving superseded rows too): every
    // non-reload row at lsn_end <= first_lsn goes; the chunk at lsn_end == first_lsn survives its
    // own purge (the kind filter); later stream rows survive.
    let old_stream_file = write_rows(
        epoch,
        "old-stream",
        common::Kind::Stream,
        &[(1, "x", "i", "0000000000000060", "0000000000000060")],
    );
    let boundary_stream_file = write_rows(
        epoch,
        "boundary-stream",
        common::Kind::Stream,
        &[(1, "x", "i", "0000000000000100", "0000000000000100")],
    );
    let reload_file = write_rows(
        epoch,
        "boundary-reload",
        common::Kind::Reload,
        &[(1, "x", "i", "0000000000000100", "0000000000000100")],
    );
    let newer_stream_file = write_rows(
        epoch,
        "newer-stream",
        common::Kind::Stream,
        &[(1, "x", "i", "0000000000000200", "0000000000000200")],
    );
    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/200", ReloadFlavor::Reload).await;
    let old_stream = seed_file(&ctx.pool, epoch, &old_stream_file, "stream", "0/60", None).await;
    let boundary_stream = seed_file(
        &ctx.pool,
        epoch,
        &boundary_stream_file,
        "stream",
        "0/100",
        None,
    )
    .await;
    let chunk_at_boundary = seed_file(
        &ctx.pool,
        epoch,
        &reload_file,
        "reload",
        "0/100",
        Some(export.reload_id),
    )
    .await;
    let newer_stream = seed_file(
        &ctx.pool,
        epoch,
        &newer_stream_file,
        "stream",
        "0/200",
        None,
    )
    .await;
    finish_reload(&ctx.pool, &export).await;
    let publication = reload::claim_publication(
        &ctx.pool,
        epoch,
        "public",
        "orders",
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();

    let purged = control::delete_publication_superseded(
        &ctx.pool,
        &publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await
    .unwrap();
    assert_eq!(purged, 2, "old + boundary stream rows purged");

    let survivors: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM public.walrus_file_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch)
    .fetch_all(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(
        survivors,
        vec![chunk_at_boundary, newer_stream],
        "chunk 1 survives its own purge; post-`W` rows survive"
    );
    assert!(!survivors.contains(&old_stream) && !survivors.contains(&boundary_stream));

    sqlx::query("WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM public.walrus_file_manifest WHERE epoch = $1 AND (SELECT protocol='2' FROM authorized)")
        .bind(epoch)
        .execute(&ctx.pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn unknown_and_post_seal_reload_files_are_rejected() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_003);
    let (ctx, _dir) = setup(epoch).await;

    let stale = write_rows(
        epoch,
        "stale",
        common::Kind::Reload,
        &[(1, "stale", "i", "0000000000000100", "0000000000000100")],
    );
    let (object_size, sha256) = support::fingerprint(&stale).await;
    let candidate = |reload_id| control::NewManifestFile {
        epoch,
        source_schema: "public".into(),
        source_table: "orders".into(),
        s3_uri: format!("{stale}-{reload_id}"),
        kind: control::ManifestKind::Reload,
        row_count: 1,
        object_size,
        sha256: sha256.clone(),
        lsn_start: "0/100".parse().unwrap(),
        lsn_end: "0/100".parse().unwrap(),
        schema_version: common::SchemaVersionNo(1),
        reload_id: Some(common::ReloadId(reload_id)),
    };
    assert!(
        control::insert_ready(&ctx.pool, &candidate(5))
            .await
            .is_err(),
        "a reload object cannot name an unknown attempt"
    );

    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    finish_reload(&ctx.pool, &export).await;
    assert!(matches!(
        control::insert_ready(&ctx.pool, &candidate(export.reload_id.0)).await,
        Err(control::ControlError::CheckViolation { .. })
    ));
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE epoch=$1 AND kind='reload'",
    )
    .bind(epoch.0)
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(queued, 0, "no unknown or late baseline entered the queue");
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn rebuild_clears_the_lossy_cast_quarantine() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_004);
    let (ctx, _dir) = setup(epoch).await;

    // The quarantine state: a lossy ALTER COLUMN TYPE cast failed; /ready is degraded. (The
    // entry path is ddl_destructive.rs's covered ground; the EXIT is under test here.)
    ctx.state.quarantine_table("public", "orders");
    assert!(!ctx.state.is_ready() || !ctx.state.is_started());

    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    let reload_id = export.reload_id;
    let chunk = write_rows(
        epoch,
        "chunk1",
        common::Kind::Reload,
        &[(1, "recovered", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    finish_reload(&ctx.pool, &export).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert!(
        !ctx.state.is_quarantined(),
        "the rebuild is the quarantine's one exit"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("recovered"));
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_alias_uses_the_same_full_rebuild_path() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_006);
    let (ctx, _dir) = setup(epoch).await;

    // A live mirror contains ids 1,2. The full dump below contains ids 1,3, so identical rebuild
    // semantics require id 2 to disappear. Latch quarantine too: either spelling is the recovery
    // path and must clear it only after publishing the replacement generation.
    let live = write_rows(
        epoch,
        "live",
        common::Kind::Stream,
        &[
            (1, "live", "i", "0000000000000050", "0000000000000050"),
            (2, "keep", "i", "0000000000000050", "0000000000000050"),
        ],
    );
    seed_file(&ctx.pool, epoch, &live, "stream", "0/50", None).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(mirror_count(&ctx), 2);
    ctx.state.quarantine_table("public", "orders");

    // The legacy `resync` spelling is retained, but it now selects the same rebuild protocol.
    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Resync).await;
    let reload_id = export.reload_id;

    // This pre-fence stream file is covered by the full dump and must be purged, not replayed into
    // the replacement generation.
    let pre_w = write_rows(
        epoch,
        "prew",
        common::Kind::Stream,
        &[(
            4,
            "must-be-purged",
            "i",
            "0000000000000060",
            "0000000000000060",
        )],
    );
    seed_file(&ctx.pool, epoch, &pre_w, "stream", "0/60", None).await;

    // The complete source image at the fence contains exactly ids 1 and 3.
    let chunk = write_rows(
        epoch,
        "chunk1",
        common::Kind::Reload,
        &[
            (1, "resynced", "i", "0000000000000100", "0000000000000100"),
            (3, "new", "i", "0000000000000100", "0000000000000100"),
        ],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    finish_reload(&ctx.pool, &export).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror_status(&ctx, 2),
        None,
        "full rebuild removes stale rows"
    );
    assert_eq!(
        mirror_status(&ctx, 1).as_deref(),
        Some("resynced"),
        "the replacement generation carries the dump value"
    );
    assert_eq!(mirror_status(&ctx, 3).as_deref(), Some("new"));
    assert_eq!(
        mirror_status(&ctx, 4),
        None,
        "the alias uses the same pre-fence purge"
    );
    assert_eq!(mirror_count(&ctx), 2);

    assert_eq!(
        ctx.db.recorded_reload_id().unwrap(),
        Some(reload_id),
        "the alias records the published rebuild identity"
    );
    assert!(
        !ctx.state.is_quarantined(),
        "the alias is the same quarantine-recovery path"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn superseded_version_crossing_file_is_skipped_not_reconciled() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(670_007);
    let (ctx, _dir) = setup(epoch).await;

    // A live mirror {1} at the transformer's current schema_version (1).
    let live = write_rows(
        epoch,
        "live",
        common::Kind::Stream,
        &[(1, "live", "i", "0000000000000050", "0000000000000050")],
    );
    seed_file(&ctx.pool, epoch, &live, "stream", "0/50", None).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // A BLOCKER stream file at a NEWER schema_version (2) — reconciling it would run the DDL path
    // (and on a lossy cast, quarantine). It sits BELOW the reload's first_lsn, so a pending rebuild
    // supersedes it. This is the quarantine-recovery blocker, simulated without the ddl
    // machinery: the skip happens BEFORE reconcile, so no v2 registry row is needed.
    let blocker = write_rows_v(
        epoch,
        "blocker",
        common::Kind::Stream,
        common::SchemaVersionNo(2),
        &[(8, "blocker", "i", "0000000000000060", "0000000000000060")],
    );
    let blocker_id = seed_file_v(
        &ctx.pool,
        epoch,
        &blocker,
        "stream",
        "0/60",
        None,
        common::SchemaVersionNo(2),
    )
    .await;

    // A drained rebuild reload at first_lsn = 0/100 (>= the blocker's 0/60 ⇒ supersedes it).
    let export = planned_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    let reload_id = export.reload_id;
    let chunk = write_rows(
        epoch,
        "chunk1",
        common::Kind::Reload,
        &[(1, "rebuilt", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    finish_reload(&ctx.pool, &export).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // The blocker was SKIPPED: never reconciled (NO quarantine), never appended to raw, and purged
    // by the rebuild's delete_superseded. The rebuild replaced the mirror from the chunk. Without
    // the skip, the blocker (lower lsn_end) would reconcile-then-quarantine before the chunk fired.
    assert!(
        !ctx.state.is_quarantined(),
        "the superseded blocker did not quarantine the transformer"
    );
    let raw_blocker: i64 = ctx
        .db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw WHERE id = 8", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        raw_blocker, 0,
        "the blocker's rows were never appended to raw"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("rebuilt"));
    let blocker_gone: i64 =
        sqlx::query_scalar("SELECT count(*) FROM public.walrus_file_manifest WHERE id = $1")
            .bind(blocker_id)
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(blocker_gone, 0, "the rebuild purged the skipped blocker");
}
