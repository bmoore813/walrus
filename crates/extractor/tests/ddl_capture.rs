#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! DDL capture against compose (`#[ignore]` — needs source PG + MinIO + control PG). An `ALTER TABLE …
//! ADD COLUMN` on the source writes a `ddl_manifest` row (stamped with the DDL's `c_lsn`), bumps the
//! table's structural `schema_version`, and cuts a fresh Parquet file — so the prior file carries the
//! old version and the next the new (the homogeneous-file rule). A `COMMENT ON` is recorded but is
//! metadata-only (no bump, no cut). `public.walrus_ddl_audit`/`public.walrus_heartbeat` are never materialised. The
//! parsing/version logic is unit-tested in `src/ddl.rs`.
//!
//!   cargo test -p extractor --test ddl_capture -- --ignored

use common::{EpochNo, Lsn, UtcTimestamp};
use extractor::batch::{BatchTriggers, SystemClock};
use extractor::consume::{BatchRouter, cache_relation, flush_batch, on_frame, persist_registry};
use extractor::ddl::{DdlConsumer, DdlEvent, TransactionScope};
use extractor::heartbeat::InternalTables;
use extractor::pgoutput::{Message, StreamCtx};
use extractor::relcache::RelationCache;
use extractor::replication::{ReplicationMessage, ReplicationStream};
use extractor::slot::verify_or_create_slot;
use extractor::staging::ParquetStager;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0002: &str = include_str!("../../../migrations/source/0002_ddl_triggers.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn source() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn minio() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

async fn clear_control_epoch(pool: &sqlx::PgPool, epoch: EpochNo) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_delete_protocol','2',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.manifest_fence_maintenance','2-delete',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('walrus.schema_registry_maintenance','1-delete',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    for table in [
        "file_manifest",
        "stream_manifest_group",
        "stream_txn_publication",
        "manifest_publication_fence",
        "ddl_manifest",
        "schema_registry",
    ] {
        let statement = format!("DELETE FROM public.walrus_{table} WHERE epoch = $1");
        sqlx::query(&statement)
            .bind(epoch)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + MinIO + control PG)"]
async fn alter_add_column_bumps_version_and_cuts_file() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_ddl";
    let epoch = EpochNo(2_330_001);
    let admin = source().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0002).await.unwrap();
    admin
        .execute(
            "ALTER TABLE public.orders DROP COLUMN IF EXISTS ddl_extra",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 850000 AND 850999",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    // The control-plane setup happens BEFORE the CopyBoth stream opens. Nothing reads the
    // replication socket or sends standby feedback until the decode loop below, and the harness runs
    // `wal_sender_timeout=5s`, so connecting + migrating + clearing the epoch while the stream is
    // already open is enough for the walsender to terminate it under a loaded serial sweep ("source
    // closed the replication connection" on the first `next()`). The slot — created above — retains
    // the WAL, so opening the stream later replays every frame from `resume.start_lsn()`.
    let stager = ParquetStager::new(minio(), "walrus", epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    clear_control_epoch(&pool, epoch).await;
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    // High row cap → the pre-DDL row stays buffered until the DDL CUTS it (the interesting path).
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: std::num::NonZeroU64::MAX,
            max_bytes: std::num::NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
    );
    let mut ddl = DdlConsumer::new(epoch);
    let mut internal = InternalTables::default();
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();
    let mut ordinary_xid = None;
    let mut ordinary_has_routed_data = false;

    // v1 row · ALTER ADD COLUMN · v2 rows · COMMENT (metadata) · a final v2 row to give a stop marker.
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (850001, 'v1')",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE public.orders ADD COLUMN ddl_extra text", &[])
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO public.orders (id, status, ddl_extra) VALUES (850002, 'v2', 'x')",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute("COMMENT ON TABLE public.orders IS 'walrus ddl test'", &[])
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO public.orders (id, status, ddl_extra) VALUES (850003, 'end', 'y')",
            &[],
        )
        .await
        .unwrap();

    let mut saw_end = false;
    let mut end_pending = false;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !saw_end {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Begin { xid, .. } => {
                    assert!(
                        ordinary_xid.replace(*xid).is_none(),
                        "ordinary transactions cannot overlap"
                    );
                    ordinary_has_routed_data = false;
                    router
                        .route(&cache, &msg, frame_lsn, common::SchemaVersionNo(1))
                        .unwrap();
                }
                Message::Relation { relation, .. } => {
                    internal.note_relation(relation);
                    let v = ddl.version_for(
                        TransactionScope::Ordinary,
                        &relation.schema,
                        &relation.name,
                    );
                    if let Some(row) =
                        cache_relation(&mut cache, epoch, relation.clone(), v).unwrap()
                    {
                        router.bind_relation(relation.oid, v);
                        if ddl.is_provisional(&relation.schema, &relation.name, v) {
                            ddl.stage_registry(TransactionScope::Ordinary, row);
                        } else {
                            persist_registry(&pool, &row).await.unwrap();
                        }
                    }
                }
                Message::Insert {
                    relation_oid, new, ..
                } if internal.is_ddl_audit(*relation_oid) => {
                    let rel = internal.ddl_audit_rel().unwrap();
                    let ev = DdlEvent::from_tuple(rel, new).unwrap();
                    let previous_for_oid = ev
                        .c_rel_oid
                        .and_then(|oid| cache.latest_for(oid))
                        .map(|cached| cached.relation.clone());
                    let Some(previous_for_oid) = previous_for_oid else {
                        continue;
                    };
                    let observation = ddl.observe(
                        TransactionScope::Ordinary,
                        ev.clone(),
                        Some(&previous_for_oid),
                    );
                    if let Some(version) = observation.structural_version
                        && !ev.is_table_drop()
                    {
                        if let Some(after) = ev.relation_after(Some(&previous_for_oid)).unwrap()
                            && let Some(row) =
                                cache_relation(&mut cache, epoch, after, version).unwrap()
                        {
                            if observation.replay {
                                persist_registry(&pool, &row).await.unwrap();
                            } else {
                                ddl.stage_registry(TransactionScope::Ordinary, row);
                            }
                        }
                        for sealed in router
                            .cut_table(&cache, &ev.source_schema, &ev.source_table)
                            .unwrap()
                        {
                            flush_batch(&stager, &pool, epoch, sealed).await.unwrap();
                        }
                    }
                }
                Message::Insert { new, .. } => {
                    ordinary_has_routed_data = true;
                    router
                        .route(&cache, &msg, frame_lsn, common::SchemaVersionNo(1))
                        .unwrap();
                    // The last row (850003) is our stop marker.
                    if matches!(new.first(), Some(common::TupleValue::Text(s)) if s == "850003") {
                        end_pending = true;
                    }
                }
                Message::Commit {
                    commit_lsn,
                    commit_ts,
                    ..
                } => {
                    let xid = ordinary_xid.take().expect("Commit has a matching Begin");
                    let has_routed_data = std::mem::take(&mut ordinary_has_routed_data);
                    let sealed = router
                        .route(&cache, &msg, frame_lsn, common::SchemaVersionNo(1))
                        .unwrap();
                    ddl.on_commit(
                        &pool,
                        xid,
                        *commit_lsn,
                        UtcTimestamp::from_pg_micros(*commit_ts).unwrap(),
                        has_routed_data,
                        0,
                    )
                    .await
                    .unwrap();
                    for sealed in sealed {
                        flush_batch(&stager, &pool, epoch, sealed).await.unwrap();
                    }
                    if end_pending {
                        saw_end = true;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the DDL + rows stream within 30s");

    // Final drain: force-seal the buffered v2 rows into a v2 file.
    for sealed in router.drain_for_shutdown().unwrap() {
        flush_batch(&stager, &pool, epoch, sealed).await.unwrap();
    }

    // --- Assertions. Files: a v1 file (the cut pre-DDL row) AND v2 file(s) (post-DDL rows).
    let files: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT schema_version, row_count FROM public.walrus_file_manifest WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        files.iter().any(|(v, _)| *v == 1),
        "a schema_version=1 file exists (pre-DDL, cut)"
    );
    assert!(
        files.iter().any(|(v, _)| *v == 2),
        "a schema_version=2 file exists (post-DDL)"
    );

    // ddl_manifest: the ALTER bumped to version 2 with a real c_lsn; the COMMENT is recorded but did
    // NOT bump (still version 2).
    let ddls: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT c_tag, schema_version, c_lsn::text FROM public.walrus_ddl_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    let alter = ddls
        .iter()
        .find(|(t, ..)| t == "ALTER TABLE")
        .expect("ALTER recorded");
    assert_eq!(alter.1, 2, "ALTER produced schema_version 2");
    assert!(
        alter.2.parse::<Lsn>().unwrap() > Lsn::ZERO,
        "the ALTER carries the DDL's c_lsn"
    );
    let comment = ddls
        .iter()
        .find(|(t, ..)| t == "COMMENT")
        .expect("COMMENT recorded");
    assert_eq!(
        comment.1, 2,
        "COMMENT is metadata-only — no version bump beyond the structural 2"
    );

    // The ordinary ALTER has a durable, ordered zero-child group. Its real Begin xid, Commit LSN,
    // and final schema version make it indistinguishable from a streamed schema-only barrier to the
    // transformer. COMMENT remains audit-only and therefore creates no second group.
    let barriers: Vec<(i64, i64, String, i64)> = sqlx::query_as(
        "SELECT expected_files, final_schema_version, commit_lsn::text, top_xid \
         FROM public.walrus_stream_manifest_group WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(barriers.len(), 1, "only structural DDL creates a barrier");
    assert_eq!(barriers[0].0, 0, "ordinary DDL barrier has no dummy files");
    assert_eq!(barriers[0].1, 2);
    assert_eq!(barriers[0].2, alter.2);
    assert!(barriers[0].3 > 0, "receipt retains the real source xid");

    // Internal tables are NEVER materialised.
    let internal_files: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_file_manifest WHERE epoch = $1 AND source_table IN ('walrus_ddl_audit', 'walrus_heartbeat')",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        internal_files, 0,
        "public.walrus_ddl_audit / public.walrus_heartbeat are never files"
    );

    // --- Cleanup.
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM public.walrus_file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&object_store::path::Path::from(key)).await;
        }
    }
    clear_control_epoch(&pool, epoch).await;
    let _ = admin
        .execute(
            "ALTER TABLE public.orders DROP COLUMN IF EXISTS ddl_extra",
            &[],
        )
        .await;
    let _ = admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 850000 AND 850999",
            &[],
        )
        .await;
    drop(stream);
    drop_slot(&admin, slot).await;
}
