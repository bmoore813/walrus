#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! End-to-end DDL transactions: a schema boundary inside ordinary and streamed transactions keeps
//! both tuple shapes, while whole-transaction and savepoint rollback publish neither DDL history nor
//! provisional schema-registry state.
#![cfg(feature = "it")]

use e2e::Harness;
use sqlx::Acquire;
use std::time::Duration;

const HALF: i64 = 6_000;

async fn nudge_other_table(h: &Harness, id: i32) {
    sqlx::query(
        "INSERT INTO public.q_target (id, status, n) VALUES ($1, 'ddl-nudge', 1) \
         ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
    )
    .bind(id)
    .execute(h.source_pool())
    .await
    .unwrap();
}

async fn converge_after_sentinel(h: &Harness, sentinel: i32, has_extra: bool) {
    let before = h.source_wal_lsn().await.unwrap();
    let sql = if has_extra {
        "INSERT INTO public.orders (id, status, ddl_txn_extra) VALUES ($1, 'sentinel', 'last')"
    } else {
        "INSERT INTO public.orders (id, status) VALUES ($1, 'sentinel')"
    };
    sqlx::query(sql)
        .bind(sentinel)
        .execute(h.source_pool())
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("pipeline converges beyond the sentinel");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn ordinary_transaction_keeps_rows_on_both_sides_of_ddl() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let audit_floor: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id), 0) FROM public.walrus_ddl_audit")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
    let mut connection = h.source_pool().acquire().await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    sqlx::query("INSERT INTO public.orders (id, status) VALUES (910001, 'pre-ddl')")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE public.orders ADD COLUMN ddl_txn_extra text")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO public.orders (id, status, ddl_txn_extra) \
         VALUES (910002, 'post-ddl', 'new-shape')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(connection);

    converge_after_sentinel(&h, 910003, true).await;

    let audit: (i64, String, String) = sqlx::query_as(
        "SELECT id, c_lsn::text, c_ddl_text FROM public.walrus_ddl_audit \
         WHERE id > $1 AND c_table = 'orders' AND c_tag = 'ALTER TABLE'",
    )
    .bind(audit_floor)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    let manifest: (i64, String, i64, Option<String>) = sqlx::query_as(
        "SELECT source_audit_id, c_lsn::text, schema_version, c_ddl_text \
         FROM public.walrus_ddl_manifest \
         WHERE epoch = $1 AND source_table = 'orders' AND c_tag = 'ALTER TABLE'",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(manifest.0, audit.0);
    assert_eq!(manifest.2, 2);
    assert_eq!(manifest.3.as_deref(), Some(audit.2.as_str()));
    assert!(manifest.1.parse::<common::Lsn>().unwrap() > audit.1.parse().unwrap());
    let commit_lsn = manifest.1.parse::<common::Lsn>().unwrap();
    let (top_xid, expected_files, row_count, final_schema_version, file_shape): (
        i64,
        i64,
        i64,
        i64,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT publication.top_xid, grouped.expected_files, grouped.row_count, \
                grouped.final_schema_version, grouped.file_shape \
         FROM public.walrus_stream_txn_publication publication \
         JOIN public.walrus_stream_manifest_group grouped \
           ON grouped.publication_id = publication.id \
         WHERE publication.epoch = $1 AND publication.commit_lsn = $2 \
           AND grouped.source_schema = 'public' AND grouped.source_table = 'orders'",
    )
    .bind(h.epoch)
    .bind(commit_lsn)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert!(top_xid > 0, "the receipt retains the ordinary Begin xid");
    assert_eq!(
        (expected_files, row_count, final_schema_version),
        (2, 2, 2),
        "both sides of the DDL must be one indivisible v2-final group"
    );
    let shapes = file_shape
        .as_array()
        .expect("group replay shape is a JSON array");
    assert_eq!(shapes.len(), 2);
    let mut child_versions = shapes
        .iter()
        .map(|shape| {
            assert_eq!(
                shape["lsn_start"].as_u64(),
                Some(commit_lsn.as_u64()),
                "a grouped child cannot absorb an older buffered commit"
            );
            assert_eq!(shape["lsn_end"].as_u64(), Some(commit_lsn.as_u64()));
            shape["schema_version"]
                .as_i64()
                .expect("child schema version")
        })
        .collect::<Vec<_>>();
    child_versions.sort_unstable();
    assert_eq!(child_versions, vec![1, 2]);
    let versions: Vec<i64> = sqlx::query_scalar(
        "SELECT schema_version FROM public.walrus_schema_registry \
         WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'orders' \
         ORDER BY schema_version",
    )
    .bind(h.epoch)
    .fetch_all(h.control_pool())
    .await
    .unwrap();
    assert_eq!(versions, vec![1, 2]);

    h.stop_transformer().await.unwrap();
    let rows = h
        .duckdb_rows(
            "orders",
            "SELECT concat(id, ':', coalesce(ddl_txn_extra, 'NULL')) \
             FROM orders WHERE id BETWEEN 910001 AND 910003 ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        rows,
        vec![
            "910001:NULL".to_string(),
            "910002:new-shape".to_string(),
            "910003:last".to_string(),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn streamed_transaction_partitions_pre_and_post_ddl_rows() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let mut connection = h.source_pool().acquire().await.unwrap();
    sqlx::raw_sql("BEGIN")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status) \
         SELECT g, 'stream-pre' FROM generate_series(920000, {}) g",
        920000 + HALF - 1
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    nudge_other_table(&h, 920001).await;
    let first_spills = h.await_spill(1, Duration::from_secs(60)).await.unwrap();

    sqlx::raw_sql("ALTER TABLE public.orders ADD COLUMN ddl_stream_extra text")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status, ddl_stream_extra) \
         SELECT g, 'stream-post', 'new-shape' FROM generate_series(930000, {}) g",
        930000 + HALF - 1
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    nudge_other_table(&h, 920002).await;
    h.await_spill(first_spills + 1, Duration::from_secs(60))
        .await
        .expect("post-DDL rows spill while the transaction remains open");
    sqlx::raw_sql("COMMIT")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec(
        "INSERT INTO public.orders (id, status, ddl_stream_extra) \
         VALUES (939999, 'sentinel', 'last')",
    )
    .await
    .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .unwrap();
    let ddl_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_ddl_manifest \
         WHERE epoch = $1 AND source_table = 'orders' AND schema_version = 2",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(ddl_count, 1);

    h.stop_transformer().await.unwrap();
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'stream-pre'"
        )
        .unwrap(),
        HALF
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'stream-post'"
        )
        .unwrap(),
        HALF
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'stream-pre' AND ddl_stream_extra IS NULL",
        )
        .unwrap(),
        HALF
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn whole_streamed_rollback_discards_ddl_registry_and_rows() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let audit_floor: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id), 0) FROM public.walrus_ddl_audit")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
    let mut connection = h.source_pool().acquire().await.unwrap();
    sqlx::raw_sql("BEGIN")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status) \
         SELECT g, 'abort-pre' FROM generate_series(940000, {}) g",
        940000 + HALF - 1
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    nudge_other_table(&h, 940001).await;
    let first_spills = h.await_spill(1, Duration::from_secs(60)).await.unwrap();
    sqlx::raw_sql("ALTER TABLE public.orders ADD COLUMN ddl_abort_extra text")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status, ddl_abort_extra) \
         SELECT g, 'abort-post', 'doomed' FROM generate_series(950000, {}) g",
        950000 + HALF - 1
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    nudge_other_table(&h, 940002).await;
    h.await_spill(first_spills + 1, Duration::from_secs(60))
        .await
        .unwrap();
    sqlx::raw_sql("ROLLBACK")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    converge_after_sentinel(&h, 959999, false).await;
    let source_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_ddl_audit \
         WHERE id > $1 AND c_ddl_text LIKE '%ddl_abort_extra%'",
    )
    .bind(audit_floor)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert_eq!(
        source_audits, 0,
        "source audit INSERT rolled back with the DDL"
    );
    let control_ddls: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_ddl_manifest WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(control_ddls, 0);
    let provisional_registry: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_schema_registry \
         WHERE epoch = $1 AND source_table = 'orders' AND schema_version > 1",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(provisional_registry, 0);
    let column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'orders' \
           AND column_name = 'ddl_abort_extra')",
    )
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert!(!column_exists);

    h.stop_transformer().await.unwrap();
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status IN ('abort-pre', 'abort-post')",
        )
        .unwrap(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn streamed_savepoint_rollback_discards_only_its_ddl_and_rows() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");
    let mut connection = h.source_pool().acquire().await.unwrap();
    sqlx::raw_sql("BEGIN")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status) \
         SELECT g, 'save-kept-a' FROM generate_series(960000, {}) g",
        960000 + HALF - 1
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    nudge_other_table(&h, 960001).await;
    let first_spills = h.await_spill(1, Duration::from_secs(60)).await.unwrap();

    sqlx::raw_sql("SAVEPOINT ddl_sp")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql("ALTER TABLE public.orders ADD COLUMN ddl_savepoint_extra text")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "INSERT INTO public.orders (id, status, ddl_savepoint_extra) \
         SELECT g, 'save-doomed', 'doomed' FROM generate_series(970000, {}) g",
        970000 + HALF - 1
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    nudge_other_table(&h, 960002).await;
    h.await_spill(first_spills + 1, Duration::from_secs(60))
        .await
        .unwrap();
    sqlx::raw_sql("ROLLBACK TO SAVEPOINT ddl_sp")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO public.orders (id, status) \
         SELECT g, 'save-kept-b' FROM generate_series(980000, 980099) g; COMMIT",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    converge_after_sentinel(&h, 989999, false).await;
    let control_ddls: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_ddl_manifest WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(control_ddls, 0);
    let provisional_registry: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM public.walrus_schema_registry \
         WHERE epoch = $1 AND source_table = 'orders' AND schema_version > 1",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(provisional_registry, 0);

    h.stop_transformer().await.unwrap();
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'save-kept-a'"
        )
        .unwrap(),
        HALF
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'save-kept-b'"
        )
        .unwrap(),
        100
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders WHERE status = 'save-doomed'"
        )
        .unwrap(),
        0
    );
}
