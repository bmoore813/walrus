#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + assertions"
)]
//! A PostgreSQL-maximum-width primary key through bootstrap, pgoutput, Parquet, and DuckLake.
#![cfg(feature = "it")]

use e2e::{Harness, WIDE_PRIMARY_KEY_COLUMNS};
use std::time::Duration;

fn key_values(last: i32) -> String {
    let mut values = vec!["1".to_string(); WIDE_PRIMARY_KEY_COLUMNS];
    *values.last_mut().unwrap() = last.to_string();
    values.join(", ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn widest_primary_key_converges_without_truncating_key_columns() {
    let mut h = Harness::start()
        .await
        .expect("bring up extractor + transformer");

    let relation: serde_json::Value = sqlx::query_scalar(
        "SELECT columns FROM public.walrus_schema_registry \
         WHERE epoch = $1 AND source_schema = 'public' AND source_table = 'wide_keys' \
         ORDER BY schema_version DESC LIMIT 1",
    )
    .bind(h.epoch)
    .fetch_one(h.control_pool())
    .await
    .expect("read bootstrapped wide-key relation");
    let columns = relation["columns"].as_array().expect("relation columns");
    assert_eq!(columns.len(), WIDE_PRIMARY_KEY_COLUMNS + 1);
    assert_eq!(
        columns
            .iter()
            .filter(|column| column["is_key"].as_bool() == Some(true))
            .count(),
        WIDE_PRIMARY_KEY_COLUMNS,
        "catalog bootstrap must retain every primary-key flag"
    );
    let final_key = columns
        .get(WIDE_PRIMARY_KEY_COLUMNS - 1)
        .expect("final key column");
    assert_eq!(final_key["name"], "key_32");
    assert_eq!(final_key["is_key"].as_bool(), Some(true));
    let payload = columns
        .get(WIDE_PRIMARY_KEY_COLUMNS)
        .expect("payload column");
    assert_eq!(payload["name"], "payload");
    assert_eq!(payload["is_key"].as_bool(), Some(false));

    let before = h.source_wal_lsn().await.unwrap();
    h.source_batch(&format!(
        "BEGIN; \
         INSERT INTO public.wide_keys VALUES ({}, 'first'); \
         INSERT INTO public.wide_keys VALUES ({}, 'other'); \
         UPDATE public.wide_keys SET payload = 'after' WHERE key_01 = 1 AND key_32 = 1; \
         UPDATE public.wide_keys SET key_32 = 3, payload = 'moved' \
             WHERE key_01 = 1 AND key_32 = 1; \
         DELETE FROM public.wide_keys WHERE key_01 = 1 AND key_32 = 2; \
         COMMIT;",
        key_values(1),
        key_values(2),
    ))
    .await
    .unwrap();

    h.await_transformed_past("wide_keys", before, Duration::from_secs(90))
        .await
        .expect("wide-key changes reach the mirror");
    h.stop_transformer().await.unwrap();

    assert_eq!(
        h.duckdb_scalar(
            "wide_keys",
            "SELECT count(*) FROM wide_keys_current \
             WHERE key_01 = 1 AND key_31 = 1 AND key_32 = 3 AND payload = 'moved'",
        )
        .unwrap(),
        1,
        "the key moved by its final component"
    );
    assert_eq!(
        h.duckdb_scalar("wide_keys", "SELECT count(*) FROM wide_keys_current")
            .unwrap(),
        1,
        "the row differing only at key component 32 was deleted independently"
    );
    assert_eq!(
        h.duckdb_scalar(
            "wide_keys",
            "SELECT count(*) FROM wide_keys_raw WHERE _walrus_op = 'i'",
        )
        .unwrap(),
        2
    );
    assert_eq!(
        h.duckdb_scalar(
            "wide_keys",
            "SELECT count(*) FROM wide_keys_raw WHERE _walrus_op = 'u'",
        )
        .unwrap(),
        2
    );
    assert_eq!(
        h.duckdb_scalar(
            "wide_keys",
            "SELECT count(*) FROM wide_keys_raw WHERE _walrus_op = 'd'",
        )
        .unwrap(),
        2,
        "a key move contributes its old-key delete plus the explicit delete"
    );
}
